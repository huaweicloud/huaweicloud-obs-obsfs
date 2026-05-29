/*
 * s3fs - FUSE-based file system backed by Amazon S3
 *
 * Copyright(C) 2007 Takeshi Nakatani <ggtakec.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/file.h>
#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <dirent.h>
// Filesystem type whitelist for tmpdir (R-20: reject NFS etc. that lack punch hole)
#ifdef __linux__
#include <sys/vfs.h>
#ifndef EXT4_SUPER_MAGIC
#define EXT4_SUPER_MAGIC  0xEF53
#endif
#ifndef XFS_SUPER_MAGIC
#define XFS_SUPER_MAGIC   0x58465342
#endif
#ifndef BTRFS_SUPER_MAGIC
#define BTRFS_SUPER_MAGIC 0x9123683E
#endif
#ifndef TMPFS_MAGIC
#define TMPFS_MAGIC       0x01021994
#endif
#ifndef NFS_SUPER_MAGIC
#define NFS_SUPER_MAGIC   0x6969
#endif
#elif defined(__APPLE__)
#include <sys/mount.h>
#endif
#include <curl/curl.h>
#include <string>
#include <iostream>
#include <sstream>
#include <map>
#include <list>
#include <vector>
#include <algorithm>
#include <memory>
#include <new>

#include "common.h"
#include "fdcache.h"
#include "s3fs.h"
#include "s3fs_util.h"
#include "string_util.h"
#include "curl.h"
#include "crc32c.h"

using namespace std;

//------------------------------------------------
// Symbols
//------------------------------------------------
static const int MAX_MULTIPART_CNT = 10 * 1000;  // S3 multipart max count

//
// For cache directory top path
//
#if defined(P_tmpdir)
#define TMPFILE_DIR_0PATH   P_tmpdir
#else
#define TMPFILE_DIR_0PATH   "/tmp"
#endif

//------------------------------------------------
// CacheFileStat class methods
//------------------------------------------------
bool CacheFileStat::MakeCacheFileStatPath(const char* path, string& sfile_path, bool is_create_dir)
{
  // make stat dir top path( "/<cache_dir>/.<bucket_name>.stat" )
  string top_path = FdManager::GetCacheDir();
  top_path       += "/.";
  top_path       += bucket;
  top_path       += ".stat";

  if(is_create_dir){
    int result;
    if(0 != (result = mkdirp(top_path + mydirname(path), 0777))){
      S3FS_PRN_ERR("failed to create dir(%s) by errno(%d).", path, result);
      return false;
    }
  }
  if(!path || '\0' == path[0]){
    sfile_path = top_path;
  }else{
    sfile_path = top_path + SAFESTRPTR(path);
  }
  return true;
}

bool CacheFileStat::CheckCacheFileStatTopDir(void)
{
  if(!FdManager::IsCacheDir()){
    return true;
  }
  // make stat dir top path( "/<cache_dir>/.<bucket_name>.stat" )
  string top_path = FdManager::GetCacheDir();
  top_path       += "/.";
  top_path       += bucket;
  top_path       += ".stat";

  return check_exist_dir_permission(top_path.c_str());
}

bool CacheFileStat::DeleteCacheFileStat(const char* path)
{
  if(!path || '\0' == path[0]){
    return false;
  }
  // stat path
  string sfile_path;
  if(!CacheFileStat::MakeCacheFileStatPath(path, sfile_path, false)){
    S3FS_PRN_ERR("failed to create cache stat file path(%s)", path);
    return false;
  }
  if(0 != unlink(sfile_path.c_str())){
    if(ENOENT == errno){
      S3FS_PRN_DBG("failed to delete file(%s): errno=%d", path, errno);
    }else{
      S3FS_PRN_ERR("failed to delete file(%s): errno=%d", path, errno);
    }
    return false;
  }
  return true;
}

bool CacheFileStat::RenameCacheFileStat(const char* oldpath, const char* newpath)
{
  if(!oldpath || '\0' == oldpath[0] || !newpath || '\0' == newpath[0]){
    return false;
  }
  string old_filestat;
  string new_filestat;
  if(!CacheFileStat::MakeCacheFileStatPath(oldpath, old_filestat, false) ||
     !CacheFileStat::MakeCacheFileStatPath(newpath, new_filestat, true)){
    return false;
  }
  // Remove destination stat if exists (overwrite case)
  struct stat st;
  if(0 == stat(new_filestat.c_str(), &st)){
    if(-1 == unlink(new_filestat.c_str())){
      S3FS_PRN_ERR("failed to unlink new cache stat(%s) errno(%d).", new_filestat.c_str(), errno);
      return false;
    }
  }
  // If old stat doesn't exist, nothing to rename — success
  if(0 != stat(old_filestat.c_str(), &st)){
    return true;
  }
  // link old→new, then unlink old
  if(-1 == link(old_filestat.c_str(), new_filestat.c_str())){
    S3FS_PRN_ERR("failed to link old stat(%s) to new(%s) errno(%d).", old_filestat.c_str(), new_filestat.c_str(), errno);
    return false;
  }
  if(-1 == unlink(old_filestat.c_str())){
    S3FS_PRN_ERR("failed to unlink old stat(%s) errno(%d).", old_filestat.c_str(), errno);
    return false;
  }
  return true;
}

// [NOTE]
// If remove stat file directory, it should do before removing
// file cache directory.
//
bool CacheFileStat::DeleteCacheFileStatDirectory(void)
{
  string top_path = FdManager::GetCacheDir();

  if(top_path.empty() || bucket.empty()){
    return true;
  }
  top_path       += "/.";
  top_path       += bucket;
  top_path       += ".stat";
  return delete_files_in_dir(top_path.c_str(), true);
}

//------------------------------------------------
// CacheFileStat methods
//------------------------------------------------
CacheFileStat::CacheFileStat(const char* tpath) : path(""), fd(-1)
{
  if(tpath && '\0' != tpath[0]){
    SetPath(tpath, true);
  }
}

CacheFileStat::~CacheFileStat()
{
  Release();
}

bool CacheFileStat::SetPath(const char* tpath, bool is_open)
{
  if(!tpath || '\0' == tpath[0]){
    return false;
  }
  if(!Release()){
    // could not close old stat file.
    return false;
  }
  if(tpath){
    path = tpath;
  }
  if(!is_open){
    return true;
  }
  return Open();
}

bool CacheFileStat::Open(void)
{
  if(0 == path.size()){
    return false;
  }
  if(-1 != fd){
    // already opened
    return true;
  }
  // stat path
  string sfile_path;
  if(!CacheFileStat::MakeCacheFileStatPath(path.c_str(), sfile_path, true)){
    S3FS_PRN_ERR("failed to create cache stat file path(%s)", path.c_str());
    return false;
  }
  // open
  if(-1 == (fd = open(sfile_path.c_str(), O_CREAT|O_RDWR, 0600))){
    S3FS_PRN_ERR("failed to open cache stat file path(%s) - errno(%d)", path.c_str(), errno);
    return false;
  }
  // lock
  if(-1 == flock(fd, LOCK_EX)){
    S3FS_PRN_ERR("failed to lock cache stat file(%s) - errno(%d)", path.c_str(), errno);
    close(fd);
    fd = -1;
    return false;
  }
  // seek top
  if(0 != lseek(fd, 0, SEEK_SET)){
    S3FS_PRN_ERR("failed to lseek cache stat file(%s) - errno(%d)", path.c_str(), errno);
    flock(fd, LOCK_UN);
    close(fd);
    fd = -1;
    return false;
  }
  S3FS_PRN_DBG("file locked(%s - %s)", path.c_str(), sfile_path.c_str());

  return true;
}

bool CacheFileStat::Release(void)
{
  if(-1 == fd){
    // already release
    return true;
  }
  // unlock
  if(-1 == flock(fd, LOCK_UN)){
    S3FS_PRN_ERR("failed to unlock cache stat file(%s) - errno(%d)", path.c_str(), errno);
    return false;
  }
  S3FS_PRN_DBG("file unlocked(%s)", path.c_str());

  if(-1 == close(fd)){
    S3FS_PRN_ERR("failed to close cache stat file(%s) - errno(%d)", path.c_str(), errno);
    return false;
  }
  fd = -1;

  return true;
}

bool CacheFileStat::OverWriteFile(const std::string& strall) const
{
  // get stat file path
  std::string sfile_path;
  if(!CacheFileStat::MakeCacheFileStatPath(path.c_str(), sfile_path, true)){
    S3FS_PRN_ERR("failed to create cache stat file path(%s)", path.c_str());
    return false;
  }

  // create temporary file in same directory (required for atomic rename)
  std::string strTmpFile = mydirname(sfile_path) + "/.tmpstat.XXXXXX";
  int tmpfd = mkstemp(&strTmpFile[0]);
  if(-1 == tmpfd){
    S3FS_PRN_ERR("failed to create temporary cache stat file(%s) for %s - errno(%d)",
                 strTmpFile.c_str(), sfile_path.c_str(), errno);
    return false;
  }

  // write contents to temporary file
  ssize_t written = pwrite(tmpfd, strall.c_str(), strall.length(), 0);
  close(tmpfd);
  if(written < static_cast<ssize_t>(strall.length())){
    S3FS_PRN_ERR("failed to write cache stat to temporary file(%s) - errno(%d)",
                 strTmpFile.c_str(), errno);
    unlink(strTmpFile.c_str());
    return false;
  }

  // atomic rename — POSIX guarantees this is atomic on the same filesystem
  if(0 != rename(strTmpFile.c_str(), sfile_path.c_str())){
    S3FS_PRN_ERR("failed to rename temporary cache stat file(%s) to %s - errno(%d)",
                 strTmpFile.c_str(), sfile_path.c_str(), errno);
    unlink(strTmpFile.c_str());
    return false;
  }
  return true;
}

//------------------------------------------------
// PageList methods
//------------------------------------------------
void PageList::FreeList(fdpage_list_t& list)
{
  for(fdpage_list_t::iterator iter = list.begin(); iter != list.end(); iter = list.erase(iter)){
    delete (*iter);
  }
  list.clear();
}

PageList::PageList(size_t size, bool is_loaded)
{
  Init(size, is_loaded);
}

PageList::~PageList()
{
  Clear();
}

void PageList::Clear(void)
{
  PageList::FreeList(pages);
}

bool PageList::Init(size_t size, bool is_loaded)
{
  Clear();
  fdpage* page = new fdpage(0, size, is_loaded);
  pages.push_back(page);
  return true;
}

size_t PageList::Size(void) const
{
  if(pages.empty()){
    return 0;
  }
  fdpage_list_t::const_reverse_iterator riter = pages.rbegin();
  return static_cast<size_t>((*riter)->next());
}

bool PageList::Compress(void)
{
  bool is_first         = true;
  bool is_last_loaded   = false;
  bool is_last_modified = false;
  for(fdpage_list_t::iterator iter = pages.begin(); iter != pages.end(); ){
    if(is_first){
      is_first         = false;
      is_last_loaded   = (*iter)->loaded;
      is_last_modified = (*iter)->modified;
      ++iter;
    }else{
      if(is_last_loaded == (*iter)->loaded && is_last_modified == (*iter)->modified){
        fdpage_list_t::iterator biter = iter;
        --biter;
        (*biter)->bytes += (*iter)->bytes;
        delete *iter;
        iter = pages.erase(iter);
      }else{
        is_last_loaded   = (*iter)->loaded;
        is_last_modified = (*iter)->modified;
        ++iter;
      }
    }
  }
  return true;
}

bool PageList::Parse(off_t new_pos)
{
  for(fdpage_list_t::iterator iter = pages.begin(); iter != pages.end(); ++iter){
    if(new_pos == (*iter)->offset){
      // nothing to do
      return true;
    }else if((*iter)->offset < new_pos && new_pos < (*iter)->next()){
      fdpage* page    = new fdpage((*iter)->offset, static_cast<size_t>(new_pos - (*iter)->offset), (*iter)->loaded, (*iter)->modified);
      (*iter)->bytes -= (new_pos - (*iter)->offset);
      (*iter)->offset = new_pos;
      pages.insert(iter, page);
      return true;
    }
  }
  return false;
}

bool PageList::Resize(size_t size, bool is_loaded)
{
  size_t total = Size();

  if(0 == total){
    Init(size, is_loaded);

  }else if(total < size){
    // add new area
    fdpage* page = new fdpage(static_cast<off_t>(total), (size - total), is_loaded);
    pages.push_back(page);

  }else if(size < total){
    // cut area
    for(fdpage_list_t::iterator iter = pages.begin(); iter != pages.end(); ){
      if(static_cast<size_t>((*iter)->next()) <= size){
        ++iter;
      }else{
        if(size <= static_cast<size_t>((*iter)->offset)){
          delete *iter;
          iter = pages.erase(iter);
        }else{
          (*iter)->bytes = size - static_cast<size_t>((*iter)->offset);
        }
      }
    }
  }else{    // total == size
    // nothing to do
  }
  // compress area
  return Compress();
}

bool PageList::IsPageLoaded(off_t start, size_t size) const
{
  for(fdpage_list_t::const_iterator iter = pages.begin(); iter != pages.end(); ++iter){
    if((*iter)->end() < start){
      continue;
    }
    if(!(*iter)->loaded){
      return false;
    }
    if(0 != size && (static_cast<size_t>(start) + size) <= static_cast<size_t>((*iter)->next())){
      break;
    }
  }
  return true;
}

bool PageList::SetPageLoadedStatus(off_t start, size_t size, bool is_loaded, bool is_compress)
{
  size_t now_size = Size();

  if(now_size <= static_cast<size_t>(start)){
    if(now_size < static_cast<size_t>(start)){
      // add
      Resize(static_cast<size_t>(start), false);
    }
    Resize(static_cast<size_t>(start) + size, is_loaded);

  }else if(now_size <= (static_cast<size_t>(start) + size)){
    // cut
    Resize(static_cast<size_t>(start), false);
    // add
    Resize(static_cast<size_t>(start) + size, is_loaded);

  }else{
    // start-size are inner pages area
    // parse "start", and "start + size" position
    Parse(start);
    Parse(start + size);

    // set loaded flag
    for(fdpage_list_t::iterator iter = pages.begin(); iter != pages.end(); ++iter){
      if((*iter)->end() < start){
        continue;
      }else if(static_cast<off_t>(start + size) <= (*iter)->offset){
        break;
      }else{
        (*iter)->loaded = is_loaded;
      }
    }
  }
  // compress area
  return (is_compress ? Compress() : true);
}

bool PageList::SetPageModifiedStatus(off_t start, size_t size, bool is_modified, bool is_compress)
{
  size_t now_size = Size();

  if(now_size <= static_cast<size_t>(start)){
    if(now_size < static_cast<size_t>(start)){
      Resize(static_cast<size_t>(start), false);
    }
    Resize(static_cast<size_t>(start) + size, false);
  }else if(now_size < (static_cast<size_t>(start) + size)){
    Resize(static_cast<size_t>(start) + size, false);
    Parse(start);
  }else{
    Parse(start);
    Parse(start + size);
  }

  for(fdpage_list_t::iterator iter = pages.begin(); iter != pages.end(); ++iter){
    if((*iter)->end() < start){
      continue;
    }else if(static_cast<off_t>(start + size) <= (*iter)->offset){
      break;
    }else{
      (*iter)->modified = is_modified;
    }
  }
  return (is_compress ? Compress() : true);
}

void PageList::GetPageListsForMultipartUpload(fdpage_list_t& dlpages, fdpage_list_t& mixuppages, off_t max_partsize) const
{
  // Step 1: Build a compressed-by-modified page list
  fdpage_list_t modpages;
  for(fdpage_list_t::const_iterator iter = pages.begin(); iter != pages.end(); ++iter){
    if(!modpages.empty() && modpages.back()->modified == (*iter)->modified){
      modpages.back()->bytes += (*iter)->bytes;
    }else{
      modpages.push_back(new fdpage((*iter)->offset, (*iter)->bytes, (*iter)->loaded, (*iter)->modified));
    }
  }

  // Step 2: Handle minimum part size constraints (MIN_MULTIPART_SIZE = 5MB)
  // Small unmodified gaps get merged into modified regions (need download first)
  // Small modified regions borrow from adjacent unmodified regions
  for(fdpage_list_t::iterator iter = modpages.begin(); iter != modpages.end(); ){
    if(!(*iter)->modified && (*iter)->bytes < static_cast<size_t>(MIN_MULTIPART_SIZE)){
      // Small unmodified gap: mark for download and merge into adjacent modified
      dlpages.push_back(new fdpage((*iter)->offset, (*iter)->bytes, false, false));
      (*iter)->modified = true;
      // Try to merge with previous
      if(iter != modpages.begin()){
        fdpage_list_t::iterator prev = iter;
        --prev;
        if((*prev)->modified){
          (*prev)->bytes += (*iter)->bytes;
          delete *iter;
          iter = modpages.erase(iter);
          // Try to merge with next
          if(iter != modpages.end() && (*iter)->modified){
            (*prev)->bytes += (*iter)->bytes;
            delete *iter;
            iter = modpages.erase(iter);
          }
          continue;
        }
      }
      // Try to merge with next
      fdpage_list_t::iterator next_iter = iter;
      ++next_iter;
      if(next_iter != modpages.end() && (*next_iter)->modified){
        (*next_iter)->offset = (*iter)->offset;
        (*next_iter)->bytes += (*iter)->bytes;
        delete *iter;
        iter = modpages.erase(iter);
        continue;
      }
      ++iter;
    }else{
      ++iter;
    }
  }

  // Handle small modified regions: borrow from adjacent unmodified to meet minimum.
  // Uses s3fs-fuse D1/D2 strategy: only partially borrow if the remainder stays >= 5MB,
  // otherwise absorb the entire unmodified region to avoid creating a runt CopyPart.
  for(fdpage_list_t::iterator iter = modpages.begin(); iter != modpages.end(); ++iter){
    if((*iter)->modified && (*iter)->bytes < static_cast<size_t>(MIN_MULTIPART_SIZE)){
      size_t need = static_cast<size_t>(MIN_MULTIPART_SIZE) - (*iter)->bytes;
      // Try to borrow from next unmodified region
      fdpage_list_t::iterator next_iter = iter;
      ++next_iter;
      if(next_iter != modpages.end() && !(*next_iter)->modified){
        if((need + static_cast<size_t>(MIN_MULTIPART_SIZE)) < (*next_iter)->bytes){
          // D1: borrow only 'need', remainder stays >= 5MB
          dlpages.push_back(new fdpage((*next_iter)->offset, need, false, false));
          (*iter)->bytes += need;
          (*next_iter)->offset += need;
          (*next_iter)->bytes -= need;
        }else{
          // D2: absorb entire unmodified region (remainder would be < 5MB)
          dlpages.push_back(new fdpage((*next_iter)->offset, (*next_iter)->bytes, false, false));
          (*iter)->bytes += (*next_iter)->bytes;
          delete *next_iter;
          modpages.erase(next_iter);
          // Also try to merge with the next-next (which should be modified)
          next_iter = iter;
          ++next_iter;
          if(next_iter != modpages.end() && (*next_iter)->modified){
            (*iter)->bytes += (*next_iter)->bytes;
            delete *next_iter;
            modpages.erase(next_iter);
          }
        }
      }else if(iter != modpages.begin()){
        // Try to borrow from previous unmodified region (same D1/D2 logic)
        fdpage_list_t::iterator prev = iter;
        --prev;
        if(!(*prev)->modified){
          if((need + static_cast<size_t>(MIN_MULTIPART_SIZE)) < (*prev)->bytes){
            // D1: borrow from end of previous, remainder stays >= 5MB
            off_t borrow_start = (*prev)->offset + static_cast<off_t>((*prev)->bytes - need);
            dlpages.push_back(new fdpage(borrow_start, need, false, false));
            (*iter)->offset -= need;
            (*iter)->bytes += need;
            (*prev)->bytes -= need;
          }else{
            // D2: absorb entire previous unmodified region
            dlpages.push_back(new fdpage((*prev)->offset, (*prev)->bytes, false, false));
            (*iter)->offset = (*prev)->offset;
            (*iter)->bytes += (*prev)->bytes;
            delete *prev;
            modpages.erase(prev);
          }
        }
      }
    }
  }

  // Step 3: Build output list, splitting modified regions by max_partsize
  for(fdpage_list_t::iterator iter = modpages.begin(); iter != modpages.end(); ++iter){
    if((*iter)->modified){
      // Split by max_partsize (s3fs-fuse strategy: don't split if remainder < 2x)
      off_t cur_offset = (*iter)->offset;
      size_t remaining = (*iter)->bytes;
      while(remaining > 0){
        if(remaining > static_cast<size_t>(max_partsize * 2)){
          // Split off one max_partsize chunk
          mixuppages.push_back(new fdpage(cur_offset, static_cast<size_t>(max_partsize), true, true));
          cur_offset += max_partsize;
          remaining -= static_cast<size_t>(max_partsize);
        }else{
          // Remainder <= 2x max_partsize: keep as single part (avoid runt trailing part)
          mixuppages.push_back(new fdpage(cur_offset, remaining, true, true));
          remaining = 0;
        }
      }
    }else{
      // Unmodified: keep as single entry (CopyPart handles its own splitting)
      mixuppages.push_back(new fdpage((*iter)->offset, (*iter)->bytes, (*iter)->loaded, false));
    }
    delete *iter;
  }
  modpages.clear();
}

void PageList::ClearAllModified(void)
{
  for(fdpage_list_t::iterator iter = pages.begin(); iter != pages.end(); ++iter){
    (*iter)->modified = false;
  }
  Compress();
}

bool PageList::FindUnloadedPage(off_t start, off_t& resstart, size_t& ressize) const
{
  for(fdpage_list_t::const_iterator iter = pages.begin(); iter != pages.end(); ++iter){
    if(start <= (*iter)->end()){
      if(!(*iter)->loaded){
        resstart = (*iter)->offset;
        ressize  = (*iter)->bytes;
        return true;
      }
    }
  }
  return false;
}

size_t PageList::GetTotalLoadedPageSize(off_t start, size_t size) const
{
  size_t total = 0;
  off_t  next  = (0 == size) ? static_cast<off_t>(Size()) : static_cast<off_t>(start + size);
  for(fdpage_list_t::const_iterator iter = pages.begin(); iter != pages.end(); ++iter){
    if((*iter)->next() <= start){
      continue;
    }
    if(next <= (*iter)->offset){
      break;
    }
    if(!(*iter)->loaded){
      continue;
    }
    size_t tmpsize;
    if((*iter)->offset <= start){
      if((*iter)->next() <= next){
        tmpsize = static_cast<size_t>((*iter)->next() - start);
      }else{
        tmpsize = static_cast<size_t>(next - start);
      }
    }else{
      if((*iter)->next() <= next){
        tmpsize = static_cast<size_t>((*iter)->next() - (*iter)->offset);
      }else{
        tmpsize = static_cast<size_t>(next - (*iter)->offset);
      }
    }
    total += tmpsize;
  }
  return total;
}

size_t PageList::GetTotalUnloadedPageSize(off_t start, size_t size) const
{
  size_t restsize = 0;
  off_t  next     = static_cast<off_t>(start + size);
  for(fdpage_list_t::const_iterator iter = pages.begin(); iter != pages.end(); ++iter){
    if((*iter)->next() <= start){
      continue;
    }
    if(next <= (*iter)->offset){
      break;
    }
    if((*iter)->loaded){
      continue;
    }
    size_t tmpsize;
    if((*iter)->offset <= start){
      if((*iter)->next() <= next){
        tmpsize = static_cast<size_t>((*iter)->next() - start);
      }else{
        tmpsize = static_cast<size_t>(next - start);                         // = size
      }
    }else{
      if((*iter)->next() <= next){
        tmpsize = static_cast<size_t>((*iter)->next() - (*iter)->offset);   // = (*iter)->bytes
      }else{
        tmpsize = static_cast<size_t>(next - (*iter)->offset);
      }
    }
    restsize += tmpsize;
  }
  return restsize;
}

int PageList::GetUnloadedPages(fdpage_list_t& unloaded_list, off_t start, size_t size) const
{
  // If size is 0, it means loading to end.
  if(0 == size){
    if(static_cast<size_t>(start) < Size()){
      size = static_cast<size_t>(Size() - start);
    }
  }
  off_t next = static_cast<off_t>(start + size);

  for(fdpage_list_t::const_iterator iter = pages.begin(); iter != pages.end(); ++iter){
    if((*iter)->next() <= start){
      continue;
    }
    if(next <= (*iter)->offset){
      break;
    }
    if((*iter)->loaded){
      continue; // already loaded
    }

    // page area
    off_t  page_start = max((*iter)->offset, start);
    off_t  page_next  = min((*iter)->next(), next);
    size_t page_size  = static_cast<size_t>(page_next - page_start);

    // add list
    fdpage_list_t::reverse_iterator riter = unloaded_list.rbegin();
    if(riter != unloaded_list.rend() && (*riter)->next() == page_start){
      // merge to before page
      (*riter)->bytes += page_size;
    }else{
      fdpage* page = new fdpage(page_start, page_size, false);
      unloaded_list.push_back(page);
    }
  }
  return unloaded_list.size();
}

bool PageList::Serialize(CacheFileStat& file, bool is_output, ino_t inode)
{
  if(is_output){
    //
    // put to file
    //
    stringstream ssall;
    ssall << inode << ":" << Size();    // [s3fs-fuse compat] header: "inode:size"

    for(fdpage_list_t::iterator iter = pages.begin(); iter != pages.end(); ++iter){
      ssall << "\n" << (*iter)->offset << ":" << (*iter)->bytes << ":" << ((*iter)->loaded ? "1" : "0") << ":" << ((*iter)->modified ? "1" : "0");
    }

    string strall = ssall.str();
    if(!file.OverWriteFile(strall)){    // [s3fs-fuse compat] atomic write via mkstemp+rename
      S3FS_PRN_ERR("failed to overwrite cache stat file");
      return false;
    }

  }else{
    if(!file.Open()){
      return false;
    }
    //
    // loading from file
    //
    struct stat st;
    memset(&st, 0, sizeof(struct stat));
    if(-1 == fstat(file.GetFd(), &st)){
      S3FS_PRN_ERR("fstat is failed. errno(%d)", errno);
      return false;
    }
    if(0 >= st.st_size){
      // nothing
      Init(0, false);
      return true;
    }
    // [DIFF vs s3fs-fuse] s3fs-fuse (fdcache_page.cpp:860) uses make_unique (C++17)
    // without try/catch, letting bad_alloc propagate to FUSE C layer.
    // We use unique_ptr + new (C++11) and explicitly catch bad_alloc for robustness
    // — allocation failure only affects this one file's cache state, not the entire
    // filesystem.
    std::unique_ptr<char[]> ptmp;
    try {
      ptmp.reset(new char[st.st_size + 1]());
    } catch (const std::bad_alloc&) {
      S3FS_PRN_ERR("could not allocate memory for cache stat file (size=%jd).",
                   (intmax_t)st.st_size);
      return false;
    }
    // read from file
    if(0 >= pread(file.GetFd(), ptmp.get(), st.st_size, 0)){
      S3FS_PRN_ERR("failed to read stats(%d)", errno);
      return false;
    }
    string       oneline;
    stringstream ssall(ptmp.get());

    // loaded
    Clear();

    // load(size)
    if(!getline(ssall, oneline, '\n')){
      S3FS_PRN_ERR("failed to parse stats.");
      return false;
    }
    // [s3fs-fuse compat] Parse header: "inode:size" (new format) or "size" (old format)
    off_t  total;
    ino_t  cache_inode;
    size_t colon_pos = oneline.find(':');
    if(colon_pos == string::npos){
      // old format: "<size>\n" — backward compatible, skip inode validation
      total = s3fs_strtoofft(oneline.c_str());
      cache_inode = 0;
    }else{
      // new format: "<inode>:<size>\n"
      cache_inode = static_cast<ino_t>(s3fs_strtoofft(oneline.substr(0, colon_pos).c_str()));
      total = s3fs_strtoofft(oneline.substr(colon_pos + 1).c_str());
      if(0 == cache_inode){
        S3FS_PRN_ERR("wrong inode number in parsed cache stats.");
        return false;
      }
    }
    // [s3fs-fuse compat] Validate inode: discard cache if file was recreated with different inode
    if(0 != cache_inode && cache_inode != inode){
      S3FS_PRN_ERR("differ inode(%jd) and cache inode(%jd) in parsed cache stats, discarding.",
                   (intmax_t)inode, (intmax_t)cache_inode);
      Clear();
      return false;
    }

    // load each part
    bool is_err = false;
    while(getline(ssall, oneline, '\n')){
      string       part;
      stringstream ssparts(oneline);
      // offset
      if(!getline(ssparts, part, ':')){
        is_err = true;
        break;
      }
      off_t offset = s3fs_strtoofft(part.c_str());
      // size
      if(!getline(ssparts, part, ':')){
        is_err = true;
        break;
      }
      off_t size = s3fs_strtoofft(part.c_str());
      // loaded
      if(!getline(ssparts, part, ':')){
        is_err = true;
        break;
      }
      bool is_loaded = (1 == s3fs_strtoofft(part.c_str()) ? true : false);
      // modified (optional, backward compatible)
      bool is_modified = false;
      if(getline(ssparts, part, ':')){
        is_modified = (1 == s3fs_strtoofft(part.c_str()) ? true : false);
      }
      // add new area
      SetPageLoadedStatus(offset, size, is_loaded);
      if(is_modified){
        SetPageModifiedStatus(offset, size, true);
      }
    }
    if(is_err){
      S3FS_PRN_ERR("failed to parse stats.");
      Clear();
      return false;
    }

    // check size
    // [ISSUE2026040900001 A1] Explicit cast silences -Wsign-compare. `total` is
    // parsed as off_t from the cache stat file's size field, always >= 0 in
    // practice, so the modular conversion is value-preserving.
    if(static_cast<size_t>(total) != Size()){
      S3FS_PRN_ERR("different size(%jd - %jd).", (intmax_t)total, (intmax_t)Size());
      Clear();
      return false;
    }
  }
  return true;
}

void PageList::Dump(void)
{
  int cnt = 0;

  S3FS_PRN_DBG("pages = {");
  for(fdpage_list_t::iterator iter = pages.begin(); iter != pages.end(); ++iter, ++cnt){
    S3FS_PRN_DBG("  [%08d] -> {%014jd - %014zu : %s : %s}", cnt, (intmax_t)((*iter)->offset), (*iter)->bytes, (*iter)->loaded ? "loaded" : "unloaded", (*iter)->modified ? "modified" : "clean");
  }
  S3FS_PRN_DBG("}");
}

//------------------------------------------------
// FdEntity class methods
//------------------------------------------------
int FdEntity::FillFile(int fd, unsigned char byte, size_t size, off_t start)
{
  unsigned char bytes[1024 * 32];         // 32kb
  memset(bytes, byte, min(sizeof(bytes), size));

  for(ssize_t total = 0, onewrote = 0; static_cast<size_t>(total) < size; total += onewrote){
    if(-1 == (onewrote = pwrite(fd, bytes, min(sizeof(bytes), (size - static_cast<size_t>(total))), start + total))){
      S3FS_PRN_ERR("pwrite failed. errno(%d)", errno);
      return -errno;
    }
  }
  return 0;
}

//------------------------------------------------
// FdEntity methods
//------------------------------------------------
// CRC32C integrity verification helpers
//------------------------------------------------

// Read a CRC segment from tmpfile and compute CRC32C.
// Range: [seg_start, min(seg_boundary, file_size))
uint32_t FdEntity::CrcFromTmpfile(size_t seg_idx, off_t file_size)
{
  off_t crc_seg_size = CRC_SEGMENT_SIZE;
  off_t seg_start = static_cast<off_t>(seg_idx) * crc_seg_size;
  off_t seg_end   = min(seg_start + crc_seg_size, file_size);
  size_t len      = static_cast<size_t>(seg_end - seg_start);

  if(0 == len){
    return 0;
  }

  std::unique_ptr<char[]> buf(new char[len]);

  ssize_t nread = pread(fd, buf.get(), len, seg_start);
  uint32_t crc = 0;
  if(nread > 0){
    crc = rocksdb::crc32c::Value(buf.get(), static_cast<size_t>(nread));
  }

  return crc;
}

// Ensure segment_crcs_ and segment_written_end_ are large enough
void FdEntity::EnsureSegmentCrcsSize(size_t needed_segs)
{
  if(segment_crcs_.size() < needed_segs){
    size_t old_size = segment_crcs_.size();
    segment_crcs_.resize(needed_segs);
    segment_written_end_.resize(needed_segs);
    // Initialize new entries' written_end to segment start offset
    off_t crc_seg_size = CRC_SEGMENT_SIZE;
    for(size_t i = old_size; i < needed_segs; i++){
      segment_written_end_[i] = static_cast<off_t>(i) * crc_seg_size;
    }
  }
}

// Update CRC after Write. Handles append (Extend) vs overwrite (pread re-calc).
// May touch multiple segments if write crosses boundary.
void FdEntity::UpdateCrcOnWrite(const char* bytes, ssize_t wsize, off_t start, off_t file_size)
{
  if(wsize <= 0 || !bytes){
    return;
  }
  off_t crc_seg_size = CRC_SEGMENT_SIZE;
  off_t write_end = start + wsize;

  // Ensure arrays cover all touched segments
  size_t last_seg = static_cast<size_t>((write_end - 1) / crc_seg_size);
  EnsureSegmentCrcsSize(last_seg + 1);

  off_t pos = start;
  const char* buf_ptr = bytes;

  while(pos < write_end){
    size_t seg_idx = static_cast<size_t>(pos / crc_seg_size);
    off_t seg_boundary = static_cast<off_t>(seg_idx + 1) * crc_seg_size;
    off_t chunk_end = min(write_end, seg_boundary);
    size_t chunk_size = static_cast<size_t>(chunk_end - pos);

    if(pos == segment_written_end_[seg_idx] &&
       (segment_crcs_[seg_idx].valid || 0 == size_orgmeta)){
      // Append: write starts exactly where previous write ended.
      //
      // Two sub-cases both safe for Extend:
      //   valid=true:  normal sequential append, CRC already tracks this segment
      //   size_orgmeta==0: new file — any data beyond written_end is zeros from
      //     ftruncate (not S3 content), so Extend is safe. At Flush time, CRC is
      //     extended with zeros to cover the full segment range before verification.
      //     This avoids O(n²) pread on ftruncate + sequential write (ISSUE2026031800001).
      segment_crcs_[seg_idx].crc32c =
          rocksdb::crc32c::Extend(segment_crcs_[seg_idx].crc32c, buf_ptr, chunk_size);
      segment_crcs_[seg_idx].valid = true;
      segment_written_end_[seg_idx] = chunk_end;
    }else{
      // Overwrite or non-contiguous: must pread entire segment to re-establish CRC
      segment_crcs_[seg_idx].crc32c = CrcFromTmpfile(seg_idx, file_size);
      segment_crcs_[seg_idx].valid = true;
      // CrcFromTmpfile reads [seg_start, min(seg_boundary, file_size)).
      // written_end must match this range so subsequent appends at the
      // correct offset can use incremental Extend without re-reading.
      off_t crc_range_end = min(seg_boundary, file_size);
      if(crc_range_end > segment_written_end_[seg_idx]){
        segment_written_end_[seg_idx] = crc_range_end;
      }
    }

    buf_ptr += chunk_size;
    pos = chunk_end;
  }
}

// Update boundary segment CRC after ftruncate (expand or shrink).
// Called from Write (gap write) and Truncate.
void FdEntity::UpdateBoundarySegmentCrc(off_t old_size, off_t new_size)
{
  if(old_size == new_size || segment_crcs_.empty()){
    return;
  }
  off_t crc_seg_size = CRC_SEGMENT_SIZE;

  if(new_size > old_size && old_size > 0){
    // Expand: old boundary segment's range changed
    size_t boundary_seg = static_cast<size_t>((old_size - 1) / crc_seg_size);
    if(boundary_seg < segment_crcs_.size() && segment_crcs_[boundary_seg].valid){
      off_t old_range_end = old_size;
      off_t new_range_end = min(static_cast<off_t>(boundary_seg + 1) * crc_seg_size, new_size);
      if(new_range_end > old_range_end){
        segment_crcs_[boundary_seg].crc32c = CrcFromTmpfile(boundary_seg, new_size);
        segment_written_end_[boundary_seg] = new_range_end;
      }
    }
  }

  if(new_size < old_size && new_size > 0){
    // Shrink: boundary segment at new_size, delete segments beyond
    size_t boundary_seg = static_cast<size_t>((new_size - 1) / crc_seg_size);
    if(boundary_seg < segment_crcs_.size() && segment_crcs_[boundary_seg].valid){
      segment_crcs_[boundary_seg].crc32c = CrcFromTmpfile(boundary_seg, new_size);
      segment_written_end_[boundary_seg] = new_size;
    }
    if(boundary_seg + 1 < segment_crcs_.size()){
      segment_crcs_.resize(boundary_seg + 1);
      segment_written_end_.resize(boundary_seg + 1);
    }
  }

  if(0 == new_size){
    // Truncate to zero
    segment_crcs_.clear();
    segment_written_end_.clear();
  }
}

// Verify CRC of modified segments before upload.
// Returns 0 on success, -EIO on mismatch or missing CRC.
int FdEntity::VerifyCrcBeforeUpload(const fdpage_list_t& upload_parts, off_t file_size)
{
  if(segment_crcs_.empty()){
    return 0;
  }
  off_t crc_seg_size = CRC_SEGMENT_SIZE;

  // Dedup: when multipart_size < CRC_SEGMENT_SIZE, multiple upload parts may
  // map to the same CRC segment. Track which segments have been verified.
  std::vector<bool> verified(segment_crcs_.size(), false);

  for(fdpage_list_t::const_iterator iter = upload_parts.begin(); iter != upload_parts.end(); ++iter){
    off_t part_start = (*iter)->offset;
    off_t part_end   = (*iter)->offset + static_cast<off_t>((*iter)->bytes);

    size_t seg_first = static_cast<size_t>(part_start / crc_seg_size);
    size_t seg_last  = (part_end > 0) ? static_cast<size_t>((part_end - 1) / crc_seg_size) : seg_first;

    for(size_t seg = seg_first; seg <= seg_last && seg < segment_crcs_.size(); seg++){
      if(verified[seg]) continue;
      verified[seg] = true;

      off_t seg_start = static_cast<off_t>(seg) * crc_seg_size;
      off_t seg_range_end = min(static_cast<off_t>(seg + 1) * crc_seg_size, file_size);

      // Determine expected CRC
      uint32_t expected;
      if(!segment_crcs_[seg].valid){
        if(0 != size_orgmeta){
          // Existing file: valid=false is a CRC tracking bug
          S3FS_PRN_ERR("[CRC] segment %zu has no CRC (valid=false) for path=%s, "
                       "possible bug in CRC tracking", seg, path.c_str());
          return -EIO;
        }
        // New file: unwritten segment is all zeros from ftruncate
        expected = 0;
      }else{
        expected = segment_crcs_[seg].crc32c;
      }

      // Zero-tail extension: for new files (size_orgmeta==0), CRC built by Extend
      // may only cover [seg_start, written_end). Extend with zeros to match the
      // full segment range [seg_start, seg_range_end) that CrcFromTmpfile reads.
      if(0 == size_orgmeta){
        off_t crc_end = segment_crcs_[seg].valid ? segment_written_end_[seg] : seg_start;
        if(crc_end < seg_range_end){
          static const char zeros[4096] = {0};
          off_t remaining = seg_range_end - crc_end;
          while(remaining > 0){
            size_t chunk = static_cast<size_t>(min(remaining, static_cast<off_t>(sizeof(zeros))));
            expected = rocksdb::crc32c::Extend(expected, zeros, chunk);
            remaining -= static_cast<off_t>(chunk);
          }
        }
      }

      uint32_t actual = CrcFromTmpfile(seg, file_size);
      if(actual != expected){
        S3FS_PRN_ERR("[CRC] segment %zu mismatch for path=%s: "
                     "expected=0x%08X tmpfile=0x%08X (upload range [%jd,%jd))",
                     seg, path.c_str(),
                     expected, actual,
                     (intmax_t)part_start, (intmax_t)part_end);
        return -EIO;
      }
    }
  }
  return 0;
}

//------------------------------------------------
void FdEntity::UpdateDiskUsedTracking()
{
  if(0 == FdManager::max_tmpdir_disk_usage){
    return;  // cap disabled, zero overhead
  }
  size_t current_disk;
  if(!cachepath.empty() && -1 != fd){
    // use_cache mode: track actual disk blocks allocated (handles sparse files correctly)
    struct stat st;
    current_disk = (0 == fstat(fd, &st)) ? static_cast<size_t>(st.st_blocks) * 512 : 0;
  }else{
    // no-cache mode: track pagelist loaded bytes
    size_t total_loaded = pagelist.Size();
    size_t total_unloaded = pagelist.GetTotalUnloadedPageSize();
    current_disk = (total_loaded > total_unloaded) ? (total_loaded - total_unloaded) : 0;
  }
  if(current_disk != last_tracked_loaded_bytes_){
    ssize_t delta = static_cast<ssize_t>(current_disk) - static_cast<ssize_t>(last_tracked_loaded_bytes_);
    FdManager::AdjustDiskUsed(delta);
    last_tracked_loaded_bytes_ = current_disk;
  }
}

void FdEntity::SaveDiskTrackingOnClose()
{
  if(0 == FdManager::max_tmpdir_disk_usage){
    return;
  }
  if(cachepath.empty() || -1 == fd){
    return;
  }
  // Get latest fstat-based tracking while fd is still valid
  UpdateDiskUsedTracking();

  // Check if cache file will persist on disk after fd is closed
  struct stat cache_st;
  if(0 == stat(cachepath.c_str(), &cache_st)){
    // Cache file persists: save tracked bytes to map, zero local tracker
    // so Clear()'s UpdateDiskUsedTracking produces delta=0 (no double release)
    FdManager::SetCacheFileTrackedBytes(path, last_tracked_loaded_bytes_);
    last_tracked_loaded_bytes_ = 0;
  }else{
    // Cache file was deleted (eviction/error): remove stale map entry
    // last_tracked stays as-is; Clear() will release via UpdateDiskUsedTracking
    FdManager::RemoveCacheFileTrackedBytes(path);
  }
}

FdEntity::FdEntity(const char* tpath, const char* cpath)
        : is_lock_init(false), refcnt(0), path(SAFESTRPTR(tpath)), cachepath(SAFESTRPTR(cpath)), mirrorpath(""),
          fd(-1), pfile(NULL), is_modify(false), size_orgmeta(0), inode(0), upload_id(""),
          prefetch_thread(0), prefetch_active(false), prefetch_start(0), prefetch_size(0),
          sr_range_start(-1), sr_range_reads(0), sr_next_launched(false),
          nocache_fallback_(false),
          last_tracked_loaded_bytes_(0)
{
  try{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, S3FS_MUTEX_RECURSIVE);   // recursive mutex
    pthread_mutex_init(&fdent_lock, &attr);
    pthread_mutex_init(&prefetch_mutex, NULL);
    is_lock_init = true;
  }catch(exception& e){
    S3FS_PRN_CRIT("failed to init mutex");
  }
}

FdEntity::~FdEntity()
{
  CancelPrefetch();
  Clear();

  if(is_lock_init){
    try{
      pthread_mutex_destroy(&fdent_lock);
      pthread_mutex_destroy(&prefetch_mutex);
    }catch(exception& e){
      S3FS_PRN_CRIT("failed to destroy mutex");
    }
    is_lock_init = false;
  }
}

// [s3fs-fuse compat] fdcache_entity.cpp:96-109
ino_t FdEntity::GetInode(int file_fd)
{
  if(-1 == file_fd){
    S3FS_PRN_ERR("file descriptor is wrong.");
    return 0;
  }
  struct stat st;
  if(0 != fstat(file_fd, &st)){
    S3FS_PRN_ERR("could not get stat for physical file descriptor(%d) by errno(%d).", file_fd, errno);
    return 0;
  }
  return st.st_ino;
}

// [s3fs-fuse compat] fdcache_entity.cpp:176-189
ino_t FdEntity::GetInode() const
{
  if(cachepath.empty()){
    S3FS_PRN_INFO("cache file path is empty, then return inode as 0.");
    return 0;
  }
  struct stat st;
  if(0 != stat(cachepath.c_str(), &st)){
    S3FS_PRN_INFO("could not get stat for file(%s) by errno(%d).", cachepath.c_str(), errno);
    return 0;
  }
  return st.st_ino;
}

void FdEntity::Clear(void)
{
  AutoLock auto_lock(&fdent_lock);

  if(-1 != fd){
    SaveDiskTrackingOnClose();
    if(!cachepath.empty()){
      // [s3fs-fuse compat] Only serialize if cache file inode matches the one at open time
      ino_t cur_inode = GetInode();
      if(0 != cur_inode && cur_inode == inode){
        CacheFileStat cfstat(path.c_str());
        if(!pagelist.Serialize(cfstat, true, inode)){
          S3FS_PRN_WARN("failed to save cache stat file(%s).", path.c_str());
        }
      }
    }
    if(pfile){
      fclose(pfile);
      pfile = NULL;
    }
    fd    = -1;
    inode = 0;

    if(!mirrorpath.empty()){
      if(-1 == unlink(mirrorpath.c_str())){
        S3FS_PRN_WARN("failed to remove mirror cache file(%s) by errno(%d).", mirrorpath.c_str(), errno);
      }
      mirrorpath.erase();
    }
  }
  pagelist.Init(0, false);
  refcnt              = 0;
  path                = "";
  cachepath           = "";
  is_modify           = false;
  sr_range_start      = -1;
  sr_range_reads      = 0;
  sr_next_launched    = false;
  untreated_list.ClearAll();
  segment_crcs_.clear();
  segment_written_end_.clear();
  nocache_fallback_   = false;

  // Update disk usage tracking (pagelist is now size 0, so loaded bytes = 0)
  UpdateDiskUsedTracking();
}

void FdEntity::Close(void)
{
  S3FS_PRN_DBG("[path=%s][fd=%d][refcnt=%d]", path.c_str(), fd, (-1 != fd ? refcnt - 1 : refcnt));

  CancelPrefetch();

  if(-1 != fd){
    AutoLock auto_lock(&fdent_lock);

    if(0 < refcnt){
      refcnt--;
    }
    if(0 == refcnt){
      SaveDiskTrackingOnClose();
      if(!cachepath.empty()){
        // [s3fs-fuse compat] Only serialize if cache file inode matches the one at open time
        ino_t cur_inode = GetInode();
        if(0 != cur_inode && cur_inode == inode){
          CacheFileStat cfstat(path.c_str());
          if(!pagelist.Serialize(cfstat, true, inode)){
            S3FS_PRN_WARN("failed to save cache stat file(%s).", path.c_str());
          }
        }
      }
      if(pfile){
        fclose(pfile);
        pfile = NULL;
      }
      fd    = -1;
      inode = 0;

      if(!mirrorpath.empty()){
        if(-1 == unlink(mirrorpath.c_str())){
          S3FS_PRN_WARN("failed to remove mirror cache file(%s) by errno(%d).", mirrorpath.c_str(), errno);
        }
        mirrorpath.erase();
      }
    }
  }
}

// CloseRef: lightweight refcnt decrement only.
// Used by read/write paths (GetFdEntityWithDup + CloseRef pattern).
// Does NOT call CancelPrefetch — prefetch continues running for subsequent reads.
// Does NOT do file cleanup — entity stays open as long as refcnt > 0.
// WARNING: Only use Close() in the release path (when the file is truly being closed).
void FdEntity::CloseRef(void)
{
  S3FS_PRN_DBG("[path=%s][fd=%d][refcnt=%d]", path.c_str(), fd, (-1 != fd ? refcnt - 1 : refcnt));

  if(-1 != fd){
    AutoLock auto_lock(&fdent_lock);
    if(0 < refcnt){
      refcnt--;
    }
  }
}

// CloseRefAndCheck: decrement refcnt and return true if it reached 0.
// Used by FdManager::Close to determine whether to delete the entity.
bool FdEntity::CloseRefAndCheck(void)
{
  S3FS_PRN_DBG("[path=%s][fd=%d][refcnt=%d]", path.c_str(), fd, (-1 != fd ? refcnt - 1 : refcnt));

  if(-1 != fd){
    AutoLock auto_lock(&fdent_lock);
    if(0 < refcnt){
      refcnt--;
    }
    return (0 == refcnt);
  }
  return true;  // not open, treat as zero
}

// CloseCleanup: final cleanup after refcnt reached 0.
// Called ONLY after entity has been removed from the fent map.
// Serializes cache, closes file descriptor, removes mirror file.
void FdEntity::CloseCleanup(void)
{
  S3FS_PRN_DBG("[path=%s][fd=%d] final cleanup", path.c_str(), fd);

  if(-1 != fd){
    AutoLock auto_lock(&fdent_lock);

    SaveDiskTrackingOnClose();
    if(!cachepath.empty()){
      ino_t cur_inode = GetInode();
      if(0 != cur_inode && cur_inode == inode){
        CacheFileStat cfstat(path.c_str());
        if(!pagelist.Serialize(cfstat, true, inode)){
          S3FS_PRN_WARN("failed to save cache stat file(%s).", path.c_str());
        }
      }
    }
    if(pfile){
      fclose(pfile);
      pfile = NULL;
    }
    fd    = -1;
    inode = 0;

    if(!mirrorpath.empty()){
      if(-1 == unlink(mirrorpath.c_str())){
        S3FS_PRN_WARN("failed to remove mirror cache file(%s) by errno(%d).", mirrorpath.c_str(), errno);
      }
      mirrorpath.erase();
    }
  }
}

int FdEntity::Dup(bool no_fd_lock_wait)
{
  S3FS_PRN_DBG("[path=%s][fd=%d][refcnt=%d]", path.c_str(), fd, (-1 != fd ? refcnt + 1 : refcnt));

  if(-1 != fd){
    AutoLock auto_lock(&fdent_lock, no_fd_lock_wait);
    if (!auto_lock.isLockAcquired()) {
      return -1;
    }
    refcnt++;
  }
  return fd;
}

//
// Open mirror file which is linked cache file.
//
int FdEntity::OpenMirrorFile(void)
{
  if(cachepath.empty()){
    S3FS_PRN_ERR("cache path is empty, why come here");
    return -EIO;
  }

  // make temporary directory
  string bupdir;
  if(!FdManager::MakeCachePath(NULL, bupdir, true, true)){
    S3FS_PRN_ERR("could not make bup cache directory path or create it.");
    return -EIO;
  }

  // try to link mirror file
  while(true){
    // make random(temp) file path
    // (do not care for threading, because allowed any value returned.)
    //
    char         szfile[NAME_MAX + 1];
    unsigned int seed = static_cast<unsigned int>(time(NULL));
    sprintf(szfile, "%x.tmp", rand_r(&seed));
    mirrorpath = bupdir + "/" + szfile;

    // link mirror file to cache file
    if(0 == link(cachepath.c_str(), mirrorpath.c_str())){
      break;
    }
    if(EEXIST != errno){
      S3FS_PRN_ERR("could not link mirror file(%s) to cache file(%s) by errno(%d).", mirrorpath.c_str(), cachepath.c_str(), errno);
      return -errno;
    }
  }

  // open mirror file
  int mirrorfd;

  // DEBUG: Check cache file size before opening mirror
  struct stat st_cache;
  if(0 == stat(cachepath.c_str(), &st_cache)){
    S3FS_PRN_DBG("[OpenMirrorFile] cache file: path=%s, st_size=%jd, st_blocks=%jd",
                 cachepath.c_str(), (intmax_t)st_cache.st_size, (intmax_t)st_cache.st_blocks);
  }

  if(-1 == (mirrorfd = open(mirrorpath.c_str(), O_RDWR))){
    S3FS_PRN_ERR("could not open mirror file(%s) by errno(%d).", mirrorpath.c_str(), errno);
    return -errno;
  }

  // DEBUG: Check mirror file size after opening
  struct stat st_mirror;
  if(0 == fstat(mirrorfd, &st_mirror)){
    S3FS_PRN_DBG("[OpenMirrorFile] mirror file: path=%s, st_size=%jd, st_blocks=%jd",
                 mirrorpath.c_str(), (intmax_t)st_mirror.st_size, (intmax_t)st_mirror.st_blocks);
  }

  return mirrorfd;
}

// [NOTE]
// This method does not lock fdent_lock, because FdManager::fd_manager_lock
// is locked before calling.
//
int FdEntity::Open(headers_t* pmeta, ssize_t size, time_t time, bool no_fd_lock_wait)
{
  S3FS_PRN_DBG("[FdEntity::Open] path=%s, bucket_type=%d, size=%jd",
              path.c_str(), g_bucket_type, (intmax_t)size);
  S3FS_PRN_DBG("[path=%s][fd=%d][size=%jd][time=%jd]", path.c_str(), fd, (intmax_t)size, (intmax_t)time);

  if(-1 != fd){
    // already opened, needs to increment refcnt.
    if (fd != Dup(no_fd_lock_wait)) {
      // had to wait for fd lock, return
      return -EIO;
    }

    // check only file size(do not need to save cfs and time.
    if(0 <= size && pagelist.Size() != static_cast<size_t>(size)){
      // truncate temporary file size
      if(-1 == ftruncate(fd, static_cast<size_t>(size))){
        S3FS_PRN_ERR("failed to truncate temporary file(%d) by errno(%d).", fd, errno);
        return -EIO;
      }
      // resize page list
      if(!pagelist.Resize(static_cast<size_t>(size), false)){
        S3FS_PRN_ERR("failed to truncate temporary file information(%d).", fd);
        return -EIO;
      }
    }
    // set original headers and set size.
    size_t new_size = (0 <= size ? static_cast<size_t>(size) : size_orgmeta);
    if(pmeta){
      orgmeta  = *pmeta;
      new_size = static_cast<size_t>(get_size(orgmeta));
    }
    if(new_size < size_orgmeta){
      size_orgmeta = new_size;
    }
    return 0;
  }

  bool  need_save_csf = false;  // need to save(reset) cache stat file
  bool  is_truncate   = false;  // need to truncate

  if(0 != cachepath.size()){
    // using cache
    S3FS_PRN_DBG("[FdEntity::Open] cachepath.size() != 0, using cache file: %s", cachepath.c_str());

    // [OBJECT BUCKET FAST PATH]
    // Object bucket: simplified logic without cache stat file dependency
    if(g_bucket_type == BUCKET_TYPE_OBJECT_SEMANTIC){
      S3FS_PRN_DBG("[object-bucket] FdEntity::Open: using simplified cache logic for path=%s", path.c_str());

      // Directly open cache file, no cache stat dependency
      fd = open(cachepath.c_str(), O_RDWR);

      if(-1 != fd){
        // Cache file exists, get actual size from fstat
        struct stat st;
        memset(&st, 0, sizeof(struct stat));

        if(-1 == fstat(fd, &st)){
          S3FS_PRN_ERR("[object-bucket] fstat failed for cache file(%s). errno(%d)", cachepath.c_str(), errno);
          close(fd);
          fd = -1;
          return (0 == errno ? -EIO : -errno);
        }

        // Use actual file size as source of truth
        if(-1 == size){
          size = static_cast<ssize_t>(st.st_size);
        }

        // Sync cache file size with target size
        // (e.g., after truncate updated S3 but cache file was not synced)
        if(st.st_size != static_cast<off_t>(size)){
          if(-1 == ftruncate(fd, static_cast<off_t>(size))){
            S3FS_PRN_ERR("[object-bucket] ftruncate cache file to %jd failed: errno(%d)", (intmax_t)size, errno);
            close(fd);
            fd = -1;
            return (0 == errno ? -EIO : -errno);
          }
          S3FS_PRN_DBG("[object-bucket] Cache file resized from %jd to %jd", (intmax_t)st.st_size, (intmax_t)size);
        }

        // Initialize pagelist with target size, pages marked as NOT loaded.
        // Following s3fs-fuse: without CacheFileStat verification, we cannot
        // assume cache file content matches the current S3 object (the file
        // may have been modified externally). Read will download from S3.
        pagelist.Init(static_cast<size_t>(size), false);

        S3FS_PRN_DBG("[object-bucket] Opened existing cache file: size=%jd, st_blocks=%jd",
                    (intmax_t)size, (intmax_t)st.st_blocks);

      } else {
        // Cache file doesn't exist, create new one
        if(errno != ENOENT){
          S3FS_PRN_ERR("[object-bucket] open failed for cache file(%s). errno(%d)", cachepath.c_str(), errno);
          return (0 == errno ? -EIO : -errno);
        }

        // Create new cache file
        fd = open(cachepath.c_str(), O_CREAT|O_RDWR|O_TRUNC, 0600);
        if(-1 == fd){
          S3FS_PRN_ERR("[object-bucket] failed to create cache file(%s). errno(%d)", cachepath.c_str(), errno);
          return (0 == errno ? -EIO : -errno);
        }

        // Initialize for new file
        if(-1 == size){
          size = 0;
        }

        // Set cache file to target size so fstat() reports correct size.
        // This is critical: getattr uses fstat(fd) to report st_size to
        // the kernel. If the file is 0 bytes, the kernel won't issue READ.
        // The file content is zeros, but pagelist marks pages as NOT loaded
        // so Read will download actual content from S3.
        if(0 < size){
          if(-1 == ftruncate(fd, static_cast<off_t>(size))){
            S3FS_PRN_ERR("[object-bucket] ftruncate new cache file to %jd failed: errno(%d)", (intmax_t)size, errno);
            close(fd);
            fd = -1;
            return (0 == errno ? -EIO : -errno);
          }
        }
        pagelist.Init(static_cast<size_t>(size), false);
        need_save_csf = false;  // Object bucket doesn't use cache stat

        S3FS_PRN_DBG("[object-bucket] Created new cache file: size=%jd", (intmax_t)size);
      }

      // Mirror file opening is handled by the shared code below (line ~993)
    }

    // [FILE BUCKET PATH - UNCHANGED]
    // Original logic preserved completely
    else{
      // open cache and cache stat file, load page info.
      CacheFileStat cfstat(path.c_str());

      // try to open cache file
      fd = open(cachepath.c_str(), O_RDWR);
      ino_t open_inode = 0;
      if(-1 != fd){
        open_inode = FdEntity::GetInode(fd);
      }
      if(-1 != fd && 0 != open_inode && pagelist.Serialize(cfstat, false, open_inode)){
        inode = open_inode;
        // succeed to open cache file and to load stats data
        struct stat st;
        memset(&st, 0, sizeof(struct stat));
        if(-1 == fstat(fd, &st)){
          S3FS_PRN_ERR("fstat is failed. errno(%d)", errno);
          fd    = -1;
          inode = 0;
          return (0 == errno ? -EIO : -errno);
        }
        // check size, st_size, loading stat file
        if(-1 == size){
          if(static_cast<size_t>(st.st_size) != pagelist.Size()){
            pagelist.Resize(st.st_size, false);
            need_save_csf = true;     // need to update page info
          }
          size = static_cast<ssize_t>(st.st_size);
        }else{
          if(static_cast<size_t>(size) != pagelist.Size()){
            pagelist.Resize(static_cast<size_t>(size), false);
            need_save_csf = true;     // need to update page info
          }
          if(static_cast<size_t>(size) != static_cast<size_t>(st.st_size)){
            is_truncate = true;
          }
        }

      }else{
        // could not open cache file or could not load stats data, so initialize it.
        if(-1 != fd){
          close(fd);
        }
        inode = 0;
        if(-1 == (fd = open(cachepath.c_str(), O_CREAT|O_RDWR|O_TRUNC, 0600))){
          S3FS_PRN_ERR("failed to open file(%s). errno(%d)", cachepath.c_str(), errno);
          return (0 == errno ? -EIO : -errno);
        }
        inode         = FdEntity::GetInode(fd);
        need_save_csf = true;       // need to update page info
        if(-1 == size){
          size = 0;
          pagelist.Init(0, false);
        }else{
          pagelist.Resize(static_cast<size_t>(size), false);
          is_truncate = true;
        }
      }
    }

    // open mirror file (shared with object bucket path)
    int mirrorfd;
    if(0 >= (mirrorfd = OpenMirrorFile())){
      S3FS_PRN_ERR("failed to open mirror file linked cache file(%s).", cachepath.c_str());
      return (0 == mirrorfd ? -EIO : mirrorfd);
    }
    S3FS_PRN_DBG("[FdEntity::Open] Mirror file opened: mirrorfd=%d", mirrorfd);
    // switch fd
    close(fd);
    fd = mirrorfd;

    // make file pointer(for being same tmpfile)
    if(NULL == (pfile = fdopen(fd, "wb"))){
      S3FS_PRN_ERR("failed to get fileno(%s). errno(%d)", cachepath.c_str(), errno);
      close(fd);
      fd    = -1;
      inode = 0;
      return (0 == errno ? -EIO : -errno);
    }

  }else{
    // not using cache
    inode = 0;
    S3FS_PRN_DBG("[FdEntity::Open] cachepath.size() == 0, using tmpfile (no cache) for path=%s", path.c_str());

    // open temporary file
    if(NULL == (pfile = FdManager::MakeTempFile()) || -1 ==(fd = fileno(pfile))){
      S3FS_PRN_ERR("failed to open tmp file. err(%d)", errno);
      if(pfile){
        fclose(pfile);
        pfile = NULL;
      }
      return (0 == errno ? -EIO : -errno);
    }

    // [FIX] Object bucket: restore file content if pagelist has data
    // When reopening a no-cache file in object bucket mode, tmpfile was deleted
    // and recreated empty. We need to restore the file size to match pagelist.
    // File bucket uses different logic and should not be affected.
    if(g_bucket_type == BUCKET_TYPE_OBJECT_SEMANTIC && pagelist.Size() > 0){
      S3FS_PRN_DBG("[object-bucket][no-cache] Restoring tmpfile: size=%zu bytes", pagelist.Size());

      // First set file size
      if(0 != ftruncate(fd, static_cast<off_t>(pagelist.Size()))){
        S3FS_PRN_WARN("[object-bucket] Failed to restore tmpfile size to %zu: errno(%d)", pagelist.Size(), errno);
      }

      // Mark all pages as unloaded
      SetAllStatusUnloaded();

      // CRITICAL: Load data from S3 to tmpfile
      // Without this, tmpfile is empty (st.st_blocks=0) and FLUSH will fail
      if(0 != Load(0, pagelist.Size())){
        S3FS_PRN_WARN("[object-bucket][no-cache] Failed to load data from S3, tmpfile may be empty");
      }else{
        S3FS_PRN_DBG("[object-bucket][no-cache] Loaded %zu bytes from S3 to tmpfile", pagelist.Size());
      }
    }

    if(-1 == size){
      size = 0;
      pagelist.Init(0, false);
    }else{
      pagelist.Resize(static_cast<size_t>(size), false);
      is_truncate = true;
    }
  }

  // truncate cache(tmp) file
  if(is_truncate){
    // Object bucket: only truncate if size is actually different
    if(g_bucket_type == BUCKET_TYPE_OBJECT_SEMANTIC){
      struct stat st;
      if(0 == fstat(fd, &st) && st.st_size == static_cast<off_t>(size)){
        // Size already correct, skip truncate
        S3FS_PRN_DBG("[object-bucket] Skipping truncate, size already correct: %jd", (intmax_t)size);
        is_truncate = false;
      }
    }

    if(is_truncate){
      if(0 != ftruncate(fd, static_cast<off_t>(size)) || 0 != fsync(fd)){
        S3FS_PRN_ERR("ftruncate(%s) or fsync returned err(%d)", cachepath.c_str(), errno);
        fclose(pfile);
        pfile = NULL;
        fd    = -1;
        inode = 0;
        return (0 == errno ? -EIO : -errno);
      }
    }
  }

  // reset cache stat file
  if(need_save_csf){
    // Object bucket: completely skip cache stat file
    if(g_bucket_type != BUCKET_TYPE_OBJECT_SEMANTIC){
      CacheFileStat cfstat(path.c_str());
      if(!pagelist.Serialize(cfstat, true, inode)){
        S3FS_PRN_WARN("failed to save cache stat file(%s), but continue...", path.c_str());
      }
    } else {
      S3FS_PRN_DBG("[object-bucket] Skipping cache stat save for path=%s", path.c_str());
    }
  }

  // init internal data
  refcnt    = 1;
  is_modify = false;

  // set original headers and size in it.
  if(pmeta){
    orgmeta      = *pmeta;
    size_orgmeta = static_cast<size_t>(get_size(orgmeta));
  }else{
    orgmeta.clear();
    size_orgmeta = 0;
  }

  // Initialize CRC32C segment arrays (object bucket only)
  segment_crcs_.clear();
  segment_written_end_.clear();
  nocache_fallback_ = false;

  // Restore tracked bytes from map for use_cache reopen (prevents double-counting)
  if(!cachepath.empty()){
    last_tracked_loaded_bytes_ = FdManager::GetCacheFileTrackedBytes(path);
  }

  // Update disk usage tracking after Open initialization
  UpdateDiskUsedTracking();

  // set mtime(set "x-amz-meta-mtime" in orgmeta)
  if(-1 != time){
    if(0 != SetMtime(time)){
      S3FS_PRN_ERR("failed to set mtime. errno(%d)", errno);
      fclose(pfile);
      pfile = NULL;
      fd    = -1;
      inode = 0;
      return (0 == errno ? -EIO : -errno);
    }
  }

  return 0;
}

// [NOTE]
// This method is called from only nocopyapi functions.
// So we do not check disk space for this option mode, if there is no enough
// disk space this method will be failed.
//
bool FdEntity::OpenAndLoadAll(headers_t* pmeta, size_t* size, bool force_load)
{
  int result;

  S3FS_PRN_INFO3("[path=%s][fd=%d]", path.c_str(), fd);

  if(-1 == fd){
    if(0 != Open(pmeta)){
      return false;
    }
  }
  AutoLock auto_lock(&fdent_lock);

  if(force_load){
    SetAllStatusUnloaded();
  }
  //
  // TODO: possibly do background for delay loading
  //
  if(0 != (result = Load())){
    S3FS_PRN_ERR("could not download, result(%d)", result);
    return false;
  }
  if(is_modify){
    is_modify = false;
  }
  if(size){
    *size = pagelist.Size();
  }
  return true;
}

bool FdEntity::GetStats(struct stat& st)
{
  AutoLock auto_lock(&fdent_lock);
  if(-1 == fd){
    return false;
  }

  memset(&st, 0, sizeof(struct stat));
  if(-1 == fstat(fd, &st)){
    S3FS_PRN_ERR("fstat failed. errno(%d)", errno);
    return false;
  }
  return true;
}

// ISSUE2026051200001 M.3
bool FdEntity::GetOriginalHeaders(headers_t& meta_out) const
{
  AutoLock auto_lock(const_cast<pthread_mutex_t*>(&fdent_lock));
  meta_out = orgmeta;
  return !orgmeta.empty();
}

int FdEntity::SetMtime(time_t time)
{
  S3FS_PRN_INFO3("[path=%s][fd=%d][time=%jd]", path.c_str(), fd, (intmax_t)time);

  if(-1 == time){
    return 0;
  }

  AutoLock auto_lock(&fdent_lock);
  if(-1 != fd){
    struct timeval tv[2];
    tv[0].tv_sec = time;
    tv[0].tv_usec= 0L;
    tv[1].tv_sec = tv[0].tv_sec;
    tv[1].tv_usec= 0L;
    if(-1 == futimes(fd, tv)){
      S3FS_PRN_ERR("futimes failed. errno(%d)", errno);
      return -errno;
    }
  }else if(0 < cachepath.size()){
    // not opened file yet.
    struct utimbuf n_mtime;
    n_mtime.modtime = time;
    n_mtime.actime  = time;
    if(-1 == utime(cachepath.c_str(), &n_mtime)){
      S3FS_PRN_ERR("utime failed. errno(%d)", errno);
      return -errno;
    }
  }
  orgmeta[hws_obs_meta_mtime] = str(time);

  return 0;
}

bool FdEntity::UpdateMtime(void)
{
  AutoLock auto_lock(&fdent_lock);
  struct stat st;
  if(!GetStats(st)){
    return false;
  }
  orgmeta[hws_obs_meta_mtime] = str(st.st_mtime);
  return true;
}

bool FdEntity::GetSize(size_t& size)
{
  if(-1 == fd){
    return false;
  }
  AutoLock auto_lock(&fdent_lock);

  size = pagelist.Size();
  return true;
}

bool FdEntity::SetMode(mode_t mode)
{
  AutoLock auto_lock(&fdent_lock);
  orgmeta[hws_obs_meta_mode] = str(mode);
  return true;
}

bool FdEntity::SetUId(uid_t uid)
{
  AutoLock auto_lock(&fdent_lock);
  orgmeta[hws_obs_meta_uid] = str(uid);
  return true;
}

bool FdEntity::SetGId(gid_t gid)
{
  AutoLock auto_lock(&fdent_lock);
  orgmeta[hws_obs_meta_gid] = str(gid);
  return true;
}

bool FdEntity::SetContentType(const char* path)
{
  if(!path){
    return false;
  }
  AutoLock auto_lock(&fdent_lock);
  orgmeta["Content-Type"] = S3fsCurl::LookupMimeType(string(path));
  return true;
}

bool FdEntity::SetAllStatus(bool is_loaded)
{
  S3FS_PRN_INFO3("[path=%s][fd=%d][%s]", path.c_str(), fd, is_loaded ? "loaded" : "unloaded");

  if(-1 == fd){
    return false;
  }
  // [NOTE]
  // this method is only internal use, and calling after locking.
  // so do not lock now.

  // get file size
  struct stat st;
  memset(&st, 0, sizeof(struct stat));
  if(-1 == fstat(fd, &st)){
    S3FS_PRN_ERR("fstat is failed. errno(%d)", errno);
    return false;
  }
  // Reinit
  pagelist.Init(st.st_size, is_loaded);

  return true;
}

int FdEntity::Load(off_t start, size_t size, bool is_modified_flag)
{
  S3FS_PRN_DBG("[path=%s][fd=%d][offset=%jd][size=%jd]", path.c_str(), fd, (intmax_t)start, (intmax_t)size);

  if(-1 == fd){
    return -EBADF;
  }
  AutoLock auto_lock(&fdent_lock);

  int result = 0;

  // check loaded area & load
  fdpage_list_t unloaded_list;
  if(0 < pagelist.GetUnloadedPages(unloaded_list, start, size)){
    for(fdpage_list_t::iterator iter = unloaded_list.begin(); iter != unloaded_list.end(); ++iter){
      if(0 != size && (static_cast<size_t>(start) + size) <= static_cast<size_t>((*iter)->offset)){
        // reached end
        break;
      }
      // check loading size
      size_t need_load_size = 0;
      if(static_cast<size_t>((*iter)->offset) < size_orgmeta){
        // original file size(on S3) is smaller than request.
        need_load_size = (static_cast<size_t>((*iter)->next()) <= size_orgmeta ? (*iter)->bytes : (size_orgmeta - (*iter)->offset));
      }
      size_t over_size = (*iter)->bytes - need_load_size;

      // download
      if(static_cast<size_t>(2 * S3fsCurl::GetMultipartSize()) <= need_load_size && !nomultipart){ // default 20MB
        // parallel request
        // Additional time is needed for large files
        time_t backup = 0;
        if(120 > S3fsCurl::GetReadwriteTimeout()){
          backup = S3fsCurl::SetReadwriteTimeout(120);
        }
        result = S3fsCurl::ParallelGetObjectRequest(path.c_str(), fd, (*iter)->offset, need_load_size);
        if(0 != backup){
          S3fsCurl::SetReadwriteTimeout(backup);
        }
      }else{
        // single request
        if(0 < need_load_size){
          S3fsCurl s3fscurl;
          result = s3fscurl.GetObjectRequest(path.c_str(), fd, (*iter)->offset, need_load_size);
        }else{
          result = 0;
        }
      }
      if(0 != result){
        break;
      }

      // initialize for the area of over original size
      if(0 < over_size){
        if(0 != (result = FdEntity::FillFile(fd, 0, over_size, (*iter)->offset + need_load_size))){
          S3FS_PRN_ERR("failed to fill rest bytes for fd(%d). errno(%d)", fd, result);
          break;
        }
      }

      // Set loaded flag first (may Resize/rebuild pages internally)
      pagelist.SetPageLoadedStatus((*iter)->offset, static_cast<off_t>((*iter)->bytes), true);

      // Then set modified flags — must come AFTER SetPageLoadedStatus because
      // SetPageLoadedStatus's Resize path destroys and recreates pages,
      // which would overwrite any modified flag set before it.
      if(0 < over_size){
        // Zero-filled region does not exist on S3 — mark modified so mixmultipart
        // uses UploadPart instead of CopyPart (which would reference non-existent ranges).
        pagelist.SetPageModifiedStatus((*iter)->offset + need_load_size, over_size, true);
      }
      if(is_modified_flag){
        pagelist.SetPageModifiedStatus((*iter)->offset, static_cast<off_t>((*iter)->bytes), true);
      }

      // CRC32C: establish initial CRC for loaded segments (D4)
      // This ensures subsequent Write can correctly distinguish append vs overwrite.
      {
        off_t crc_seg_size = CRC_SEGMENT_SIZE;
        off_t load_start = (*iter)->offset;
        off_t load_end   = (*iter)->offset + static_cast<off_t>((*iter)->bytes);
        off_t cur_file_size = static_cast<off_t>(pagelist.Size());
        size_t last_seg = (load_end > 0) ? static_cast<size_t>((load_end - 1) / crc_seg_size) : 0;
        EnsureSegmentCrcsSize(last_seg + 1);

        for(off_t pos = load_start; pos < load_end; ){
          size_t seg = static_cast<size_t>(pos / crc_seg_size);
          off_t seg_boundary = static_cast<off_t>(seg + 1) * crc_seg_size;
          off_t chunk_end = min(load_end, seg_boundary);

          // CrcFromTmpfile reads [seg_start, min(seg_boundary, file_size)).
          // written_end must match this actual range for correct append detection.
          off_t crc_range_end = min(seg_boundary, cur_file_size);
          if(crc_range_end > segment_written_end_[seg]){
            segment_written_end_[seg] = crc_range_end;
          }
          segment_crcs_[seg].crc32c = CrcFromTmpfile(seg, cur_file_size);
          segment_crcs_[seg].valid = true;

          pos = chunk_end;
        }
      }

      // Update disk usage tracking after load
      UpdateDiskUsedTracking();
    }
    PageList::FreeList(unloaded_list);
  }
  return result;
}

// [NOTE]
// At no disk space for caching object.
// This method is downloading by dividing an object of the specified range
// and uploading by multipart after finishing downloading it.
//
// [NOTICE]
// Need to lock before calling this method.
//
int FdEntity::NoCacheLoadAndPost(off_t start, size_t size)
{
  int result = 0;

  S3FS_PRN_INFO3("[path=%s][fd=%d][offset=%jd][size=%jd]", path.c_str(), fd, (intmax_t)start, (intmax_t)size);

  if(-1 == fd){
    return -EBADF;
  }

  // CRC data corresponds to original tmpfile which will be deleted;
  // clear CRC and skip verification for this flush cycle (D2)
  segment_crcs_.clear();
  segment_written_end_.clear();
  nocache_fallback_ = true;

  // [NOTE]
  // This method calling means that the cache file is never used no more.
  //
  if(0 != cachepath.size()){
    // Clean map entry before deleting cache file
    FdManager::RemoveCacheFileTrackedBytes(path);
    // remove cache files(and cache stat file)
    FdManager::DeleteCacheFile(path.c_str());
    // cache file path does not use no more.
    cachepath.erase();
    mirrorpath.erase();
  }

  // Change entity key in manager mapping
  FdManager::get()->ChangeEntityToTempPath(this, path.c_str());

  // open temporary file
  FILE* ptmpfp;
  int   tmpfd;
  if(NULL == (ptmpfp = FdManager::MakeTempFile()) || -1 ==(tmpfd = fileno(ptmpfp))){
    S3FS_PRN_ERR("failed to open tmp file. err(%d)", errno);
    if(ptmpfp){
      fclose(ptmpfp);
    }
    return (0 == errno ? -EIO : -errno);
  }

  // loop uploading by multipart
  for(fdpage_list_t::iterator iter = pagelist.pages.begin(); iter != pagelist.pages.end(); ++iter){
    if((*iter)->end() < start){
      continue;
    }
    if(0 != size && (static_cast<size_t>(start) + size) <= static_cast<size_t>((*iter)->offset)){
      break;
    }
    // download each multipart size(default 10MB) in unit
    for(size_t oneread = 0, totalread = ((*iter)->offset < start ? start : 0); totalread < (*iter)->bytes; totalread += oneread){
      int   upload_fd = fd;
      off_t offset    = (*iter)->offset + totalread;
      oneread         = min(((*iter)->bytes - totalread), static_cast<size_t>(S3fsCurl::GetMultipartSize()));

      // check rest size is over minimum part size
      //
      // [NOTE]
      // If the final part size is smaller than 5MB, it is not allowed by S3 API.
      // For this case, if the previous part of the final part is not over 5GB,
      // we incorporate the final part to the previous part. If the previous part
      // is over 5GB, we want to even out the last part and the previous part.
      //
      if(((*iter)->bytes - totalread - oneread) < MIN_MULTIPART_SIZE){
        if(FIVE_GB < ((*iter)->bytes - totalread)){
          oneread = ((*iter)->bytes - totalread) / 2;
        }else{
          oneread = ((*iter)->bytes - totalread);
        }
      }

      bool nocache_disk_adjusted = false;

      if(!(*iter)->loaded){
        //
        // loading or initializing
        //
        upload_fd = tmpfd;

        // load offset & size
        size_t need_load_size = 0;
        if(size_orgmeta <= static_cast<size_t>(offset)){
          // all area is over of original size
          need_load_size      = 0;
        }else{
          if(size_orgmeta < (offset + oneread)){
            // original file size(on S3) is smaller than request.
            need_load_size    = size_orgmeta - offset;
          }else{
            need_load_size    = oneread;
          }
        }
        size_t over_size      = oneread - need_load_size;

        // [ISSUE2026032400002] Check physical disk space before allocating tmpfd.
        // NoCacheLoadAndPost is the cap-full fallback, so we don't check cap here —
        // only physical disk to prevent filling the filesystem.
        {
          struct statvfs vfsbuf;
          if(0 == statvfs(FdManager::GetTmpDir(), &vfsbuf)){
            uint64_t avail = static_cast<uint64_t>(vfsbuf.f_bavail) * vfsbuf.f_frsize;
            if(avail < static_cast<uint64_t>(oneread)){
              S3FS_PRN_ERR("[NoCacheLoadAndPost] insufficient disk space: avail=%llu need=%zu",
                           (unsigned long long)avail, oneread);
              result = -ENOSPC;
              break;
            }
          }
        }

        // [NOTE]
        // truncate file to zero and set length to part offset + size
        // after this, file length is (offset + size), but file does not use any disk space.
        //
        if(-1 == ftruncate(tmpfd, 0) || -1 == ftruncate(tmpfd, (offset + oneread))){
          S3FS_PRN_ERR("failed to truncate temporary file(%d).", tmpfd);
          result = -EIO;
          break;
        }

        // single area get request
        if(0 < need_load_size){
          S3fsCurl s3fscurl;
          if(0 != (result = s3fscurl.GetObjectRequest(path.c_str(), tmpfd, offset, oneread))){
            S3FS_PRN_ERR("failed to get object(start=%zd, size=%zu) for file(%d).", offset, oneread, tmpfd);
            break;
          }
        }
        // initialize fd without loading
        if(0 < over_size){
          if(0 != (result = FdEntity::FillFile(tmpfd, 0, over_size, offset + need_load_size))){
            S3FS_PRN_ERR("failed to fill rest bytes for fd(%d). errno(%d)", tmpfd, result);
            break;
          }
          // set modify flag
          is_modify = false;
        }

        // [ISSUE2026032400002] Track tmpfd disk usage in global counter.
        // This makes obsfs_disk_used_bytes_ reflect NoCacheLoadAndPost's temporary
        // disk allocation, enabling accurate monitoring and cache eviction triggers.
        if(0 != FdManager::max_tmpdir_disk_usage){
          FdManager::AdjustDiskUsed(static_cast<ssize_t>(oneread));
          nocache_disk_adjusted = true;
        }

      }else{
        // already loaded area
      }

      // single area upload by multipart post
      if(0 != (result = NoCacheMultipartPost(upload_fd, offset, oneread))){
        S3FS_PRN_ERR("failed to multipart post(start=%zd, size=%zu) for file(%d).", offset, oneread, upload_fd);
      }
      // [ISSUE2026032400002] Release tmpfd tracked space after upload (success or fail).
      // Next iteration's ftruncate(tmpfd, 0) will free the actual disk blocks.
      if(nocache_disk_adjusted){
        FdManager::AdjustDiskUsed(-static_cast<ssize_t>(oneread));
      }
      if(0 != result){
        break;
      }
    }
    if(0 != result){
      break;
    }

    // set loaded flag
    if(!(*iter)->loaded){
      if((*iter)->offset < start){
        fdpage* page    = new fdpage((*iter)->offset, static_cast<size_t>(start - (*iter)->offset), (*iter)->loaded);
        (*iter)->bytes -= (start - (*iter)->offset);
        (*iter)->offset = start;
        pagelist.pages.insert(iter, page);
      }
      if(0 != size && (static_cast<size_t>(start) + size) < static_cast<size_t>((*iter)->next())){
        fdpage* page    = new fdpage((*iter)->offset, static_cast<size_t>((start + size) - (*iter)->offset), true);
        (*iter)->bytes -= static_cast<size_t>((start + size) - (*iter)->offset);
        (*iter)->offset = start + size;
        pagelist.pages.insert(iter, page);
      }else{
        (*iter)->loaded = true;
      }
    }
  }
  if(0 == result){
    // compress pagelist
    pagelist.Compress();

    // fd data do empty
    if(-1 == ftruncate(fd, 0)){
      S3FS_PRN_ERR("failed to truncate file(%d), but continue...", fd);
    }
  }

  // close temporary
  fclose(ptmpfp);

  return result;
}

// [NOTE]
// At no disk space for caching object.
// This method is starting multipart uploading.
//
int FdEntity::NoCachePreMultipartPost(void)
{
  // initialize multipart upload values
  upload_id.erase();
  etaglist.clear();

  S3fsCurl s3fscurl(true);
  int      result;
  if(0 != (result = s3fscurl.PreMultipartPostRequest(path.c_str(), orgmeta, upload_id, false))){
    return result;
  }
  s3fscurl.DestroyCurlHandle();
  return 0;
}

// [NOTE]
// At no disk space for caching object.
// This method is uploading one part of multipart.
//
int FdEntity::NoCacheMultipartPost(int tgfd, off_t start, size_t size)
{
  if(-1 == tgfd || upload_id.empty()){
    S3FS_PRN_ERR("Need to initialize for multipart post.");
    return -EIO;
  }
  S3fsCurl s3fscurl(true);
  return s3fscurl.MultipartUploadRequest(upload_id, path.c_str(), tgfd, start, size, etaglist);
}

// [NOTE]
// At no disk space for caching object.
// This method is finishing multipart uploading.
//
int FdEntity::NoCacheCompleteMultipartPost(void)
{
  if(upload_id.empty() || etaglist.empty()){
    S3FS_PRN_ERR("There is no upload id or etag list.");
    return -EIO;
  }

  S3fsCurl s3fscurl(true);
  int      result;
  if(0 != (result = s3fscurl.CompleteMultipartPostRequest(path.c_str(), upload_id, etaglist))){
    return result;
  }
  s3fscurl.DestroyCurlHandle();

  // reset values
  upload_id.erase();
  etaglist.clear();
  untreated_list.ClearAll();

  return 0;
}

int FdEntity::RowFlush(const char* tpath, bool force_sync)
{
  int result;

  S3FS_PRN_INFO3("[tpath=%s][path=%s][fd=%d]", SAFESTRPTR(tpath), path.c_str(), fd);

  if(-1 == fd){
    return -EBADF;
  }
  if(streamread){
    CancelPrefetch();
  }
  AutoLock auto_lock(&fdent_lock);

  if(!force_sync && !is_modify){
    // Nothing to update — no data was written to this FdEntity.
    // IMPORTANT: Callers (fsync, flush) should NOT pass force_sync=true
    // unless they are certain the tmpfile has real data. Otherwise, the
    // sparse-zero tmpfile from Open(ftruncate) would be uploaded to S3,
    // corrupting the object. See s3fs_fsync_s3 for details.
    return 0;
  }

  // Check if mixmultipart will handle gap downloads itself
  bool will_use_mixmultipart = false;
  if(0 == upload_id.length() && !nomixupload && size_orgmeta > 0 &&
     pagelist.Size() >= static_cast<size_t>(2 * S3fsCurl::GetMultipartSize()) && !nomultipart){
    will_use_mixmultipart = true;
  }

  // If there is no loading all of the area, loading all area.
  size_t restsize = pagelist.GetTotalUnloadedPageSize();
  if(0 < restsize && !will_use_mixmultipart){
    if(0 == upload_id.length()){
      // check disk space — use entity-level Reserve with retry + cleanup
      if(ReserveDiskSpace(restsize)){
        FdManager::ReleaseDiskSpace(restsize);
        // enough disk space — Load all uninitialized area
        if(0 != (result = Load())){
          S3FS_PRN_ERR("failed to upload all area(errno=%d)", result);
          return static_cast<ssize_t>(result);
        }
      }else{
        // no enough disk space
        // upload all by multipart uploading
        if(0 != (result = NoCacheLoadAndPost())){
          S3FS_PRN_ERR("failed to upload all area by multipart uploading(errno=%d)", result);
          return static_cast<ssize_t>(result);
        }
      }
    }else{
      // already start multipart uploading
    }
  }

  if(0 == upload_id.length()){
    // normal uploading

    /*
     * Make decision to do multi upload (or not) based upon file size
     * 
     * According to the AWS spec:
     *  - 1 to 10,000 parts are allowed
     *  - minimum size of parts is 5MB (expect for the last part)
     * 
     * For our application, we will define minimum part size to be 10MB (10 * 2^20 Bytes)
     * minimum file size will be 64 GB - 2 ** 36 
     * 
     * Initially uploads will be done serially
     * 
     * If file is > 20MB, then multipart will kick in
     */
    if(pagelist.Size() > static_cast<size_t>(MAX_MULTIPART_CNT * S3fsCurl::GetMultipartSize())){
      // close f ?
      return -ENOTSUP;
    }

    // seek to head of file.
    if(0 != lseek(fd, 0, SEEK_SET)){
      S3FS_PRN_ERR("lseek error(%d)", errno);
      return -errno;
    }
    // backup upload file size
    struct stat st;
    memset(&st, 0, sizeof(struct stat));
    if(-1 == fstat(fd, &st)){
      S3FS_PRN_ERR("fstat is failed by errno(%d), but continue...", errno);
    }
    if(pagelist.Size() >= static_cast<size_t>(2 * S3fsCurl::GetMultipartSize()) && !nomultipart){ // default 20MB
      // Additional time is needed for large files
      time_t backup = 0;
      if(120 > S3fsCurl::GetReadwriteTimeout()){
        backup = S3fsCurl::SetReadwriteTimeout(120);
      }
      if(will_use_mixmultipart){
        // mixmultipart: only upload dirty regions, use CopyPart for unmodified
        fdpage_list_t dlpages;
        fdpage_list_t mixuppages;
        pagelist.GetPageListsForMultipartUpload(dlpages, mixuppages, S3fsCurl::GetMultipartSize());

        // Download needed gap regions, marking as modified so they stay as
        // UploadPart candidates on retry (aligned with s3fs-fuse RowFlushMultipart)
        for(fdpage_list_t::iterator dp = dlpages.begin(); dp != dlpages.end(); ++dp){
          if(0 != (result = Load((*dp)->offset, (*dp)->bytes, true))){
            S3FS_PRN_ERR("failed to load gap region for mixmultipart(offset=%jd, size=%zu, errno=%d)",
                         (intmax_t)(*dp)->offset, (*dp)->bytes, result);
            PageList::FreeList(dlpages);
            PageList::FreeList(mixuppages);
            if(0 != backup){
              S3fsCurl::SetReadwriteTimeout(backup);
            }
            return result;
          }
        }
        PageList::FreeList(dlpages);

        // CRC32C: verify modified segments before upload (step 2)
        if(!nocache_fallback_ && !segment_crcs_.empty()){
          fdpage_list_t upload_parts;
          for(fdpage_list_t::iterator mp = mixuppages.begin(); mp != mixuppages.end(); ++mp){
            if((*mp)->modified){
              upload_parts.push_back(*mp);  // shallow ref, not owned
            }
          }
          result = VerifyCrcBeforeUpload(upload_parts, static_cast<off_t>(pagelist.Size()));
          upload_parts.clear();  // clear without freeing (owned by mixuppages)
          if(0 != result){
            S3FS_PRN_ERR("[CRC] integrity check failed before MixMultipart upload, aborting");
            PageList::FreeList(mixuppages);
            if(0 != backup){
              S3fsCurl::SetReadwriteTimeout(backup);
            }
            return result;
          }
        }

        result = S3fsCurl::MixMultipartUploadRequest(
            tpath ? tpath : path.c_str(), orgmeta, fd, mixuppages);
        PageList::FreeList(mixuppages);
      }else{
        // CRC32C: verify all segments before ParallelMultipart upload
        if(!nocache_fallback_ && !segment_crcs_.empty()){
          // Entire file is modified → verify all segments
          fdpage_list_t full_file;
          full_file.push_back(new fdpage(0, pagelist.Size(), true, true));
          result = VerifyCrcBeforeUpload(full_file, static_cast<off_t>(pagelist.Size()));
          PageList::FreeList(full_file);
          if(0 != result){
            S3FS_PRN_ERR("[CRC] integrity check failed before ParallelMultipart upload, aborting");
            if(0 != backup){
              S3fsCurl::SetReadwriteTimeout(backup);
            }
            return result;
          }
        }
        result = S3fsCurl::ParallelMultipartUploadRequest(tpath ? tpath : path.c_str(), orgmeta, fd);
      }
      if(0 != backup){
        S3fsCurl::SetReadwriteTimeout(backup);
      }
    }else{
      // CRC32C: verify all segments before PutRequest (small file upload)
      if(!nocache_fallback_ && !segment_crcs_.empty()){
        fdpage_list_t full_file;
        full_file.push_back(new fdpage(0, pagelist.Size(), true, true));
        result = VerifyCrcBeforeUpload(full_file, static_cast<off_t>(pagelist.Size()));
        PageList::FreeList(full_file);
        if(0 != result){
          S3FS_PRN_ERR("[CRC] integrity check failed before PutRequest upload, aborting");
          return result;
        }
      }
      S3fsCurl s3fscurl(true);
      result = s3fscurl.PutRequest(tpath ? tpath : path.c_str(), orgmeta, fd);
    }

    // seek to head of file.
    if(0 == result && 0 != lseek(fd, 0, SEEK_SET)){
      S3FS_PRN_ERR("lseek error(%d)", errno);
      return -errno;
    }

    // reset uploaded file size
    size_orgmeta = static_cast<size_t>(st.st_size);

  }else{
    // upload all remaining untreated parts
    {
      untreated_list_t parts;
      untreated_list.Duplicate(parts);
      for(untreated_list_t::iterator iter = parts.begin(); iter != parts.end(); ++iter){
        if(0 != (result = NoCacheMultipartPost(fd, iter->start, static_cast<size_t>(iter->size)))){
          S3FS_PRN_ERR("failed to multipart post(start=%jd, size=%jd) for file(%d).",
                       (intmax_t)iter->start, (intmax_t)iter->size, fd);
          return result;
        }
      }
      untreated_list.ClearAll();
    }
    // complete multipart uploading.
    if(0 != (result = NoCacheCompleteMultipartPost())){
      S3FS_PRN_ERR("failed to complete(finish) multipart post for file(%d).", fd);
      return result;
    }
    // truncate file to zero
    if(-1 == ftruncate(fd, 0)){
      // So the file has already been removed, skip error.
      S3FS_PRN_ERR("failed to truncate file(%d) to zero, but continue...", fd);
    }
    // Sync pagelist with tmpfile: all pages unloaded (tmpfile is now empty)
    pagelist.Init(pagelist.Size(), false);
    UpdateDiskUsedTracking();
  }

  // [s3fs-fuse compat] if flush failed and using cache, delete the (possibly corrupt) cache file
  if(0 != result && !cachepath.empty()){
    FdManager::DeleteCacheFile(tpath ? tpath : path.c_str());
  }

  // [s3fs-fuse compat] Serialize pagelist after successful flush.
  // FUSE may call getattr between flush->release, so the pagelist
  // must be persisted here to avoid stale cache state.
  if(0 == result && !cachepath.empty()){
    ino_t cur_inode = GetInode();
    if(0 != cur_inode && cur_inode == inode){
      CacheFileStat cfstat(path.c_str());
      if(!pagelist.Serialize(cfstat, true, inode)){
        S3FS_PRN_WARN("failed to save cache stat file(%s).", path.c_str());
      }
    }
  }

  if(0 == result){
    is_modify = false;
    pagelist.ClearAllModified();
  }
  nocache_fallback_ = false;
  return result;
}

bool FdEntity::ReserveDiskSpace(size_t size)
{
  // First attempt
  if(FdManager::ReserveDiskSpace(size)){
    return true;
  }
  // Second attempt: if file is unmodified, truncate tmpfile to reclaim space
  if(!is_modify){
    pagelist.Init(pagelist.Size(), false);
    if(-1 == ftruncate(fd, 0) || -1 == ftruncate(fd, static_cast<off_t>(pagelist.Size()))){
      S3FS_PRN_ERR("failed to truncate temporary file(%d).", fd);
      return false;
    }
    UpdateDiskUsedTracking();
    if(FdManager::ReserveDiskSpace(size)){
      return true;
    }
  }
  // Third attempt: global cache cleanup then retry
  FdManager::get()->CleanupCacheDir();
  return FdManager::ReserveDiskSpace(size);
}

//
// Async prefetch support (streamread v2: range-based double-buffer)
//
void* FdEntity::PrefetchWorkerEntry(void* arg)
{
  PrefetchArg* pa = static_cast<PrefetchArg*>(arg);
  pa->entity->PrefetchWorker(pa->start, pa->size);
  delete pa;
  return NULL;
}

void FdEntity::PrefetchWorker(off_t start, size_t size)
{
  S3FS_PRN_DBG("[streamread-v2] prefetch start: path=%s offset=%jd size=%zu",
               path.c_str(), (intmax_t)start, size);

  size_t reserved = size;

  // Step 1: Under lock, get unloaded page list and validate
  fdpage_list_t unloaded_list;
  size_t file_orgsize = 0;
  {
    AutoLock auto_lock(&fdent_lock);

    // Check: file may have been closed or truncated
    if(-1 != fd && static_cast<size_t>(start) < pagelist.Size()){
      // Trim size to avoid overrun
      if((static_cast<size_t>(start) + size) > pagelist.Size()){
        size_t new_size = pagelist.Size() - static_cast<size_t>(start);
        if(new_size < reserved){
          FdManager::ReleaseDiskSpace(reserved - new_size);
          reserved = new_size;
        }
        size = new_size;
      }
      file_orgsize = size_orgmeta;
      pagelist.GetUnloadedPages(unloaded_list, start, size);
    }
  }
  // fdent_lock released here

  // Step 2: Download without holding fdent_lock
  // pwrite to different offsets is thread-safe; overlapping writes of identical
  // S3 data to the same offset are idempotent and safe.
  {
    int result = 0;
    for(fdpage_list_t::iterator iter = unloaded_list.begin(); iter != unloaded_list.end(); ++iter){
      if(0 != size && (static_cast<size_t>(start) + size) <= static_cast<size_t>((*iter)->offset)){
        break;
      }

      size_t need_load_size = 0;
      if(static_cast<size_t>((*iter)->offset) < file_orgsize){
        need_load_size = (static_cast<size_t>((*iter)->next()) <= file_orgsize ? (*iter)->bytes : (file_orgsize - (*iter)->offset));
      }
      size_t over_size = (*iter)->bytes - need_load_size;

      // A-3: Check cancellation flag before each download chunk.
      // This allows CancelPrefetch to abort the worker quickly instead of
      // waiting for the current curl download to complete (up to 120s timeout).
      {
        AutoLock plock(&prefetch_mutex);
        if(!prefetch_active){
          S3FS_PRN_DBG("[streamread-v2] prefetch cancelled before download, exiting early");
          break;
        }
      }

      // Check if file was closed during download
      if(-1 == fd){
        S3FS_PRN_WARN("[streamread-v2] prefetch aborted: fd closed during download");
        break;
      }

      if(static_cast<size_t>(2 * S3fsCurl::GetMultipartSize()) <= need_load_size && !nomultipart){
        // parallel request for large ranges
        time_t backup = 0;
        if(120 > S3fsCurl::GetReadwriteTimeout()){
          backup = S3fsCurl::SetReadwriteTimeout(120);
        }
        result = S3fsCurl::ParallelGetObjectRequest(path.c_str(), fd, (*iter)->offset, need_load_size);
        if(0 != backup){
          S3fsCurl::SetReadwriteTimeout(backup);
        }
      }else{
        if(0 < need_load_size){
          S3fsCurl s3fscurl;
          result = s3fscurl.GetObjectRequest(path.c_str(), fd, (*iter)->offset, need_load_size);
        }else{
          result = 0;
        }
      }

      if(0 != result){
        S3FS_PRN_WARN("[streamread-v2] prefetch download failed: errno=%d offset=%jd",
                       result, (intmax_t)(*iter)->offset);
        break;
      }

      // Fill beyond-original-size area with zeros
      if(0 < over_size){
        if(0 != FdEntity::FillFile(fd, 0, over_size, (*iter)->offset + need_load_size)){
          S3FS_PRN_ERR("[streamread-v2] failed to fill rest bytes for fd(%d)", fd);
          break;
        }
      }

      // Step 3: Under lock, update pagelist for this chunk
      {
        AutoLock auto_lock(&fdent_lock);
        if(-1 != fd){
          pagelist.SetPageLoadedStatus((*iter)->offset, static_cast<off_t>((*iter)->bytes), true);
          if(0 < over_size){
            // Zero-filled region beyond S3 original — mark modified for mixmultipart safety
            pagelist.SetPageModifiedStatus((*iter)->offset + need_load_size, over_size, true);
          }
        }
      }
    }
    PageList::FreeList(unloaded_list);
  }

  // Release the disk reservation (actual disk space is now occupied by data,
  // will be reclaimed by PunchRange later)
  FdManager::ReleaseDiskSpace(reserved);

  AutoLock plock(&prefetch_mutex);
  prefetch_active = false;
  S3FS_PRN_DBG("[streamread-v2] prefetch done: path=%s offset=%jd", path.c_str(), (intmax_t)start);
}

void FdEntity::CancelPrefetch(void)
{
  pthread_t tid;
  bool was_active;
  {
    AutoLock plock(&prefetch_mutex);
    was_active = prefetch_active;
    tid = prefetch_thread;
    prefetch_active = false;  // Atomically clear: only the first caller will join
  }
  if(was_active){
    // Must NOT hold fdent_lock here, or prefetch thread waiting for fdent_lock will deadlock
    pthread_join(tid, NULL);
  }
}

void FdEntity::WaitPrefetch(void)
{
  // Semantically identical to CancelPrefetch (both do pthread_join),
  // but used at range boundaries where we expect prefetch to be nearly done.
  CancelPrefetch();
}

void FdEntity::LaunchRangePrefetch(off_t start, size_t size)
{
  if(0 == size){
    return;
  }
  if(!FdManager::ReserveDiskSpace(size)){
    S3FS_PRN_WARN("[streamread-v2] cannot reserve %zu bytes for prefetch at offset %jd",
                   size, (intmax_t)start);
    return;
  }
  {
    AutoLock plock(&prefetch_mutex);
    if(prefetch_active){
      // [ISSUE2026032500001] Already running (concurrent Read from kernel readahead).
      // Skip duplicate launch — one prefetch per range is sufficient.
      FdManager::ReleaseDiskSpace(size);
      return;
    }
    prefetch_start  = start;
    prefetch_size   = size;

    // pthread_create + prefetch_active must be atomic under prefetch_mutex.
    // Otherwise CancelPrefetch can snapshot a stale prefetch_thread value
    // between setting prefetch_active=true and the pthread_create write.
    PrefetchArg* arg = new PrefetchArg{this, start, size};
    int rc = pthread_create(&prefetch_thread, NULL, PrefetchWorkerEntry, arg);
    if(0 != rc){
      S3FS_PRN_ERR("[streamread-v2] failed to create prefetch thread: rc=%d", rc);
      delete arg;
      FdManager::ReleaseDiskSpace(size);
      return;
    }
    prefetch_active = true;  // Set AFTER pthread_create so prefetch_thread is valid
  }
  S3FS_PRN_DBG("[streamread-v2] launched prefetch: offset=%jd size=%zu", (intmax_t)start, size);
}

void FdEntity::PunchRange(off_t range_start, size_t range_size)
{
  // Punch hole for an entire consumed range to reclaim disk space.
  // Only called at range boundaries, not per-Read.
  if(range_start < 0 || 0 == range_size){
    return;
  }
  if(!is_modify){
    if(PunchHole(range_start, range_size)){
      pagelist.SetPageLoadedStatus(range_start, range_size, false);
      S3FS_PRN_DBG("[streamread-v2] punched range [%jd-%jd], reclaimed %zu bytes",
                    (intmax_t)range_start, (intmax_t)(range_start + range_size), range_size);

      // CRC32C: invalidate punched segments (data no longer on disk)
      off_t crc_seg_size = CRC_SEGMENT_SIZE;
      size_t seg_first = static_cast<size_t>(range_start / crc_seg_size);
      size_t seg_last  = static_cast<size_t>((range_start + static_cast<off_t>(range_size) - 1) / crc_seg_size);
      for(size_t seg = seg_first; seg <= seg_last && seg < segment_crcs_.size(); seg++){
        segment_crcs_[seg].valid = false;
      }

      // Update disk usage tracking after punch
      UpdateDiskUsedTracking();
    }
  }
}

size_t FdEntity::GetStreamReadWindow(void)
{
  // Returns window size in bytes. Currently fixed from mount option.
  // Future: dynamic adjustment based on download speed.
  return streamread_window * 1024 * 1024;
}

ssize_t FdEntity::Read(char* bytes, off_t start, size_t size, bool force_load)
{
  S3FS_PRN_DBG("[path=%s][fd=%d][offset=%jd][size=%zu]", path.c_str(), fd, (intmax_t)start, size);

  if(-1 == fd){
    return -EBADF;
  }
  // check if not enough disk space left BEFORE locking fd
  if(FdManager::IsCacheDir() && !FdManager::IsSafeDiskSpace(NULL, size)){
    FdManager::get()->CleanupCacheDir();
  }

  if(streamread && !force_load){
    // ====== STREAMREAD v2: range-based double-buffer fast path ======

    size_t window = GetStreamReadWindow();
    // Align range_start to window boundary: floor(start / window) * window
    off_t cur_range_start = (start / static_cast<off_t>(window)) * static_cast<off_t>(window);

    // === Range switch detection ===
    if(cur_range_start != sr_range_start){
      // Entering a new range
      WaitPrefetch();  // join previous prefetch (should have downloaded this range)

      if(sr_range_start >= 0){
        // Punch the old range to reclaim disk space (needs fdent_lock for pagelist)
        AutoLock auto_lock(&fdent_lock);
        size_t old_range_size = min(window, pagelist.Size() - static_cast<size_t>(sr_range_start > 0 ? sr_range_start : 0));
        PunchRange(sr_range_start, old_range_size);
      }

      sr_range_start   = cur_range_start;
      sr_range_reads   = 0;
      sr_next_launched = false;
    }

    sr_range_reads++;

    // === Ensure user's requested data is available ===
    ssize_t rsize;
    size_t file_size;
    {
      AutoLock auto_lock(&fdent_lock);

      file_size = pagelist.Size();

      // Disk space check — use Reserve/Release for atomicity
      if(0 < pagelist.GetTotalUnloadedPageSize(start, size)){
        if(!FdManager::ReserveDiskSpace(size)){
          if(!is_modify){
            pagelist.Init(file_size, false);
            if(-1 == ftruncate(fd, 0) || -1 == ftruncate(fd, file_size)){
              S3FS_PRN_ERR("failed to truncate temporary file(%d).", fd);
              return -ENOSPC;
            }
          }
        }else{
          FdManager::ReleaseDiskSpace(size);
        }
        // Synchronously load only the user's requested range
        int result;
        if(0 != (result = Load(start, size))){
          S3FS_PRN_ERR("could not download. start(%jd), size(%zu), errno(%d)", (intmax_t)start, size, result);
          return -EIO;
        }
      }

      // pread user data
      if(-1 == (rsize = pread(fd, bytes, size, start))){
        S3FS_PRN_ERR("pread failed. errno(%d)", errno);
        return -errno;
      }
    }
    // fdent_lock released

    // === Prefetch trigger logic (no lock needed) ===
    //
    // Range-based prefetch state machine per range [R, R+W):
    //
    //   Read #1 (sr_range_reads == 1):
    //     - User's 1MB was sync-loaded above. Now launch async prefetch for
    //       the REST of this range: [start+size, R+W). This is a large
    //       contiguous download (e.g., 255MB) that triggers ParallelGetObjectRequest.
    //
    //   Read #2 (sr_range_reads == 2):
    //     - By now, the current range prefetch from Read #1 may be done or nearly done.
    //       Wait for it, then launch async prefetch for the NEXT range [R+W, R+2W).
    //       This is the "double-buffer": while the reader consumes range K, we
    //       pre-download range K+1. sr_next_launched prevents re-triggering.
    //
    //   Read #3, #4, ..., #N (sr_range_reads >= 3):
    //     - No prefetch action. Data for this range was loaded by Read #1's prefetch.
    //       Data for the next range was launched by Read #2. The reader simply does
    //       pread() on already-loaded pages. This is the steady-state fast path:
    //       pure pread with zero HTTP requests and zero thread joins.
    //
    //   Range boundary crossing (next Read falls in range [R+W, R+2W)):
    //     - Detected at the top of this function (cur_range_start != sr_range_start).
    //       WaitPrefetch for next range to complete, PunchRange for old range,
    //       reset counters, and the cycle repeats.
    //
    off_t range_end = min(cur_range_start + static_cast<off_t>(window), static_cast<off_t>(file_size));

    if(1 == sr_range_reads){
      // First Read in range: async prefetch rest of current range
      off_t rest_start = start + static_cast<off_t>(size);
      if(rest_start < range_end){
        size_t rest_size = static_cast<size_t>(range_end - rest_start);
        LaunchRangePrefetch(rest_start, rest_size);
      }
    }else if(2 == sr_range_reads && !sr_next_launched){
      // Second Read in range: wait for current range fill, then prefetch next range
      WaitPrefetch();
      off_t next_start = cur_range_start + static_cast<off_t>(window);
      if(next_start < static_cast<off_t>(file_size)){
        size_t next_size = min(window, file_size - static_cast<size_t>(next_start));
        LaunchRangePrefetch(next_start, next_size);
        sr_next_launched = true;
      }
    }
    // Read #3 and beyond: no prefetch action, just pread from loaded data

    return rsize;

  }else{
    // ====== Original path (unchanged) ======
    AutoLock auto_lock(&fdent_lock);

    if(force_load){
      pagelist.SetPageLoadedStatus(start, size, false);
    }

    int     result;
    ssize_t rsize;

    // check disk space — use Reserve/Release with two-level degradation
    if(0 < pagelist.GetTotalUnloadedPageSize(start, size)){
      // load size(for prefetch)
      size_t load_size = size;
      if((static_cast<size_t>(start) + size) < pagelist.Size()){
        size_t prefetch_max_size = max(size, static_cast<size_t>(S3fsCurl::GetMultipartSize() * S3fsCurl::GetMaxParallelCount()));

        if(static_cast<size_t>(start + prefetch_max_size) < pagelist.Size()){
          load_size = prefetch_max_size;
        }else{
          load_size = static_cast<size_t>(pagelist.Size() - start);
        }
      }
      // Try prefetch load_size → degrade to exact size → truncate+best-effort
      bool disk_reserved = false;
      if(!FdManager::ReserveDiskSpace(load_size)){
        load_size = size;
        if(!FdManager::ReserveDiskSpace(load_size)){
          if(!is_modify){
            pagelist.Init(pagelist.Size(), false);
            if(-1 == ftruncate(fd, 0) || -1 == ftruncate(fd, pagelist.Size())){
              S3FS_PRN_ERR("failed to truncate temporary file(%d).", fd);
              return -ENOSPC;
            }
          }
          // best-effort Load without reservation
        }else{
          disk_reserved = true;
        }
      }else{
        disk_reserved = true;
      }
      if(disk_reserved){
        FdManager::ReleaseDiskSpace(load_size);
      }
      // Loading
      if(0 < size && 0 != (result = Load(start, load_size))){
        S3FS_PRN_ERR("could not download. start(%jd), size(%zu), errno(%d)", (intmax_t)start, size, result);
        return -EIO;
      }
    }
    // Reading
    if(-1 == (rsize = pread(fd, bytes, size, start))){
      S3FS_PRN_ERR("pread failed. fd=%d, errno(%d), path=%s", fd, errno, path.c_str());
      return -errno;
    }
    return rsize;
  }
}

ssize_t FdEntity::Write(const char* bytes, off_t start, size_t size)
{
  S3FS_PRN_DBG("[path=%s][fd=%d][offset=%jd][size=%zu]", path.c_str(), fd, (intmax_t)start, size);

  if(-1 == fd){
    return -EBADF;
  }
  if(streamread){
    CancelPrefetch();
  }
  // check if not enough disk space left BEFORE locking fd
  if(FdManager::IsCacheDir() && !FdManager::IsSafeDiskSpace(NULL, size)){
    FdManager::get()->CleanupCacheDir();
  }
  AutoLock auto_lock(&fdent_lock);

  // check file size
  if(pagelist.Size() < static_cast<size_t>(start)){
    // grow file size (gap write: zero-fill between current end and write start)
    off_t old_size = static_cast<off_t>(pagelist.Size());
    if(-1 == ftruncate(fd, static_cast<size_t>(start))){
      S3FS_PRN_ERR("failed to truncate temporary file(%d).", fd);
      return -EIO;
    }
    // Update boundary segment CRC for the gap (old end → new start)
    UpdateBoundarySegmentCrc(old_size, static_cast<off_t>(start));
    // add new area
    pagelist.SetPageLoadedStatus(static_cast<off_t>(pagelist.Size()), static_cast<size_t>(start) - pagelist.Size(), false);
  }

  int     result;
  ssize_t wsize;

  if(0 == upload_id.length()){
    // check disk space — use entity-level Reserve with retry + cleanup
    // For new files (size_orgmeta==0), no unloaded pages exist — skip expensive pagelist scan
    size_t restsize = (0 < size_orgmeta ? pagelist.GetTotalUnloadedPageSize(0, start) : 0) + size;
    if(ReserveDiskSpace(restsize)){
      FdManager::ReleaseDiskSpace(restsize);
      // enough disk space

      // Load uninitialized area which starts from 0 to (start + size) before writing.
      // Skip for new files (size_orgmeta==0): no data to load from S3
      if(0 < start && 0 < size_orgmeta && 0 != (result = Load(0, static_cast<size_t>(start)))){
        S3FS_PRN_ERR("failed to load uninitialized area before writing(errno=%d)", result);
        return static_cast<ssize_t>(result);
      }
    }else{
      // no enough disk space
      if(0 != (result = NoCachePreMultipartPost())){
        S3FS_PRN_ERR("failed to switch multipart uploading with no cache(errno=%d)", result);
        return static_cast<ssize_t>(result);
      }
      // start multipart uploading
      if(0 != (result = NoCacheLoadAndPost(0, start))){
        S3FS_PRN_ERR("failed to load uninitialized area and multipart uploading it(errno=%d)", result);
        return static_cast<ssize_t>(result);
      }
      untreated_list.ClearAll();
    }
  }else{
    // already start multipart uploading
  }

  // Writing
  if(-1 == (wsize = pwrite(fd, bytes, size, start))){
    S3FS_PRN_ERR("pwrite failed. errno(%d)", errno);
    return -errno;
  }
  if(!is_modify){
    is_modify = true;
  }
  if(0 < wsize){
    pagelist.SetPageLoadedStatus(start, static_cast<size_t>(wsize), true);
    pagelist.SetPageModifiedStatus(start, static_cast<size_t>(wsize), true);

    // CRC32C: update per-segment CRC from user buffer (append) or tmpfile pread (overwrite)
    UpdateCrcOnWrite(bytes, wsize, start, static_cast<off_t>(pagelist.Size()));

    // Update disk usage tracking after write
    UpdateDiskUsedTracking();
  }

  // check multipart uploading
  if(0 < upload_id.length()){
    untreated_list.AddPart(start, static_cast<off_t>(wsize));

    // Check if any part is large enough to upload
    off_t part_start = 0, part_size = 0;
    off_t max_mp = static_cast<off_t>(2 * S3fsCurl::GetMultipartSize());
    off_t min_mp = static_cast<off_t>(S3fsCurl::GetMultipartSize());
    while(untreated_list.GetLastUpdatedPart(part_start, part_size, max_mp, min_mp)){
      if(0 != (result = NoCacheMultipartPost(fd, part_start, static_cast<size_t>(part_size)))){
        S3FS_PRN_ERR("failed to multipart post(start=%jd, size=%jd) for file(%d).",
                     (intmax_t)part_start, (intmax_t)part_size, fd);
        return result;
      }
      // Reclaim disk space first, then clear tracking (aligned with s3fs-fuse)
      // If ftruncate fails, untreated_list is preserved for retry/recovery
      if(-1 == ftruncate(fd, 0) || -1 == ftruncate(fd, static_cast<off_t>(pagelist.Size()))){
        S3FS_PRN_ERR("failed to truncate file(%d).", fd);
        return -EIO;
      }
      untreated_list.ClearParts(part_start, part_size);
    }
  }
  return wsize;
}

bool FdEntity::PunchHole(off_t start, size_t size)
{
#ifdef FALLOC_FL_PUNCH_HOLE
  if(-1 == fd){
    return false;
  }

  struct stat st;
  if(-1 == fstat(fd, &st)){
    S3FS_PRN_WARN("failed to stat fd(%d) for punch hole", fd);
    return false;
  }

  off_t punch_start = start;
  off_t punch_end   = (0 == size) ? st.st_size : min(static_cast<off_t>(start + size), st.st_size);

  // align to 4K page boundary
  punch_start = (punch_start + 4095) & ~static_cast<off_t>(4095);
  punch_end   = punch_end & ~static_cast<off_t>(4095);

  if(punch_start >= punch_end){
    return true;  // region too small to punch
  }

  if(0 != fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                     punch_start, punch_end - punch_start))
  {
    if(EOPNOTSUPP == errno || ENOSYS == errno){
      S3FS_PRN_DBG("punch hole not supported on this filesystem (fd=%d, errno=%d)", fd, errno);
    }else{
      S3FS_PRN_WARN("failed to punch hole [%jd-%jd] for fd(%d), errno=%d",
                     (intmax_t)punch_start, (intmax_t)punch_end, fd, errno);
    }
    return false;
  }

  S3FS_PRN_DBG("[punch hole] fd=%d [%jd-%jd] recovered %jd bytes",
                fd, (intmax_t)punch_start, (intmax_t)punch_end,
                (intmax_t)(punch_end - punch_start));
  return true;
#else
  S3FS_PRN_DBG("FALLOC_FL_PUNCH_HOLE not supported on this platform");
  return false;
#endif
}

// [max_dirty_data]
// Transition from normal write-back mode to multipart upload mode.
// Uploads all data written so far via multipart, then truncates local file to reclaim disk space.
// Subsequent writes accumulate in the local file and are uploaded as multipart parts.
//
// This is the obsfs equivalent of s3fs-fuse's RowFlush+PunchHole cycle.
// s3fs-fuse can use flush+punchhole because its mixmultipart mode uses S3 Copy API
// to reassemble parts without re-downloading. obsfs lacks this, so we transition to
// multipart upload mode instead — each byte is uploaded exactly once.
//
bool FdEntity::TransitionToMultipart(void)
{
  AutoLock auto_lock(&fdent_lock);

  if(-1 == fd){
    return false;
  }
  if(0 < upload_id.length()){
    // already in multipart mode
    return true;
  }

  S3FS_PRN_INFO("[max_dirty_data] transitioning to multipart mode: path=%s, size=%zu",
                 path.c_str(), pagelist.Size());

  int result;

  // Step 1: Start multipart upload
  if(0 != (result = NoCachePreMultipartPost())){
    S3FS_PRN_ERR("[max_dirty_data] failed to start multipart upload(%d) for path=%s", result, path.c_str());
    return false;
  }

  // Step 2: Upload all loaded data via multipart parts
  // NoCacheLoadAndPost iterates pagelist, uploads each chunk, then truncates fd to 0
  if(0 != (result = NoCacheLoadAndPost())){
    S3FS_PRN_ERR("[max_dirty_data] failed to upload existing data(%d) for path=%s", result, path.c_str());
    return false;
  }

  // Step 3: Clear untreated parts tracking.
  // All data from [0, pagelist.Size()) has been uploaded.
  // Future writes will be tracked by untreated_list.
  untreated_list.ClearAll();

  S3FS_PRN_INFO("[max_dirty_data] multipart transition complete: path=%s, pagelist_size=%zu",
                 path.c_str(), pagelist.Size());
  return true;
}

void FdEntity::CleanupCache()
{
  AutoLock auto_lock(&fdent_lock, true);

  if (!auto_lock.isLockAcquired()) {
    return;
  }

  if (is_modify) {
    // cache is not commited to s3, cannot cleanup
    return;
  }

  // Release tracked bytes for this cache file before deleting
  size_t tracked = FdManager::GetCacheFileTrackedBytes(path);
  if(tracked > 0){
    FdManager::AdjustDiskUsed(-static_cast<ssize_t>(tracked));
    FdManager::RemoveCacheFileTrackedBytes(path);
  }
  FdManager::DeleteCacheFile(path.c_str());
}

//------------------------------------------------
// FdManager symbol
//------------------------------------------------
// [NOTE]
// NOCACHE_PATH_PREFIX symbol needs for not using cache mode.
// Now s3fs I/F functions in s3fs.cpp has left the processing
// to FdManager and FdEntity class. FdManager class manages
// the list of local file stat and file descriptor in conjunction
// with the FdEntity class.
// When s3fs is not using local cache, it means FdManager must
// return new temporary file descriptor at each opening it.
// Then FdManager caches fd by key which is dummy file path
// instead of real file path.
// This process may not be complete, but it is easy way can
// be realized.
//
#define NOCACHE_PATH_PREFIX_FORM    " __S3FS_UNEXISTED_PATH_%lx__ / "      // important space words for simply

//------------------------------------------------
// FdManager class variable
//------------------------------------------------
FdManager       FdManager::singleton;
pthread_mutex_t FdManager::fd_manager_lock;
pthread_mutex_t FdManager::cache_cleanup_lock;
std::mutex      FdManager::disk_reservation_lock;
bool            FdManager::is_lock_init(false);
string          FdManager::cache_dir("");
bool            FdManager::check_cache_dir_exist(false);
size_t          FdManager::free_disk_space = 0;
size_t          FdManager::disk_reserved_bytes = 0;
size_t          FdManager::max_tmpdir_disk_usage = 0;
size_t          FdManager::obsfs_disk_used_bytes_ = 0;
std::map<std::string, size_t> FdManager::cache_file_tracked_bytes_;
string          FdManager::tmp_dir("/tmp");
off_t           FdManager::max_dirty_data = 5LL * 1024LL * 1024LL * 1024LL; // default 5GB

//------------------------------------------------
// FdManager class methods
//------------------------------------------------
bool FdManager::SetTmpDir(const char* dir)
{
  if(!dir || '\0' == dir[0]){
    tmp_dir = "/tmp";
  }else{
    tmp_dir = dir;
  }
  return true;
}

bool FdManager::IsDir(const std::string& dir)
{
  struct stat st;
  if(0 != stat(dir.c_str(), &st)){
    S3FS_PRN_ERR("could not stat() directory %s by errno(%d).", dir.c_str(), errno);
    return false;
  }
  if(!S_ISDIR(st.st_mode)){
    S3FS_PRN_ERR("the directory %s is not a directory.", dir.c_str());
    return false;
  }
  return true;
}

bool FdManager::CheckTmpDirExist(void)
{
  if(FdManager::tmp_dir.empty()){
    return true;
  }
  return IsDir(tmp_dir);
}

bool FdManager::IsSupportedFsType(long fs_type)
{
#ifdef __linux__
  // Whitelist: filesystems that support fallocate FALLOC_FL_PUNCH_HOLE.
  // NFS, FAT, NTFS etc. do not support punch hole, which causes disk space
  // to accumulate during sequential read (R-20).
  switch(fs_type){
    case EXT4_SUPER_MAGIC:    // ext2/ext3/ext4 (same magic 0xEF53)
    case XFS_SUPER_MAGIC:     // xfs
    case BTRFS_SUPER_MAGIC:   // btrfs
    case TMPFS_MAGIC:         // tmpfs
      return true;
    default:
      return false;
  }
#else
  (void)fs_type;
  return true;
#endif
}

bool FdManager::IsSupportedFsName(const char* fs_name)
{
#ifdef __APPLE__
  // macOS whitelist: local filesystems with sparse file / hole-punch capability.
  // Reject NFS, SMB, and other network/legacy filesystems.
  static const char* whitelist[] = { "apfs", "hfs", "devfs", "tmpfs", NULL };
  if(!fs_name){
    return false;
  }
  for(const char** p = whitelist; *p; ++p){
    if(0 == strcmp(fs_name, *p)){
      return true;
    }
  }
  return false;
#else
  (void)fs_name;
  return true;
#endif
}

bool FdManager::CheckTmpDirFsType(void)
{
  std::string check_dir = FdManager::tmp_dir.empty() ? "/tmp" : FdManager::tmp_dir;

  struct statfs fsinfo;
  if(0 != statfs(check_dir.c_str(), &fsinfo)){
    S3FS_PRN_ERR("could not statfs() directory %s, errno=%d.", check_dir.c_str(), errno);
    return false;
  }

#ifdef __linux__
  if(!IsSupportedFsType(fsinfo.f_type)){
    S3FS_PRN_EXIT("tmpdir %s is on an unsupported filesystem (type=0x%lx). "
                   "obsfs requires a filesystem that supports fallocate punch hole "
                   "(ext4, xfs, btrfs, or tmpfs). NFS and other network/legacy "
                   "filesystems are not supported.",
                   check_dir.c_str(), (unsigned long)fsinfo.f_type);
    return false;
  }
#elif defined(__APPLE__)
  if(!IsSupportedFsName(fsinfo.f_fstypename)){
    S3FS_PRN_EXIT("tmpdir %s is on an unsupported filesystem (%s). "
                   "obsfs requires a local filesystem (apfs, hfs, or tmpfs). "
                   "NFS and other network filesystems are not supported.",
                   check_dir.c_str(), fsinfo.f_fstypename);
    return false;
  }
#endif
  return true;
}

FILE* FdManager::MakeTempFile(void)
{
  int fd;
  char cfn[PATH_MAX];
  string fn = tmp_dir + "/obsfstmp.XXXXXX";
  strncpy(cfn, fn.c_str(), sizeof(cfn) - 1);
  cfn[sizeof(cfn) - 1] = '\0';

  fd = mkstemp(cfn);
  if(-1 == fd){
    S3FS_PRN_ERR("failed to create tmp file. errno(%d)", errno);
    return NULL;
  }
  if(-1 == unlink(cfn)){
    S3FS_PRN_ERR("failed to delete tmp file. errno(%d)", errno);
    close(fd);
    return NULL;
  }
  return fdopen(fd, "rb+");
}

bool FdManager::SetCacheDir(const char* dir)
{
  if(!dir || '\0' == dir[0]){
    cache_dir = "";
  }else{
    cache_dir = dir;
  }
  return true;
}

bool FdManager::DeleteCacheDirectory(void)
{
  if(0 == FdManager::cache_dir.size()){
    return true;
  }
  string cache_dir;
  if(!FdManager::MakeCachePath(NULL, cache_dir, false)){
    return false;
  }
  return delete_files_in_dir(cache_dir.c_str(), true);
}

int FdManager::DeleteCacheFile(const char* path)
{
  S3FS_PRN_INFO3("[path=%s]", SAFESTRPTR(path));

  if(!path){
    return -EIO;
  }
  if(0 == FdManager::cache_dir.size()){
    return 0;
  }
  string cache_path = "";
  if(!FdManager::MakeCachePath(path, cache_path, false)){
    return 0;
  }
  int result = 0;
  if(0 != unlink(cache_path.c_str())){
    if(ENOENT == errno){
      S3FS_PRN_DBG("failed to delete file(%s): errno=%d", path, errno);
    }else{
      S3FS_PRN_ERR("failed to delete file(%s): errno=%d", path, errno);
    }
    result = -errno;
  }
  if(!CacheFileStat::DeleteCacheFileStat(path)){
    if(ENOENT == errno){
      S3FS_PRN_DBG("failed to delete stat file(%s): errno=%d", path, errno);
    }else{
      S3FS_PRN_ERR("failed to delete stat file(%s): errno=%d", path, errno);
    }
    if(0 != errno){
      result = -errno;
    }else{
      result = -EIO;
    }
  }
  return result;
}

bool FdManager::MakeCachePath(const char* path, string& cache_path, bool is_create_dir, bool is_mirror_path)
{
  if(0 == FdManager::cache_dir.size()){
    cache_path = "";
    return true;
  }

  string resolved_path(FdManager::cache_dir);
  if(!is_mirror_path){
    resolved_path += "/";
    resolved_path += bucket;
  }else{
    resolved_path += "/.";
    resolved_path += bucket;
    resolved_path += ".mirror";
  }

  if(is_create_dir){
    int result;
    if(0 != (result = mkdirp(resolved_path + mydirname(path), 0777))){
      S3FS_PRN_ERR("failed to create dir(%s) by errno(%d).", path, result);
      return false;
    }
  }
  if(!path || '\0' == path[0]){
    cache_path = resolved_path;
  }else{
    cache_path = resolved_path + SAFESTRPTR(path);
  }
  return true;
}

bool FdManager::CheckCacheTopDir(void)
{
  if(0 == FdManager::cache_dir.size()){
    return true;
  }
  string toppath(FdManager::cache_dir + "/" + bucket);

  return check_exist_dir_permission(toppath.c_str());
}

bool FdManager::MakeRandomTempPath(const char* path, string& tmppath)
{
  char szBuff[64];

  sprintf(szBuff, NOCACHE_PATH_PREFIX_FORM, random());     // worry for performance, but maybe don't worry.
  tmppath  = szBuff;
  tmppath += path ? path : "";
  return true;
}

bool FdManager::SetCheckCacheDirExist(bool is_check)
{
  bool old = FdManager::check_cache_dir_exist;
  FdManager::check_cache_dir_exist = is_check;
  return old;
}

bool FdManager::CheckCacheDirExist(void)
{
  if(!FdManager::check_cache_dir_exist){
    return true;
  }
  if(0 == FdManager::cache_dir.size()){
    return true;
  }
  // check the directory
  struct stat st;
  if(0 != stat(cache_dir.c_str(), &st)){
    S3FS_PRN_ERR("could not access to cache directory(%s) by errno(%d).", cache_dir.c_str(), errno);
    return false;
  }
  if(!S_ISDIR(st.st_mode)){
    S3FS_PRN_ERR("the cache directory(%s) is not directory.", cache_dir.c_str());
    return false;
  }
  return true;
}

size_t FdManager::GetEnsureFreeDiskSpace(void)
{
  const std::lock_guard<std::mutex> lock(FdManager::disk_reservation_lock);
  return FdManager::free_disk_space;
}

size_t FdManager::GetReservedDiskSpace(void)
{
  const std::lock_guard<std::mutex> lock(FdManager::disk_reservation_lock);
  return FdManager::disk_reserved_bytes;
}

size_t FdManager::SetEnsureFreeDiskSpace(size_t size)
{
  const std::lock_guard<std::mutex> lock(FdManager::disk_reservation_lock);
  size_t old = FdManager::free_disk_space;
  if(0 == size){
    if(0 == FdManager::free_disk_space){
      FdManager::free_disk_space = static_cast<size_t>(S3fsCurl::GetMultipartSize() * S3fsCurl::GetMaxParallelCount());
    }
  }else{
    if(0 == FdManager::free_disk_space){
      FdManager::free_disk_space = max(size, static_cast<size_t>(S3fsCurl::GetMultipartSize() * S3fsCurl::GetMaxParallelCount()));
    }else{
      if(static_cast<size_t>(S3fsCurl::GetMultipartSize() * S3fsCurl::GetMaxParallelCount()) <= size){
        FdManager::free_disk_space = size;
      }
    }
  }
  return old;
}

// [HasLock] — caller must hold disk_reservation_lock
uint64_t FdManager::GetFreeDiskSpaceHasLock(const char* path)
{
  struct statvfs vfsbuf;
  string         ctoppath;
  if(0 < FdManager::cache_dir.size()){
    ctoppath = FdManager::cache_dir + "/";
    ctoppath = get_exist_directory_path(ctoppath);	// existed directory
    if(ctoppath != "/"){
      ctoppath += "/";
    }
  }else{
    ctoppath = tmp_dir + "/";
  }
  if(path && '\0' != *path){
    ctoppath += path;
  }else{
    ctoppath += ".";
  }
  if(-1 == statvfs(ctoppath.c_str(), &vfsbuf)){
    S3FS_PRN_ERR("could not get vfs stat by errno(%d)", errno);
    return 0;
  }
  uint64_t actual = vfsbuf.f_bavail * vfsbuf.f_frsize;

  if(max_tmpdir_disk_usage > 0){
    uint64_t remaining = (static_cast<uint64_t>(max_tmpdir_disk_usage) > static_cast<uint64_t>(obsfs_disk_used_bytes_)) ?
                          (static_cast<uint64_t>(max_tmpdir_disk_usage) - static_cast<uint64_t>(obsfs_disk_used_bytes_)) : 0;
    return std::min(actual, remaining);
  }
  return actual;
}

uint64_t FdManager::GetFreeDiskSpace(const char* path)
{
  const std::lock_guard<std::mutex> lock(FdManager::disk_reservation_lock);
  return GetFreeDiskSpaceHasLock(path);
}

// [HasLock] — caller must hold disk_reservation_lock
// Core fix: accounts for disk_reserved_bytes so concurrent threads
// cannot all pass with the same statvfs snapshot.
//
bool FdManager::IsSafeDiskSpaceHasLock(const char* path, size_t size)
{
  uint64_t fsize = GetFreeDiskSpaceHasLock(path);
  size_t needsize = size + free_disk_space + disk_reserved_bytes;
  return (needsize <= fsize);
}

bool FdManager::IsSafeDiskSpace(const char* path, size_t size, bool withmsg)
{
  const std::lock_guard<std::mutex> lock(FdManager::disk_reservation_lock);
  bool safe = IsSafeDiskSpaceHasLock(path, size);
  if(!safe && withmsg){
    uint64_t fsize = GetFreeDiskSpaceHasLock(path);
    S3FS_PRN_ERR("Not enough disk space. need=%.3f MB, avail=%.3f MB, reserved=%.3f MB, ensure=%.3f MB",
      (double)size/1024/1024, (double)fsize/1024/1024,
      (double)disk_reserved_bytes/1024/1024, (double)free_disk_space/1024/1024);
  }
  return safe;
}

bool FdManager::ReserveDiskSpace(size_t size)
{
  const std::lock_guard<std::mutex> lock(FdManager::disk_reservation_lock);
  if(!IsSafeDiskSpaceHasLock(NULL, size)){
    S3FS_PRN_DBG("[disk_reserve] denied: request=%zu reserved=%zu ensure=%zu",
                  size, disk_reserved_bytes, free_disk_space);
    return false;
  }
  disk_reserved_bytes += size;
  S3FS_PRN_DBG("[disk_reserve] granted: request=%zu total_reserved=%zu",
                size, disk_reserved_bytes);
  return true;
}

void FdManager::ReleaseDiskSpace(size_t size)
{
  const std::lock_guard<std::mutex> lock(FdManager::disk_reservation_lock);
  if(size > disk_reserved_bytes){
    S3FS_PRN_WARN("[disk_reserve] release(%zu) > reserved(%zu), clamping", size, disk_reserved_bytes);
    disk_reserved_bytes = 0;
  }else{
    disk_reserved_bytes -= size;
  }
  S3FS_PRN_DBG("[disk_reserve] released: size=%zu total_reserved=%zu",
                size, disk_reserved_bytes);
}

void FdManager::AdjustDiskUsed(ssize_t delta)
{
  const std::lock_guard<std::mutex> lock(FdManager::disk_reservation_lock);
  if(delta >= 0){
    obsfs_disk_used_bytes_ += static_cast<size_t>(delta);
  }else{
    size_t decrease = static_cast<size_t>(-delta);
    if(decrease > obsfs_disk_used_bytes_){
      S3FS_PRN_WARN("[disk_used] decrease(%zu) > used(%zu), clamping to 0", decrease, obsfs_disk_used_bytes_);
      obsfs_disk_used_bytes_ = 0;
    }else{
      obsfs_disk_used_bytes_ -= decrease;
    }
  }
}

void FdManager::SetCacheFileTrackedBytes(const std::string& path, size_t bytes)
{
  const std::lock_guard<std::mutex> lock(FdManager::disk_reservation_lock);
  cache_file_tracked_bytes_[path] = bytes;
}

size_t FdManager::GetCacheFileTrackedBytes(const std::string& path)
{
  const std::lock_guard<std::mutex> lock(FdManager::disk_reservation_lock);
  std::map<std::string, size_t>::const_iterator it = cache_file_tracked_bytes_.find(path);
  return (it != cache_file_tracked_bytes_.end()) ? it->second : 0;
}

void FdManager::RemoveCacheFileTrackedBytes(const std::string& path)
{
  const std::lock_guard<std::mutex> lock(FdManager::disk_reservation_lock);
  cache_file_tracked_bytes_.erase(path);
}

void FdManager::SetMaxTmpdirDiskUsage(size_t bytes)
{
  const std::lock_guard<std::mutex> lock(FdManager::disk_reservation_lock);
  max_tmpdir_disk_usage = bytes;
}

size_t FdManager::GetMaxTmpdirDiskUsage(void)
{
  const std::lock_guard<std::mutex> lock(FdManager::disk_reservation_lock);
  return max_tmpdir_disk_usage;
}

//------------------------------------------------
// FdManager methods
//------------------------------------------------
FdManager::FdManager()
{
  if(this == FdManager::get()){
    try{
      pthread_mutex_init(&FdManager::fd_manager_lock, NULL);
      pthread_mutex_init(&FdManager::cache_cleanup_lock, NULL);
      FdManager::is_lock_init = true;
    }catch(exception& e){
      FdManager::is_lock_init = false;
      S3FS_PRN_CRIT("failed to init mutex");
    }
  }else{
    assert(false);
  }
}

FdManager::~FdManager()
{
  if(this == FdManager::get()){
    for(fdent_map_t::iterator iter = fent.begin(); fent.end() != iter; ++iter){
      FdEntity* ent = (*iter).second;
      delete ent;
    }
    fent.clear();

    if(FdManager::is_lock_init){
      try{
        pthread_mutex_destroy(&FdManager::fd_manager_lock);
        pthread_mutex_destroy(&FdManager::cache_cleanup_lock);
      }catch(exception& e){
        S3FS_PRN_CRIT("failed to init mutex");
      }
      FdManager::is_lock_init = false;
    }
  }else{
    assert(false);
  }
}

FdEntity* FdManager::GetFdEntity(const char* path, int existfd)
{
  S3FS_PRN_INFO3("[path=%s][fd=%d]", SAFESTRPTR(path), existfd);

  if(!path || '\0' == path[0]){
    return NULL;
  }
  AutoLock auto_lock(&FdManager::fd_manager_lock);

  fdent_map_t::iterator iter = fent.find(string(path));
  if(fent.end() != iter && (-1 == existfd || (*iter).second->GetFd() == existfd)){
    return (*iter).second;
  }

  if(-1 != existfd){
    for(iter = fent.begin(); iter != fent.end(); ++iter){
      if((*iter).second && (*iter).second->GetFd() == existfd){
        // found opened fd in map
        if(0 == strcmp((*iter).second->GetPath(), path)){
          return (*iter).second;
        }
        break;
      }
    }
  }
  return NULL;
}

//
// [FIX] Atomically find entity AND increment refcnt under fd_manager_lock.
// Prevents use-after-free: without this, a concurrent FUSE RELEASE can delete
// the entity between GetFdEntity returning and the caller using the pointer.
// Caller MUST call FdManager::Close(ent) when done.
//
FdEntity* FdManager::GetFdEntityWithDup(const char* path, int existfd)
{
  AutoLock auto_lock(&FdManager::fd_manager_lock);

  fdent_map_t::iterator iter = fent.find(string(path));
  FdEntity* ent = NULL;
  if(fent.end() != iter && (-1 == existfd || (*iter).second->GetFd() == existfd)){
    ent = (*iter).second;
  }else if(-1 != existfd){
    for(iter = fent.begin(); iter != fent.end(); ++iter){
      if((*iter).second && (*iter).second->GetFd() == existfd){
        if(0 == strcmp((*iter).second->GetPath(), path)){
          ent = (*iter).second;
        }
        break;
      }
    }
  }
  if(ent){
    int dupfd = ent->Dup();
    if(dupfd < 0){
      S3FS_PRN_ERR("GetFdEntityWithDup: Dup failed for path=%s", SAFESTRPTR(path));
      return NULL;
    }
  }
  return ent;
}

//
// Atomically look up an open FdEntity and read its st_size under fd_manager_lock.
// No pointer is exposed to the caller, no refcnt change. Safe for getattr hot path.
// Lock order: fd_manager_lock -> fdent_lock (via GetStats), same as FdManager::Open.
// fstat() inside GetStats reads kernel inode metadata only (microsecond-level on local FS).
//
bool FdManager::GetOpenEntitySize(const char* path, off_t* size)
{
  if(!path || '\0' == path[0] || !size){
    return false;
  }
  AutoLock auto_lock(&FdManager::fd_manager_lock);

  // Try direct path lookup first (cache mode: key == path)
  FdEntity* ent = NULL;
  fdent_map_t::iterator iter = fent.find(string(path));
  if(fent.end() != iter && iter->second){
    ent = iter->second;
  } else {
    // No-cache mode: key is random temp path, scan by entity's stored path
    for(iter = fent.begin(); iter != fent.end(); ++iter){
      if(iter->second && 0 == strcmp(iter->second->GetPath(), path)){
        ent = iter->second;
        break;
      }
    }
  }
  if(!ent || !ent->IsOpen()){
    return false;
  }
  struct stat st;
  if(!ent->GetStats(st)){
    return false;
  }
  *size = st.st_size;
  return true;
}

FdEntity* FdManager::Open(const char* path, headers_t* pmeta, ssize_t size, time_t time, bool force_tmpfile, bool is_create, bool no_fd_lock_wait)
{
  S3FS_PRN_DBG("[path=%s][size=%jd][time=%jd]", SAFESTRPTR(path), (intmax_t)size, (intmax_t)time);

  if(!path || '\0' == path[0]){
    return NULL;
  }
  AutoLock auto_lock(&FdManager::fd_manager_lock);

  // search in mapping by key(path)
  fdent_map_t::iterator iter = fent.find(string(path));

  if(fent.end() == iter && !force_tmpfile && !FdManager::IsCacheDir()){
    // If the cache directory is not specified, s3fs opens a temporary file
    // when the file is opened.
    // Then if it could not find a entity in map for the file, s3fs should
    // search a entity in all which opened the temporary file.
    //
    for(iter = fent.begin(); iter != fent.end(); ++iter){
      if((*iter).second && (*iter).second->IsOpen() && 0 == strcmp((*iter).second->GetPath(), path)){
        break;      // found opened fd in mapping
      }
    }
  }

  FdEntity* ent;
  if(fent.end() != iter){
    // found
    ent = (*iter).second;

  }else if(is_create){
    // not found
    string cache_path = "";
    if(!force_tmpfile && !FdManager::MakeCachePath(path, cache_path, true)){
      S3FS_PRN_ERR("failed to make cache path for object(%s).", path);
      return NULL;
    }
    // make new obj
    ent = new FdEntity(path, cache_path.c_str());

    if(0 < cache_path.size()){
      // using cache
      fent[string(path)] = ent;
    }else{
      // not using cache, so the key of fdentity is set not really existing path.
      // (but not strictly unexisting path.)
      //
      // [NOTE]
      // The reason why this process here, please look at the definition of the
      // comments of NOCACHE_PATH_PREFIX_FORM symbol.
      //
      string tmppath("");
      FdManager::MakeRandomTempPath(path, tmppath);
      fent[tmppath] = ent;
    }
  }else{
    return NULL;
  }

  // open
  if(0 != ent->Open(pmeta, size, time, no_fd_lock_wait)){
    // Open failed: remove from fent map and delete to prevent leak
    for(fdent_map_t::iterator iter = fent.begin(); iter != fent.end(); ++iter){
      if(iter->second == ent){
        fent.erase(iter);
        break;
      }
    }
    delete ent;
    return NULL;
  }
  return ent;
}

FdEntity* FdManager::ExistOpen(const char* path, int existfd, bool ignore_existfd)
{
  S3FS_PRN_DBG("[path=%s][fd=%d][ignore_existfd=%s]", SAFESTRPTR(path), existfd, ignore_existfd ? "true" : "false");

  // search by real path
  FdEntity* ent = Open(path, NULL, -1, -1, false, false);

  if(!ent && (ignore_existfd || (-1 != existfd))){
    // search from all fdentity because of not using cache.
    AutoLock auto_lock(&FdManager::fd_manager_lock);

    for(fdent_map_t::iterator iter = fent.begin(); iter != fent.end(); ++iter){
      if((*iter).second && (*iter).second->IsOpen() && (ignore_existfd || ((*iter).second->GetFd() == existfd))){
        // found opened fd in map
        if(0 == strcmp((*iter).second->GetPath(), path)){
          ent = (*iter).second;
          ent->Dup();
        }else{
          // found fd, but it is used another file(file descriptor is recycled)
          // so returns NULL.
        }
        break;
      }
    }
  }
  return ent;
}

// [FIX ISSUE2026030200003/4] RenamePath: atomically rename disk cache file,
// stat file, and update internal path/cachepath members.
// Reference: s3fs-fuse fdcache_entity.cpp:691-733
bool FdEntity::RenamePath(const std::string& newpath, std::string& fentmapkey)
{
  AutoLock auto_lock(&fdent_lock);

  if(!cachepath.empty()){
    // Cache mode: rename disk cache file + stat file
    string newcachepath;
    if(!FdManager::MakeCachePath(newpath.c_str(), newcachepath, true)){
      S3FS_PRN_ERR("failed to make cache path for object(%s).", newpath.c_str());
      return false;
    }
    if(-1 == rename(cachepath.c_str(), newcachepath.c_str())){
      S3FS_PRN_ERR("failed to rename cache file(%s to %s) errno(%d).",
                   cachepath.c_str(), newcachepath.c_str(), errno);
      return false;
    }
    if(!CacheFileStat::RenameCacheFileStat(path.c_str(), newpath.c_str())){
      S3FS_PRN_WARN("failed to rename cache stat file(%s to %s).", path.c_str(), newpath.c_str());
      // Non-fatal: stat file is auxiliary, continue
    }
    // Transfer disk tracking map entry to new path
    size_t tracked = FdManager::GetCacheFileTrackedBytes(path);
    if(tracked > 0){
      FdManager::SetCacheFileTrackedBytes(newpath, tracked);
      FdManager::RemoveCacheFileTrackedBytes(path);
    }
    fentmapkey = newpath;
    cachepath  = newcachepath;
  }else{
    // No-cache mode: generate new random temp key
    fentmapkey.clear();
    FdManager::MakeRandomTempPath(newpath.c_str(), fentmapkey);
  }
  path = newpath;
  return true;
}

void FdManager::Rename(const std::string &from, const std::string &to)
{
  AutoLock auto_lock(&FdManager::fd_manager_lock);

  // Evict any existing destination FdEntity (stale content from overwrite case)
  fdent_map_t::iterator to_iter = fent.find(to);
  // [FIX] No-cache fallback for destination eviction
  if(fent.end() == to_iter && !FdManager::IsCacheDir()){
    for(to_iter = fent.begin(); to_iter != fent.end(); ++to_iter){
      if((*to_iter).second && (*to_iter).second->IsOpen()
         && 0 == strcmp((*to_iter).second->GetPath(), to.c_str())){
        break;
      }
    }
  }
  if(fent.end() != to_iter){
    S3FS_PRN_DBG("Evicting stale destination FdEntity [to=%s]", to.c_str());
    FdEntity* to_ent = (*to_iter).second;
    fent.erase(to_iter);
    if(to_ent && !to_ent->IsOpen()){
      delete to_ent;
    }
  }

  // Find source entity
  fdent_map_t::iterator iter = fent.find(from);
  // [FIX ISSUE2026030200002] No-cache fallback: entity stored under random
  // temp key, fent.find(real_path) misses. Linear search by GetPath().
  if(fent.end() == iter && !FdManager::IsCacheDir()){
    for(iter = fent.begin(); iter != fent.end(); ++iter){
      if((*iter).second && (*iter).second->IsOpen()
         && 0 == strcmp((*iter).second->GetPath(), from.c_str())){
        break;
      }
    }
  }

  if(fent.end() != iter){
    S3FS_PRN_DBG("[from=%s][to=%s]", from.c_str(), to.c_str());
    FdEntity* ent = (*iter).second;
    string oldkey = (*iter).first;
    fent.erase(iter);

    // [FIX ISSUE2026030200003/4] RenamePath: rename cache file + stat file + update path
    string fentmapkey;
    if(!ent->RenamePath(to, fentmapkey)){
      S3FS_PRN_ERR("Failed to rename FdEntity [from=%s][to=%s]", from.c_str(), to.c_str());
      fent[oldkey] = ent;  // Re-insert under original key
      return;
    }
    fent[fentmapkey] = ent;
  }
}

// Close: full close for release path only.
// Decrements refcnt; if refcnt reaches 0, cancels prefetch (outside lock)
// and deletes the entity. CancelPrefetch + pthread_join happen OUTSIDE
// fd_manager_lock to avoid blocking all other FUSE operations.
//
// WARNING: Only use this in the release path (file truly being closed).
// For read/write paths, use CloseRef() instead — it only decrements refcnt
// without cancelling prefetch. Misusing Close() in read/write paths kills
// the prefetch pipeline and reduces read throughput to near zero.
// See ISSUE2026031900002.
bool FdManager::Close(FdEntity* ent)
{
  S3FS_PRN_DBG("[ent->file=%s][ent->fd=%d]", ent ? ent->GetPath() : "", ent ? ent->GetFd() : -1);

  if(!ent){
    return true;  // returns success
  }

  bool found = false;
  bool need_cancel = false;
  bool need_delete = false;

  // Step 1: Under lock — decrement refcnt and conditionally remove from map
  {
    AutoLock auto_lock(&FdManager::fd_manager_lock);

    for(fdent_map_t::iterator iter = fent.begin(); iter != fent.end(); ++iter){
      if((*iter).second == ent){
        found = true;
        bool refcnt_zero = ent->CloseRefAndCheck();   // refcnt-- only, no CancelPrefetch
        if(refcnt_zero){
          // refcnt reached 0: remove from map, will cancel + delete outside lock
          need_cancel = true;
          need_delete = true;
          fent.erase(iter++);

          // check another key name for entity value to be on the safe side
          for(; iter != fent.end(); ){
            if((*iter).second == ent){
              fent.erase(iter++);
            }else{
              ++iter;
            }
          }
        }
        break;
      }
    }
  }
  // fd_manager_lock released — other FUSE ops can proceed

  // Step 2: Outside lock — cancel prefetch (may block on pthread_join)
  // and do final file cleanup. Entity is already removed from map,
  // so no other thread can obtain a new reference to it.
  if(need_cancel){
    ent->CancelPrefetch();
  }
  if(need_delete){
    ent->CloseCleanup();
    delete ent;
  }
  return found;
}

// CloseRef: lightweight release for read/write paths.
// Only decrements refcnt via FdEntity::CloseRef(). Does NOT cancel prefetch,
// does NOT acquire fd_manager_lock (safe because caller holds a Dup'd reference,
// guaranteeing refcnt >= 2 and the entity cannot be deleted by another thread).
// See ISSUE2026031900002.
bool FdManager::CloseRef(FdEntity* ent)
{
  S3FS_PRN_DBG("[ent->file=%s][ent->fd=%d]", ent ? ent->GetPath() : "", ent ? ent->GetFd() : -1);

  if(!ent){
    return true;
  }
  ent->CloseRef();
  return true;
}

bool FdManager::ChangeEntityToTempPath(FdEntity* ent, const char* path)
{
  AutoLock auto_lock(&FdManager::fd_manager_lock);

  for(fdent_map_t::iterator iter = fent.begin(); iter != fent.end(); ){
    if((*iter).second == ent){
      fent.erase(iter++);

      string tmppath("");
      FdManager::MakeRandomTempPath(path, tmppath);
      fent[tmppath] = ent;
    }else{
      ++iter;
    }
  }
  return false;
}

void FdManager::CleanupCacheDir()
{
  if (!FdManager::IsCacheDir()) {
    return;
  }

  AutoLock auto_lock(&FdManager::cache_cleanup_lock, true);

  if (!auto_lock.isLockAcquired()) {
    return;
  }

  CleanupCacheDirInternal("");
}

void FdManager::CleanupCacheDirInternal(const std::string &path)
{
  DIR*           dp;
  struct dirent* dent;
  std::string    abs_path = cache_dir + "/" + bucket + path;

  if(NULL == (dp = opendir(abs_path.c_str()))){
    S3FS_PRN_ERR("could not open cache dir(%s) - errno(%d)", abs_path.c_str(), errno);
    return;
  }

  for(dent = readdir(dp); dent; dent = readdir(dp)){
    if(0 == strcmp(dent->d_name, "..") || 0 == strcmp(dent->d_name, ".")){
      continue;
    }
    string   fullpath = abs_path;
    fullpath         += "/";
    fullpath         += dent->d_name;
    struct stat st;
    if(0 != lstat(fullpath.c_str(), &st)){
      S3FS_PRN_ERR("could not get stats of file(%s) - errno(%d)", fullpath.c_str(), errno);
      closedir(dp);
      return;
    }
    string next_path = path + "/" + dent->d_name;
    if(S_ISDIR(st.st_mode)){
      CleanupCacheDirInternal(next_path);
    }else{
      FdEntity* ent;
      if(NULL == (ent = FdManager::get()->Open(next_path.c_str(), NULL, -1, -1, false, true, true))){
        continue;
      }

      ent->CleanupCache();
      Close(ent);
    }
  }
  closedir(dp);
}

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: noet sw=4 ts=4 fdm=marker
* vim<600: noet sw=4 ts=4
*/
