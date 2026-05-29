/*
 * s3fs - FUSE-based file system backed by Amazon S3
 *
 * Copyright(C) 2007 Randy Rizun <rrizun@gmail.com>
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
#include <sys/stat.h>
#include <sys/types.h>
#ifndef HAVE_CLOCK_GETTIME
#include <sys/time.h>
#endif
#include <unistd.h>
#include <stdint.h>
#include <mutex>
#include <string.h>
#include <assert.h>
#include <string>
#include <map>
#include <vector>
#include <algorithm>
#include <list>

#include "cache.h"
#include "s3fs.h"
#include "s3fs_util.h"
#include "string_util.h"

using namespace std;

//-------------------------------------------------------------------
// Utility
//-------------------------------------------------------------------
#ifndef CLOCK_REALTIME
#define CLOCK_REALTIME          0
#endif
#ifndef CLOCK_MONOTONIC
#define CLOCK_MONOTONIC         CLOCK_REALTIME
#endif
#ifndef CLOCK_MONOTONIC_COARSE
#define CLOCK_MONOTONIC_COARSE  CLOCK_MONOTONIC
#endif

#ifdef HAVE_CLOCK_GETTIME
static int s3fs_clock_gettime(int clk_id, struct timespec* ts)
{
  return clock_gettime(static_cast<clockid_t>(clk_id), ts);
}
#else
static int s3fs_clock_gettime(int clk_id, struct timespec* ts)
{
  struct timeval now;
  if(0 != gettimeofday(&now, NULL)){
    return -1;
  }
  ts->tv_sec  = now.tv_sec;
  ts->tv_nsec = now.tv_usec * 1000;
  return 0;
}
#endif

inline void SetStatCacheTime(struct timespec& ts)
{
  if(-1 == s3fs_clock_gettime(CLOCK_MONOTONIC_COARSE, &ts)){
    ts.tv_sec  = time(NULL);
    ts.tv_nsec = 0;
  }
}

inline void InitStatCacheTime(struct timespec& ts)
{
  ts.tv_sec  = 0;
  ts.tv_nsec = 0;
}

inline int CompareStatCacheTime(struct timespec& ts1, struct timespec& ts2)
{
  // return -1:  ts1 < ts2
  //         0:  ts1 == ts2
  //         1:  ts1 > ts2
  if(ts1.tv_sec < ts2.tv_sec){
    return -1;
  }else if(ts1.tv_sec > ts2.tv_sec){
    return 1;
  }else{
    if(ts1.tv_nsec < ts2.tv_nsec){
      return -1;
    }else if(ts1.tv_nsec > ts2.tv_nsec){
      return 1;
    }
  }
  return 0;
}

inline bool IsExpireStatCacheTime(const struct timespec& ts, const time_t& expire)
{
  struct timespec nowts;
  SetStatCacheTime(nowts);
  return ((ts.tv_sec + expire) < nowts.tv_sec);
}

//
// For cache out 
//
typedef std::vector<stat_cache_t::iterator>   statiterlist_t;

struct sort_statiterlist{
  // ascending order
  bool operator()(const stat_cache_t::iterator& src1, const stat_cache_t::iterator& src2) const
  {
    int result = CompareStatCacheTime(src1->second->cache_date, src2->second->cache_date);
    if(0 == result){
      if(src1->second->hit_count < src2->second->hit_count){
        result = -1;
      }
    }
    return (result < 0);
  }
};

//-------------------------------------------------------------------
// Static
//-------------------------------------------------------------------
StatCache       StatCache::singleton;
std::mutex      StatCache::stat_cache_lock;

//-------------------------------------------------------------------
// Constructor/Destructor
//-------------------------------------------------------------------
StatCache::StatCache() : IsExpireTime(false), IsExpireIntervalType(false), ExpireTime(0), CacheSize(1000), IsCacheNoObject(false)
{
  if(this == StatCache::getStatCacheData()){
    stat_cache.clear();
  }else{
    assert(false);
  }
}

StatCache::~StatCache()
{
  if(this == StatCache::getStatCacheData()){
    Clear();
  }else{
    assert(false);
  }
}

//-------------------------------------------------------------------
// Methods
//-------------------------------------------------------------------
unsigned long StatCache::GetCacheSize(void) const
{
  return CacheSize;
}

unsigned long StatCache::SetCacheSize(unsigned long size)
{
  unsigned long old = CacheSize;
  CacheSize = size;
  return old;
}

time_t StatCache::GetExpireTime(void) const
{
  return (IsExpireTime ? ExpireTime : (-1));
}

time_t StatCache::SetExpireTime(time_t expire, bool is_interval)
{
  time_t old           = ExpireTime;
  ExpireTime           = expire;
  IsExpireTime         = true;
  IsExpireIntervalType = is_interval;
  return old;
}

time_t StatCache::UnsetExpireTime(void)
{
  time_t old           = IsExpireTime ? ExpireTime : (-1);
  ExpireTime           = 0;
  IsExpireTime         = false;
  IsExpireIntervalType = false;
  return old;
}

bool StatCache::SetCacheNoObject(bool flag)
{
  bool old = IsCacheNoObject;
  IsCacheNoObject = flag;
  return old;
}

void StatCache::Clear(void)
{
  const std::lock_guard<std::mutex> lock(StatCache::stat_cache_lock);

  for(stat_cache_t::iterator iter = stat_cache.begin(); iter != stat_cache.end(); stat_cache.erase(iter++)){
    if((*iter).second){
      delete (*iter).second;
    }
  }
  S3FS_MALLOCTRIM(0);
}

bool StatCache::GetStat(string& key, struct stat* pst, headers_t* meta, bool overcheck, const char* petag, bool* pisforce)
{
  bool is_delete_cache = false;
  string strpath = key;

  const std::lock_guard<std::mutex> lock(StatCache::stat_cache_lock);

  stat_cache_t::iterator iter = stat_cache.end();
  if(overcheck && '/' != strpath[strpath.length() - 1]){
    strpath += "/";
    iter = stat_cache.find(strpath.c_str());
  }
  if(iter == stat_cache.end()){
    strpath = key;
    iter = stat_cache.find(strpath.c_str());
  }

  if(iter != stat_cache.end() && (*iter).second){
    stat_cache_entry* ent = (*iter).second;
    if(!IsExpireTime || !IsExpireStatCacheTime(ent->cache_date, ExpireTime)){
      if(ent->noobjcache){
        if(!IsCacheNoObject){
          // need to delete this cache.
          DelStatHasLock(strpath.c_str());
        }
        return false;
      }
      // hit without checking etag
      string stretag;
      if(petag){
        // find & check ETag
        for(headers_t::iterator it = ent->meta.begin(); it != ent->meta.end(); ++it){
          string tag = lower(it->first);
          if(tag == "etag"){
            stretag = it->second;
            if('\0' != petag[0] && 0 != strcmp(petag, stretag.c_str())){
              is_delete_cache = true;
            }
            break;
          }
        }
      }
      if(is_delete_cache){
        // not hit by different ETag
        S3FS_PRN_DBG("stat cache not hit by ETag[path=%s][time=%jd.%09ld][hit count=%lu][ETag(%s)!=(%s)]",
          strpath.c_str(), (intmax_t)(ent->cache_date.tv_sec), ent->cache_date.tv_nsec, ent->hit_count, petag ? petag : "null", stretag.c_str());
      }else{
        // hit
        S3FS_PRN_DBG("stat cache hit [path=%s][time=%jd.%09ld][hit count=%lu]",
          strpath.c_str(), (intmax_t)(ent->cache_date.tv_sec), ent->cache_date.tv_nsec, ent->hit_count);

        if(pst!= NULL){
          *pst= ent->stbuf;
        }
        if(meta != NULL){
          *meta = ent->meta;
        }
        if(pisforce != NULL){
          (*pisforce) = ent->isforce;
        }
        ent->hit_count++;

        if(IsExpireIntervalType){
          SetStatCacheTime(ent->cache_date);
        }
        return true;
      }

    }else{
      // timeout
      is_delete_cache = true;
    }
  }

  if(is_delete_cache){
    DelStatHasLock(strpath.c_str());
  }
  return false;
}

bool StatCache::IsNoObjectCache(string& key, bool overcheck)
{
  bool is_delete_cache = false;
  string strpath = key;

  if(!IsCacheNoObject){
    return false;
  }

  const std::lock_guard<std::mutex> lock(StatCache::stat_cache_lock);

  stat_cache_t::iterator iter = stat_cache.end();
  if(overcheck && '/' != strpath[strpath.length() - 1]){
    strpath += "/";
    iter = stat_cache.find(strpath.c_str());
  }
  if(iter == stat_cache.end()){
    strpath = key;
    iter = stat_cache.find(strpath.c_str());
  }

  if(iter != stat_cache.end() && (*iter).second) {
    if(!IsExpireTime || !IsExpireStatCacheTime((*iter).second->cache_date, ExpireTime)){
      if((*iter).second->noobjcache){
        // noobjcache = true means no object.
        SetStatCacheTime((*iter).second->cache_date);
        return true;
      }
    }else{
      // timeout
      is_delete_cache = true;
    }
  }

  if(is_delete_cache){
    DelStatHasLock(strpath.c_str());
  }
  return false;
}

bool StatCache::AddStatHasLock(std::string& key, headers_t& meta, bool forcedir, bool no_truncate)
{
  bool found = stat_cache.end() != stat_cache.find(key);
  if(found){
    DelStatHasLock(key.c_str());
  }else if(stat_cache.size() > CacheSize){
    TruncateCacheHasLock();
  }

  // make new
  stat_cache_entry* ent = new stat_cache_entry();
  if(!convert_header_to_stat(key.c_str(), meta, &(ent->stbuf), forcedir)){
    delete ent;
    return false;
  }
  ent->hit_count  = 0;
  ent->isforce    = forcedir;
  ent->noobjcache = false;
  ent->notruncate = (no_truncate ? 1L : 0L);
  ent->meta.clear();
  SetStatCacheTime(ent->cache_date);    // Set time.
  //copy only some keys
  for(headers_t::iterator iter = meta.begin(); iter != meta.end(); ++iter){
    string tag   = lower(iter->first);
    string value = iter->second;
    if(tag == "content-type"){
      ent->meta[iter->first] = value;
    }else if(tag == Contentlength){
      ent->meta[iter->first] = value;
    }else if(tag == "etag"){
      ent->meta[iter->first] = value;
    }else if(tag == "last-modified"){
      ent->meta[iter->first] = value;
    }else if(tag.substr(0, 5) == "x-amz"){
      ent->meta[tag] = value;		// key is lower case for "x-amz"
    }
  }

  stat_cache[key] = ent;
  return true;
}

bool StatCache::AddStat(std::string& key, headers_t& meta, bool forcedir, bool no_truncate)
{
  if(!no_truncate && CacheSize< 1){
    return true;
  }
  S3FS_PRN_INFO3("add stat cache entry[path=%s]", key.c_str());

  const std::lock_guard<std::mutex> lock(StatCache::stat_cache_lock);
  return AddStatHasLock(key, meta, forcedir, no_truncate);
}

bool StatCache::AddNoObjectCache(string& key)
{
  if(!IsCacheNoObject){
    return true;    // pretend successful
  }
  if(CacheSize < 1){
    return true;
  }
  S3FS_PRN_INFO3("add no object cache entry[path=%s]", key.c_str());

  const std::lock_guard<std::mutex> lock(StatCache::stat_cache_lock);

  bool found = stat_cache.end() != stat_cache.find(key);
  if(found){
    DelStatHasLock(key.c_str());
  }else if(stat_cache.size() > CacheSize){
    TruncateCacheHasLock();
  }

  // make new
  stat_cache_entry* ent = new stat_cache_entry();
  memset(&(ent->stbuf), 0, sizeof(struct stat));
  ent->hit_count  = 0;
  ent->isforce    = false;
  ent->noobjcache = true;
  ent->notruncate = 0L;
  ent->meta.clear();
  SetStatCacheTime(ent->cache_date);    // Set time.

  stat_cache[key] = ent;
  return true;
}

void StatCache::ChangeNoTruncateFlag(std::string key, bool no_truncate)
{
  const std::lock_guard<std::mutex> lock(StatCache::stat_cache_lock);

  stat_cache_t::iterator iter = stat_cache.find(key);

  if(stat_cache.end() != iter){
    stat_cache_entry* ent = iter->second;
    if(ent){
      if(no_truncate){
        ++(ent->notruncate);
      }else{
        if(0L < ent->notruncate){
          --(ent->notruncate);
        }
      }
    }
  }
}

bool StatCache::TruncateCacheHasLock(void)
{
  if(stat_cache.empty()){
    return true;
  }

  // 1) erase over expire time
  if(IsExpireTime){
    for(stat_cache_t::iterator iter = stat_cache.begin(); iter != stat_cache.end(); ){
      stat_cache_entry* entry = iter->second;
      if(!entry || (0L == entry->notruncate && IsExpireStatCacheTime(entry->cache_date, ExpireTime))){
        if(entry){
            delete entry;
        }
        stat_cache.erase(iter++);
      }else{
        ++iter;
      }
    }
  }

  // 2) check stat cache count
  if(stat_cache.size() < CacheSize){
    return true;
  }

  // 3) erase from the old cache in order
  size_t            erase_count= stat_cache.size() - CacheSize + 1;
  statiterlist_t    erase_iters;
  for(stat_cache_t::iterator iter = stat_cache.begin(); iter != stat_cache.end(); ++iter){
    // check no truncate
    stat_cache_entry* ent = iter->second;
    if(ent && 0L < ent->notruncate){
      // skip for no truncate entry
      if(0 < erase_count){
        --erase_count;     // decrement
      }
    }
    // iter is not have notruncate flag
    erase_iters.push_back(iter);
    sort(erase_iters.begin(), erase_iters.end(), sort_statiterlist());
    if(erase_count < erase_iters.size()){
      erase_iters.pop_back();
    }
  }
  for(statiterlist_t::iterator iiter = erase_iters.begin(); iiter != erase_iters.end(); ++iiter){
    stat_cache_t::iterator siter = *iiter;

    S3FS_PRN_DBG("truncate stat cache[path=%s]", siter->first.c_str());
    if(siter->second){
        delete siter->second;
    }
    stat_cache.erase(siter);
  }
  S3FS_MALLOCTRIM(0);

  return true;
}

bool StatCache::DelStatHasLock(const char* key)
{
  if(!key){
    return false;
  }
  S3FS_PRN_INFO3("delete stat cache entry[path=%s]", key);

  stat_cache_t::iterator iter;
  if(stat_cache.end() != (iter = stat_cache.find(string(key)))){
    if((*iter).second){
      delete (*iter).second;
    }
    stat_cache.erase(iter);
  }
  if(0 < strlen(key) && 0 != strcmp(key, "/")){
    string strpath = key;
    if('/' == strpath[strpath.length() - 1]){
      // If there is "path" cache, delete it.
      strpath = strpath.substr(0, strpath.length() - 1);
    }else{
      // If there is "path/" cache, delete it.
      strpath += "/";
    }
    if(stat_cache.end() != (iter = stat_cache.find(strpath.c_str()))){
      if((*iter).second){
        delete (*iter).second;
      }
      stat_cache.erase(iter);
    }
  }
  S3FS_MALLOCTRIM(0);

  return true;
}

bool StatCache::DelStat(const char* key)
{
  const std::lock_guard<std::mutex> lock(StatCache::stat_cache_lock);
  return DelStatHasLock(key);
}

// ISSUE2026051100001 iter-7: delete entry only if notruncate==0.
// Used by s3fs_readdir_s3 stale-entry cleanup. notruncate>0 entries belong to
// in-progress mkdir/create/open operations that may not yet be visible in OBS
// ListBucket (eventual consistency window) — they must not be evicted by an
// "external delete" signal derived from a missing ListBucket entry.
//
// ISSUE2026051200001 review fix (cleanup-race): also refuse deletion when the
// entry is younger than DEL_TRUNCATABLE_MIN_AGE_SEC seconds. After M.3 changed
// every setter (mkdir/mknod/create/...) to notruncate=false, a mkdir
// immediately followed by readdir could otherwise self-evict its own entry if
// the ListBucket response lagged the PUT (post-2020 S3 is strongly consistent
// for new puts, but OBS regional replicas / network partitions may still
// surface a transient lag). A 1-second grace is enough to ride out that
// transient without delaying legitimate external-delete cleanup
// (EM11 sleeps KERNEL_ATTR_WAIT=2s before the cleanup readdir).
//
// Returns true if an entry was actually removed; false if missing, pinned, or
// within the grace window.
//
// `now_monotonic_sec` is on the same clock as cache_date.tv_sec
// (CLOCK_MONOTONIC_COARSE, set via SetStatCacheTime). Pass 0 to read it
// internally; pass a snapshot so one readdir cleanup pass sees a consistent
// grace boundary across all entries.
static const long DEL_TRUNCATABLE_MIN_AGE_SEC = 1;

bool StatCache::DelStatIfTruncatable(const std::string& key, time_t now_monotonic_sec)
{
  const std::lock_guard<std::mutex> lock(StatCache::stat_cache_lock);
  stat_cache_t::iterator iter = stat_cache.find(key);
  if(iter == stat_cache.end()){
    return false;
  }
  if(iter->second && 0L < iter->second->notruncate){
    return false;
  }
  if(iter->second){
    if(0 == now_monotonic_sec){
      struct timespec ts;
      SetStatCacheTime(ts);
      now_monotonic_sec = ts.tv_sec;
    }
    if(now_monotonic_sec - iter->second->cache_date.tv_sec < DEL_TRUNCATABLE_MIN_AGE_SEC){
      return false;
    }
  }
  S3FS_PRN_INFO3("delete stat cache entry (truncatable)[path=%s]", key.c_str());
  if(iter->second){
    delete iter->second;
  }
  stat_cache.erase(iter);
  return true;
}

bool StatCache::TestBackdateEntry(const std::string& key, long seconds)
{
  const std::lock_guard<std::mutex> lock(StatCache::stat_cache_lock);
  stat_cache_t::iterator iter = stat_cache.find(key);
  if(iter == stat_cache.end() || !iter->second){
    return false;
  }
  iter->second->cache_date.tv_sec -= seconds;
  return true;
}

bool StatCache::DelStatWildcard(const std::string& pattern)
{
  if(pattern.empty()){
    S3FS_PRN_DBG("DelStatWildcard called with empty pattern");
    return false;
  }

  S3FS_PRN_INFO3("delete stat cache entries with pattern[path=%s]", pattern.c_str());

  const std::lock_guard<std::mutex> lock(StatCache::stat_cache_lock);

  size_t erase_count = 0;
  stat_cache_t::iterator iter = stat_cache.begin();

  // 安全遍历并删除所有匹配的条目
  while(iter != stat_cache.end()){
    // 前缀匹配: 删除所有以 pattern 开头的条目
    if(iter->first.find(pattern) == 0){
      S3FS_PRN_DBG("deleting cache entry[path=%s]", iter->first.c_str());
      if((*iter).second){
        delete (*iter).second;  // 释放 stat_cache_entry 内存
      }
      iter = stat_cache.erase(iter);  // erase 返回下一个迭代器
      erase_count++;
    } else {
      ++iter;
    }
  }

  S3FS_PRN_INFO3("deleted %zu cache entries matching pattern[path=%s]",
               erase_count, pattern.c_str());
  S3FS_MALLOCTRIM(0);

  return true;
}

bool StatCache::GetChildStatMap(const std::string& dir, s3obj_type_map_t& objmap)
{
    if(dir.empty()){
        S3FS_PRN_DBG("GetChildStatMap called with empty directory");
        return false;
    }

    const std::lock_guard<std::mutex> lock(StatCache::stat_cache_lock);

    // Ensure directory path ends with '/'
    std::string prefix = dir;
    if(prefix != "/" && prefix[prefix.length() - 1] != '/'){
        prefix += "/";
    }

    S3FS_PRN_INFO3("Extracting child entries from StatCache for directory: %s", prefix.c_str());

    // Traverse entire stat_cache to find direct children
    for(stat_cache_t::iterator iter = stat_cache.begin(); iter != stat_cache.end(); ++iter){
        const std::string& path = iter->first;

        // Check if path starts with prefix
        if(path.length() > prefix.length() &&
           path.compare(0, prefix.length(), prefix) == 0){

            // Find next '/' to ensure it's a direct child
            size_t next_slash = path.find('/', prefix.length());
            if(next_slash == std::string::npos){
                // It's a direct child (no more slashes after prefix)
                std::string name = path.substr(prefix.length());

                // Check if it's a symlink by examining stat mode
                if(iter->second && S_ISLNK(iter->second->stbuf.st_mode)){
                    objmap[name] = OBJTYPE_SYMLINK;
                    S3FS_PRN_DBG("Found symlink child: %s -> SYMLINK", name.c_str());
                } else if(iter->second && S_ISDIR(iter->second->stbuf.st_mode)){
                    // It's a directory stored without trailing slash
                    objmap[name] = OBJTYPE_DIR;
                    S3FS_PRN_DBG("Found directory child: %s -> DIR", name.c_str());
                } else {
                    // It's a regular file
                    objmap[name] = OBJTYPE_FILE;
                    S3FS_PRN_DBG("Found file child: %s -> FILE", name.c_str());
                }
            } else if(next_slash == path.length() - 1){
                // It's a directory (ends with '/')
                std::string name = path.substr(prefix.length(), path.length() - prefix.length() - 1);
                objmap[name] = OBJTYPE_DIR;
                S3FS_PRN_DBG("Found directory child: %s -> DIR", name.c_str());
            }
            // If next_slash is elsewhere, it's a nested child (skip)
        }
    }

    S3FS_PRN_INFO3("Extracted %zu children from StatCache for directory: %s",
                  objmap.size(), prefix.c_str());
    return true;
}

//-------------------------------------------------------------------
// Functions
//-------------------------------------------------------------------
bool convert_header_to_stat(const char* path, headers_t& meta, struct stat* pst, bool forcedir)
{
  if(!path || !pst){
    return false;
  }
  memset(pst, 0, sizeof(struct stat));

  pst->st_nlink = 1; // see fuse FAQ

  // mode
  pst->st_mode = get_mode(meta, path, true, forcedir);

/* file gateway modify begin */
  // size
  pst->st_size = get_size(meta);
  
  // blocks
  if(S_ISREG(pst->st_mode)){
    pst->st_blocks = get_blocks(pst->st_size);
  }
  pst->st_blksize = 4096;
/* file gateway modify end */

  // mtime
  pst->st_mtime = get_mtime(meta);

  // uid/gid
  pst->st_uid = get_uid(meta);
  pst->st_gid = get_gid(meta);

  return true;
}

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: noet sw=4 ts=4 fdm=marker
* vim<600: noet sw=4 ts=4
*/
