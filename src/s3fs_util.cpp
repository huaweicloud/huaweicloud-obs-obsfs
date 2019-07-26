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
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <libgen.h>
#include <sys/stat.h>
#include <pwd.h>
#include <grp.h>
#include <syslog.h>
#include <pthread.h>
#include <sys/types.h>
#include <dirent.h>

#include <string>
#include <sstream>
#include <map>
#include <list>

#include "common.h"
#include "s3fs_util.h"
#include "string_util.h"
#include "s3fs.h"
#include "s3fs_auth.h"

using namespace std;

typedef struct dht_view_version_info{
    int maxDhtViewVersion;
    pthread_spinlock_t maxDhtViewVersionLock;

    int curDhtViewVersion;
    time_t curDhtViewVersionUpdateTime;
    pthread_spinlock_t curDhtViewVersionLock;
}DHT_VIEW_VERSION_INFO;

//-------------------------------------------------------------------
// Global variables
//-------------------------------------------------------------------
std::string mount_prefix   = "";

// RootInodeNo, default 1024, the same with osc
long long   gRootInodNo     = 1024;

#define INIT_DHTVIEW_VERSION_ID (-1)
DHT_VIEW_VERSION_INFO g_dht_view_version_info;
time_t g_repairMaxDhtViewVersionTime   = 60;   // default

//-------------------------------------------------------------------
// Utility
//-------------------------------------------------------------------
string get_realpath(const char *path) {
  string realpath = mount_prefix;
  realpath += path;
/* file gateway modify begin */
  S3FS_PRN_DBG("[path=%s],[realpath=%s]", path, realpath.c_str());
/* file gateway modify end */
  return realpath;
}

/* file gateway modify begin */
//-------------------------------------------------------------------
// Class S3ObjList
//-------------------------------------------------------------------
bool S3ObjList::insert(const std::string name, bool is_dir){
  if(name.empty()){
    // Ignore empty name.
    return false;
  }

  // Normalize the key name.
  string key_name = name;
  if('/' == name[name.length() - 1]){
    // A name terminated by "/" is forced dir type.
    is_dir = true;
    // Remove the tail '/' of the name of direcotry.
    key_name = name.substr(0, name.length() - 1);
  }

  // Normalize the value name.
  string value_name = key_name;
  if(is_dir){
    // Forced the name of direcotry tailed by '/'.
    value_name += "/";
  }

  // Insert the name into map.
  objects.insert(pair<string,string>(key_name, value_name));
  return true;
}

std::string S3ObjList::get_last_name() const {
  if(empty()){
    return std::string("");
  }
  std::string last = objects.rbegin()->second;
  if(last.empty() || ('/' != last[last.length()-1])){
    return last;
  }
  return last.substr(0, last.length()-1);
}

S3ObjListStatistic S3ObjList::get_statistic() const {
  S3ObjListStatistic list_info;
  if(!empty()){
    list_info.set_size(objects.size());
    list_info.set_first(objects.begin()->second);
    list_info.set_last(objects.rbegin()->second);
  }
  return list_info;
}
/* file gateway modify end */

//-------------------------------------------------------------------
// Class AutoLock
//-------------------------------------------------------------------
AutoLock::AutoLock(pthread_mutex_t* pmutex, bool no_wait) : auto_mutex(pmutex)
{
  if (no_wait) {
    is_lock_acquired = pthread_mutex_trylock(auto_mutex) == 0;
  } else {
    is_lock_acquired = pthread_mutex_lock(auto_mutex) == 0;
  }
}

bool AutoLock::isLockAcquired() const
{
  return is_lock_acquired;
}

AutoLock::~AutoLock()
{
  if (is_lock_acquired) {
    pthread_mutex_unlock(auto_mutex);
  }
}

//-------------------------------------------------------------------
// Utility for UID/GID
//-------------------------------------------------------------------
// get user name from uid
string get_username(uid_t uid)
{
  static size_t maxlen = 0;	// set once
  char* pbuf;
  struct passwd pwinfo;
  struct passwd* ppwinfo = NULL;

  // make buffer
  if(0 == maxlen){
    long res = sysconf(_SC_GETPW_R_SIZE_MAX);
    if(0 > res){
      S3FS_PRN_WARN("could not get max pw length.");
      maxlen = 0;
      return string("");
    }
    maxlen = res;
  }
  if(NULL == (pbuf = (char*)malloc(sizeof(char) * maxlen))){
    S3FS_PRN_CRIT("failed to allocate memory.");
    return string("");
  }
  // get group information
  if(0 != getpwuid_r(uid, &pwinfo, pbuf, maxlen, &ppwinfo)){
    S3FS_PRN_WARN("could not get pw information.");
    free(pbuf);
    return string("");
  }
  // check pw
  if(NULL == ppwinfo){
    free(pbuf);
    return string("");
  }
  string name = SAFESTRPTR(ppwinfo->pw_name);
  free(pbuf);
  return name;
}

int is_uid_include_group(uid_t uid, gid_t gid)
{
  static size_t maxlen = 0;	// set once
  int result;
  char* pbuf;
  struct group ginfo;
  struct group* pginfo = NULL;

  // make buffer
  if(0 == maxlen){
    long res = sysconf(_SC_GETGR_R_SIZE_MAX);
    if(0 > res){
      S3FS_PRN_ERR("could not get max name length.");
      maxlen = 0;
      return -ERANGE;
    }
    maxlen = res;
  }
  if(NULL == (pbuf = (char*)malloc(sizeof(char) * maxlen))){
    S3FS_PRN_CRIT("failed to allocate memory.");
    return -ENOMEM;
  }
  // get group information
  while(ERANGE == (result = getgrgid_r(gid, &ginfo, pbuf, maxlen, &pginfo))){
    free(pbuf);
    maxlen *= 2;
    if(NULL == (pbuf = (char*)malloc(sizeof(char) * maxlen))){
      S3FS_PRN_CRIT("failed to allocate memory.");
      return -ENOMEM;
    }
  }

  if(0 != result){
    S3FS_PRN_ERR("could not get group information(%d).", result);
    free(pbuf);
    return -result;
  }

  // check group
  if(NULL == pginfo){
    // there is not gid in group.
    free(pbuf);
    return -EINVAL;
  }

  string username = get_username(uid);

  char** ppgr_mem;
  for(ppgr_mem = pginfo->gr_mem; ppgr_mem && *ppgr_mem; ppgr_mem++){
    if(username == *ppgr_mem){
      // Found username in group.
      free(pbuf);
      return 1;
    }
  }
  free(pbuf);
  return 0;
}

//-------------------------------------------------------------------
// Utility for file and directory
//-------------------------------------------------------------------
// safe variant of dirname
// dirname clobbers path so let it operate on a tmp copy
string mydirname(const char* path)
{
  if(!path || '\0' == path[0]){
    return string("");
  }
  return mydirname(string(path));
}

string mydirname(string path)
{
  return string(dirname((char*)path.c_str()));
}

// safe variant of basename
// basename clobbers path so let it operate on a tmp copy
string mybasename(const char* path)
{
  if(!path || '\0' == path[0]){
    return string("");
  }
  return mybasename(string(path));
}

string mybasename(string path)
{
  return string(basename((char*)path.c_str()));
}

// mkdir --parents
int mkdirp(const string& path, mode_t mode)
{
  string       base;
  string       component;
  stringstream ss(path);
  while (getline(ss, component, '/')) {
    base += "/" + component;

    struct stat st;
    if(0 == stat(base.c_str(), &st)){
      if(!S_ISDIR(st.st_mode)){
        return EPERM;
      }
    }else{
      if(0 != mkdir(base.c_str(), mode)){
        return errno;
     }
    }
  }
  return 0;
}

// get existed directory path
string get_exist_directory_path(const string& path)
{
  string       existed("/");    // "/" is existed.
  string       base;
  string       component;
  stringstream ss(path);
  while (getline(ss, component, '/')) {
    if(base != "/"){
      base += "/";
    }
    base += component;
    struct stat st;
    if(0 == stat(base.c_str(), &st) && S_ISDIR(st.st_mode)){
      existed = base;
    }else{
      break;
    }
  }
  return existed;
}

bool check_exist_dir_permission(const char* dirpath)
{
  if(!dirpath || '\0' == dirpath[0]){
    return false;
  }

  // exists
  struct stat st;
  if(0 != stat(dirpath, &st)){
    if(ENOENT == errno){
      // dir does not exitst
      return true;
    }
    if(EACCES == errno){
      // could not access directory
      return false;
    }
    // something error occurred
    return false;
  }

  // check type
  if(!S_ISDIR(st.st_mode)){
    // path is not directory
    return false;
  }

  // check permission
  uid_t myuid = geteuid();
  if(myuid == st.st_uid){
    if(S_IRWXU != (st.st_mode & S_IRWXU)){
      return false;
    }
  }else{
    if(1 == is_uid_include_group(myuid, st.st_gid)){
      if(S_IRWXG != (st.st_mode & S_IRWXG)){
        return false;
      }
    }else{
      if(S_IRWXO != (st.st_mode & S_IRWXO)){
        return false;
      }
    }
  }
  return true;
}

bool delete_files_in_dir(const char* dir, bool is_remove_own)
{
  DIR*           dp;
  struct dirent* dent;

  if(NULL == (dp = opendir(dir))){
    S3FS_PRN_ERR("could not open dir(%s) - errno(%d)", dir, errno);
    return false;
  }

  for(dent = readdir(dp); dent; dent = readdir(dp)){
    if(0 == strcmp(dent->d_name, "..") || 0 == strcmp(dent->d_name, ".")){
      continue;
    }
    string   fullpath = dir;
    fullpath         += "/";
    fullpath         += dent->d_name;
    struct stat st;
    if(0 != lstat(fullpath.c_str(), &st)){
      S3FS_PRN_ERR("could not get stats of file(%s) - errno(%d)", fullpath.c_str(), errno);
      closedir(dp);
      return false;
    }
    if(S_ISDIR(st.st_mode)){
      // dir -> Reentrant
      if(!delete_files_in_dir(fullpath.c_str(), true)){
        S3FS_PRN_ERR("could not remove sub dir(%s) - errno(%d)", fullpath.c_str(), errno);
        closedir(dp);
        return false;
      }
    }else{
      if(0 != unlink(fullpath.c_str())){
        S3FS_PRN_ERR("could not remove file(%s) - errno(%d)", fullpath.c_str(), errno);
        closedir(dp);
        return false;
      }
    }
  }
  closedir(dp);

  if(is_remove_own && 0 != rmdir(dir)){
    S3FS_PRN_ERR("could not remove dir(%s) - errno(%d)", dir, errno);
    return false;
  }
  return true;
}

//-------------------------------------------------------------------
// Utility functions for convert
//-------------------------------------------------------------------
time_t get_mtime(const char *s)
{
  return static_cast<time_t>(s3fs_strtoofft(s));
}

time_t get_mtime(headers_t& meta, bool overcheck)
{
  headers_t::const_iterator iter;
  if(meta.end() == (iter = meta.find(hws_obs_meta_mtime))){
    if(overcheck){
      return get_lastmodified(meta);
    }
    return 0;
  }
  return get_mtime((*iter).second.c_str());
}

/* file gateway modify begin */
long long get_inodeNo(const char *s)
{
  return static_cast<long long>(s3fs_strtoofft(s));
}

int get_dhtVersion(const char *s)
{
  return static_cast<int>(s3fs_strtoofft(s));
}
/* file gateway modify end */

off_t get_size(const char *s)
{
  return s3fs_strtoofft(s);
}

off_t get_size(headers_t& meta)
{
  headers_t::const_iterator iter = meta.find("Content-Length");
  if(meta.end() == iter){
    return 0;
  }
  return get_size((*iter).second.c_str());
}

mode_t get_mode(const char *s)
{
  return static_cast<mode_t>(s3fs_strtoofft(s));
}

mode_t get_mode(headers_t& meta, const char* path, bool checkdir, bool forcedir)
{
  mode_t mode = 0;
  bool isS3sync = false;
  headers_t::const_iterator iter;

  if(meta.end() != (iter = meta.find(hws_obs_meta_mode))){
    mode = get_mode((*iter).second.c_str()); 
  }else{
    if(meta.end() != (iter = meta.find("x-amz-meta-permissions"))){ // for s3sync
      mode = get_mode((*iter).second.c_str());
      isS3sync = true;
    }
  }
  // Checking the bitmask, if the last 3 bits are all zero then process as a regular
  // file type (S_IFDIR or S_IFREG), otherwise return mode unmodified so that S_IFIFO,
  // S_IFSOCK, S_IFCHR, S_IFLNK and S_IFBLK devices can be processed properly by fuse.
  if(!(mode & S_IFMT)){
    if(!isS3sync){
      if(checkdir){
        if(forcedir){
          mode |= S_IFDIR;
        }else{
          if(meta.end() != (iter = meta.find("Content-Type"))){
            string strConType = (*iter).second;
            // Leave just the mime type, remove any optional parameters (eg charset)
            string::size_type pos = strConType.find(";");
            if(string::npos != pos){
              strConType = strConType.substr(0, pos);
            }
            if(strConType == "application/x-directory"){
              mode |= S_IFDIR;
            }else if(path && 0 < strlen(path) && '/' == path[strlen(path) - 1]){
              if(strConType == "binary/octet-stream" || strConType == "application/octet-stream"){
                mode |= S_IFDIR;
              }else{
                if(complement_stat){
                  // If complement lack stat mode, when the object has '/' charactor at end of name
                  // and content type is text/plain and the object's size is 0 or 1, it should be
                  // directory.
                  off_t size = get_size(meta);
                  if(strConType == "text/plain" && (0 == size || 1 == size)){
                    mode |= S_IFDIR;
                  }else{
                    mode |= S_IFREG;
                  }
                }else{
                  mode |= S_IFREG;
                }
              }
            }else{
              mode |= S_IFREG;
            }
          }else{
            mode |= S_IFREG;
          }
        }
      }
      // If complement lack stat mode, when it's mode is not set any permission,
      // the object is added minimal mode only for read permission.
      if(complement_stat && 0 == (mode & (S_IRWXU | S_IRWXG | S_IRWXO))){
        mode |= (S_IRUSR | (0 == (mode & S_IFDIR) ? 0 : S_IXUSR));
      }
    }else{
      if(!checkdir){
        // cut dir/reg flag.
        mode &= ~S_IFDIR;
        mode &= ~S_IFREG;
      }
    }
  }
  return mode;
}

uid_t get_uid(const char *s)
{
  return static_cast<uid_t>(s3fs_strtoofft(s));
}

uid_t get_uid(headers_t& meta)
{
  headers_t::const_iterator iter;
  if(meta.end() == (iter = meta.find(hws_obs_meta_uid))){
    if(meta.end() == (iter = meta.find("x-amz-meta-owner"))){ // for s3sync
      return 0;
    }
  }
  return get_uid((*iter).second.c_str());
}

gid_t get_gid(const char *s)
{
  return static_cast<gid_t>(s3fs_strtoofft(s));
}

gid_t get_gid(headers_t& meta)
{
  headers_t::const_iterator iter;
  if(meta.end() == (iter = meta.find(hws_obs_meta_gid))){
    if(meta.end() == (iter = meta.find("x-amz-meta-group"))){ // for s3sync
      return 0;
    }
  }
  return get_gid((*iter).second.c_str());
}

blkcnt_t get_blocks(off_t size)
{
  return size / 512 + 1;
}

time_t cvtIAMExpireStringToTime(const char* s)
{
  struct tm tm;
  if(!s){
    return 0L;
  }
  memset(&tm, 0, sizeof(struct tm));
  strptime(s, "%Y-%m-%dT%H:%M:%S", &tm);
  return timegm(&tm); // GMT
}

time_t get_lastmodified(const char* s)
{
  struct tm tm;
  if(!s){
    return 0L;
  }
  memset(&tm, 0, sizeof(struct tm));
  strptime(s, "%a, %d %b %Y %H:%M:%S %Z", &tm);
  return timegm(&tm); // GMT
}

time_t get_lastmodified(headers_t& meta)
{
  headers_t::const_iterator iter = meta.find("Last-Modified");
  if(meta.end() == iter){
    return 0;
  }
  return get_lastmodified((*iter).second.c_str());
}

//
// Returns it whether it is an object with need checking in detail.
// If this function returns true, the object is possible to be directory
// and is needed checking detail(searching sub object).
//
bool is_need_check_obj_detail(headers_t& meta)
{
  headers_t::const_iterator iter;

  // directory object is Content-Length as 0.
  if(0 != get_size(meta)){
    return false;
  }
  // if the object has x-amz-meta information, checking is no more.
  if(meta.end() != meta.find(hws_obs_meta_mode)  ||
     meta.end() != meta.find(hws_obs_meta_mtime) ||
     meta.end() != meta.find(hws_obs_meta_uid)   ||
     meta.end() != meta.find(hws_obs_meta_gid)   ||
     meta.end() != meta.find("x-amz-meta-owner") ||
     meta.end() != meta.find("x-amz-meta-group") ||
     meta.end() != meta.find("x-amz-meta-permissions") )
  {
    return false;
  }
  // if there is not Content-Type, or Content-Type is "x-directory",
  // checking is no more.
  if(meta.end() == (iter = meta.find("Content-Type"))){
    return false;
  }
  if("application/x-directory" == (*iter).second){
    return false;
  }
  return true;
}

//-------------------------------------------------------------------
// Help
//-------------------------------------------------------------------
void show_usage (void)
{
  printf("Usage: %s BUCKET:[PATH] MOUNTPOINT [OPTION]...\n",
    program_name.c_str());
}

void show_help (void)
{
  show_usage();
  printf(
    "\n"
    "Mount an Amazon S3 bucket as a file system.\n"
    "\n"
    "Usage:\n"
    "   mounting\n"
    "     s3fs bucket[:/path] mountpoint [options]\n"
    "     s3fs mountpoint [options(must specify bucket= option)]\n"
    "\n"
    "   umounting\n"
    "     umount mountpoint\n"
    "\n"
    "   utility mode (remove interrupted multipart uploading objects)\n"
    "     s3fs -u bucket\n"
    "\n"
    "   General forms for s3fs and FUSE/mount options:\n"
    "      -o opt[,opt...]\n"
    "      -o opt [-o opt] ...\n"
    "\n"
    "s3fs Options:\n"
    "\n"
    "   Most s3fs options are given in the form where \"opt\" is:\n"
    "\n"
    "             <option_name>=<option_value>\n"
    "\n"
    "   bucket\n"
    "      - if it is not specified bucket name(and path) in command line,\n"
    "        must specify this option after -o option for bucket name.\n"
    "\n"
    "   default_acl (default=\"private\")\n"
    "      - the default canned acl to apply to all written s3 objects,\n"
    "        e.g., private, public-read.  empty string means do not send\n"
    "        header.  see http://aws.amazon.com/documentation/s3/ for the\n"
    "        full list of canned acls\n"
    "\n"
    "   retries (default=\"2\")\n"
    "      - number of times to retry a failed s3 transaction\n"
    "\n"
    "   use_cache (default=\"\" which means disabled)\n"
    "      - local folder to use for local file cache\n"
    "\n"
    "   check_cache_dir_exist (default is disable)\n"
    "      - if use_cache is set, check if the cache directory exists.\n"
    "        if this option is not specified, it will be created at runtime\n"
    "        when the cache directory does not exist.\n"
    "\n"
    "   del_cache (delete local file cache)\n"
    "      - delete local file cache when s3fs starts and exits.\n"
    "\n"
    "   storage_class (default=\"standard\")\n"
    "      - store object with specified storage class.  Possible values:\n"
    "        standard, standard_ia, and reduced_redundancy.\n"
    "\n"
    "   use_sse (default is disable)\n"
    "      - Specify three type Amazon's Server-Site Encryption: SSE-S3,\n"
    "        SSE-C or SSE-KMS. SSE-S3 uses Amazon S3-managed encryption\n"
    "        keys, SSE-C uses customer-provided encryption keys, and\n"
    "        SSE-KMS uses the master key which you manage in AWS KMS.\n"
    "        You can specify \"use_sse\" or \"use_sse=1\" enables SSE-S3\n"
    "        type(use_sse=1 is old type parameter).\n"
    "        Case of setting SSE-C, you can specify \"use_sse=custom\",\n"
    "        \"use_sse=custom:<custom key file path>\" or\n"
    "        \"use_sse=<custom key file path>\"(only <custom key file path>\n"
    "        specified is old type parameter). You can use \"c\" for\n"
    "        short \"custom\".\n"
    "        The custom key file must be 600 permission. The file can\n"
    "        have some lines, each line is one SSE-C key. The first line\n"
    "        in file is used as Customer-Provided Encryption Keys for\n"
    "        uploading and changing headers etc. If there are some keys\n"
    "        after first line, those are used downloading object which\n"
    "        are encrypted by not first key. So that, you can keep all\n"
    "        SSE-C keys in file, that is SSE-C key history.\n"
    "        If you specify \"custom\"(\"c\") without file path, you\n"
    "        need to set custom key by load_sse_c option or AWSSSECKEYS\n"
    "        environment.(AWSSSECKEYS environment has some SSE-C keys\n"
    "        with \":\" separator.) This option is used to decide the\n"
    "        SSE type. So that if you do not want to encrypt a object\n"
    "        object at uploading, but you need to decrypt encrypted\n"
    "        object at downloading, you can use load_sse_c option instead\n"
    "        of this option.\n"
    "        For setting SSE-KMS, specify \"use_sse=kmsid\" or\n"
    "        \"use_sse=kmsid:<kms id>\". You can use \"k\" for short \"kmsid\".\n"
    "        If you san specify SSE-KMS type with your <kms id> in AWS\n"
    "        KMS, you can set it after \"kmsid:\"(or \"k:\"). If you\n"
    "        specify only \"kmsid\"(\"k\"), you need to set AWSSSEKMSID\n"
    "        environment which value is <kms id>. You must be careful\n"
    "        about that you can not use the KMS id which is not same EC2\n"
    "        region.\n"
    "\n"
    "   load_sse_c - specify SSE-C keys\n"
    "        Specify the custom-provided encryption keys file path for decrypting\n"
    "        at downloading.\n"
    "        If you use the custom-provided encryption key at uploading, you\n"
    "        specify with \"use_sse=custom\". The file has many lines, one line\n"
    "        means one custom key. So that you can keep all SSE-C keys in file,\n"
    "        that is SSE-C key history. AWSSSECKEYS environment is as same as this\n"
    "        file contents.\n"
    "\n"
    "   public_bucket (default=\"\" which means disabled)\n"
    "      - anonymously mount a public bucket when set to 1, ignores the \n"
    "        $HOME/.passwd-s3fs and /etc/passwd-s3fs files.\n"
    "        S3 does not allow copy object api for anonymous users, then\n"
    "        s3fs sets nocopyapi option automatically when public_bucket=1\n"
    "        option is specified.\n"
    "\n"
    "   passwd_file (default=\"\")\n"
    "      - specify which s3fs password file to use\n"
    "\n"
    "   ahbe_conf (default=\"\" which means disabled)\n"
    "      - This option specifies the configuration file path which\n"
    "      file is the additional HTTP header by file(object) extension.\n"
    "      The configuration file format is below:\n"
    "      -----------\n"
    "      line         = [file suffix or regex] HTTP-header [HTTP-values]\n"
    "      file suffix  = file(object) suffix, if this field is empty,\n"
    "                     it means \"reg:(.*)\".(=all object).\n"
    "      regex        = regular expression to match the file(object) path.\n"
    "                     this type starts with \"reg:\" prefix.\n"
    "      HTTP-header  = additional HTTP header name\n"
    "      HTTP-values  = additional HTTP header value\n"
    "      -----------\n"
    "      Sample:\n"
    "      -----------\n"
    "      .gz                    Content-Encoding  gzip\n"
    "      .Z                     Content-Encoding  compress\n"
    "      reg:^/MYDIR/(.*)[.]t2$ Content-Encoding  text2\n"
    "      -----------\n"
    "      A sample configuration file is uploaded in \"test\" directory.\n"
    "      If you specify this option for set \"Content-Encoding\" HTTP \n"
    "      header, please take care for RFC 2616.\n"
    "\n"
    "   connect_timeout (default=\"300\" seconds)\n"
    "      - time to wait for connection before giving up\n"
    "\n"
    "   readwrite_timeout (default=\"60\" seconds)\n"
    "      - time to wait between read/write activity before giving up\n"
    "\n"
    "   max_stat_cache_size (default=\"1000\" entries (about 4MB))\n"
    "      - maximum number of entries in the stat cache\n"
    "\n"
    "   stat_cache_expire (default is no expire)\n"
    "      - specify expire time(seconds) for entries in the stat cache.\n"
    "        This expire time indicates the time since stat cached.\n"
    "\n"
    "   enable_noobj_cache (default is disable)\n"
    "      - enable cache entries for the object which does not exist.\n"
    "      s3fs always has to check whether file(or sub directory) exists \n"
    "      under object(path) when s3fs does some command, since s3fs has \n"
    "      recognized a directory which does not exist and has files or \n"
    "      sub directories under itself. It increases ListBucket request \n"
    "      and makes performance bad.\n"
    "      You can specify this option for performance, s3fs memorizes \n"
    "      in stat cache that the object(file or directory) does not exist.\n"
    "\n"
    "   no_check_certificate\n"
    "      - server certificate won't be checked against the available \n"
	"      certificate authorities.\n"
    "\n"
    "   nodnscache (disable dns cache)\n"
    "      - s3fs is always using dns cache, this option make dns cache disable.\n"
    "\n"
    "   nosscache (disable ssl session cache)\n"
    "      - s3fs is always using ssl session cache, this option make ssl \n"
    "      session cache disable.\n"
    "\n"
    "   multireq_max (default=\"20\")\n"
    "      - maximum number of parallel request for listing objects.\n"
    "\n"
    "   parallel_count (default=\"5\")\n"
    "      - number of parallel request for uploading big objects.\n"
    "      s3fs uploads large object(over 20MB) by multipart post request, \n"
    "      and sends parallel requests.\n"
    "      This option limits parallel request count which s3fs requests \n"
    "      at once. It is necessary to set this value depending on a CPU \n"
    "      and a network band.\n"
    "\n"
    "   multipart_size (default=\"10\")\n"
    "      - part size, in MB, for each multipart request.\n"
    "\n"
    "   ensure_diskfree (default same multipart_size value)\n"
    "      - sets MB to ensure disk free space. s3fs makes file for\n"
    "        downloading, uploading and caching files. If the disk free\n"
    "        space is smaller than this value, s3fs do not use diskspace\n"
    "        as possible in exchange for the performance.\n"
    "\n"
    "   singlepart_copy_limit (default=\"5120\")\n"
    "      - maximum size, in MB, of a single-part copy before trying \n"
    "      multipart copy.\n"
    "\n"
    "   url (default=\"https://s3.amazonaws.com\")\n"
    "      - sets the url to use to access Amazon S3. If you want to use HTTP,\n"
    "        then you can set \"url=http://s3.amazonaws.com\".\n"
    "        If you do not use https, please specify the URL with the url\n"
    "        option.\n"
    "\n"
    "   endpoint (default=\"us-east-1\")\n"
    "      - sets the endpoint to use on signature version 4\n"
    "      If this option is not specified, s3fs uses \"us-east-1\" region as\n"
    "      the default. If the s3fs could not connect to the region specified\n"
    "      by this option, s3fs could not run. But if you do not specify this\n"
    "      option, and if you can not connect with the default region, s3fs\n"
    "      will retry to automatically connect to the other region. So s3fs\n"
    "      can know the correct region name, because s3fs can find it in an\n"
    "      error from the S3 server.\n"
    "\n"
    "   sigv2 (default is signature version 4)\n"
    "      - sets signing AWS requests by sing Signature Version 2\n"
    "\n"
    "   mp_umask (default is \"0000\")\n"
    "      - sets umask for the mount point directory.\n"
    "      If allow_other option is not set, s3fs allows access to the mount\n"
    "      point only to the owner. In the opposite case s3fs allows access\n"
    "      to all users as the default. But if you set the allow_other with\n"
    "      this option, you can control the permissions of the\n"
    "      mount point by this option like umask.\n"
    "\n"
    "   nomultipart (disable multipart uploads)\n"
    "\n"
    "   enable_content_md5 (default is disable)\n"
    "      - ensure data integrity during writes with MD5 hash.\n"
    "\n"
    "   ecs\n"
    "      - This option instructs s3fs to query the ECS container credential\n"
    "      metadata address instead of the instance metadata address.\n"
    "\n"
    "   iam_role (default is no IAM role)\n"
    "      - This option requires the IAM role name or \"auto\". If you specify\n"
    "      \"auto\", s3fs will automatically use the IAM role names that are set\n"
    "      to an instance. If you specify this option without any argument, it\n"
    "      is the same as that you have specified the \"auto\".\n"
    "\n"
    "   ibm_iam_auth\n"
    "      - This option instructs s3fs to use IBM IAM authentication.\n"
    "      In this mode, the AWSAccessKey and AWSSecretKey will be used as\n"
    "      IBM's Service-Instance-ID and APIKey, respectively.\n"
    "\n"
    "   use_xattr (default is not handling the extended attribute)\n"
    "      Enable to handle the extended attribute(xattrs).\n"
    "      If you set this option, you can use the extended attribute.\n"
    "      For example, encfs and ecryptfs need to support the extended attribute.\n"
    "      Notice: if s3fs handles the extended attribute, s3fs can not work to\n"
    "      copy command with preserve=mode.\n"
    "\n"
    "   noxmlns (disable registering xml name space)\n"
    "        disable registering xml name space for response of \n"
    "        ListBucketResult and ListVersionsResult etc. Default name \n"
    "        space is looked up from \"http://s3.amazonaws.com/doc/2006-03-01\".\n"
    "        This option should not be specified now, because s3fs looks up\n"
    "        xmlns automatically after v1.66.\n"
    "\n"
    "   nocopyapi (for other incomplete compatibility object storage)\n"
    "        For a distributed object storage which is compatibility S3\n"
    "        API without PUT(copy api).\n"
    "        If you set this option, s3fs do not use PUT with \n"
    "        \"x-amz-copy-source\"(copy api). Because traffic is increased\n"
    "        2-3 times by this option, we do not recommend this.\n"
    "\n"
    "   norenameapi (for other incomplete compatibility object storage)\n"
    "        For a distributed object storage which is compatibility S3\n"
    "        API without PUT(copy api).\n"
    "        This option is a subset of nocopyapi option. The nocopyapi\n"
    "        option does not use copy-api for all command(ex. chmod, chown,\n"
    "        touch, mv, etc), but this option does not use copy-api for\n"
    "        only rename command(ex. mv). If this option is specified with\n"
    "        nocopyapi, then s3fs ignores it.\n"
    "\n"
    "   use_path_request_style (use legacy API calling style)\n"
    "        Enable compatibility with S3-like APIs which do not support\n"
    "        the virtual-host request style, by using the older path request\n"
    "        style.\n"
    "\n"
    "   noua (suppress User-Agent header)\n"
    "        Usually s3fs outputs of the User-Agent in \"s3fs/<version> (commit\n"
    "        hash <hash>; <using ssl library name>)\" format.\n"
    "        If this option is specified, s3fs suppresses the output of the\n"
    "        User-Agent.\n"
    "\n"
    "   dbglevel (default=\"crit\")\n"
    "        Set the debug message level. set value as crit(critical), err\n"
    "        (error), warn(warning), info(information) to debug level.\n"
    "        default debug level is critical. If s3fs run with \"-d\" option,\n"
    "        the debug level is set information. When s3fs catch the signal\n"
    "        SIGUSR2, the debug level is bumpup.\n"
    "\n"
    "   curldbg - put curl debug message\n"
    "        Put the debug message from libcurl when this option is specified.\n"
    "\n"
    "   cipher_suites - customize TLS cipher suite list\n"
    "        Customize the list of TLS cipher suites.\n"
    "        Expects a colon separated list of cipher suite names.\n"
    "        A list of available cipher suites, depending on your TLS engine,\n"
    "        can be found on the CURL library documentation:\n"
    "        https://curl.haxx.se/docs/ssl-ciphers.html\n"
    "\n"
    "   complement_stat (complement lack of file/directory mode)\n"
    "        s3fs complements lack of information about file/directory mode\n"
    "        if a file or a directory object does not have x-amz-meta-mode\n"
    "        header. As default, s3fs does not complements stat information\n"
    "        for a object, then the object will not be able to be allowed to\n"
    "        list/modify.\n"
    "\n"
    "   notsup_compat_dir (not support compatibility directory types)\n"
    "        As a default, s3fs supports objects of the directory type as\n"
    "        much as possible and recognizes them as directories.\n"
    "        Objects that can be recognized as directory objects are \"dir/\",\n"
    "        \"dir\", \"dir_$folder$\", and there is a file object that does\n"
    "        not have a directory object but contains that directory path.\n"
    "        s3fs needs redundant communication to support all these\n"
    "        directory types. The object as the directory created by s3fs\n"
    "        is \"dir/\". By restricting s3fs to recognize only \"dir/\" as\n"
    "        a directory, communication traffic can be reduced. This option\n"
    "        is used to give this restriction to s3fs.\n"
    "        However, if there is a directory object other than \"dir/\" in\n"
    "        the bucket, specifying this option is not recommended. s3fs may\n"
    "        not be able to recognize the object correctly if an object\n"
    "        created by s3fs exists in the bucket.\n"
    "        Please use this option when the directory in the bucket is\n"
    "        only \"dir/\" object.\n"
    "\n"
    "FUSE/mount Options:\n"
    "\n"
    "   Most of the generic mount options described in 'man mount' are\n"
    "   supported (ro, rw, suid, nosuid, dev, nodev, exec, noexec, atime,\n"
    "   noatime, sync async, dirsync).  Filesystems are mounted with\n"
    "   '-onodev,nosuid' by default, which can only be overridden by a\n"
    "   privileged user.\n"
    "   \n"
    "   There are many FUSE specific mount options that can be specified.\n"
    "   e.g. allow_other  See the FUSE's README for the full set.\n"
    "\n"
    "Miscellaneous Options:\n"
    "\n"
    " -h, --help        Output this help.\n"
    "     --version     Output version info.\n"
    " -d  --debug       Turn on DEBUG messages to syslog. Specifying -d\n"
    "                   twice turns on FUSE debug messages to STDOUT.\n"
    " -f                FUSE foreground option - do not run as daemon.\n"
    " -s                FUSE singlethreaded option\n"
    "                   disable multi-threaded operation\n"
    "\n"
    "\n"
    "s3fs home page: <https://github.com/s3fs-fuse/s3fs-fuse>\n"
  );
  return;
}

void show_version(void)
{
  printf(
  "Amazon Simple Storage Service File System V%s(commit:%s) with %s\n"
  "Copyright (C) 2010 Randy Rizun <rrizun@gmail.com>\n"
  "License GPL2: GNU GPL version 2 <http://gnu.org/licenses/gpl.html>\n"
  "This is free software: you are free to change and redistribute it.\n"
  "There is NO WARRANTY, to the extent permitted by law.\n",
  VERSION, COMMIT_HASH_VAL, s3fs_crypt_lib_name());
  return;
}

void initDHTViewVersinoInfo()
{
    g_dht_view_version_info.maxDhtViewVersion = INIT_DHTVIEW_VERSION_ID;
    pthread_spin_init(&g_dht_view_version_info.maxDhtViewVersionLock, PTHREAD_PROCESS_PRIVATE);

    g_dht_view_version_info.curDhtViewVersion = INIT_DHTVIEW_VERSION_ID;
    pthread_spin_init(&g_dht_view_version_info.curDhtViewVersionLock, PTHREAD_PROCESS_PRIVATE); 
    g_dht_view_version_info.curDhtViewVersionUpdateTime = time(0);
}

void destroyDHTViewVersinoInfo()
{
    g_dht_view_version_info.maxDhtViewVersion = INIT_DHTVIEW_VERSION_ID;
    pthread_spin_destroy(&g_dht_view_version_info.maxDhtViewVersionLock);

    g_dht_view_version_info.curDhtViewVersion = INIT_DHTVIEW_VERSION_ID;
    pthread_spin_destroy(&g_dht_view_version_info.curDhtViewVersionLock);     
}

int getMaxDHTViewVersionId()
{
    return g_dht_view_version_info.maxDhtViewVersion;
}

void updateMaxDHTViewVersionId(int dhtversion)
{
    if (dhtversion > g_dht_view_version_info.maxDhtViewVersion)
    {
        S3FS_PRN_WARN("try update maxDHTViewVersionId from(%d) to(%d).", 
            g_dht_view_version_info.maxDhtViewVersion, dhtversion);    
        pthread_spin_lock(&(g_dht_view_version_info.maxDhtViewVersionLock));
        if (dhtversion > g_dht_view_version_info.maxDhtViewVersion)
        {
            g_dht_view_version_info.maxDhtViewVersion = dhtversion;
        }
        pthread_spin_unlock(&(g_dht_view_version_info.maxDhtViewVersionLock));
    }
}

void repairMaxDHTViewVersionId(int dhtversion)
{
    if (dhtversion != g_dht_view_version_info.curDhtViewVersion)
    {
        time_t now = time(0);
        S3FS_PRN_WARN("update curDHTViewVersionId from(%d) to(%d) at time(%lu).", 
            g_dht_view_version_info.curDhtViewVersion, dhtversion, now);

        pthread_spin_lock(&(g_dht_view_version_info.curDhtViewVersionLock));
        if (dhtversion != g_dht_view_version_info.curDhtViewVersion)
        {
            /* if not equal, then update curDhtViewVersion and return */            
            g_dht_view_version_info.curDhtViewVersion = dhtversion;
            g_dht_view_version_info.curDhtViewVersionUpdateTime = now;
           
            pthread_spin_unlock(&(g_dht_view_version_info.curDhtViewVersionLock)); 
            return;
        }
        pthread_spin_unlock(&(g_dht_view_version_info.curDhtViewVersionLock));        
    }

    /* if equal and beyond 60s after first update this curDhtViewVersion, then repair */
    time_t now = time(0);
    if((now - g_dht_view_version_info.curDhtViewVersionUpdateTime > g_repairMaxDhtViewVersionTime)
        && (g_dht_view_version_info.curDhtViewVersion < g_dht_view_version_info.maxDhtViewVersion))
    {
        S3FS_PRN_WARN("repair maxDhtViewVersion from(%d) to(%d) at time(%lu).", 
            g_dht_view_version_info.maxDhtViewVersion, g_dht_view_version_info.curDhtViewVersion, now);   
        
        pthread_spin_lock(&(g_dht_view_version_info.maxDhtViewVersionLock));
     
        g_dht_view_version_info.maxDhtViewVersion = g_dht_view_version_info.curDhtViewVersion;
        
        pthread_spin_unlock(&(g_dht_view_version_info.maxDhtViewVersionLock));        
    }
}

void updateDHTViewVersionInfo(int dhtversion)
{
    updateMaxDHTViewVersionId(dhtversion);
    
    repairMaxDHTViewVersionId(dhtversion);
}





/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: noet sw=4 ts=4 fdm=marker
* vim<600: noet sw=4 ts=4
*/
