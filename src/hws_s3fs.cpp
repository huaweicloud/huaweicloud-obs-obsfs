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
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <libxml/tree.h>
#include <curl/curl.h>
#include <pwd.h>
#include <grp.h>
#include <getopt.h>
#include <signal.h>

#include <fstream>
#include <vector>
#include <algorithm>
#include <map>
#include <string>
#include <list>

#include "common.h"
#include "s3fs.h"
#include "curl.h"
#include "cache.h"
#include "string_util.h"
#include "s3fs_util.h"
#include "fdcache.h"
#include "s3fs_auth.h"
#include "addhead.h"

#include "hws_index_cache.h"
#include "hws_s3fs_statis_api.h"
#include "hws_fd_cache.h"
#include "hws_configure.h"
#include "obsfs_log.h"

#include "readdir_marker_map.h"

#ifdef S3_MOCK

#include "hws_s3bucket_mock.h"
#include "hws_s3object_mock.h"

#endif

using namespace std;

//-------------------------------------------------------------------
// Define
//-------------------------------------------------------------------
enum dirtype {
  DIRTYPE_UNKNOWN = -1,
  DIRTYPE_NEW = 0,
  DIRTYPE_OLD = 1,
  DIRTYPE_FOLDER = 2,
  DIRTYPE_NOOBJ = 3,
};

#define INVALID_INODE_NO (-1)
#define INVALIDE_VALUE   (-1)
#define HWS_RETRY_WRITE_OBS_NUM  8   /*max retry write osc num*/
#define READ_PASSWD_CONFIGURE_INTERVAL 3000 /*ms*/
#define PASSWD_CHANGE_FILENAME_MAX_LEN 256
#define PRINT_LENGTH 8              /*print start and end 8 length content when write*/
//static bool IS_REPLACEDIR(dirtype type) { return DIRTYPE_OLD == type || DIRTYPE_FOLDER == type || DIRTYPE_NOOBJ == type; }
static bool IS_RMTYPEDIR(dirtype type) { return DIRTYPE_OLD == type || DIRTYPE_FOLDER == type; }

#if !defined(ENOATTR)
#define ENOATTR				ENODATA
#endif

//-------------------------------------------------------------------
// Structs
//-------------------------------------------------------------------
typedef struct incomplete_multipart_info{
  string key;
  string id;
  string date;
}UNCOMP_MP_INFO;

typedef std::list<UNCOMP_MP_INFO>          uncomp_mp_list_t;
typedef std::list<std::string>             readline_t;
typedef std::map<std::string, std::string> kvmap_t;
typedef std::map<std::string, kvmap_t>     bucketkvmap_t;

//-------------------------------------------------------------------
// Global variables
//-------------------------------------------------------------------
s3fs_log_mode debug_log_mode      = LOG_MODE_SYSLOG;
int           g_LogModeNotObsfs   = 0;
int           g_backupLogMode     = 0;
struct timespec hws_s3fs_start_ts;   /*record start time*/
bool use_obsfs_log                = false;
bool nomultipart                  = false;
bool pathrequeststyle             = false;
bool complement_stat              = false;
std::string program_name;
std::string service_path          = "/";
std::string host                  = "https://s3.amazonaws.com";
std::string bucket                = "";
std::string writeparmname         = "modify=1&position=";
std::string endpoint              = "us-east-1";
std::string cipher_suites         = "";
const char* hws_s3fs_client       = "x-hws-fs-client";
const char* hws_s3fs_req_objname_use_path = "x-hws-fs-req-objname-use-path";
const char* hws_s3fs_connection   = "Connection";
const char* hws_s3fs_inodeNo      = "x-hws-fs-inodeno";
const char* hws_s3fs_shardkey     = "x-hws-fs-inode-dentryname";
const char* hws_s3fs_version_id   = "x-hws-fs-version-id";
const char* Contentlength         = "content-length";
const char* hws_s3fs_first_write  = "x-hws-fs-firstwrite";
const char* hws_s3fs_dht_version  = "x-hws-fs-dht-id";
const char* hws_s3fs_origin_name  = "x-hws-fs-origin-name";
const char* hws_s3fs_plog_headversion  = "x-hws-fs-header-version";

const char* hws_obs_meta_mtime    = "x-amz-meta-mtime";
const char* hws_obs_meta_uid      = "x-amz-meta-uid";
const char* hws_obs_meta_gid      = "x-amz-meta-gid";
const char* hws_obs_meta_mode     = "x-amz-meta-mode";

const char* hws_list_key_size     = "Size";
const char* hws_list_key_mode     = "Mode";
const char* hws_list_key_uid      = "Uid";
const char* hws_list_key_gid      = "Gid";
const char* hws_list_key_mtime    = "MTime";
const char* hws_list_key_inode    = "InodeNo";
const char* hws_list_key_last_modify = "LastModified";

s3fs_log_level debug_level        = S3FS_LOG_WARN;
const char*    s3fs_log_nest[S3FS_LOG_NEST_MAX] = {"", "  ", "    ", "      "};

bool filter_check_access          = false; // default disable access check
extern off64_t gMaxCacheMemSize;
extern bool gIsCheckCRC;
extern s3fs_log_level data_cache_log_level;
extern std::atomic<off64_t> g_wholeCacheTotalMemSize;
extern std::atomic<int> g_wholeCacheReadTasks;

bool cache_assert                 = false;  // default not assert
std::thread g_thread_daemon_;
bool g_thread_deamon_start = false;
bool g_s3fs_start_flag;
int gCliCannotResolveRetryCount  = 0;
int gMetaCacheSize = 20000;

const long INVALID_INO = -1;
const long LIST_MAX_KEY = 110;


//-------------------------------------------------------------------
// Static variables
//-------------------------------------------------------------------
static uid_t mp_uid               = 0;    // owner of mount point(only not specified uid opt)
static gid_t mp_gid               = 0;    // group of mount point(only not specified gid opt)
static mode_t mp_mode             = 0;    // mode of mount point
static mode_t mp_umask            = 0;    // umask for mount point
static bool is_mp_umask           = false;// default does not set.
static time_t mp_mtime            = 0;
static off_t mp_st_size           = 0;
static std::string mountpoint;
static std::string passwd_file    = "";
static bool utility_mode          = false;
static bool noxmlns               = false;
static bool nocopyapi             = false;
static bool norenameapi           = false;
static bool nonempty              = false;
static bool allow_other           = false;
static bool load_iamrole          = false;
static uid_t s3fs_uid             = 0;
static gid_t s3fs_gid             = 0;
static mode_t s3fs_umask          = 0;
static bool is_s3fs_uid           = false;// default does not set.
static bool is_s3fs_gid           = false;// default does not set.
static bool is_s3fs_umask         = false;// default does not set.
static bool is_remove_cache       = false;
static bool is_ecs                = false;
static bool is_ibm_iam_auth       = false;
static bool is_use_xattr          = false;
static bool create_bucket         = false;
static int64_t singlepart_copy_limit = FIVE_GB;
static bool is_specified_endpoint = false;
static int s3fs_init_deferred_exit_status = 0;
static bool support_compat_dir    = true;// default supports compatibility directory type
static bool nohwscache            = false;  /*default support cache*/


static const std::string allbucket_fields_type = "";         // special key for mapping(This name is absolutely not used as a bucket name)
static const std::string keyval_fields_type    = "\t";       // special key for mapping(This name is absolutely not used as a bucket name)
static const std::string aws_accesskeyid       = "AWSAccessKeyId";
static const std::string aws_secretkey         = "AWSSecretKey";

static const std::string obsfs_log_file_path   = "/var/log/obsfs/";
static const std::string obsfs_log_file_name   = "obsfs_log";

static const char* c_strErrorObjectName = "FILE or SUBDIR in DIR";
s3fs_log_level set_s3fs_log_level(s3fs_log_level level);
extern long diff_in_ms(struct timespec *start, struct timespec *end);
extern bool can_pint_log_with_fc(int uiLineNum);
extern unsigned long long getDiffUs(timeval* pBeginTime,timeval* pEndTime);
//-------------------------------------------------------------------
// Static functions : prototype
//-------------------------------------------------------------------
static void s3fs_usr2_handler(int sig);
static bool set_s3fs_usr2_handler(void);
static s3fs_log_level bumpup_s3fs_log_level(void);
static bool is_special_name_folder_object(const char* path);
static int chk_dir_object_type(const char* path, string& newpath, string& nowpath, string& nowcache, headers_t* pmeta = NULL, dirtype* pDirType = NULL);
static int remove_old_type_dir(const string& path, dirtype type);

static int get_object_attribute(const char* path, struct stat* pstbuf, headers_t* pmeta = NULL, tag_index_cache_entry_t* p_index_cache_entry = NULL, long* headLastResponseCode = NULL);
static int check_object_access(const char* path, int mask, struct stat* pstbuf, long long* inodeNo = NULL);
static int check_object_access_with_openflag(const char* path, int mask, struct stat* pstbuf, long long* inodeNo = NULL, bool openflag = false);
static int check_object_owner(const char* path, struct stat* pstbuf);
static int check_parent_object_access(const char* path, int mask);
static FdEntity* get_local_fent(const char* path, bool is_load = false);
#if 0
static bool multi_head_callback(S3fsCurl* s3fscurl);
static S3fsCurl* multi_head_retry_callback(S3fsCurl* s3fscurl);
static int readdir_multi_head(const char* path, S3ObjList& head, void* buf, fuse_fill_dir_t filler);
#endif
/* file gateway modify begin */
typedef enum
{
  FILL_NONE = 0,// the buffer is filled with nothing
  FILL_PART,    // the buffer is filled partly
  FILL_FULL     // the buffer is filled full
}PAGE_FILL_STATE;

//static int readdir_multi_head_hw_obs(const char* path, off_t offset, const S3ObjList& head,
//                void* buf, fuse_fill_dir_t filler, PAGE_FILL_STATE& fill_state, S3ObjListStatistic& fill_statistic);
static int list_bucket_hw_obs(const char* path, off_t marker_pos,
                const char* delimiter, int max_keys, long long ino, S3ObjList& head, string& marker);
static int append_objects_from_xml(const char* path, xmlDocPtr doc, S3ObjList& head);
/* file gateway modify end */
static int directory_empty(const char* path, long long inodeNo = -1);
/* file gateway modify begin */
static int append_objects_from_xml_ex(const char* path, xmlDocPtr doc, xmlXPathContextPtr ctx,
              const char* ex_contents, const char* ex_key, int isCPrefix, S3ObjList& head);

static int list_bucket_hw_obs_with_optimization(const char* path, const char* delimiter, const string& next_marker, long long ino,
    S3HwObjList& head, int list_max_key);

static int append_objects_from_xml_with_optimization(const char* path, xmlDocPtr doc, S3HwObjList& head);

static int append_objects_from_xml_ex_with_optimization(xmlDocPtr doc, xmlXPathContextPtr ctx, const char* path, const char* ex_contents, 
    const char* ex_key,S3HwObjList& head);

static void add_list_result_to_filler(const char* path, string dentryPrefix, S3HwObjList& head, void* buf, off_t offset,
    fuse_fill_dir_t filler, PAGE_FILL_STATE& fill_state, string& next_marker);





/* file gateway modify end */
static bool GetXmlNsUrl(xmlDocPtr doc, string& nsurl);
static xmlChar* get_base_exp(xmlDocPtr doc, const char* exp);
static xmlChar* get_prefix(xmlDocPtr doc);
static char* get_object_name(xmlDocPtr doc, xmlNodePtr node, const char* path);
static int put_headers(const char* path, headers_t& meta, bool is_copy,
                            const std::string& query_string);
static int create_directory_object(const char* path, mode_t mode, time_t time, uid_t uid, gid_t gid);
static int rename_object(const char* from, const char* to);
static int remote_mountpath_exists(const char* path);
static xmlChar* get_exp_value_xml(xmlDocPtr doc, xmlXPathContextPtr ctx, const char* exp_key);
static void print_uncomp_mp_list(uncomp_mp_list_t& list);
static bool abort_uncomp_mp_list(uncomp_mp_list_t& list);
static bool get_uncomp_mp_list(xmlDocPtr doc, uncomp_mp_list_t& list);
static void free_xattrs(xattrs_t& xattrs);
static bool parse_xattr_keyval(const std::string& xattrpair, string& key, PXATTRVAL& pval);
static size_t parse_xattrs(const std::string& strxattrs, xattrs_t& xattrs);
static std::string build_xattrs(const xattrs_t& xattrs);
static int s3fs_utility_mode(void);
static int s3fs_check_service(void);
static int parse_passwd_file(bucketkvmap_t& resmap);
static int update_aksk(void);
static int check_for_aws_format(const kvmap_t& kvmap);
static int check_passwd_file_perms(void);
static int read_passwd_file(void);
static bool is_aksk_same(const string strAccessKey, const string strSecretKey);
static int get_access_keys(void);
static int set_mountpoint_attribute(struct stat& mpst);
static int set_bucket(const char* arg);
static int my_fuse_opt_proc(void* data, const char* arg, int key, struct fuse_args* outargs);

/* add for object_file_gateway begin */
static void init_index_cache_entry(tag_index_cache_entry_t &index_cache_entry);
static int create_file_object_hw_obs(const char* path, mode_t mode, uid_t uid, gid_t gid, long long *inodeNo);
/* add for object_file_gateway end */

/* add for object_file_gateway begin */
static int get_object_attribute_with_open_flag_hw_obs(const char* path, struct stat* pstbuf,
    headers_t* pmeta, long long* inodeNo, bool open_flag, tag_index_cache_entry_t* p_index_cache_entry = NULL, long* headLastResponseCode = NULL, 
    bool useIndexCache = true);

static void parse_one_value_from_node(xmlDocPtr doc, const char* exp, xmlXPathContextPtr ctx, string& value);
static void parse_value_from_node_with_ns(xmlDocPtr doc, const char* exp, xmlXPathContextPtr ctx, string& value);

/* add for object_file_gateway end */


// fuse interface functions
#if 0

static int s3fs_getattr(const char* path, struct stat* stbuf);
static int s3fs_mkdir(const char* path, mode_t mode);
static int s3fs_create(const char* path, mode_t mode, struct fuse_file_info* fi);
static int s3fs_open(const char* path, struct fuse_file_info* fi);
static int s3fs_read(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* fi);
static int s3fs_write(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* fi);
static int s3fs_flush(const char* path, struct fuse_file_info* fi);
static int s3fs_release(const char* path, struct fuse_file_info* fi);
static int s3fs_opendir(const char* path, struct fuse_file_info* fi);
static int s3fs_utimens(const char* path, const struct timespec ts[2]);

#endif
static int s3fs_readlink(const char* path, char* buf, size_t size);
static int s3fs_rmdir_hw_obs(const char* path);
static int s3fs_symlink(const char* from, const char* to);
static int s3fs_rename(const char* from, const char* to);
static int s3fs_link(const char* from, const char* to);
static int s3fs_chmod(const char* path, mode_t mode);
static int s3fs_chmod_nocopy(const char* path, mode_t mode);
static int s3fs_chown_nocopy(const char* path, uid_t uid, gid_t gid);
static int s3fs_utimens_nocopy(const char* path, const struct timespec ts[2]);
static int s3fs_truncate(const char* path, off_t length);


static int s3fs_statfs(const char* path, struct statvfs* stbuf);

#if 0
static int s3fs_readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* fi);
#endif
static int s3fs_readdir_hw_obs(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* fi);
static int s3fs_access_hw_obs(const char* path, int mask);
static void* s3fs_init(struct fuse_conn_info* conn);
static void s3fs_destroy(void*);
#if defined(__APPLE__)
static int s3fs_setxattr(const char* path, const char* name, const char* value, size_t size, int flags, uint32_t position);
static int s3fs_getxattr(const char* path, const char* name, char* value, size_t size, uint32_t position);
#else
static int s3fs_setxattr(const char* path, const char* name, const char* value, size_t size, int flags);
static int s3fs_getxattr(const char* path, const char* name, char* value, size_t size);
#endif
static int s3fs_listxattr(const char* path, char* list, size_t size);
static int s3fs_removexattr(const char* path, const char* name);

/* file gateway modify begin */
static int s3fs_getattr_hw_obs(const char* path, struct stat* stbuf);
static int s3fs_create_hw_obs(const char* path, mode_t mode, struct fuse_file_info* fi);
static int s3fs_open_hw_obs(const char* path, struct fuse_file_info* fi);
static int s3fs_flush_hw_obs(const char* path, struct fuse_file_info* fi);
static int s3fs_release_hw_obs(const char* path, struct fuse_file_info* fi);
static int s3fs_fsync_hw_obs(const char* path, int datasync, struct fuse_file_info* fi);
/* file gateway modify end */

//-------------------------------------------------------------------
// Functions
//-------------------------------------------------------------------

//
//main task update aksk
//
void hws_daemon_task()
{
    pthread_setname_np(pthread_self(), "aksk_d");
    while(g_s3fs_start_flag){
        update_aksk();
        /*sleep 3 second*/
        std::this_thread::sleep_for(std::chrono::milliseconds(READ_PASSWD_CONFIGURE_INTERVAL));
    }
}
static void s3fs_usr2_handler(int sig)
{
  if(SIGUSR2 == sig){
    bumpup_s3fs_log_level();
  }
}
static bool set_s3fs_usr2_handler(void)
{
  struct sigaction sa;

  memset(&sa, 0, sizeof(struct sigaction));
  sa.sa_handler = s3fs_usr2_handler;
  sa.sa_flags   = SA_RESTART;
  if(0 != sigaction(SIGUSR2, &sa, NULL)){
    return false;
  }
  return true;
}

s3fs_log_level set_s3fs_log_level(s3fs_log_level level)
{
  if(level == debug_level){
    return debug_level;
  }
  s3fs_log_level old = debug_level;
  debug_level        = level;
  setlogmask(LOG_UPTO(S3FS_LOG_LEVEL_TO_SYSLOG(debug_level)));
  S3FS_PRN_CRIT("change debug level from %sto %s", S3FS_LOG_LEVEL_STRING(old), S3FS_LOG_LEVEL_STRING(debug_level));
  return old;
}

static s3fs_log_level bumpup_s3fs_log_level(void)
{
  s3fs_log_level old = debug_level;
  debug_level        = ( S3FS_LOG_CRIT == debug_level ? S3FS_LOG_ERR :
                         S3FS_LOG_ERR  == debug_level ? S3FS_LOG_WARN :
                         S3FS_LOG_WARN == debug_level ? S3FS_LOG_INFO :
                         S3FS_LOG_INFO == debug_level ? S3FS_LOG_DBG :
                         S3FS_LOG_CRIT );
  setlogmask(LOG_UPTO(S3FS_LOG_LEVEL_TO_SYSLOG(debug_level)));
  S3FS_PRN_CRIT("change debug level from %sto %s", S3FS_LOG_LEVEL_STRING(old), S3FS_LOG_LEVEL_STRING(debug_level));
  return old;
}

static bool is_special_name_folder_object(const char* path)
{
  if(!support_compat_dir){
    // s3fs does not support compatibility directory type("_$folder$" etc) now,
    // thus always returns false.
    return false;
  }

  if(!path || '\0' == path[0]){
    return false;
  }

  string    strpath = path;
  headers_t header;

  if(string::npos == strpath.find("_$folder$", 0)){
    if('/' == strpath[strpath.length() - 1]){
      strpath = strpath.substr(0, strpath.length() - 1);
    }
    strpath += "_$folder$";
  }
  S3fsCurl s3fscurl;
  if(0 != s3fscurl.HeadRequest(strpath.c_str(), header)){
    return false;
  }
  header.clear();
  S3FS_MALLOCTRIM(0);
  return true;
}

// [Detail]
// This function is complicated for checking directory object type.
// Arguments is used for deleting cache/path, and remake directory object.
// Please see the codes which calls this function.
//
// path:      target path
// newpath:   should be object path for making/putting/getting after checking
// nowpath:   now object name for deleting after checking
// nowcache:  now cache path for deleting after checking
// pmeta:     headers map
// pDirType:  directory object type
//
static int chk_dir_object_type(const char* path, string& newpath, string& nowpath, string& nowcache, headers_t* pmeta, dirtype* pDirType)
{
  dirtype TypeTmp;
  int  result  = -1;
  bool isforce = false;
  dirtype* pType = pDirType ? pDirType : &TypeTmp;

  // Normalize new path.
  newpath = path;
  if('/' != newpath[newpath.length() - 1]){
    string::size_type Pos;
    if(string::npos != (Pos = newpath.find("_$folder$", 0))){
      newpath = newpath.substr(0, Pos);
    }
    newpath += "/";
  }

  // Always check "dir/" at first.
  if(0 == (result = get_object_attribute(newpath.c_str(), NULL, pmeta))){
    // Found "dir/" cache --> Check for "_$folder$", "no dir object"
    nowcache = newpath;
    if(is_special_name_folder_object(newpath.c_str())){     // check support_compat_dir in this function
      // "_$folder$" type.
      (*pType) = DIRTYPE_FOLDER;
      nowpath = newpath.substr(0, newpath.length() - 1) + "_$folder$"; // cut and add
    }else if(isforce){
      // "no dir object" type.
      (*pType) = DIRTYPE_NOOBJ;
      nowpath  = "";
    }else{
      nowpath = path;
      if(0 < nowpath.length() && '/' == nowpath[nowpath.length() - 1]){
        // "dir/" type
        (*pType) = DIRTYPE_NEW;
      }else{
        // "dir" type
        (*pType) = DIRTYPE_OLD;
      }
    }
  }else if(support_compat_dir){
    // Check "dir" when support_compat_dir is enabled
    nowpath = newpath.substr(0, newpath.length() - 1);
    if(0 == (result = get_object_attribute(nowpath.c_str(), NULL, pmeta))){
      // Found "dir" cache --> this case is only "dir" type.
      // Because, if object is "_$folder$" or "no dir object", the cache is "dir/" type.
      // (But "no dir object" is checked here.)
      nowcache = nowpath;
      if(isforce){
        (*pType) = DIRTYPE_NOOBJ;
        nowpath  = "";
      }else{
        (*pType) = DIRTYPE_OLD;
      }
    }else{
      // Not found cache --> check for "_$folder$" and "no dir object".
      // (come here is that support_compat_dir is enabled)
      nowcache = "";  // This case is no cache.
      nowpath += "_$folder$";
      if(is_special_name_folder_object(nowpath.c_str())){
        // "_$folder$" type.
        (*pType) = DIRTYPE_FOLDER;
        result   = 0;             // result is OK.
      }else if(-ENOTEMPTY == directory_empty(newpath.c_str())){
        // "no dir object" type.
        (*pType) = DIRTYPE_NOOBJ;
        nowpath  = "";            // now path.
        result   = 0;             // result is OK.
      }else{
        // Error: Unknown type.
        (*pType) = DIRTYPE_UNKNOWN;
        newpath = "";
        nowpath = "";
      }
    }
  }
  return result;
}

static int remove_old_type_dir(const string& path, dirtype type)
{
  if(IS_RMTYPEDIR(type)){
    S3fsCurl s3fscurl;
    int      result = s3fscurl.DeleteRequest(path.c_str());
    if(0 != result && -ENOENT != result){
      return result;
    }
    // succeed removing or not found the directory
  }else{
    // nothing to do
  }
  return 0;
}

//
// Get object attributes with stat cache.
// This function is base for s3fs_getattr().
//
// [NOTICE]
// Checking order is changed following list because of reducing the number of the requests.
// 1) "dir"
// 2) "dir/"
// 3) "dir_$folder$"
//
static int get_object_attribute(const char* path, struct stat* pstbuf, headers_t* pmeta, tag_index_cache_entry_t* p_index_cache_entry, long* headLastResponseCode)
{
    return get_object_attribute_with_open_flag_hw_obs(path, pstbuf, pmeta, NULL, false, p_index_cache_entry, headLastResponseCode);
}

//
// Check the object uid and gid for write/read/execute.
// The param "mask" is as same as access() function.
// If there is not a target file, this function returns -ENOENT.
// If the target file can be accessed, the result always is 0.
//
// path:   the target object path
// mask:   bit field(F_OK, R_OK, W_OK, X_OK) like access().
// stat:   NULL or the pointer of struct stat.
//
static int check_object_access(const char* path, int mask, struct stat* pstbuf, long long* inodeNo)
{
    return check_object_access_with_openflag(path, mask, pstbuf, inodeNo, false);
}

static int check_object_access_with_openflag(const char* path, int mask, struct stat* pstbuf,
    long long* inodeNo, bool openflag)
{
  int result;
  struct stat st;
  struct stat* pst = (pstbuf ? pstbuf : &st);
  struct fuse_context* pcxt;
  //timeval begin_tv;
  S3FS_PRN_DBG("[path=%s]", path);
  timeval fuse_tv;

  //s3fsStatisStart(GETATTR_OBJECT_ACCESS_TOTAL, &begin_tv);
  //s3fsStatisStart(GETATTR_FUSE_GET_CTX, &fuse_tv);
  if(NULL == (pcxt = fuse_get_context())){
    return -EIO;
  }
  //s3fsStatisEnd(GETATTR_FUSE_GET_CTX, &fuse_tv);
  s3fsStatisStart(GETATTR_OBJECT_ACCESS, &fuse_tv);
  if(0 != (result = get_object_attribute_with_open_flag_hw_obs(path, pst, NULL, inodeNo, openflag))){
    // If there is not the target file(object), result is -ENOENT.
    s3fsStatisEnd(GETATTR_OBJECT_ACCESS, &fuse_tv);
    return result;
  }
  s3fsStatisEnd(GETATTR_OBJECT_ACCESS, &fuse_tv);
  //s3fsStatisStart(GETATTR_OBJECT_ACCESS_OTHER, &begin_tv);
  if(0 == pcxt->uid){
    // root is allowed all accessing.
    //s3fsStatisEnd(GETATTR_OBJECT_ACCESS_TOTAL, &begin_tv);
    return 0;
  }
  if(is_s3fs_uid && s3fs_uid == pcxt->uid){
    // "uid" user is allowed all accessing.
    //s3fsStatisEnd(GETATTR_OBJECT_ACCESS_TOTAL, &begin_tv);
    return 0;
  }
  if(F_OK == mask){
    // if there is a file, always return allowed.
    //s3fsStatisEnd(GETATTR_OBJECT_ACCESS_TOTAL, &begin_tv);
    return 0;
  }

  // for "uid", "gid" option
  uid_t  obj_uid = (is_s3fs_uid ? s3fs_uid : pst->st_uid);
  gid_t  obj_gid = (is_s3fs_gid ? s3fs_gid : pst->st_gid);

  // compare file mode and uid/gid + mask.
  mode_t mode;
  mode_t base_mask = S_IRWXO;
  if(is_s3fs_umask){
    // If umask is set, all object attributes set ~umask.
    mode = ((S_IRWXU | S_IRWXG | S_IRWXO) & ~s3fs_umask);
  }else{
    mode = pst->st_mode;
  }
  if(pcxt->uid == obj_uid){
    base_mask |= S_IRWXU;
  }
  if(pcxt->gid == obj_gid){
    base_mask |= S_IRWXG;
  }
  if(1 == is_uid_include_group(pcxt->uid, obj_gid)){
    base_mask |= S_IRWXG;
  }
  mode &= base_mask;

  if(X_OK == (mask & X_OK)){
    if(0 == (mode & (S_IXUSR | S_IXGRP | S_IXOTH))){
      return -EPERM;
    }
  }
  if(W_OK == (mask & W_OK)){
    if(0 == (mode & (S_IWUSR | S_IWGRP | S_IWOTH))){
      return -EACCES;
    }
  }
  if(R_OK == (mask & R_OK)){
    if(0 == (mode & (S_IRUSR | S_IRGRP | S_IROTH))){
      return -EACCES;
    }
  }
  if(0 == mode){
    return -EACCES;
  }
  //s3fsStatisEnd(GETATTR_OBJECT_ACCESS_TOTAL, &begin_tv);
  return 0;
}

static int check_object_owner(const char* path, struct stat* pstbuf)
{
  int result;
  struct stat st;
  struct stat* pst = (pstbuf ? pstbuf : &st);
  struct fuse_context* pcxt;

  S3FS_PRN_DBG("[path=%s]", path);

  if(NULL == (pcxt = fuse_get_context())){
    return -EIO;
  }
  if(0 != (result = get_object_attribute(path, pst))){
    // If there is not the target file(object), result is -ENOENT.
    return result;
  }
  // check owner
  if(0 == pcxt->uid){
    // root is allowed all accessing.
    return 0;
  }
  if(is_s3fs_uid && s3fs_uid == pcxt->uid){
    // "uid" user is allowed all accessing.
    return 0;
  }
  if(pcxt->uid == pst->st_uid){
    return 0;
  }
  return -EPERM;
}

//
// Check accessing the parent directories of the object by uid and gid.
//
static int check_parent_object_access(const char* path, int mask)
{
  string parent;
  int result;

  S3FS_PRN_DBG("[path=%s]", path);

  if (!filter_check_access)
  {
    S3FS_PRN_DBG("filter check access is disabled, [path=%s]", path);
    return 0;
  }

  if(0 == strcmp(path, "/") || 0 == strcmp(path, ".")){
    // path is mount point.
    return 0;
  }
  if(X_OK == (mask & X_OK)){
    for(parent = mydirname(path); 0 < parent.size(); parent = mydirname(parent)){
      if(parent == "."){
        parent = "/";
      }
      if(0 != (result = check_object_access(parent.c_str(), X_OK, NULL))){
        return result;
      }
      if(parent == "/" || parent == "."){
        break;
      }
    }
  }
  mask = (mask & ~X_OK);
  if(0 != mask){
    parent = mydirname(path);
    if(parent == "."){
      parent = "/";
    }
    if(0 != (result = check_object_access(parent.c_str(), mask, NULL))){
      return result;
    }
  }
  return 0;
}

//
// ssevalue is MD5 for SSE-C type, or KMS id for SSE-KMS
//
bool get_object_sse_type(const char* path, sse_type_t& ssetype, string& ssevalue)
{
  if(!path){
    return false;
  }

  headers_t meta;
  if(0 != get_object_attribute(path, NULL, &meta)){
    S3FS_PRN_ERR("Failed to get object(%s) headers", path);
    return false;
  }

  ssetype = SSE_DISABLE;
  ssevalue.erase();
  for(headers_t::iterator iter = meta.begin(); iter != meta.end(); ++iter){
    string key = (*iter).first;
    if(0 == strcasecmp(key.c_str(), "x-amz-server-side-encryption") && 0 == strcasecmp((*iter).second.c_str(), "AES256")){
      ssetype  = SSE_S3;
    }else if(0 == strcasecmp(key.c_str(), "x-amz-server-side-encryption-aws-kms-key-id")){
      ssetype  = SSE_KMS;
      ssevalue = (*iter).second;
    }else if(0 == strcasecmp(key.c_str(), "x-amz-server-side-encryption-customer-key-md5")){
      ssetype  = SSE_C;
      ssevalue = (*iter).second;
    }
  }
  return true;
}

static FdEntity* get_local_fent(const char* path, bool is_load)
{
  struct stat stobj;
  FdEntity*   ent;
  headers_t   meta;

  S3FS_PRN_INFO2("[path=%s]", path);

  if(0 != get_object_attribute(path, &stobj, &meta)){
    return NULL;
  }

  // open
  time_t mtime         = (!S_ISREG(stobj.st_mode) || S_ISLNK(stobj.st_mode)) ? -1 : stobj.st_mtime;
  bool   force_tmpfile = S_ISREG(stobj.st_mode) ? false : true;

  if(NULL == (ent = FdManager::get()->Open(path, &meta, static_cast<ssize_t>(stobj.st_size), mtime, force_tmpfile, true))){
    S3FS_PRN_ERR("Could not open file. errno(%d)", errno);
    return NULL;
  }
  // load
  if(is_load && !ent->OpenAndLoadAll(&meta)){
    S3FS_PRN_ERR("Could not load file. errno(%d)", errno);
    FdManager::get()->Close(ent);
    return NULL;
  }
  return ent;
}

/**
 * create or update s3 meta
 * ow_sse_flg is for over writing sse header by use_sse option.
 */
static int put_headers(const char* path, headers_t& meta, bool is_copy,
                const string& query_string)
{
  int         result;
  S3fsCurl    s3fscurl(true);

  S3FS_PRN_INFO2("[path=%s]", path);

  result = s3fscurl.PutHeadRequest(path, meta, is_copy,query_string);
  return result;
}

/* modify for object_file_gateway*/
static int s3fs_readlink(const char* path, char* buf, size_t size)
{
    S3fsCurl s3fscurl;
    int result;
    timeval begin_tv;
    s3fsStatisStart(READLINK_TOTAL, &begin_tv);

    if(!path || !buf || 0 >= size)
    {
        return 0;
    }
    S3FS_INTF_PRN_LOG("cmd start: [path=%s]", path);

    // Get file information
    struct stat statBuf;
    tag_index_cache_entry_t index_cache_entry;
    init_index_cache_entry(index_cache_entry);
    if(0 == (result = get_object_attribute(path, &statBuf, NULL, &index_cache_entry)))
    {
        if(!S_ISLNK(statBuf.st_mode))
        {
            /*is not symlink*/
            S3FS_PRN_WARN("[path=%s] is not symlink", path);
            return -EIO;
        }
    }
    else
    {
         // Not found
         return -ENOENT;
    }

    size_t readsize = statBuf.st_size;
    if(size <= readsize + 1)
    {
        S3FS_PRN_ERR("[path=%s] readlink size small,size=%u,filesize=%u",
            path,(unsigned int)size,(unsigned int)readsize);
        return -EIO;
    }

    string inodeNoStr = toString(index_cache_entry.inodeNo);
    result = s3fscurl.ReadFromFileObject(path, buf, 0, readsize,
        index_cache_entry.dentryname.c_str(), inodeNoStr.c_str());
    if (0 > result)
    {
        S3FS_PRN_ERR("[path=%s] readlink read file fail", path);
        return -EIO;
    }
    buf[readsize] = '\0';

    // check buf if it has space words.
    string strTmp = trim(string(buf));
    strcpy(buf, strTmp.c_str());
    s3fsStatisEnd(READLINK_TOTAL, &begin_tv,path);

    S3FS_PRN_INFO("readlink success,[path=%s] size=%u,readsize=%u,fromFile=%s ",
        path,(unsigned int)size,(unsigned int)readsize,buf);

    return 0;
}

static int do_create_bucket(void)
{
  S3FS_PRN_INFO2("/");

  FILE* ptmpfp;
  int   tmpfd;
  if(endpoint == "us-east-1"){
    ptmpfp = NULL;
    tmpfd = -1;
  }else{
    if(NULL == (ptmpfp = tmpfile()) ||
       -1 == (tmpfd = fileno(ptmpfp)) ||
       0 >= fprintf(ptmpfp, "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n"
        "  <LocationConstraint>%s</LocationConstraint>\n"
        "</CreateBucketConfiguration>", endpoint.c_str()) ||
       0 != fflush(ptmpfp) ||
       -1 == fseek(ptmpfp, 0L, SEEK_SET)){
      S3FS_PRN_ERR("failed to create temporary file. err(%d)", errno);
      if(ptmpfp){
        fclose(ptmpfp);
      }
      return (0 == errno ? -EIO : -errno);
    }
  }

  headers_t meta;

  S3fsCurl s3fscurl(true);
  long     res = s3fscurl.PutRequest("/", meta, tmpfd);
  if(res < 0){
    long responseCode = s3fscurl.GetLastResponseCode();
    if((responseCode == 400 || responseCode == 403) && S3fsCurl::IsSignatureV4()){
      S3FS_PRN_ERR("Could not connect, so retry to connect by signature version 2.");
      S3fsCurl::SetSignatureV4(false);

      // retry to check
      s3fscurl.DestroyCurlHandle();
      res = s3fscurl.PutRequest("/", meta, tmpfd);
    }
  }
  if(ptmpfp != NULL){
    fclose(ptmpfp);
  }
  return res;
}

static int create_directory_object(const char* path, mode_t mode, time_t time, uid_t uid, gid_t gid)
{
  S3FS_PRN_INFO1("[path=%s][mode=%04o][time=%jd][uid=%u][gid=%u]", path, mode, (intmax_t)time, (unsigned int)uid, (unsigned int)gid);

  if(!path || '\0' == path[0]){
    return -1;
  }
  string tpath = path;
  if('/' != tpath[tpath.length() - 1]){
    tpath += "/";
  }

  headers_t meta;
  meta["Content-Type"]     = string("application/x-directory");
  meta[hws_obs_meta_uid]   = str(uid);
  meta[hws_obs_meta_gid]   = str(gid);
  meta[hws_obs_meta_mode]  = str(mode);
  meta[hws_obs_meta_mtime] = str(time);

  S3fsCurl s3fscurl;
  return s3fscurl.PutRequest(tpath.c_str(), meta, -1);    // fd=-1 means for creating zero byte object.
}


/* file gateway modify begin */
static int directory_empty(const char* path, long long inodeNo)
{
  int result;
  S3ObjList head;
  string marker = "";
  int max_keys = 2; /* Just need to know if there are child objects in dir
                       For dir with children, expect "dir/" and "dir/child" */
  if((result = list_bucket_hw_obs(path, 0, "/", max_keys, inodeNo, head, marker)) != 0){
    S3FS_PRN_ERR("list_bucket returns error.");
    return result;
  }
  if(!head.empty()){
    return -ENOTEMPTY;
  }
  return 0;
}
/* file gateway modify end */


/* modify for object_file_gateway*/
static int s3fs_symlink(const char* from, const char* to)
{
    int result;
    struct fuse_context* pcxt;
    struct stat buf;
    timeval begin_tv;
    s3fsStatisStart(SYMLINK_TOTAL, &begin_tv);

    S3fsCurl s3fscurl(true);

    S3FS_PRN_INFO("cmd start: [from=%s][to=%s]", from, to);

    if(NULL == (pcxt = fuse_get_context()))
    {
        return -EIO;
    }
    if(-ENOENT != (result = get_object_attribute(to, &buf, NULL)))
    {
        S3FS_PRN_DBG("symlink get object attribute [to=%s] result=%d",to,result);
        if(0 == result)
        {
            result = -EEXIST;
        }
        return result;
    }

    mode_t mode = S_IFLNK | S_IRWXU | S_IRWXG | S_IRWXO;
    long long inodeNo = INVALID_INODE_NO;
    result = create_file_object_hw_obs(to, mode, pcxt->uid, pcxt->gid, &inodeNo);
    if(result != 0)
    {
        S3FS_PRN_ERR("create file failed for symlink [to=%s] result=%d",
                to,result);
        return result;
    }
    S3FS_PRN_DBG("create file success for symlink [to=%s],mode=%x",
                to,(unsigned int)mode);

    tag_index_cache_entry_t index_cache_entry;
    init_index_cache_entry(index_cache_entry);

    if(0 != (result = get_object_attribute(to, &buf, NULL, &index_cache_entry)))
    {
        S3FS_PRN_ERR("symlink get attribute again fail [to=%s] result=%d",to,result);
        return result;
    }

    // write(without space words)
    string  strFrom   = trim(string(from));
    ssize_t from_size = static_cast<ssize_t>(strFrom.length());


    tagDataBuffer databuff;
    databuff.pcBuffer     = strFrom.c_str();
    databuff.ulBufferSize = from_size;
    databuff.offset       = 0;

    result = s3fscurl.WriteBytesToFileObject(to, &databuff,0, &index_cache_entry);
    S3FS_PRN_INFO("symlink WriteBytesToFileObject [to=%s] result=%d",to,result);
    if (0 != result)
    {
		//1.delete index cache 2.head again 3.retry write once more
        IndexCache::getIndexCache()->DeleteIndex(to);
        if(0 != (result = get_object_attribute(to, &buf, NULL, &index_cache_entry)))
        {
            S3FS_PRN_ERR("symlink retry get attribute again fail [to=%s] result=%d",to,result);
            return -EIO;;
        }
        if(0 != (result = s3fscurl.WriteBytesToFileObject(to, &databuff,0, &index_cache_entry)))
        {
            S3FS_PRN_ERR("[result=%d][to=%s],retry write_file failed", result, to);
            return -EIO;
        }
        S3FS_PRN_INFO("symlink WriteBytesToFileObject [to=%s] result=%d",to,result);
    }
    s3fsStatisEnd(SYMLINK_TOTAL, &begin_tv);

    return 0;
}

static int rename_object(const char* from, const char* to)
{
  int result;
  struct stat statBuf;
  headers_t pmeta;
  long long inodeNo = INVALID_INODE_NO;
  long long inodeNoCheck = INVALID_INODE_NO;
  bool need_check_to = false;
  S3FS_INTF_PRN_LOG("[mount_prefix=%s][from=%s][to=%s]", mount_prefix.c_str(), from, to);

  if(0 != (result = check_parent_object_access(to, W_OK | X_OK))){
    // not permit writing "to" object parent dir.
    return result;
  }
  if(0 != (result = check_parent_object_access(from, W_OK | X_OK))){
    // not permit removing "from" object parent dir.
    return result;
  }

  if(0 == (result = get_object_attribute(to, &statBuf, NULL))){
    // Exists
    if(S_ISDIR(statBuf.st_mode)){
      S3FS_PRN_ERR("[to=%s]The dest of rename is dir and it existed , return err", to);
      return -EIO;
    }
  }

  tag_index_cache_entry_t index_cache_entry;
  init_index_cache_entry(index_cache_entry);
  bool get_ok = IndexCache::getIndexCache()->GetIndex(from, &index_cache_entry);
  if (get_ok){
      inodeNo = index_cache_entry.inodeNo;
  } else if (0 != (result = get_object_attribute_with_open_flag_hw_obs(from, &statBuf, &pmeta, &inodeNo, false))){
      S3FS_PRN_ERR("[from=%s]The src of rename is not existed , return err", from);
      return -EIO;
  }
  //flush src data to osc
  //if not found,then file not open,no need to flush
  if (get_ok)
  {
	  std::shared_ptr<HwsFdEntity> ent = HwsFdManager::GetInstance().Get(inodeNo);
	  if (ent)
	  {
	    ent->Flush();
	  }
  }

  //delete "from" cache, if rename faild but trans success, then cache_attr_switch_open is wrong
  IndexCache::getIndexCache()->DeleteIndex(toString(from));

  S3fsCurl s3fscurl;
  result = s3fscurl.RenameFileOrDirObject(from, to, need_check_to);
  // rename retry failed, need check [to] whether exist.
  if (need_check_to){
      int head_to_result = get_object_attribute_with_open_flag_hw_obs(to, &statBuf, &pmeta, &inodeNoCheck, false);
      if (0 == head_to_result && inodeNo == inodeNoCheck) {
          result = 0;
      }
  }
  S3FS_PRN_INFO1("RenameFileOrDirObject end,[from=%s][to=%s] [result=%d]",
      from,to,result);

  tag_index_cache_entry_t to_cache_entry;
  init_index_cache_entry(to_cache_entry);
  int result_head = 0;
  if (0 == result)
  {
  	bool get_ok = IndexCache::getIndexCache()->GetIndex(to, &to_cache_entry);
	if (!get_ok)
	{
		//if dest path not exist in cache,send head request to get dentry
	    struct stat statBuf;
	    result_head = get_object_attribute(to, &statBuf, NULL, &to_cache_entry);
	    if(0 != result_head){
	      S3FS_PRN_ERR("[to=%s]The rename operation success, but the dest path not found.", to);
	    }
	}

    if (0 == result_head) {
      IndexCache::getIndexCache()->ReplaceIndex(toString(from), toString(to), &to_cache_entry);
    } else {
      IndexCache::getIndexCache()->DeleteIndex(toString(from));
    }
  }

  return result;
}

static int s3fs_rename(const char* from, const char* to)
{
  int result;
  timeval begin_tv;
  s3fsStatisStart(RENAME_TOTAL, &begin_tv);
  
  S3FS_PRN_INFO("cmd start: [from=%s][to=%s]", from, to);

  if(!nocopyapi && !norenameapi){
    result = rename_object(from, to);
  }else{
    S3FS_PRN_ERR("[not set copyapi param]");
    return -1;
  }
  S3FS_MALLOCTRIM(0);
  s3fsStatisEnd(RENAME_TOTAL, &begin_tv);

  return result;
}

static int s3fs_link(const char* from, const char* to)
{
  S3FS_PRN_INFO("cmd start: [from=%s][to=%s]", from, to);
  return -EPERM;
}

static int s3fs_chmod(const char* path, mode_t mode)
{
  int result;
  string strpath;
  string newpath;
  string nowcache;
  headers_t meta;
  struct stat stbuf;
  timeval begin_tv;
  s3fsStatisStart(CHMOD_TOTAL, &begin_tv);

  S3FS_INTF_PRN_LOG("cmd start: [path=%s][mode=%04o]", path, mode);

  if(0 == strcmp(path, "/")){
    S3FS_PRN_WARN("Could not change mode for mount point.");
    return 0;
  }
  if(0 != (result = check_parent_object_access(path, X_OK))){
    return result;
  }
  if(0 != (result = check_object_owner(path, &stbuf))){
    return result;
  }

  strpath  = path;
  result = get_object_attribute(strpath.c_str(), NULL, &meta);
  if(0 != result){
    return result;
  }

  // normal object or directory object of newer version
  meta["x-amz-copy-source"]        = urlEncode(service_path + bucket + get_realpath(strpath.c_str()));
  meta[hws_obs_meta_mode]          = str(mode);
  meta["x-amz-metadata-directive"] = "REPLACE";

  if(put_headers(strpath.c_str(), meta, true,"") != 0){
    return -EIO;
  }
  S3FS_MALLOCTRIM(0);
  s3fsStatisEnd(CHMOD_TOTAL, &begin_tv,path);
  // clear attr stat cache
  IndexCache::getIndexCache()->setFilesizeOrClearGetAttrStat(strpath,0,true);            

  return 0;
}

static int s3fs_chmod_nocopy(const char* path, mode_t mode)
{
  int         result;
  string      strpath;
  string      newpath;
  string      nowcache;
  struct stat stbuf;
  dirtype     nDirType = DIRTYPE_UNKNOWN;

  S3FS_PRN_INFO1("cmd start: [path=%s][mode=%04o]", path, mode);

  if(0 == strcmp(path, "/")){
    S3FS_PRN_ERR("Could not change mode for mount point.");
    return -EIO;
  }
  if(0 != (result = check_parent_object_access(path, X_OK))){
    return result;
  }
  if(0 != (result = check_object_owner(path, &stbuf))){
    return result;
  }

  // Get attributes
  if(S_ISDIR(stbuf.st_mode)){
    result = chk_dir_object_type(path, newpath, strpath, nowcache, NULL, &nDirType);
  }else{
    strpath  = path;
    nowcache = strpath;
    result   = get_object_attribute(strpath.c_str(), NULL, NULL);
  }
  if(0 != result){
    return result;
  }

  if(S_ISDIR(stbuf.st_mode)){
    // Should rebuild all directory object
    // Need to remove old dir("dir" etc) and make new dir("dir/")

    // At first, remove directory old object
    if(0 != (result = remove_old_type_dir(strpath, nDirType))){
      return result;
    }
    StatCache::getStatCacheData()->DelStat(nowcache);

    // Make new directory object("dir/")
    if(0 != (result = create_directory_object(newpath.c_str(), mode, stbuf.st_mtime, stbuf.st_uid, stbuf.st_gid))){
      return result;
    }
  }else{
    // normal object or directory object of newer version

    // open & load
    FdEntity* ent;
    if(NULL == (ent = get_local_fent(strpath.c_str(), true))){
      S3FS_PRN_ERR("could not open and read file(%s)", strpath.c_str());
      return -EIO;
    }

    // Change file mode
    ent->SetMode(mode);

    // upload
    if(0 != (result = ent->Flush(true))){
      S3FS_PRN_ERR("could not upload file(%s): result=%d", strpath.c_str(), result);
      FdManager::get()->Close(ent);
      return result;
    }
    FdManager::get()->Close(ent);

    StatCache::getStatCacheData()->DelStat(nowcache);
  }
  S3FS_MALLOCTRIM(0);

  return result;
}

static int s3fs_chown_hw_obs(const char* path, uid_t uid, gid_t gid)
{
  int result;
  string strpath;
  string newpath;
  string nowcache;
  headers_t meta;
  struct stat stbuf;
  timeval begin_tv;
  s3fsStatisStart(CHOWN_TOTAL, &begin_tv);

  S3FS_INTF_PRN_LOG("cmd start: [path=%s][uid=%u][gid=%u]", path, (unsigned int)uid, (unsigned int)gid);

  if(0 == strcmp(path, "/")){
    S3FS_PRN_WARN("Could not change owner for mount point.");
    return 0;
  }
  if(0 != (result = check_parent_object_access(path, X_OK))){
    S3FS_PRN_ERR("chown %s err, result = %d", path, result);
    return result;
  }
  if(0 != (result = check_object_owner(path, &stbuf))){
    S3FS_PRN_ERR("chown %s err, result = %d", path, result);
    return result;
  }

  if((uid_t)(-1) == uid){
    uid = stbuf.st_uid;
  }
  if((gid_t)(-1) == gid){
    gid = stbuf.st_gid;
  }

  strpath  = path;
  result = get_object_attribute(strpath.c_str(), NULL, &meta);
  if(0 != result){
    S3FS_PRN_ERR("chown %s err, result = %d", strpath.c_str(), result);
    return result;
  }

  meta[hws_obs_meta_uid]           = str(uid);
  meta[hws_obs_meta_gid]           = str(gid);
  meta["x-amz-copy-source"]        = urlEncode(service_path + bucket + get_realpath(strpath.c_str()));
  meta["x-amz-metadata-directive"] = "REPLACE";

  if((result = put_headers(strpath.c_str(), meta, true,"")) != 0){
    S3FS_PRN_ERR("chown %s err, result = %d", strpath.c_str(), result);
    return -EIO;
  }

  s3fsStatisEnd(CHOWN_TOTAL, &begin_tv,path);
  // clear attr stat cache
  IndexCache::getIndexCache()->setFilesizeOrClearGetAttrStat(strpath,0,true);            
  
  return 0;
}

static int s3fs_chown_nocopy(const char* path, uid_t uid, gid_t gid)
{
  int         result;
  string      strpath;
  string      newpath;
  string      nowcache;
  struct stat stbuf;
  dirtype     nDirType = DIRTYPE_UNKNOWN;

  S3FS_PRN_INFO1("cmd start: [path=%s][uid=%u][gid=%u]", path, (unsigned int)uid, (unsigned int)gid);

  if(0 == strcmp(path, "/")){
    S3FS_PRN_ERR("Could not change owner for mount point.");
    return -EIO;
  }
  if(0 != (result = check_parent_object_access(path, X_OK))){
    return result;
  }
  if(0 != (result = check_object_owner(path, &stbuf))){
    return result;
  }

  if((uid_t)(-1) == uid){
    uid = stbuf.st_uid;
  }
  if((gid_t)(-1) == gid){
    gid = stbuf.st_gid;
  }

  // Get attributes
  if(S_ISDIR(stbuf.st_mode)){
    result = chk_dir_object_type(path, newpath, strpath, nowcache, NULL, &nDirType);
  }else{
    strpath  = path;
    nowcache = strpath;
    result   = get_object_attribute(strpath.c_str(), NULL, NULL);
  }
  if(0 != result){
    return result;
  }

  if(S_ISDIR(stbuf.st_mode)){
    // Should rebuild all directory object
    // Need to remove old dir("dir" etc) and make new dir("dir/")

    // At first, remove directory old object
    if(0 != (result = remove_old_type_dir(strpath, nDirType))){
      return result;
    }
    StatCache::getStatCacheData()->DelStat(nowcache);

    // Make new directory object("dir/")
    if(0 != (result = create_directory_object(newpath.c_str(), stbuf.st_mode, stbuf.st_mtime, uid, gid))){
      return result;
    }
  }else{
    // normal object or directory object of newer version

    // open & load
    FdEntity* ent;
    if(NULL == (ent = get_local_fent(strpath.c_str(), true))){
      S3FS_PRN_ERR("could not open and read file(%s)", strpath.c_str());
      return -EIO;
    }

    // Change owner
    ent->SetUId(uid);
    ent->SetGId(gid);

    // upload
    if(0 != (result = ent->Flush(true))){
      S3FS_PRN_ERR("could not upload file(%s): result=%d", strpath.c_str(), result);
      FdManager::get()->Close(ent);
      return result;
    }
    FdManager::get()->Close(ent);

    StatCache::getStatCacheData()->DelStat(nowcache);
  }
  S3FS_MALLOCTRIM(0);

  return result;
}

static int s3fs_utimens_nocopy(const char* path, const struct timespec ts[2])
{
  int         result;
  string      strpath;
  string      newpath;
  string      nowcache;
  struct stat stbuf;
  dirtype     nDirType = DIRTYPE_UNKNOWN;

  S3FS_PRN_INFO1("cmd start: [path=%s][mtime=%s]", path, str(ts[1].tv_sec).c_str());

  if(0 == strcmp(path, "/")){
    S3FS_PRN_ERR("Could not change mtime for mount point.");
    return -EIO;
  }
  if(0 != (result = check_parent_object_access(path, X_OK))){
    return result;
  }
  if(0 != (result = check_object_access(path, W_OK, &stbuf))){
    if(0 != check_object_owner(path, &stbuf)){
      return result;
    }
  }

  // Get attributes
  if(S_ISDIR(stbuf.st_mode)){
    result = chk_dir_object_type(path, newpath, strpath, nowcache, NULL, &nDirType);
  }else{
    strpath  = path;
    nowcache = strpath;
    result   = get_object_attribute(strpath.c_str(), NULL, NULL);
  }
  if(0 != result){
    return result;
  }

  if(S_ISDIR(stbuf.st_mode)){
    // Should rebuild all directory object
    // Need to remove old dir("dir" etc) and make new dir("dir/")

    // At first, remove directory old object
    if(0 != (result = remove_old_type_dir(strpath, nDirType))){
      return result;
    }
    StatCache::getStatCacheData()->DelStat(nowcache);

    // Make new directory object("dir/")
    if(0 != (result = create_directory_object(newpath.c_str(), stbuf.st_mode, ts[1].tv_sec, stbuf.st_uid, stbuf.st_gid))){
      return result;
    }
  }else{
    // normal object or directory object of newer version

    // open & load
    FdEntity* ent;
    if(NULL == (ent = get_local_fent(strpath.c_str(), true))){
      S3FS_PRN_ERR("could not open and read file(%s)", strpath.c_str());
      return -EIO;
    }

    // set mtime
    if(0 != (result = ent->SetMtime(ts[1].tv_sec))){
      S3FS_PRN_ERR("could not set mtime to file(%s): result=%d", strpath.c_str(), result);
      FdManager::get()->Close(ent);
      return result;
    }

    // upload
    if(0 != (result = ent->Flush(true))){
      S3FS_PRN_ERR("could not upload file(%s): result=%d", strpath.c_str(), result);
      FdManager::get()->Close(ent);
      return result;
    }
    FdManager::get()->Close(ent);

    StatCache::getStatCacheData()->DelStat(nowcache);
  }
  S3FS_MALLOCTRIM(0);

  return result;
}

static int s3fs_truncate(const char* path, off_t length)
{
  int result;
  struct stat statBuf;
  headers_t meta;
  S3fsCurl    s3fscurl(true);
  string str_path = path;
  timeval begin_tv;
  s3fsStatisStart(TRUNCATE_TOTAL, &begin_tv);

  S3FS_INTF_PRN_LOG("cmd start: [path=%s][length=%jd]", path, (intmax_t)length);

  if(length < 0){
    length = 0;
  }

  tag_index_cache_entry_t index_cache_entry;
  init_index_cache_entry(index_cache_entry);
  std::shared_ptr<HwsFdEntity> ent = nullptr;
  // Get file information
  if(0 == (result = get_object_attribute(path, &statBuf, NULL, &index_cache_entry)))
  {
      // Exists
      if(S_ISDIR(statBuf.st_mode))
      {
        S3FS_PRN_WARN("[path=%s] truncate dir and return", path);
        return 0;
      }
  }
  else
  {
     // Not found
     return -ENOENT;
  }

  if(!nohwscache)
  {
      ent = HwsFdManager::GetInstance().Get(index_cache_entry.inodeNo);
      if (ent)
      {
        ent->Flush();
      }
  }

  string query_string = "length=" + toString(length) + "&truncate";
  meta[hws_obs_meta_mtime] = str(time(NULL)).c_str();
  if(0 != (result = s3fscurl.PutHeadRequest(path, meta, false, query_string)))
  {
      S3FS_PRN_ERR("truncate failed [path=%s] length=%u,result=%x",
            path, (unsigned int)length, result);
      return result;
  }

  // when use hwscache and entity is not null, update file size
  if (ent)
  {
      ent->UpdateFileSizeForTruncate(length);
  }
  //clear get attr stat after truncate succeed
  IndexCache::getIndexCache()->setFilesizeOrClearGetAttrStat(str_path,0,true);

  s3fsStatisEnd(TRUNCATE_TOTAL, &begin_tv,path);
  return result;
}

static int s3fs_statfs(const char* path, struct statvfs* stbuf)
{
  S3FS_PRN_INFO("cmd start: [path=%s]", path);

  // 256T
  stbuf->f_bsize  = 0X1000000;
  stbuf->f_blocks = 0X1000000;
  stbuf->f_bfree  = 0x1000000;
  stbuf->f_bavail = 0x1000000;
  stbuf->f_namemax = NAME_MAX;
  return 0;
}

#if 0
/* file gateway modify begin */
/*对list_bucket_hw_obs的结果发head请求*/
static int readdir_multi_head_hw_obs(
            const char*         path,
            off_t               offset,
            const S3ObjList&    head,
            void*               buf,
            fuse_fill_dir_t     filler,
            PAGE_FILL_STATE&    fill_state, // output the fill state by this call
            S3ObjListStatistic& fill_statistic)
{
  long          headLastResponseCode = 0;
  bool          is_full = false;

  S3FS_PRN_INFO1("[fill begin][path=%s,off=%ld,headsize=%ld]", path, offset, head.size());

  for(auto iter = head.begin(); iter != head.end(); iter++){
    string dispath = path;
    struct stat statBuf;
    if (0 != strcmp(dispath.c_str(),"/")){
      dispath += "/";
    }

    string currpath = iter->first;
    dispath += currpath;

    int result = get_object_attribute(dispath.c_str(), &statBuf, NULL, NULL, &headLastResponseCode);
    if(0 == result){
      is_full = filler(buf, currpath.c_str(), &statBuf, offset);
      S3FS_PRN_INFO("[path=%s,off=%ld,filename=%s,filemode=%o,is_full=%d]",
          path, offset, dispath.c_str(), statBuf.st_mode, is_full);
      if(is_full){
        // not filled current object name due to failure or full buffer
        fill_state = FILL_FULL;
        break;
      }
      // filled current object name
      fill_state = FILL_PART;
      fill_statistic.update(currpath);
    }else if(404 != headLastResponseCode && 403 != headLastResponseCode){
      S3FS_PRN_ERR("[path=%s,off=%ld,filename=%s] get failed (%ld)",
          path, offset, dispath.c_str(), headLastResponseCode);
      return result;
    }else{
      // 404: current object name is not existed
      S3FS_PRN_INFO("[path=%s,off=%ld,filename=%s] get_attr return unexisted",
          path, offset, dispath.c_str());
    }
  }

  S3FS_PRN_INFO("[fill end][path=%s,off=%ld,fill=(state:%d,content:%s)]",
      path, offset, fill_state, fill_statistic.to_string().c_str());
  return 0;
}

#endif

static int list_bucket_hw_obs_with_optimization(
                const char*     path,
                const char*     delimiter,
                const string&   next_marker,
                long long       ino,
                S3HwObjList&    head,
                int             list_max_key)
{

  int       result = 0;
  string    ino_str = toString(ino);
  string    query_delimiter = "";
  string    query_marker = "";
  string    query_max_keys = "";
  string    query_prefix = "";
  string    each_query = "";
  S3fsCurl  s3fscurl;
  xmlDocPtr doc;
  BodyData* body = nullptr;

  S3FS_PRN_INFO1("[list begin][path=%s,next_marker=%s]",
      path, next_marker.c_str());

  // delimiter
  if(delimiter && 0 < strlen(delimiter)){
    query_delimiter = string("delimiter=") + delimiter + "&";
  }
  // marker
  if(!next_marker.empty()){
    string marker = ino_str + "/" + next_marker;
    query_marker = string("marker=") + urlEncode(marker) + "&";
  }

  // max-keys
  query_max_keys = string("max-keys=") + toString(list_max_key) + "&";
  // prefix
  query_prefix = "prefix=" + ino_str;
  // query filter composed of delimiter, marker, maxkey and prefix
  each_query = query_delimiter + query_marker + query_max_keys + query_prefix;

  // request one batch of path names */
  result = s3fscurl.ListBucketRequest(path, each_query.c_str());
  if(0 != result){
    S3FS_PRN_ERR("[path=%s,marker=%s] ListBucketRequest returns with error %d.",
        path, next_marker.c_str(), result);
    return result;
  }
  body = s3fscurl.GetBodyData();

  // xmlDocPtr
  if(NULL == (doc = xmlReadMemory(body->str(), static_cast<int>(body->size()), "", NULL, 0))){
    S3FS_PRN_ERR("[path=%s,marker=%s] xmlReadMemory returns with error.",
        path, next_marker.c_str());
    return -1;
  }

  // extract list result from documents.
  if(0 != append_objects_from_xml_with_optimization(path, doc, head)){
    S3FS_PRN_ERR("[path=%s,marker=%s] append_objects_from_xml returns with error.",
        path, next_marker.c_str());
    xmlFreeDoc(doc);
    return -1;
  }
  xmlFreeDoc(doc);
  return 0;
}


/* file gateway modify begin */
/*使用目录的inodeNo作为prefix发送list请求(只请求一批)*/
static int list_bucket_hw_obs(
                const char*     path,
                off_t           marker_pos,
                const char*     delimiter,
                int             max_keys,
                long long       ino,
                S3ObjList&      head,   //output objects list
                string&         next_marker)//input&ouput the marker
{
  int       result = 0;
  string    ino_str = toString(ino);
  string    query_delimiter = "";
  string    query_marker = "";
  string    query_max_keys = "";
  string    query_prefix = "";
  string    each_query = "";
  S3fsCurl  s3fscurl;
  xmlDocPtr doc;
  BodyData* body = nullptr;

  S3FS_PRN_INFO1("[list begin][path=%s,off=%ld,next_marker=%s]",
      path, marker_pos, next_marker.c_str());

  // delimiter
  if(delimiter && 0 < strlen(delimiter)){
    query_delimiter = string("delimiter=") + delimiter + "&";
  }
  // marker
  if(!next_marker.empty()){
    string marker = ino_str + "/" + next_marker;
    query_marker = string("marker=") + urlEncode(marker) + "&";
  }
  // max-keys
  query_max_keys = string("max-keys=") + toString(max_keys) + "&";
  // prefix
  query_prefix = "prefix=" + ino_str;
  // query filter composed of delimiter, marker, maxkey and prefix
  each_query = query_delimiter + query_marker + query_max_keys + query_prefix;

  // request one batch of path names */
  result = s3fscurl.ListBucketRequest(path, each_query.c_str());
  if(0 != result){
    S3FS_PRN_ERR("[path=%s,marker=%s] ListBucketRequest returns with error %d.",
        path, next_marker.c_str(), result);
    return result;
  }
  body = s3fscurl.GetBodyData();

  // xmlDocPtr
  if(NULL == (doc = xmlReadMemory(body->str(), static_cast<int>(body->size()), "", NULL, 0))){
    S3FS_PRN_ERR("[path=%s,marker=%s] xmlReadMemory returns with error.",
        path, next_marker.c_str());
    return -1;
  }

  // extract object names from documents.
  if(0 != append_objects_from_xml(path, doc, head)){
    S3FS_PRN_ERR("[path=%s,marker=%s] append_objects_from_xml returns with error.",
        path, next_marker.c_str());
    xmlFreeDoc(doc);
    return -1;
  }
  xmlFreeDoc(doc);

  // update marker for next batch
  if (!head.empty()) {
    next_marker = head.get_last_name();
  }

  S3FS_PRN_INFO1("[list end][path=%s,off=%ld,next_marker=%s,list_info=%s]",
      path, marker_pos, next_marker.c_str(), head.get_statistic().to_string().c_str());
  return 0;
}


static void parse_one_value_from_node(xmlDocPtr doc, const char* exp, xmlXPathContextPtr ctx, string& value)
{
    string xmlnsurl;
    string exp_string = "";
    if(!noxmlns && GetXmlNsUrl(doc, xmlnsurl)){
        xmlXPathRegisterNs(ctx, (xmlChar*)"s3", (xmlChar*)xmlnsurl.c_str());
        exp_string += "s3:";
    }

    exp_string += exp;
    return parse_value_from_node_with_ns(doc, exp_string.c_str(), ctx, value);
}

static void parse_value_from_node_with_ns(xmlDocPtr doc, const char* exp, xmlXPathContextPtr ctx, string& value)
{
    xmlXPathObjectPtr marker_xp;
    if(NULL == (marker_xp = xmlXPathEvalExpression((xmlChar*)exp, ctx))){
      return;
    }
    if(xmlXPathNodeSetIsEmpty(marker_xp->nodesetval)){
      xmlXPathFreeObject(marker_xp);
      return;
    }
    xmlNodeSetPtr key_nodes = marker_xp->nodesetval;

    // get data in childrenNode
    xmlChar*      result = xmlNodeListGetString(doc, key_nodes->nodeTab[0]->xmlChildrenNode, 1);
    value = (result ? (char*)result : "");
    xmlXPathFreeObject(marker_xp);
    xmlFree(result);

    return;
}


static char* extract_object_name_for_show(const char* path, const char* fullpath)
{
  // Make dir path and filename
  string   strdirpath = mydirname(string((char*)fullpath));
  string   strmybpath = mybasename(string((char*)fullpath));
  const char* dirpath = strdirpath.c_str();
  const char* mybname = strmybpath.c_str();
  const char* basepath= (path && '/' == path[0]) ? &path[1] : path;


  if(!mybname || '\0' == mybname[0]){
    return NULL;
  }

  // check subdir & file in subdir
  if(dirpath && 0 < strlen(dirpath)){
    // case of "/"
    if(0 == strcmp(mybname, "/") && 0 == strcmp(dirpath, "/")){
      return (char*)c_strErrorObjectName;
    }
    // case of "."
    if(0 == strcmp(mybname, ".") && 0 == strcmp(dirpath, ".")){
      return (char*)c_strErrorObjectName;
    }
    // case of ".."
    if(0 == strcmp(mybname, "..") && 0 == strcmp(dirpath, ".")){
      return (char*)c_strErrorObjectName;
    }
    // case of "name"
    if(0 == strcmp(dirpath, ".")){
      // OK
      return strdup(mybname);
    }else{
      if(basepath && 0 == strcmp(dirpath, basepath)){
        // OK
        return strdup(mybname);
      }else if(basepath && 0 < strlen(basepath) && '/' == basepath[strlen(basepath) - 1] && 0 == strncmp(dirpath, basepath, strlen(basepath) - 1)){
        string withdirname = "";
        if(strlen(dirpath) > strlen(basepath)){
          withdirname = &dirpath[strlen(basepath)];
        }
        if(0 < withdirname.length() && '/' != withdirname[withdirname.length() - 1]){
          withdirname += "/";
        }
        withdirname += mybname;
        return strdup(withdirname.c_str());
      }
    }
  }
  return (char*)c_strErrorObjectName;
}



static int parse_obj_name_from_node(xmlDocPtr doc, xmlXPathContextPtr ctx, const char* ex_key, const char* path, string& obj_name)
{
    string dentry_name = "";
    parse_value_from_node_with_ns(doc, ex_key, ctx, dentry_name);
    if (dentry_name.empty())
    {
        S3FS_PRN_ERR("path %s, ex_key %s name not exist.", path, ex_key);
        return -1;
    }

    char* pure_name = extract_object_name_for_show(path, dentry_name.c_str());
    if (!pure_name)
    {
        S3FS_PRN_ERR("path %s, name %s is invalid.", path, dentry_name.c_str());
           return -1;
    }

    if((const char*)pure_name == c_strErrorObjectName){
      S3FS_PRN_ERR("path %s, name %s is file or subdir in dir. but continue.", path, dentry_name.c_str());
      return -1;
    }

    obj_name = pure_name;
    free(pure_name);

    return 0;
}
static mode_t parse_mode_from_node(xmlDocPtr doc, xmlXPathContextPtr ctx)
{
    headers_t::const_iterator iter;

    string mode_val = "";
    parse_one_value_from_node(doc, hws_list_key_mode, ctx, mode_val);
    if (mode_val.empty())
    {
        return 0;
    }

    return get_mode(mode_val.c_str());
}

static long parse_ino_from_node(xmlDocPtr doc, xmlXPathContextPtr ctx)
{
    string ino_str = "";
    parse_one_value_from_node(doc, hws_list_key_inode, ctx, ino_str);
    if (ino_str.empty())
    {
        return INVALID_INO;
    }
    return atol(ino_str.c_str());
}

static uid_t parse_uid_from_node(xmlDocPtr doc, xmlXPathContextPtr ctx)
{
    string uid = "";
    parse_one_value_from_node(doc, hws_list_key_uid, ctx, uid);
    if (uid.empty())
    {
        return 0;
    }

    return get_uid(uid.c_str());
}

static gid_t parse_gid_from_node(xmlDocPtr doc, xmlXPathContextPtr ctx)
{
    string gid = "";
    parse_one_value_from_node(doc, hws_list_key_gid, ctx, gid);
    if (gid.empty())
    {
        return 0;
    }

    return get_gid(gid.c_str());
}

static off_t parse_size_from_node(xmlDocPtr doc, xmlXPathContextPtr ctx)
{
    string size = "";
    parse_one_value_from_node(doc, hws_list_key_size, ctx, size);
    if (size.empty())
    {
        return 0;
    }

    return get_size(size.c_str());
}

static time_t parse_mtime_from_node(xmlDocPtr doc, xmlXPathContextPtr ctx)
{
    string mtime_str = "";
    parse_one_value_from_node(doc, hws_list_key_mtime, ctx, mtime_str);

    if (mtime_str.empty())
    {
        return 0;
    }

    return get_mtime(mtime_str.c_str());
}


static int parse_stat_from_node(xmlDocPtr doc, xmlXPathContextPtr ctx, struct stat* pstat, const char* path, const char* name)
{
    memset(pstat, 0, sizeof(struct stat));

    long inode_no = parse_ino_from_node(doc, ctx);
    if (INVALID_INO == inode_no)
    {
        S3FS_PRN_ERR("path %s, name %s, inode not in element.", path, name);
        return -1;
    }

    time_t mtime = parse_mtime_from_node(doc, ctx);
    if (mtime == 0)
    {
        S3FS_PRN_ERR("path %s, name %s, node not in element.", path, name);
    }

    mode_t mode = parse_mode_from_node(doc, ctx);
    if (mode == 0)
    {
        S3FS_PRN_ERR("path %s, name %s, mode not in element.", path, name);
    }

    off_t size = parse_size_from_node(doc, ctx);
    uid_t uid = parse_uid_from_node(doc, ctx);
    gid_t gid = parse_gid_from_node(doc, ctx);

    pstat->st_ino = inode_no;
    pstat->st_nlink = 1;
    pstat->st_mode = mode;
    pstat->st_size = size;

    if(S_ISREG(pstat->st_mode)){
        pstat->st_blocks = get_blocks(pstat->st_size);
    }
    pstat->st_blksize = 4096;

    pstat->st_uid = uid;
    pstat->st_gid = gid;
    pstat->st_mtime = mtime;
    return 0;
}


static int append_objects_from_xml_ex_with_optimization(
    xmlDocPtr           doc,
    xmlXPathContextPtr  ctx,
    const char*         path,
    const char*         ex_contents,
    const char*         ex_key,
    S3HwObjList&        head)
{
  xmlXPathObjectPtr contents_xp;
  xmlNodeSetPtr content_nodes;

  if(NULL == (contents_xp = xmlXPathEvalExpression((xmlChar*)ex_contents, ctx))){
    S3FS_PRN_ERR("xmlXPathEvalExpression returns null.");
    return -1;
  }
  if(xmlXPathNodeSetIsEmpty(contents_xp->nodesetval)){
    S3FS_XMLXPATHFREEOBJECT(contents_xp);
    return 0;
  }
  content_nodes = contents_xp->nodesetval;

  int    result = 0;
  for(int i = 0; i < content_nodes->nodeNr; i++){
    ctx->node = content_nodes->nodeTab[i];

    string obj_name = "";
    if ((result = parse_obj_name_from_node(doc, ctx, ex_key, path, obj_name)) != 0){
      break;
    }

    struct stat statBuf;
    if ((result = parse_stat_from_node(doc, ctx, &statBuf, path, obj_name.c_str())) != 0){
      break;
    }

    if ((result = (head.insert(obj_name, statBuf) ? 0 : -1)) !=0){
        S3FS_PRN_ERR("insert object returns with error, name is empty! path %s", path);
        break;
    };

  }
  S3FS_XMLXPATHFREEOBJECT(contents_xp);

  return result;
}



static int append_objects_from_xml_ex(const char* path, xmlDocPtr doc, xmlXPathContextPtr ctx,
       const char* ex_contents, const char* ex_key, int isCPrefix, S3ObjList& head)
{
  xmlXPathObjectPtr contents_xp;
  xmlNodeSetPtr content_nodes;

  if(NULL == (contents_xp = xmlXPathEvalExpression((xmlChar*)ex_contents, ctx))){
    S3FS_PRN_ERR("xmlXPathEvalExpression returns null.");
    return -1;
  }
  if(xmlXPathNodeSetIsEmpty(contents_xp->nodesetval)){
    S3FS_PRN_INFO("contents_xp->nodesetval is empty.");
    S3FS_XMLXPATHFREEOBJECT(contents_xp);
    return 0;
  }
  content_nodes = contents_xp->nodesetval;

  bool   is_dir;
  int    i;
  for(i = 0; i < content_nodes->nodeNr; i++){
    ctx->node = content_nodes->nodeTab[i];

    // extract object name
    xmlXPathObjectPtr key;
    if(NULL == (key = xmlXPathEvalExpression((xmlChar*)ex_key, ctx))){
      S3FS_PRN_WARN("key is null. but continue.");
      continue;
    }
    if(xmlXPathNodeSetIsEmpty(key->nodesetval)){
      S3FS_PRN_WARN("node is empty. but continue.");
      xmlXPathFreeObject(key);
      continue;
    }
    xmlNodeSetPtr key_nodes = key->nodesetval;
    char* name = get_object_name(doc, key_nodes->nodeTab[0]->xmlChildrenNode, path);
    if(!name){
      S3FS_PRN_WARN("name is something wrong. but continue.");
      xmlXPathFreeObject(key);
      continue;
    }
    if((const char*)name == c_strErrorObjectName){
      S3FS_PRN_DBG("name is file or subdir in dir. but continue.");
      xmlXPathFreeObject(key);
      continue;
    }

    // append object name
    is_dir = isCPrefix ? true : false;
    if(!head.insert(name, is_dir)){
      S3FS_PRN_ERR("insert_object returns with error.");
      free(name);
      xmlXPathFreeObject(key);
      S3FS_XMLXPATHFREEOBJECT(contents_xp);
      return -1;
    }
    free(name);
    xmlXPathFreeObject(key);
  }
  S3FS_XMLXPATHFREEOBJECT(contents_xp);

  return 0;
}
/* file gateway modify end */

static bool GetXmlNsUrl(xmlDocPtr doc, string& nsurl)
{
  static time_t tmLast = 0;  // cache for 60 sec.
  static string strNs("");
  bool result = false;

  if(!doc){
    return result;
  }
  if((tmLast + 60) < time(NULL)){
    // refresh
    tmLast = time(NULL);
    strNs  = "";
    xmlNodePtr pRootNode = xmlDocGetRootElement(doc);
    if(pRootNode){
      xmlNsPtr* nslist = xmlGetNsList(doc, pRootNode);
      if(nslist){
        if(nslist[0] && nslist[0]->href){
          strNs  = (const char*)(nslist[0]->href);
        }
        S3FS_XMLFREE(nslist);
      }
    }
  }
  if(0 < strNs.size()){
    nsurl  = strNs;
    result = true;
  }
  return result;
}

// Extract list result from xml, add put result to S3HwObjList.
static int append_objects_from_xml_with_optimization(
    const char*      path,
    xmlDocPtr        doc,
    S3HwObjList&     head)
{
  string xmlnsurl;
  string ex_contents = "//";
  string ex_key      = "";
  string ex_cprefix  = "//";
  string ex_prefix   = "";

  if(!doc){
    return -1;
  }

  // If there is not <Prefix>, use path instead of it.
  xmlChar* pprefix = get_prefix(doc);
  string   prefix  = (pprefix ? (char*)pprefix : path ? path : "");
  if(pprefix){
    xmlFree(pprefix);
  }

  xmlXPathContextPtr ctx = xmlXPathNewContext(doc);

  if(!noxmlns && GetXmlNsUrl(doc, xmlnsurl)){
    xmlXPathRegisterNs(ctx, (xmlChar*)"s3", (xmlChar*)xmlnsurl.c_str());
    ex_contents+= "s3:";
    ex_key     += "s3:";
    ex_cprefix += "s3:";
    ex_prefix  += "s3:";
  }
  // Contents-Key (files in current directory)
  ex_contents+= "Contents";
  ex_key     += "Key";
  // CommonPrefixes-Prefix (directories in current directory)
  ex_cprefix += "CommonPrefixes";
  ex_prefix  += "Prefix";

  // Parse Contents(files) and CommonPrefixes(directory) separately.
  if(-1 == append_objects_from_xml_ex_with_optimization(doc, ctx, prefix.c_str(), ex_contents.c_str(), ex_key.c_str(), head) ||
     -1 == append_objects_from_xml_ex_with_optimization(doc, ctx, prefix.c_str(), ex_cprefix.c_str(), ex_prefix.c_str(), head))
  {
    S3FS_PRN_ERR("append_objects_from_xml_ex returns with error.");
    S3FS_XMLXPATHFREECONTEXT(ctx);
    return -1;
  }
  S3FS_XMLXPATHFREECONTEXT(ctx);

  return 0;
}


/* file gateway modify begin */
static int append_objects_from_xml(const char* path, xmlDocPtr doc, S3ObjList& head)
{
  string xmlnsurl;
  string ex_contents = "//";
  string ex_key      = "";
  string ex_cprefix  = "//";
  string ex_prefix   = "";

  if(!doc){
    return -1;
  }

  // If there is not <Prefix>, use path instead of it.
  xmlChar* pprefix = get_prefix(doc);
  string   prefix  = (pprefix ? (char*)pprefix : path ? path : "");
  if(pprefix){
    xmlFree(pprefix);
  }

  xmlXPathContextPtr ctx = xmlXPathNewContext(doc);

  if(!noxmlns && GetXmlNsUrl(doc, xmlnsurl)){
    xmlXPathRegisterNs(ctx, (xmlChar*)"s3", (xmlChar*)xmlnsurl.c_str());
    ex_contents+= "s3:";
    ex_key     += "s3:";
    ex_cprefix += "s3:";
    ex_prefix  += "s3:";
  }
  ex_contents+= "Contents";
  ex_key     += "Key";
  ex_cprefix += "CommonPrefixes";
  ex_prefix  += "Prefix";

  if(-1 == append_objects_from_xml_ex(prefix.c_str(), doc, ctx, ex_contents.c_str(), ex_key.c_str(), 0, head) ||
     -1 == append_objects_from_xml_ex(prefix.c_str(), doc, ctx, ex_cprefix.c_str(), ex_prefix.c_str(), 1, head) )
  {
    S3FS_PRN_ERR("append_objects_from_xml_ex returns with error.");
    S3FS_XMLXPATHFREECONTEXT(ctx);
    return -1;
  }
  S3FS_XMLXPATHFREECONTEXT(ctx);

  return 0;
}
/* file gateway modify end */

static xmlChar* get_base_exp(xmlDocPtr doc, const char* exp)
{
  xmlXPathObjectPtr  marker_xp;
  string xmlnsurl;
  string exp_string = "//";

  if(!doc){
    return NULL;
  }
  xmlXPathContextPtr ctx = xmlXPathNewContext(doc);

  if(!noxmlns && GetXmlNsUrl(doc, xmlnsurl)){
    xmlXPathRegisterNs(ctx, (xmlChar*)"s3", (xmlChar*)xmlnsurl.c_str());
    exp_string += "s3:";
  }
  exp_string += exp;

  if(NULL == (marker_xp = xmlXPathEvalExpression((xmlChar *)exp_string.c_str(), ctx))){
    xmlXPathFreeContext(ctx);
    return NULL;
  }
  if(xmlXPathNodeSetIsEmpty(marker_xp->nodesetval)){
    S3FS_PRN_ERR("marker_xp->nodesetval is empty.");
    xmlXPathFreeObject(marker_xp);
    xmlXPathFreeContext(ctx);
    return NULL;
  }
  xmlNodeSetPtr nodes  = marker_xp->nodesetval;
  xmlChar*      result = xmlNodeListGetString(doc, nodes->nodeTab[0]->xmlChildrenNode, 1);

  xmlXPathFreeObject(marker_xp);
  xmlXPathFreeContext(ctx);

  return result;
}

static xmlChar* get_prefix(xmlDocPtr doc)
{
  return get_base_exp(doc, "Prefix");
}

/* file gateway modify begin */
/*
static xmlChar* get_next_marker(xmlDocPtr doc)
{
  return get_base_exp(doc, "NextMarker");
}

static bool is_truncated(xmlDocPtr doc)
{
  bool result = false;

  xmlChar* strTruncate = get_base_exp(doc, "IsTruncated");
  if(!strTruncate){
    return result;
  }
  if(0 == strcasecmp((const char*)strTruncate, "true")){
    result = true;
  }
  xmlFree(strTruncate);
  return result;
}
*/
/* file gateway modify end */

// return: the pointer to object name on allocated memory.
//         the pointer to "c_strErrorObjectName".(not allocated)
//         NULL(a case of something error occurred)
static char* get_object_name(xmlDocPtr doc, xmlNodePtr node, const char* path)
{
  // Get full path
  xmlChar* fullpath = xmlNodeListGetString(doc, node, 1);
  if(!fullpath){
    S3FS_PRN_ERR("could not get object full path name..");
    return NULL;
  }
  // basepath(path) is as same as fullpath.
  if(0 == strcmp((char*)fullpath, path)){
    xmlFree(fullpath);
    return (char*)c_strErrorObjectName;
  }

  // Make dir path and filename
  string   strdirpath = mydirname(string((char*)fullpath));
  string   strmybpath = mybasename(string((char*)fullpath));
  const char* dirpath = strdirpath.c_str();
  const char* mybname = strmybpath.c_str();
  const char* basepath= (path && '/' == path[0]) ? &path[1] : path;
  xmlFree(fullpath);

  if(!mybname || '\0' == mybname[0]){
    return NULL;
  }

  // check subdir & file in subdir
  if(dirpath && 0 < strlen(dirpath)){
    // case of "/"
    if(0 == strcmp(mybname, "/") && 0 == strcmp(dirpath, "/")){
      return (char*)c_strErrorObjectName;
    }
    // case of "."
    if(0 == strcmp(mybname, ".") && 0 == strcmp(dirpath, ".")){
      return (char*)c_strErrorObjectName;
    }
    // case of ".."
    if(0 == strcmp(mybname, "..") && 0 == strcmp(dirpath, ".")){
      return (char*)c_strErrorObjectName;
    }
    // case of "name"
    if(0 == strcmp(dirpath, ".")){
      // OK
      return strdup(mybname);
    }else{
      if(basepath && 0 == strcmp(dirpath, basepath)){
        // OK
        return strdup(mybname);
      }else if(basepath && 0 < strlen(basepath) && '/' == basepath[strlen(basepath) - 1] && 0 == strncmp(dirpath, basepath, strlen(basepath) - 1)){
        string withdirname = "";
        if(strlen(dirpath) > strlen(basepath)){
          withdirname = &dirpath[strlen(basepath)];
        }
        if(0 < withdirname.length() && '/' != withdirname[withdirname.length() - 1]){
          withdirname += "/";
        }
        withdirname += mybname;
        return strdup(withdirname.c_str());
      }
    }
  }
  // case of something wrong
  return (char*)c_strErrorObjectName;
}

static int remote_mountpath_exists(const char* path)
{
  struct stat stbuf;

  S3FS_PRN_INFO1("[path=%s]", path);

  // getattr will prefix the path with the remote mountpoint
  if(0 != get_object_attribute("/", &stbuf, NULL)){
    return -1;
  }
  if(!S_ISDIR(stbuf.st_mode)){
    return -1;
  }
  return 0;
}


static void free_xattrs(xattrs_t& xattrs)
{
  for(xattrs_t::iterator iter = xattrs.begin(); iter != xattrs.end(); xattrs.erase(iter++)){
    if(iter->second){
      delete iter->second;
    }
  }
}

static bool parse_xattr_keyval(const std::string& xattrpair, string& key, PXATTRVAL& pval)
{
  // parse key and value
  size_t pos;
  string tmpval;
  if(string::npos == (pos = xattrpair.find_first_of(":"))){
    S3FS_PRN_ERR("one of xattr pair(%s) is wrong format.", xattrpair.c_str());
    return false;
  }
  key    = xattrpair.substr(0, pos);
  tmpval = xattrpair.substr(pos + 1);

  if(!takeout_str_dquart(key) || !takeout_str_dquart(tmpval)){
    S3FS_PRN_ERR("one of xattr pair(%s) is wrong format.", xattrpair.c_str());
    return false;
  }

  pval = new XATTRVAL;
  pval->length = 0;
  pval->pvalue = s3fs_decode64(tmpval.c_str(), &pval->length);

  return true;
}

static size_t parse_xattrs(const std::string& strxattrs, xattrs_t& xattrs)
{
  xattrs.clear();

  // decode
  string jsonxattrs = urlDecode(strxattrs);

  // get from "{" to "}"
  string restxattrs;
  {
    size_t startpos = string::npos;
    size_t endpos   = string::npos;
    if(string::npos != (startpos = jsonxattrs.find_first_of("{"))){
      endpos = jsonxattrs.find_last_of("}");
    }
    if(startpos == string::npos || endpos == string::npos || endpos <= startpos){
      S3FS_PRN_WARN("xattr header(%s) is not json format.", jsonxattrs.c_str());
      return 0;
    }
    restxattrs = jsonxattrs.substr(startpos + 1, endpos - (startpos + 1));
  }

  // parse each key:val
  for(size_t pair_nextpos = restxattrs.find_first_of(","); 0 < restxattrs.length(); restxattrs = (pair_nextpos != string::npos ? restxattrs.substr(pair_nextpos + 1) : string("")), pair_nextpos = restxattrs.find_first_of(",")){
    string pair = pair_nextpos != string::npos ? restxattrs.substr(0, pair_nextpos) : restxattrs;
    string    key  = "";
    PXATTRVAL pval = NULL;
    if(!parse_xattr_keyval(pair, key, pval)){
      // something format error, so skip this.
      continue;
    }
    xattrs[key] = pval;
  }
  return xattrs.size();
}

static std::string build_xattrs(const xattrs_t& xattrs)
{
  string strxattrs("{");

  bool is_set = false;
  for(xattrs_t::const_iterator iter = xattrs.begin(); iter != xattrs.end(); ++iter){
    if(is_set){
      strxattrs += ',';
    }else{
      is_set = true;
    }
    strxattrs += '\"';
    strxattrs += iter->first;
    strxattrs += "\":\"";

    if(iter->second){
      char* base64val = s3fs_base64((iter->second)->pvalue, (iter->second)->length);
      if(base64val){
        strxattrs += base64val;
        free(base64val);
      }
    }
    strxattrs += '\"';
  }
  strxattrs += '}';

  strxattrs = urlEncode(strxattrs);

  return strxattrs;
}

static int set_xattrs_to_header(headers_t& meta, const char* name, const char* value, size_t size, int flags)
{
  string   strxattrs;
  xattrs_t xattrs;

  headers_t::iterator iter;
  if(meta.end() == (iter = meta.find("x-amz-meta-xattr"))){
    if(XATTR_REPLACE == (flags & XATTR_REPLACE)){
      // there is no xattr header but flags is replace, so failure.
      return -ENOATTR;
    }
  }else{
    if(XATTR_CREATE == (flags & XATTR_CREATE)){
      // found xattr header but flags is only creating, so failure.
      return -EEXIST;
    }
    strxattrs = iter->second;
  }

  // get map as xattrs_t
  parse_xattrs(strxattrs, xattrs);

  // add name(do not care overwrite and empty name/value)
  xattrs_t::iterator xiter;
  if(xattrs.end() != (xiter = xattrs.find(string(name)))){
    // found same head. free value.
    delete xiter->second;
  }

  PXATTRVAL pval = new XATTRVAL;
  pval->length = size;
  if(0 < size){
    if(NULL == (pval->pvalue = (unsigned char*)malloc(size +1))){
      delete pval;
      free_xattrs(xattrs);
      return -ENOMEM;
    }
    memset(pval->pvalue, 0, size);
    memcpy(pval->pvalue, value, size);
    pval->pvalue[size] = '\0';
  }else{
    pval->pvalue = NULL;
  }
  xattrs[string(name)] = pval;

  // build new strxattrs(not encoded) and set it to headers_t
  meta["x-amz-meta-xattr"] = build_xattrs(xattrs);
  S3FS_PRN_INFO("x-amz-meta-xattr = %s", meta["x-amz-meta-xattr"].c_str());

  free_xattrs(xattrs);

  return 0;
}

#if defined(__APPLE__)
static int s3fs_setxattr(const char* path, const char* name, const char* value, size_t size, int flags, uint32_t position)
#else
static int s3fs_setxattr(const char* path, const char* name, const char* value, size_t size, int flags)
#endif
{
  timeval begin_tv;
  s3fsStatisStart(SET_XATTR_TOTAL, &begin_tv);
    
  S3FS_PRN_INFO("cmd start: [path=%s][name=%s][value=%p][size=%zu][flags=%d]", path, name, value, size, flags);

  if((value && 0 == size) || (!value && 0 < size)){
    S3FS_PRN_ERR("Wrong parameter: value(%p), size(%zu)", value, size);
    return 0;
  }

#if defined(__APPLE__)
  if (position != 0) {
    // No resource fork support
    return -EINVAL;
  }
#endif

  int         result;
  string      strpath;
  string      newpath;
  string      nowcache;
  headers_t   meta;
  struct stat stbuf;

  if(0 == strcmp(path, "/")){
    S3FS_PRN_ERR("Could not change mode for mount point.");
    return -EIO;
  }
  if(0 != (result = check_parent_object_access(path, X_OK))){
    return result;
  }
  if(0 != (result = check_object_owner(path, &stbuf))){
    return result;
  }

  strpath  = path;
  result   = get_object_attribute(strpath.c_str(), NULL, &meta);
  if(0 != result){
    return result;
  }

  // make new header_t
  if(0 != (result = set_xattrs_to_header(meta, name, value, size, flags))){
    return result;
  }

  // set xattr all object
  meta["x-amz-copy-source"]        = urlEncode(service_path + bucket + get_realpath(strpath.c_str()));
  meta["x-amz-metadata-directive"] = "REPLACE";

  if(0 != put_headers(strpath.c_str(), meta, true,"")){
    return -EIO;
  }

  s3fsStatisEnd(SET_XATTR_TOTAL, &begin_tv,path);
  // clear attr stat cache
  IndexCache::getIndexCache()->setFilesizeOrClearGetAttrStat(strpath,0,true);            
  
  return 0;
}

#if defined(__APPLE__)
static int s3fs_getxattr(const char* path, const char* name, char* value, size_t size, uint32_t position)
#else
static int s3fs_getxattr(const char* path, const char* name, char* value, size_t size)
#endif
{    
  timeval begin_tv;
  s3fsStatisStart(GET_XATTR_TOTAL, &begin_tv);
    
  if(!path || !name){
    return -EIO;
  }
#if defined(__APPLE__)
  if (position != 0) {
    // No resource fork support
    return -EINVAL;
  }
#endif
  
  int       result;
  headers_t meta;
  xattrs_t  xattrs;

  // check parent directory attribute.
  if(0 != (result = check_parent_object_access(path, X_OK))){
    return result;
  }

  // get headers
  if(0 != (result = get_object_attribute(path, NULL, &meta))){
    return result;
  }

  // get xattrs
  headers_t::iterator hiter = meta.find("x-amz-meta-xattr");
  if(meta.end() == hiter){
    // object does not have xattrs
    return -ENOATTR;
  }
  string strxattrs = hiter->second;

  parse_xattrs(strxattrs, xattrs);

  // search name
  string             strname = name;
  xattrs_t::iterator xiter   = xattrs.find(strname);
  if(xattrs.end() == xiter){
    // not found name in xattrs
    free_xattrs(xattrs);
    return -ENOATTR;
  }

  // decode
  size_t         length = 0;
  unsigned char* pvalue = NULL;
  if(NULL != xiter->second){
    length = xiter->second->length;
    pvalue = xiter->second->pvalue;
  }

  if(0 < size){
    if(static_cast<size_t>(size) < length){
      // over buffer size
      free_xattrs(xattrs);
      return -ERANGE;
    }
    if(pvalue){
      memcpy(value, pvalue, length);
    }
  }
  free_xattrs(xattrs);

  S3FS_INTF_PRN_LOG("cmd start: [path=%s][name=%s][value=%p][size=%zu],len=%zu",
    path, name, value, size,length);
  s3fsStatisEnd(GET_XATTR_TOTAL, &begin_tv,path);

  return static_cast<int>(length);
}

static int s3fs_listxattr(const char* path, char* list, size_t size)
{
  timeval begin_tv;
  s3fsStatisStart(LIST_XATTR_TOTAL, &begin_tv);
    
  S3FS_PRN_INFO("cmd start: [path=%s][list=%p][size=%zu]", path, list, size);

  if(!path){
    return -EIO;
  }

  int       result;
  headers_t meta;
  xattrs_t  xattrs;

  // check parent directory attribute.
  if(0 != (result = check_parent_object_access(path, X_OK))){
    return result;
  }

  // get headers
  if(0 != (result = get_object_attribute(path, NULL, &meta))){
    return result;
  }

  // get xattrs
  headers_t::iterator iter;
  if(meta.end() == (iter = meta.find("x-amz-meta-xattr"))){
    // object does not have xattrs
    return 0;
  }
  string strxattrs = iter->second;

  parse_xattrs(strxattrs, xattrs);

  // calculate total name length
  size_t total = 0;
  for(xattrs_t::const_iterator iter = xattrs.begin(); iter != xattrs.end(); ++iter){
    if(0 < iter->first.length()){
      total += iter->first.length() + 1;
    }
  }

  if(0 == total){
    free_xattrs(xattrs);
    return 0;
  }

  // check parameters
  if(size <= 0){
    free_xattrs(xattrs);
    return total;
  }
  if(!list || size < total){
    free_xattrs(xattrs);
    return -ERANGE;
  }

  // copy to list
  char* setpos = list;
  for(xattrs_t::const_iterator iter = xattrs.begin(); iter != xattrs.end(); ++iter){
    if(0 < iter->first.length()){
      strcpy(setpos, iter->first.c_str());
      setpos = &setpos[strlen(setpos) + 1];
    }
  }
  free_xattrs(xattrs);

  s3fsStatisEnd(LIST_XATTR_TOTAL, &begin_tv,path);
  return total;
}

static int s3fs_removexattr(const char* path, const char* name)
{
  timeval begin_tv;
  s3fsStatisStart(REMOVE_XATTR_TOTAL, &begin_tv);
    
  S3FS_PRN_INFO("cmd start: [path=%s][name=%s]", path, name);

  if(!path || !name){
    return -EIO;
  }

  int         result;
  string      strpath;
  string      newpath;
  string      nowcache;
  headers_t   meta;
  xattrs_t    xattrs;
  struct stat stbuf;

  if(0 == strcmp(path, "/")){
    S3FS_PRN_ERR("Could not change mode for mount point.");
    return -EIO;
  }
  if(0 != (result = check_parent_object_access(path, X_OK))){
    return result;
  }
  if(0 != (result = check_object_owner(path, &stbuf))){
    return result;
  }

  strpath  = path;
  result   = get_object_attribute(strpath.c_str(), NULL, &meta);
  if(0 != result){
    return result;
  }

  // get xattrs
  headers_t::iterator hiter = meta.find("x-amz-meta-xattr");
  if(meta.end() == hiter){
    // object does not have xattrs
    return -ENOATTR;
  }
  string strxattrs = hiter->second;

  parse_xattrs(strxattrs, xattrs);

  // check name xattrs
  string             strname = name;
  xattrs_t::iterator xiter   = xattrs.find(strname);
  if(xattrs.end() == xiter){
    free_xattrs(xattrs);
    return -ENOATTR;
  }

  // make new header_t after deleting name xattr
  if(xiter->second){
    delete xiter->second;
  }
  xattrs.erase(xiter);

  // build new xattr
  if(!xattrs.empty()){
    meta["x-amz-meta-xattr"] = build_xattrs(xattrs);
  }else{
    meta.erase("x-amz-meta-xattr");
  }

  // set xattr all object
  meta["x-amz-copy-source"]        = urlEncode(service_path + bucket + get_realpath(strpath.c_str()));
  meta["x-amz-metadata-directive"] = "REPLACE";

  if(0 != put_headers(strpath.c_str(), meta, true,"")){
    free_xattrs(xattrs);
    return -EIO;
  }
  StatCache::getStatCacheData()->DelStat(nowcache);

  free_xattrs(xattrs);

  s3fsStatisEnd(REMOVE_XATTR_TOTAL, &begin_tv,path);
  // clear attr stat cache
  IndexCache::getIndexCache()->setFilesizeOrClearGetAttrStat(strpath,0,true);            
  
  return 0;
}

// s3fs_init calls this function to exit cleanly from the fuse event loop.
//
// There's no way to pass an exit status to the high-level event loop API, so
// this function stores the exit value in a global for main()
static void s3fs_exit_fuseloop(int exit_status) {
    S3FS_PRN_ERR("Exiting FUSE event loop due to errors\n");
    s3fs_init_deferred_exit_status = exit_status;
    struct fuse_context *ctx = fuse_get_context();
    if (NULL != ctx) {
        fuse_exit(ctx->fuse);
    }
}

static void* s3fs_init(struct fuse_conn_info* conn)
{
  S3FS_PRN_INIT_INFO("init v%s(commit:%s) with %s", VERSION, COMMIT_HASH_VAL, s3fs_crypt_lib_name());
  g_s3fs_start_flag = true;
  if ( 0 != EnableLogService(obsfs_log_file_path,obsfs_log_file_name))
  {
      S3FS_PRN_EXIT("EnableLogService fail.");
      s3fs_exit_fuseloop(EXIT_FAILURE);
      return NULL;
  }
  HwsFdManager::GetInstance().StartDaemonThread();
  //check configure
  HwsConfigure::GetInstance().startThread();

  // cache(remove cache dirs at first)
  if(is_remove_cache && (!CacheFileStat::DeleteCacheFileStatDirectory() || !FdManager::DeleteCacheDirectory())){
      S3FS_PRN_EXIT("Could not initialize cache directory.");
      s3fs_exit_fuseloop(EXIT_FAILURE);
      return NULL;
  }

  // ssl init
  if(!s3fs_init_global_ssl()){
    S3FS_PRN_CRIT("could not initialize for ssl libraries.");
    s3fs_exit_fuseloop(EXIT_FAILURE);
    return NULL;
  }

  // init curl
  if(!S3fsCurl::InitS3fsCurl("/etc/mime.types")){
    S3FS_PRN_CRIT("Could not initiate curl library.");
    s3fs_exit_fuseloop(EXIT_FAILURE);
    return NULL;
  }

  // check loading IAM role name
  if(load_iamrole){
    // load IAM role name from http://169.254.169.254/latest/meta-data/iam/security-credentials
    //
    S3fsCurl s3fscurl;
    if(!s3fscurl.LoadIAMRoleFromMetaData()){
      S3FS_PRN_CRIT("could not load IAM role name from meta data.");
      s3fs_exit_fuseloop(EXIT_FAILURE);
      return NULL;
    }
    S3FS_PRN_INFO("loaded IAM role name = %s", S3fsCurl::GetIAMRole());
  }

  if (create_bucket){
    int result = do_create_bucket();
    if(result != 0){
      s3fs_exit_fuseloop(result);
      return NULL;
    }
  }

  // Check Bucket
  {
    int result;
    if(EXIT_SUCCESS != (result = s3fs_check_service())){
      s3fs_exit_fuseloop(result);
      return NULL;
    }
  }

  // Investigate system capabilities
  #ifndef __APPLE__
  if((unsigned int)conn->capable & FUSE_CAP_ATOMIC_O_TRUNC){
     conn->want |= FUSE_CAP_ATOMIC_O_TRUNC;
  }
  #endif

  g_thread_daemon_ = std::thread(hws_daemon_task);
  g_thread_deamon_start = true;

  return NULL;
}

static void s3fs_destroy(void*)
{
  S3FS_PRN_INFO("destroy");


  // Destroy curl
  if(!S3fsCurl::DestroyS3fsCurl()){
    S3FS_PRN_WARN("Could not release curl library.");
  }
  // cache(remove at last)
  if(is_remove_cache && (!CacheFileStat::DeleteCacheFileStatDirectory() || !FdManager::DeleteCacheDirectory())){
    S3FS_PRN_WARN("Could not remove cache directory.");
  }
  // ssl
  s3fs_destroy_global_ssl();

  //stop all thread
  g_s3fs_start_flag = false;
  if (g_thread_deamon_start && g_thread_daemon_.joinable())
  {
      S3FS_PRN_INFO("ak sk deamon thread join");  
      g_thread_daemon_.join();
  }
  DisableLogService();
}

static int s3fs_access_hw_obs(const char* path, int mask)
{
  timeval begin_tv;
  s3fsStatisStart(ACCESS_TOTAL, &begin_tv);
    
  S3FS_PRN_INFO("cmd start: [path=%s][mask=%s%s%s%s]", path,
          ((mask & R_OK) == R_OK) ? "R_OK " : "",
          ((mask & W_OK) == W_OK) ? "W_OK " : "",
          ((mask & X_OK) == X_OK) ? "X_OK " : "",
          (mask == F_OK) ? "F_OK" : "");

  int result = check_object_access(path, mask, NULL, NULL);
  s3fsStatisEnd(ACCESS_TOTAL, &begin_tv,path);

  return result;
}

static xmlChar* get_exp_value_xml(xmlDocPtr doc, xmlXPathContextPtr ctx, const char* exp_key)
{
  if(!doc || !ctx || !exp_key){
    return NULL;
  }

  xmlXPathObjectPtr exp;
  xmlNodeSetPtr     exp_nodes;
  xmlChar*          exp_value;

  // search exp_key tag
  if(NULL == (exp = xmlXPathEvalExpression((xmlChar*)exp_key, ctx))){
    S3FS_PRN_ERR("Could not find key(%s).", exp_key);
    return NULL;
  }
  if(xmlXPathNodeSetIsEmpty(exp->nodesetval)){
    S3FS_PRN_ERR("Key(%s) node is empty.", exp_key);
    S3FS_XMLXPATHFREEOBJECT(exp);
    return NULL;
  }
  // get exp_key value & set in struct
  exp_nodes = exp->nodesetval;
  if(NULL == (exp_value = xmlNodeListGetString(doc, exp_nodes->nodeTab[0]->xmlChildrenNode, 1))){
    S3FS_PRN_ERR("Key(%s) value is empty.", exp_key);
    S3FS_XMLXPATHFREEOBJECT(exp);
    return NULL;
  }

  S3FS_XMLXPATHFREEOBJECT(exp);
  return exp_value;
}

static void print_uncomp_mp_list(uncomp_mp_list_t& list)
{
  printf("\n");
  printf("Lists the parts that have been uploaded for a specific multipart upload.\n");
  printf("\n");

  if(!list.empty()){
    printf("---------------------------------------------------------------\n");

    int cnt = 0;
    for(uncomp_mp_list_t::iterator iter = list.begin(); iter != list.end(); ++iter, ++cnt){
      printf(" Path     : %s\n", (*iter).key.c_str());
      printf(" UploadId : %s\n", (*iter).id.c_str());
      printf(" Date     : %s\n", (*iter).date.c_str());
      printf("\n");
    }
    printf("---------------------------------------------------------------\n");

  }else{
    printf("There is no list.\n");
  }
}

static bool abort_uncomp_mp_list(uncomp_mp_list_t& list)
{
  char buff[1024];

  if(list.empty()){
    return true;
  }
  memset(buff, 0, sizeof(buff));

  // confirm
  while(true){
    printf("Would you remove all objects? [Y/N]\n");
    if(NULL != fgets(buff, sizeof(buff), stdin)){
      if(0 == strcasecmp(buff, "Y\n") || 0 == strcasecmp(buff, "YES\n")){
        break;
      }else if(0 == strcasecmp(buff, "N\n") || 0 == strcasecmp(buff, "NO\n")){
        return true;
      }
      printf("*** please put Y(yes) or N(no).\n");
    }
  }

  // do removing their.
  S3fsCurl s3fscurl;
  bool     result = true;
  for(uncomp_mp_list_t::iterator iter = list.begin(); iter != list.end(); ++iter){
    const char* tpath     = (*iter).key.c_str();
    string      upload_id = (*iter).id;

    if(0 != s3fscurl.AbortMultipartUpload(tpath, upload_id)){
      S3FS_PRN_EXIT("Failed to remove %s multipart uploading object.", tpath);
      result = false;
    }else{
      printf("Succeed to remove %s multipart uploading object.\n", tpath);
    }

    // reset(initialize) curl object
    s3fscurl.DestroyCurlHandle();
  }

  return result;
}

static bool get_uncomp_mp_list(xmlDocPtr doc, uncomp_mp_list_t& list)
{
  if(!doc){
    return false;
  }

  xmlXPathContextPtr ctx = xmlXPathNewContext(doc);;

  string xmlnsurl;
  string ex_upload = "//";
  string ex_key    = "";
  string ex_id     = "";
  string ex_date   = "";

  if(!noxmlns && GetXmlNsUrl(doc, xmlnsurl)){
    xmlXPathRegisterNs(ctx, (xmlChar*)"s3", (xmlChar*)xmlnsurl.c_str());
    ex_upload += "s3:";
    ex_key    += "s3:";
    ex_id     += "s3:";
    ex_date   += "s3:";
  }
  ex_upload += "Upload";
  ex_key    += "Key";
  ex_id     += "UploadId";
  ex_date   += "Initiated";

  // get "Upload" Tags
  xmlXPathObjectPtr  upload_xp;
  if(NULL == (upload_xp = xmlXPathEvalExpression((xmlChar*)ex_upload.c_str(), ctx))){
    S3FS_PRN_ERR("xmlXPathEvalExpression returns null.");
    return false;
  }
  if(xmlXPathNodeSetIsEmpty(upload_xp->nodesetval)){
    S3FS_PRN_INFO("upload_xp->nodesetval is empty.");
    S3FS_XMLXPATHFREEOBJECT(upload_xp);
    S3FS_XMLXPATHFREECONTEXT(ctx);
    return true;
  }

  // Make list
  int           cnt;
  xmlNodeSetPtr upload_nodes;
  list.clear();
  for(cnt = 0, upload_nodes = upload_xp->nodesetval; cnt < upload_nodes->nodeNr; cnt++){
    ctx->node = upload_nodes->nodeTab[cnt];

    UNCOMP_MP_INFO  part;
    xmlChar*        ex_value;

    // search "Key" tag
    if(NULL == (ex_value = get_exp_value_xml(doc, ctx, ex_key.c_str()))){
      continue;
    }
    if('/' != *((char*)ex_value)){
      part.key = "/";
    }else{
      part.key = "";
    }
    part.key += (char*)ex_value;
    S3FS_XMLFREE(ex_value);

    // search "UploadId" tag
    if(NULL == (ex_value = get_exp_value_xml(doc, ctx, ex_id.c_str()))){
      continue;
    }
    part.id = (char*)ex_value;
    S3FS_XMLFREE(ex_value);

    // search "Initiated" tag
    if(NULL == (ex_value = get_exp_value_xml(doc, ctx, ex_date.c_str()))){
      continue;
    }
    part.date = (char*)ex_value;
    S3FS_XMLFREE(ex_value);

    list.push_back(part);
  }

  S3FS_XMLXPATHFREEOBJECT(upload_xp);
  S3FS_XMLXPATHFREECONTEXT(ctx);

  return true;
}

static int s3fs_utility_mode(void)
{
  if(!utility_mode){
    return EXIT_FAILURE;
  }

  // ssl init
  if(!s3fs_init_global_ssl()){
    S3FS_PRN_EXIT("could not initialize for ssl libraries.");
    return EXIT_FAILURE;
  }

  // init curl
  if(!S3fsCurl::InitS3fsCurl("/etc/mime.types")){
    S3FS_PRN_EXIT("Could not initiate curl library.");
    s3fs_destroy_global_ssl();
    return EXIT_FAILURE;
  }

  printf("Utility Mode\n");

  S3fsCurl s3fscurl;
  string   body;
  int      result = EXIT_SUCCESS;
  if(0 != s3fscurl.MultipartListRequest(body)){
    S3FS_PRN_EXIT("Could not get list multipart upload.");
    result = EXIT_FAILURE;
  }else{
    // parse result(incomplete multipart upload information)
    S3FS_PRN_DBG("response body = {\n%s\n}", body.c_str());

    xmlDocPtr doc;
    if(NULL == (doc = xmlReadMemory(body.c_str(), static_cast<int>(body.size()), "", NULL, 0))){
      S3FS_PRN_DBG("xmlReadMemory exited with error.");
      result = EXIT_FAILURE;

    }else{
      // make working uploads list
      uncomp_mp_list_t list;
      if(!get_uncomp_mp_list(doc, list)){
        S3FS_PRN_DBG("get_uncomp_mp_list exited with error.");
        result = EXIT_FAILURE;

      }else{
        // print list
        print_uncomp_mp_list(list);
        // remove
        if(!abort_uncomp_mp_list(list)){
          S3FS_PRN_DBG("an error occurred during removal process.");
          result = EXIT_FAILURE;
        }
      }
      S3FS_XMLFREEDOC(doc);
    }
  }

  // Destroy curl
  if(!S3fsCurl::DestroyS3fsCurl()){
    S3FS_PRN_WARN("Could not release curl library.");
  }

  // ssl
  s3fs_destroy_global_ssl();

  return result;
}

//
// If calling with wrong region, s3fs gets following error body as 400 error code.
// "<Error><Code>AuthorizationHeaderMalformed</Code><Message>The authorization header is
//  malformed; the region 'us-east-1' is wrong; expecting 'ap-northeast-1'</Message>
//  <Region>ap-northeast-1</Region><RequestId>...</RequestId><HostId>...</HostId>
//  </Error>"
//
// So this is cheep codes but s3fs should get correct region automatically.
//
static bool check_region_error(const char* pbody, string& expectregion)
{
  if(!pbody){
    return false;
  }
  const char* region;
  const char* regionend;
  if(NULL == (region = strcasestr(pbody, "<Message>The authorization header is malformed; the region "))){
    return false;
  }
  if(NULL == (region = strcasestr(region, "expecting \'"))){
    return false;
  }
  region += strlen("expecting \'");
  if(NULL == (regionend = strchr(region, '\''))){
    return false;
  }
  string strtmp(region, (regionend - region));
  if(0 == strtmp.length()){
    return false;
  }
  expectregion = strtmp;

  return true;
}

static int s3fs_check_service(void)
{
  S3FS_PRN_INFO("check services.");

  // At first time for access S3, we check IAM role if it sets.
  if(!S3fsCurl::CheckIAMCredentialUpdate()){
    S3FS_PRN_CRIT("Failed to check IAM role name(%s).", S3fsCurl::GetIAMRole());
    return EXIT_FAILURE;
  }

  S3fsCurl s3fscurl;
  int      res;
  if(0 > (res = s3fscurl.CheckBucket())){
    // get response code
    long responseCode = s3fscurl.GetLastResponseCode();

    // check wrong endpoint, and automatically switch endpoint
    if(responseCode == 400 && !is_specified_endpoint){
      // check region error
      BodyData* body = s3fscurl.GetBodyData();
      string    expectregion;
      if(check_region_error(body->str(), expectregion)){
        // not specified endpoint, so try to connect to expected region.
        S3FS_PRN_CRIT("Could not connect wrong region %s, so retry to connect region %s.", endpoint.c_str(), expectregion.c_str());
        endpoint = expectregion;
        if(S3fsCurl::IsSignatureV4()){
            if(host == "http://s3.amazonaws.com"){
                host = "http://s3-" + endpoint + ".amazonaws.com";
            }else if(host == "https://s3.amazonaws.com"){
                host = "https://s3-" + endpoint + ".amazonaws.com";
            }
        }

        // retry to check with new endpoint
        s3fscurl.DestroyCurlHandle();
        res          = s3fscurl.CheckBucket();
        responseCode = s3fscurl.GetLastResponseCode();
      }
    }

    // try signature v2
    if(0 > res && (responseCode == 400 || responseCode == 403) && S3fsCurl::IsSignatureV4()){
      // switch sigv2
      S3FS_PRN_WARN("Could not connect, so retry to connect by signature version 2.");
      S3fsCurl::SetSignatureV4(false);

      // retry to check with sigv2
      s3fscurl.DestroyCurlHandle();
      res          = s3fscurl.CheckBucket();
      responseCode = s3fscurl.GetLastResponseCode();
    }

    // check errors(after retrying)
    if(0 > res && responseCode != 200 && responseCode != 301){
      if(responseCode == 400){
        S3FS_PRN_CRIT("Bad Request(host=%s) - result of checking service.", host.c_str());

      }else if(responseCode == 403){
        S3FS_PRN_CRIT("invalid credentials(host=%s) - result of checking service.", host.c_str());

      }else if(responseCode == 404){
        S3FS_PRN_CRIT("bucket not found(host=%s) - result of checking service.", host.c_str());

      }else if(responseCode == CURLE_OPERATION_TIMEDOUT){
        // unable to connect
        S3FS_PRN_CRIT("unable to connect bucket and timeout(host=%s) - result of checking service.", host.c_str());
      }else{
        // another error
        S3FS_PRN_CRIT("unable to connect(host=%s) - result of checking service.", host.c_str());
      }
      return EXIT_FAILURE;
    }
  }

  // make sure remote mountpath exists and is a directory
  if(mount_prefix.size() > 0){
    if(s3fscurl.getMountPrefixInode() != 0){
      S3FS_PRN_CRIT("get mountpath %s RootInodeNo failed.", mount_prefix.c_str());
      return EXIT_FAILURE;
    }
    if(remote_mountpath_exists(mount_prefix.c_str()) != 0){
      S3FS_PRN_CRIT("remote mountpath %s not found.", mount_prefix.c_str());
      return EXIT_FAILURE;
    }
  }
  S3FS_MALLOCTRIM(0);

  return EXIT_SUCCESS;
}

//
// Read and Parse passwd file
//
// The line of the password file is one of the following formats:
//   (1) "accesskey:secretkey"         : AWS format for default(all) access key/secret key
//   (2) "bucket:accesskey:secretkey"  : AWS format for bucket's access key/secret key
//   (3) "key=value"                   : Content-dependent KeyValue contents
//
// This function sets result into bucketkvmap_t, it bucket name and key&value mapping.
// If bucket name is empty(1 or 3 format), bucket name for mapping is set "\t" or "".
//
// Return:  1 - OK(could parse and set mapping etc.)
//          0 - NG(could not read any value)
//         -1 - Should shutdown immediately
//
static int parse_passwd_file(bucketkvmap_t& resmap)
{
  string line;
  size_t first_pos;
  size_t last_pos;
  readline_t linelist;
  readline_t::iterator iter;

  // open passwd file
  ifstream PF(passwd_file.c_str());
  if(!PF.good()){
    S3FS_PRN_EXIT("could not open passwd file : %s", passwd_file.c_str());
    return -1;
  }

  // read each line
  while(getline(PF, line)){
    line = trim(line);
    if(0 == line.size()){
      continue;
    }
    if('#' == line[0]){
      continue;
    }
    if(string::npos != line.find_first_of(" \t")){
      S3FS_PRN_EXIT("invalid line in passwd file, found whitespace character.");
      return -1;
    }
    if(0 == line.find_first_of("[")){
      S3FS_PRN_EXIT("invalid line in passwd file, found a bracket \"[\" character.");
      return -1;
    }
    linelist.push_back(line);
  }

  // read '=' type
  kvmap_t kv;
  for(iter = linelist.begin(); iter != linelist.end(); ++iter){
    first_pos = iter->find_first_of("=");
    if(first_pos == string::npos){
      continue;
    }
    // formatted by "key=val"
    string key = trim(iter->substr(0, first_pos));
    string val = trim(iter->substr(first_pos + 1, string::npos));
    if(key.empty()){
      continue;
    }
    if(kv.end() != kv.find(key)){
      S3FS_PRN_WARN("same key name(%s) found in passwd file, skip this.", key.c_str());
      continue;
    }
    kv[key] = val;
  }
  // set special key name
  resmap[string(keyval_fields_type)] = kv;

  // read ':' type
  for(iter = linelist.begin(); iter != linelist.end(); ++iter){
    first_pos = iter->find_first_of(":");
    last_pos  = iter->find_last_of(":");
    if(first_pos == string::npos){
      continue;
    }
    string bucket;
    string accesskey;
    string secret;
    if(first_pos != last_pos){
      // formatted by "bucket:accesskey:secretkey"
      bucket    = trim(iter->substr(0, first_pos));
      accesskey = trim(iter->substr(first_pos + 1, last_pos - first_pos - 1));
      secret    = trim(iter->substr(last_pos + 1, string::npos));
    }else{
      // formatted by "accesskey:secretkey"
      bucket    = allbucket_fields_type;
      accesskey = trim(iter->substr(0, first_pos));
      secret    = trim(iter->substr(first_pos + 1, string::npos));
    }
    if(resmap.end() != resmap.find(bucket)){
      S3FS_PRN_EXIT("same bucket(%s) passwd setting found in passwd file.", ("" == bucket ? "default" : bucket.c_str()));
      return -1;
    }
    kv.clear();
    kv[string(aws_accesskeyid)] = accesskey;
    kv[string(aws_secretkey)]   = secret;
    resmap[bucket]              = kv;
  }
  return (resmap.empty() ? 0 : 1);
}

//
// Return:  1 - OK(could read and set accesskey etc.)
//          0 - NG(could not read)
//         -1 - Should shutdown immediately
//
static int check_for_aws_format(const kvmap_t& kvmap)
{
  string str1(aws_accesskeyid);
  string str2(aws_secretkey);
  bool IsSame;
  if(kvmap.empty()){
    return 0;
  }
  if(kvmap.end() == kvmap.find(str1) && kvmap.end() == kvmap.find(str2)){
    return 0;
  }
  if(kvmap.end() == kvmap.find(str1) || kvmap.end() == kvmap.find(str2)){
    S3FS_PRN_EXIT("AWSAccesskey or AWSSecretkey is not specified.");
    return -1;
  }
  IsSame = is_aksk_same(kvmap.at(str1).c_str(), kvmap.at(str2).c_str());

  if(!IsSame){
    if(!S3fsCurl::SetAccessKey(kvmap.at(str1).c_str(), kvmap.at(str2).c_str())){
      S3FS_PRN_WARN("failed to set access key/secret key.");
      return -1;
    }
    return 1;
  }

  return 0;
}

//
// check_passwd_file_perms
//
// expect that global passwd_file variable contains
// a non-empty value and is readable by the current user
//
// Check for too permissive access to the file
// help save users from themselves via a security hole
//
// only two options: return or error out
//
static int check_passwd_file_perms(void)
{
  struct stat info;

  // let's get the file info
  if(stat(passwd_file.c_str(), &info) != 0){
    S3FS_PRN_INFO("unexpected error from stat(%s).", passwd_file.c_str());
    return EXIT_FAILURE;
  }

  // return error if any file has others permissions
  if( (info.st_mode & S_IROTH) ||
      (info.st_mode & S_IWOTH) ||
      (info.st_mode & S_IXOTH)) {
    S3FS_PRN_EXIT("credentials file %s should not have others permissions.", passwd_file.c_str());
    return EXIT_FAILURE;
  }

  // Any local file should not have any group permissions
  // /etc/passwd-s3fs can have group permissions
  if(passwd_file != "/etc/passwd-s3fs"){
    if( (info.st_mode & S_IRGRP) ||
        (info.st_mode & S_IWGRP) ||
        (info.st_mode & S_IXGRP)) {
      S3FS_PRN_EXIT("credentials file %s should not have group permissions.", passwd_file.c_str());
      return EXIT_FAILURE;
    }
  }else{
    // "/etc/passwd-s3fs" does not allow group write.
    if((info.st_mode & S_IWGRP)){
      S3FS_PRN_EXIT("credentials file %s should not have group writable permissions.", passwd_file.c_str());
      return EXIT_FAILURE;
    }
  }
  if((info.st_mode & S_IXUSR) || (info.st_mode & S_IXGRP)){
    S3FS_PRN_EXIT("credentials file %s should not have executable permissions.", passwd_file.c_str());
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
//
// the task updata the new ak/sk in a thread
//
static int update_aksk(void)
{
    string strAKOld = "";
    string strSKOld = "";

    if(EXIT_SUCCESS != read_passwd_file()){
        S3FS_PRN_INFO("failed to Get access key/secret key from passwd file.");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
//
// compare the new ak/sk and the old
//
static bool is_aksk_same(const string strAccessKey, const string strSecretKey)
{
    string strAKOld = "";
    string strSKOld = "";

    if(strAccessKey == "" || strSecretKey == ""){
        S3FS_PRN_WARN(" access key or secret key  is not specified.");
        return false;
    }
    if(!S3fsCurl::GetAccessKey(strAKOld, strSKOld)){
        S3FS_PRN_WARN("failed to Get access key/secret key from curl.");
        return false;
    }

    S3FS_PRN_DBG("success to Get access key = %s,secret key  = %s .from passwd curl.", strAKOld.data(), strSKOld.data());
    if(strAKOld != strAccessKey || strSKOld != strSecretKey){
        return false;
    }
    else{
        return true;
    }
}

static bool write_passwd_change_file(const string strAccessKey, const string strSecretKey)
{
    string   databuf = strAccessKey + ":" + strSecretKey;
    uint64_t pid     = (long long)getpid();

    char     process_id_str[PASSWD_CHANGE_FILENAME_MAX_LEN] = {0};
    snprintf(process_id_str, sizeof(process_id_str), "%ld", pid);

    string   passwd_change_file = passwd_file + process_id_str + ".changlog";

    int fhandle = open((const char*)(passwd_change_file.c_str()), O_RDWR | O_TRUNC| O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP);
    if (fhandle < 0)
    {
        S3FS_PRN_WARN("open file %s failed !", passwd_change_file.c_str())
        return false;
    }

    int len = write(fhandle, databuf.c_str(), databuf.length());
    if (len < 0)
    {
        S3FS_PRN_WARN("write file %s failed, actual write %d, expected %lu", passwd_change_file.c_str(), len, databuf.length());
        close(fhandle);
        return false;
    }

    S3FS_PRN_INFO("write file %s success", passwd_change_file.c_str());
    close(fhandle);
    return true;
}
//
// read_passwd_file
//
// Support for per bucket credentials
//
// Format for the credentials file:
// [bucket:]AccessKeyId:SecretAccessKey
//
// Lines beginning with # are considered comments
// and ignored, as are empty lines
//
// Uncommented lines without the ":" character are flagged as
// an error, so are lines with spaces or tabs
//
// only one default key pair is allowed, but not required
//
static int read_passwd_file(void)
{
  bucketkvmap_t bucketmap;
  kvmap_t       keyval;
  int           result;
  bool          IsSame;

  // if you got here, the password file
  // exists and is readable by the
  // current user, check for permissions
  if(EXIT_SUCCESS != check_passwd_file_perms()){
    return EXIT_FAILURE;
  }

  //
  // parse passwd file
  //
  result = parse_passwd_file(bucketmap);
  if(-1 == result){
     return EXIT_FAILURE;
  }

  //
  // check key=value type format.
  //
  if(bucketmap.end() != bucketmap.find(keyval_fields_type)){
    // aws format
    result = check_for_aws_format(bucketmap[keyval_fields_type]);
    if(-1 == result){
       return EXIT_FAILURE;
    }else if(1 == result){
       // success to set
       return EXIT_SUCCESS;
    }
  }

  string bucket_key = allbucket_fields_type;
  if(0 < bucket.size() && bucketmap.end() != bucketmap.find(bucket)){
    bucket_key = bucket;
  }
  if(bucketmap.end() == bucketmap.find(bucket_key)){
    S3FS_PRN_EXIT("Not found access key/secret key in passwd file.");
    return EXIT_FAILURE;
  }
  keyval = bucketmap[bucket_key];
  if(keyval.end() == keyval.find(string(aws_accesskeyid)) || keyval.end() == keyval.find(string(aws_secretkey))){
    S3FS_PRN_EXIT("Not found access key/secret key in passwd file.");
    return EXIT_FAILURE;
  }
  IsSame = is_aksk_same(keyval.at(string(aws_accesskeyid)).c_str(), keyval.at(string(aws_secretkey)).c_str());

  if(!IsSame){
    if(!S3fsCurl::SetAccessKey(keyval.at(string(aws_accesskeyid)).c_str(), keyval.at(string(aws_secretkey)).c_str())){
        S3FS_PRN_WARN("failed to set internal data for access key/secret key from passwd file.");
        return EXIT_FAILURE;
    }
    if(!write_passwd_change_file(keyval.at(string(aws_accesskeyid)).c_str(), keyval.at(string(aws_secretkey)).c_str()))
    {
       S3FS_PRN_WARN("faild to write passwd changelog file");
       return EXIT_FAILURE;
    }
  }

  return EXIT_SUCCESS;
}

//
// get_access_keys
//
// called only when were are not mounting a
// public bucket
//
// Here is the order precedence for getting the
// keys:
//
// 1 - from a password file specified on the command line
// 2 - from the users ~/.passwd-s3fs
// 3 - from /etc/passwd-s3fs
//
static int get_access_keys(void)
{
  // should be redundant
  if(S3fsCurl::IsPublicBucket()){
     return EXIT_SUCCESS;
  }

  // access key loading is deferred
  if(load_iamrole || is_ecs){
     return EXIT_SUCCESS;
  }
  // 1 - was specified on the command line
  if(passwd_file.size() > 0){
    ifstream PF(passwd_file.c_str());
    if(PF.good()){
       PF.close();
       return read_passwd_file();
    }else{
      S3FS_PRN_EXIT("specified passwd_file is not readable.");
      return EXIT_FAILURE;
    }
  }

  // 2 - from the default location in the users home directory
  char * HOME;
  HOME = getenv ("HOME");
  if(HOME != NULL){
     passwd_file.assign(HOME);
     passwd_file.append("/.passwd-s3fs");
     ifstream PF(passwd_file.c_str());
     if(PF.good()){
       PF.close();
       if(EXIT_SUCCESS != read_passwd_file()){
         return EXIT_FAILURE;
       }
       // It is possible that the user's file was there but
       // contained no key pairs i.e. commented out
       // in that case, go look in the final location
       if(S3fsCurl::IsSetAccessKeys()){
          return EXIT_SUCCESS;
       }
     }
   }

  // 3 - from the system default location
  passwd_file.assign("/etc/passwd-s3fs");
  ifstream PF(passwd_file.c_str());
  if(PF.good()){
    PF.close();
    return read_passwd_file();
  }
  S3FS_PRN_EXIT("could not determine how to establish security credentials.");

  return EXIT_FAILURE;
}

//
// Check & Set attributes for mount point.
//
static int set_mountpoint_attribute(struct stat& mpst)
{
  mp_uid  = geteuid();
  mp_gid  = getegid();
  mp_mode = S_IFDIR | (allow_other ? (is_mp_umask ? (~mp_umask & (S_IRWXU | S_IRWXG | S_IRWXO)) : (S_IRWXU | S_IRWXG | S_IRWXO)) : S_IRWXU);

  S3FS_PRN_INFO2("PROC(uid=%u, gid=%u) - MountPoint(uid=%u, gid=%u, mode=%04o)",
         (unsigned int)mp_uid, (unsigned int)mp_gid, (unsigned int)(mpst.st_uid), (unsigned int)(mpst.st_gid), mpst.st_mode);

  mp_mtime = mpst.st_mtime;
  mp_st_size = mpst.st_size;
  S3FS_PRN_INFO2("set mountpoint mtime:%s,st_size(%ld)",ctime(&mp_mtime), mp_st_size);
  // check owner
  if(0 == mp_uid || mpst.st_uid == mp_uid){
    return true;
  }
  // check group permission
  if(mpst.st_gid == mp_gid || 1 == is_uid_include_group(mp_uid, mpst.st_gid)){
    if(S_IRWXG == (mpst.st_mode & S_IRWXG)){
      return true;
    }
  }
  // check other permission
  if(S_IRWXO == (mpst.st_mode & S_IRWXO)){
    return true;
  }

  return false;
}

//
// Set bucket and mount_prefix based on passed bucket name.
//
static int set_bucket(const char* arg)
{
  char *bucket_name = (char*)arg;
  if(strstr(arg, ":")){
    if(strstr(arg, "://")){
      S3FS_PRN_EXIT("bucket name and path(\"%s\") is wrong, it must be \"bucket[:/path]\".", arg);
      return -1;
    }
    bucket = strtok(bucket_name, ":");
    char* pmount_prefix = strtok(NULL, ":");
    if(pmount_prefix){
      if(0 == strlen(pmount_prefix) || '/' != pmount_prefix[0]){
        S3FS_PRN_EXIT("path(%s) must be prefix \"/\".", pmount_prefix);
        return -1;
      }
      mount_prefix = pmount_prefix;
      // remove trailing slash
      if(mount_prefix.at(mount_prefix.size() - 1) == '/'){
        mount_prefix = mount_prefix.substr(0, mount_prefix.size() - 1);
      }
    }
  }else{
    bucket = arg;
  }
  return 0;
}


// This is repeatedly called by the fuse option parser
// if the key is equal to FUSE_OPT_KEY_OPT, it's an option passed in prefixed by
// '-' or '--' e.g.: -f -d -ousecache=/tmp
//
// if the key is equal to FUSE_OPT_KEY_NONOPT, it's either the bucket name
//  or the mountpoint. The bucket name will always come before the mountpoint
static int my_fuse_opt_proc(void* data, const char* arg, int key, struct fuse_args* outargs)
{
  int ret;
  if(key == FUSE_OPT_KEY_NONOPT){
    // the first NONOPT option is the bucket name
    if(bucket.size() == 0){
      if ((ret = set_bucket(arg))){
        return ret;
      }
      return 0;
    }
    else if (!strcmp(arg, "s3fs")) {
      return 0;
    }

    // the second NONPOT option is the mountpoint(not utility mode)
    if(0 == mountpoint.size() && 0 == utility_mode){
      // save the mountpoint and do some basic error checking
      mountpoint = arg;
      struct stat stbuf;

      if(stat(arg, &stbuf) == -1){
        S3FS_PRN_EXIT("unable to access MOUNTPOINT %s: %s", mountpoint.c_str(), strerror(errno));
        return -1;
      }
      if(!(S_ISDIR(stbuf.st_mode))){
        S3FS_PRN_EXIT("MOUNTPOINT: %s is not a directory.", mountpoint.c_str());
        return -1;
      }
      if(!set_mountpoint_attribute(stbuf)){
        S3FS_PRN_EXIT("MOUNTPOINT: %s permission denied.", mountpoint.c_str());
        return -1;
      }

      if(!nonempty){
        struct dirent *ent;
        DIR *dp = opendir(mountpoint.c_str());
        if(dp == NULL){
          S3FS_PRN_EXIT("failed to open MOUNTPOINT: %s: %s", mountpoint.c_str(), strerror(errno));
          return -1;
        }
        while((ent = readdir(dp)) != NULL){
          if(strcmp(ent->d_name, ".") != 0 && strcmp(ent->d_name, "..") != 0){
            closedir(dp);
            S3FS_PRN_EXIT("MOUNTPOINT directory %s is not empty. if you are sure this is safe, can use the 'nonempty' mount option.", mountpoint.c_str());
            return -1;
          }
        }
        closedir(dp);
      }
      return 1;
    }

    // Unknown option
    if(0 == utility_mode){
      S3FS_PRN_EXIT("specified unknown third optioni(%s).", arg);
    }else{
      S3FS_PRN_EXIT("specified unknown second optioni(%s). you don't need to specify second option(mountpoint) for utility mode(-u).", arg);
    }
    return -1;

  }else if(key == FUSE_OPT_KEY_OPT){
    if(0 == STR2NCMP(arg, "uid=")){
      s3fs_uid = get_uid(strchr(arg, '=') + sizeof(char));
      if(0 != geteuid() && 0 == s3fs_uid){
        S3FS_PRN_EXIT("root user can only specify uid=0.");
        return -1;
      }
      is_s3fs_uid = true;
      return 1; // continue for fuse option
    }
    if(0 == STR2NCMP(arg, "gid=")){
      s3fs_gid = get_gid(strchr(arg, '=') + sizeof(char));
      if(0 != getegid() && 0 == s3fs_gid){
        S3FS_PRN_EXIT("root user can only specify gid=0.");
        return -1;
      }
      is_s3fs_gid = true;
      return 1; // continue for fuse option
    }
    if(0 == STR2NCMP(arg, "umask=")){
      s3fs_umask = strtol(strchr(arg, '=') + sizeof(char), NULL, 0);
      s3fs_umask &= (S_IRWXU | S_IRWXG | S_IRWXO);
      is_s3fs_umask = true;
      return 1; // continue for fuse option
    }
    if(0 == strcmp(arg, "allow_other")){
      allow_other = true;
      return 1; // continue for fuse option
    }
    if(0 == STR2NCMP(arg, "mp_umask=")){
      mp_umask = strtol(strchr(arg, '=') + sizeof(char), NULL, 0);
      mp_umask &= (S_IRWXU | S_IRWXG | S_IRWXO);
      is_mp_umask = true;
      return 0;
    }
    if(0 == STR2NCMP(arg, "default_acl=")){
      const char* acl = strchr(arg, '=') + sizeof(char);
      S3fsCurl::SetDefaultAcl(acl);
      return 0;
    }
    if(0 == STR2NCMP(arg, "retries=")){
      S3fsCurl::SetRetries(static_cast<int>(s3fs_strtoofft(strchr(arg, '=') + sizeof(char))));
      return 0;
    }
    if(0 == STR2NCMP(arg, "use_cache=")){
      FdManager::SetCacheDir(strchr(arg, '=') + sizeof(char));
      return 0;
    }
    if(0 == STR2NCMP(arg, "check_cache_dir_exist")){
      FdManager::SetCheckCacheDirExist(true);
      return 0;
    }
    if(0 == strcmp(arg, "del_cache")){
      is_remove_cache = true;
      return 0;
    }
    if(0 == STR2NCMP(arg, "multireq_max=")){
      long maxreq = static_cast<long>(s3fs_strtoofft(strchr(arg, '=') + sizeof(char)));
      S3fsMultiCurl::SetMaxMultiRequest(maxreq);
      return 0;
    }
    if(0 == strcmp(arg, "nonempty")){
      nonempty = true;
      return 1; // need to continue for fuse.
    }
    if(0 == strcmp(arg, "nomultipart")){
      nomultipart = true;
      return 0;
    }
    // old format for storage_class
    if(0 == strcmp(arg, "use_rrs") || 0 == STR2NCMP(arg, "use_rrs=")){
      off_t rrs = 1;
      // for an old format.
      if(0 == STR2NCMP(arg, "use_rrs=")){
        rrs = s3fs_strtoofft(strchr(arg, '=') + sizeof(char));
      }
      if(0 == rrs){
        S3fsCurl::SetStorageClass(STANDARD);
      }else if(1 == rrs){
        S3fsCurl::SetStorageClass(REDUCED_REDUNDANCY);
      }else{
        S3FS_PRN_EXIT("poorly formed argument to option: use_rrs");
        return -1;
      }
      return 0;
    }
    if(0 == STR2NCMP(arg, "storage_class=")){
      const char *storage_class = strchr(arg, '=') + sizeof(char);
      if(0 == strcmp(storage_class, "standard")){
        S3fsCurl::SetStorageClass(STANDARD);
      }else if(0 == strcmp(storage_class, "standard_ia")){
        S3fsCurl::SetStorageClass(STANDARD_IA);
      }else if(0 == strcmp(storage_class, "reduced_redundancy")){
        S3fsCurl::SetStorageClass(REDUCED_REDUNDANCY);
      }else{
        S3FS_PRN_EXIT("unknown value for storage_class: %s", storage_class);
        return -1;
      }
      return 0;
    }
    //
    // [NOTE]
    // use_sse                        Set Server Side Encrypting type to SSE-S3
    // use_sse=1
    // use_sse=file                   Set Server Side Encrypting type to Custom key(SSE-C) and load custom keys
    // use_sse=custom(c):file
    // use_sse=custom(c)              Set Server Side Encrypting type to Custom key(SSE-C)
    // use_sse=kmsid(k):kms-key-id    Set Server Side Encrypting type to AWS Key Management key id(SSE-KMS) and load KMS id
    // use_sse=kmsid(k)               Set Server Side Encrypting type to AWS Key Management key id(SSE-KMS)
    //
    // load_sse_c=file                Load Server Side Encrypting custom keys
    //
    // AWSSSECKEYS                    Loading Environment for Server Side Encrypting custom keys
    // AWSSSEKMSID                    Loading Environment for Server Side Encrypting Key id
    //
    if(0 == STR2NCMP(arg, "use_sse")){
      if(0 == strcmp(arg, "use_sse") || 0 == strcmp(arg, "use_sse=1")){ // use_sse=1 is old type parameter
        // sse type is SSE_S3
        if(!S3fsCurl::IsSseDisable() && !S3fsCurl::IsSseS3Type()){
          S3FS_PRN_EXIT("already set SSE another type, so conflict use_sse option or environment.");
          return -1;
        }
        S3fsCurl::SetSseType(SSE_S3);

      }else if(0 == strcmp(arg, "use_sse=kmsid") || 0 == strcmp(arg, "use_sse=k")){
        // sse type is SSE_KMS with out kmsid(expecting id is loaded by environment)
        if(!S3fsCurl::IsSseDisable() && !S3fsCurl::IsSseKmsType()){
          S3FS_PRN_EXIT("already set SSE another type, so conflict use_sse option or environment.");
          return -1;
        }
        if(!S3fsCurl::IsSetSseKmsId()){
          S3FS_PRN_EXIT("use_sse=kms but not loaded kms id by environment.");
          return -1;
        }
        S3fsCurl::SetSseType(SSE_KMS);

      }else if(0 == STR2NCMP(arg, "use_sse=kmsid:") || 0 == STR2NCMP(arg, "use_sse=k:")){
        // sse type is SSE_KMS with kmsid
        if(!S3fsCurl::IsSseDisable() && !S3fsCurl::IsSseKmsType()){
          S3FS_PRN_EXIT("already set SSE another type, so conflict use_sse option or environment.");
          return -1;
        }
        const char* kmsid;
        if(0 == STR2NCMP(arg, "use_sse=kmsid:")){
          kmsid = &arg[strlen("use_sse=kmsid:")];
        }else{
          kmsid = &arg[strlen("use_sse=k:")];
        }
        if(!S3fsCurl::SetSseKmsid(kmsid)){
          S3FS_PRN_EXIT("failed to load use_sse kms id.");
          return -1;
        }
        S3fsCurl::SetSseType(SSE_KMS);

      }else if(0 == strcmp(arg, "use_sse=custom") || 0 == strcmp(arg, "use_sse=c")){
        // sse type is SSE_C with out custom keys(expecting keys are loaded by environment or load_sse_c option)
        if(!S3fsCurl::IsSseDisable() && !S3fsCurl::IsSseCType()){
          S3FS_PRN_EXIT("already set SSE another type, so conflict use_sse option or environment.");
          return -1;
        }
        // [NOTE]
        // do not check ckeys exists here.
        //
        S3fsCurl::SetSseType(SSE_C);

      }else if(0 == STR2NCMP(arg, "use_sse=custom:") || 0 == STR2NCMP(arg, "use_sse=c:")){
        // sse type is SSE_C with custom keys
        if(!S3fsCurl::IsSseDisable() && !S3fsCurl::IsSseCType()){
          S3FS_PRN_EXIT("already set SSE another type, so conflict use_sse option or environment.");
          return -1;
        }
        const char* ssecfile;
        if(0 == STR2NCMP(arg, "use_sse=custom:")){
          ssecfile = &arg[strlen("use_sse=custom:")];
        }else{
          ssecfile = &arg[strlen("use_sse=c:")];
        }
        if(!S3fsCurl::SetSseCKeys(ssecfile)){
          S3FS_PRN_EXIT("failed to load use_sse custom key file(%s).", ssecfile);
          return -1;
        }
        S3fsCurl::SetSseType(SSE_C);

      }else if(0 == strcmp(arg, "use_sse=")){    // this type is old style(parameter is custom key file path)
        // SSE_C with custom keys.
        const char* ssecfile = &arg[strlen("use_sse=")];
        if(!S3fsCurl::SetSseCKeys(ssecfile)){
          S3FS_PRN_EXIT("failed to load use_sse custom key file(%s).", ssecfile);
          return -1;
        }
        S3fsCurl::SetSseType(SSE_C);

      }else{
        // never come here.
        S3FS_PRN_EXIT("something wrong use_sse option.");
        return -1;
      }
      return 0;
    }
    // [NOTE]
    // Do only load SSE custom keys, care for set without set sse type.
    if(0 == STR2NCMP(arg, "load_sse_c=")){
      const char* ssecfile = &arg[strlen("load_sse_c=")];
      if(!S3fsCurl::SetSseCKeys(ssecfile)){
        S3FS_PRN_EXIT("failed to load use_sse custom key file(%s).", ssecfile);
        return -1;
      }
      return 0;
    }
    if(0 == STR2NCMP(arg, "ssl_verify_hostname=")){
      long sslvh = static_cast<long>(s3fs_strtoofft(strchr(arg, '=') + sizeof(char)));
      if(-1 == S3fsCurl::SetSslVerifyHostname(sslvh)){
        S3FS_PRN_EXIT("poorly formed argument to option: ssl_verify_hostname.");
        return -1;
      }
      return 0;
    }
    if(0 == STR2NCMP(arg, "passwd_file=")){
      passwd_file = strchr(arg, '=') + sizeof(char);
      return 0;
    }
    if(0 == strcmp(arg, "ibm_iam_auth")){
      S3fsCurl::SetIsIBMIAMAuth(true);
      S3fsCurl::SetIAMCredentialsURL("https://iam.bluemix.net/oidc/token");
      S3fsCurl::SetIAMTokenField("access_token");
      S3fsCurl::SetIAMExpiryField("expiration");
      S3fsCurl::SetIAMFieldCount(2);
      is_ibm_iam_auth = true;
      return 0;
    }
    if(0 == strcmp(arg, "ecs")){
      if (is_ibm_iam_auth) {
        S3FS_PRN_EXIT("option ecs cannot be used in conjunction with ibm");
        return -1;
      }
      S3fsCurl::SetIsECS(true);
      S3fsCurl::SetIAMCredentialsURL("http://169.254.170.2");
      S3fsCurl::SetIAMFieldCount(5);
      is_ecs = true;
      return 0;
    }
    if(0 == STR2NCMP(arg, "iam_role")){
      if (is_ecs || is_ibm_iam_auth) {
        S3FS_PRN_EXIT("option iam_role cannot be used in conjunction with ecs or ibm");
        return -1;
      }
      if(0 == strcmp(arg, "iam_role") || 0 == strcmp(arg, "iam_role=auto")){
        // loading IAM role name in s3fs_init(), because we need to wait initializing curl.
        //
        load_iamrole = true;
        return 0;

      }else if(0 == STR2NCMP(arg, "iam_role=")){
        const char* role = strchr(arg, '=') + sizeof(char);
        S3fsCurl::SetIAMRole(role);
        load_iamrole = false;
        return 0;
      }
    }
    if(0 == STR2NCMP(arg, "public_bucket=")){
      off_t pubbucket = s3fs_strtoofft(strchr(arg, '=') + sizeof(char));
      if(1 == pubbucket){
        S3fsCurl::SetPublicBucket(true);
        // [NOTE]
        // if bucket is public(without credential), s3 do not allow copy api.
        // so s3fs sets nocopyapi mode.
        //
        nocopyapi = true;
      }else if(0 == pubbucket){
        S3fsCurl::SetPublicBucket(false);
      }else{
        S3FS_PRN_EXIT("poorly formed argument to option: public_bucket.");
        return -1;
      }
      return 0;
    }
    if(0 == STR2NCMP(arg, "bucket=")){
      std::string bname = strchr(arg, '=') + sizeof(char);
      if ((ret = set_bucket(bname.c_str()))){
        return ret;
      }
      return 0;
    }
    if(0 == STR2NCMP(arg, "host=")){
      host = strchr(arg, '=') + sizeof(char);
      return 0;
    }
    if(0 == STR2NCMP(arg, "servicepath=")){
      service_path = strchr(arg, '=') + sizeof(char);
      return 0;
    }
    if(0 == strcmp(arg, "no_check_certificate")){
        S3fsCurl::SetCheckCertificate(false);
        return 0;
    }
    if(0 == STR2NCMP(arg, "connect_timeout=")){
      long contimeout = static_cast<long>(s3fs_strtoofft(strchr(arg, '=') + sizeof(char)));
      S3fsCurl::SetConnectTimeout(contimeout);
      return 0;
    }
    if(0 == STR2NCMP(arg, "readwrite_timeout=")){
      time_t rwtimeout = static_cast<time_t>(s3fs_strtoofft(strchr(arg, '=') + sizeof(char)));
      S3fsCurl::SetReadwriteTimeout(rwtimeout);
      return 0;
    }
    if(0 == STR2NCMP(arg, "max_stat_cache_size=")){
      unsigned long cache_size = static_cast<unsigned long>(s3fs_strtoofft(strchr(arg, '=') + sizeof(char)));
      StatCache::getStatCacheData()->SetCacheSize(cache_size);
      return 0;
    }
    if(0 == STR2NCMP(arg, "stat_cache_expire=")){
      time_t expr_time = static_cast<time_t>(s3fs_strtoofft(strchr(arg, '=') + sizeof(char)));
      StatCache::getStatCacheData()->SetExpireTime(expr_time);
      return 0;
    }
    // [NOTE]
    // This option is for compatibility old version.
    if(0 == STR2NCMP(arg, "stat_cache_interval_expire=")){
      time_t expr_time = static_cast<time_t>(s3fs_strtoofft(strchr(arg, '=') + sizeof(char)));
      StatCache::getStatCacheData()->SetExpireTime(expr_time, true);
      return 0;
    }
    if(0 == strcmp(arg, "enable_noobj_cache")){
      StatCache::getStatCacheData()->EnableCacheNoObject();
      return 0;
    }
    if(0 == strcmp(arg, "nodnscache")){
      S3fsCurl::SetDnsCache(false);
      return 0;
    }
    if(0 == strcmp(arg, "nosscache")){
      S3fsCurl::SetSslSessionCache(false);
      return 0;
    }
    if(0 == STR2NCMP(arg, "parallel_count=") || 0 == STR2NCMP(arg, "parallel_upload=")){
      int maxpara = static_cast<int>(s3fs_strtoofft(strchr(arg, '=') + sizeof(char)));
      if(0 >= maxpara){
        S3FS_PRN_EXIT("argument should be over 1: parallel_count");
        return -1;
      }
      S3fsCurl::SetMaxParallelCount(maxpara);
      return 0;
    }
    if(0 == STR2NCMP(arg, "fd_page_size=")){
      S3FS_PRN_ERR("option fd_page_size is no longer supported, so skip this option.");
      return 0;
    }
    if(0 == STR2NCMP(arg, "multipart_size=")){
      off_t size = static_cast<off_t>(s3fs_strtoofft(strchr(arg, '=') + sizeof(char)));
      if(!S3fsCurl::SetMultipartSize(size)){
        S3FS_PRN_EXIT("multipart_size option must be at least 5 MB.");
        return -1;
      }
      // update ensure free disk space if it is not set.
      FdManager::InitEnsureFreeDiskSpace();
      return 0;
    }
    if(0 == STR2NCMP(arg, "hwcache_write_size=")){
      size_t size = static_cast<size_t>(s3fs_strtoofft(strchr(arg, '=') + sizeof(char)));
      if(size <= 0){
        S3FS_PRN_EXIT("hwcache_write_size option should be over 0.");
        return -1;
      }
      gWritePageSize = size;
      return 0;
    }
    if(0 == STR2NCMP(arg, "ensure_diskfree=")){
      size_t dfsize = static_cast<size_t>(s3fs_strtoofft(strchr(arg, '=') + sizeof(char))) * 1024 * 1024;
      if(dfsize < static_cast<size_t>(S3fsCurl::GetMultipartSize())){
        S3FS_PRN_WARN("specified size to ensure disk free space is smaller than multipart size, so set multipart size to it.");
        dfsize = static_cast<size_t>(S3fsCurl::GetMultipartSize());
      }
      FdManager::SetEnsureFreeDiskSpace(dfsize);
      return 0;
    }
    if(0 == STR2NCMP(arg, "singlepart_copy_limit=")){
      singlepart_copy_limit = static_cast<int64_t>(s3fs_strtoofft(strchr(arg, '=') + sizeof(char))) * 1024;
      return 0;
    }
    if(0 == STR2NCMP(arg, "ahbe_conf=")){
      string ahbe_conf = strchr(arg, '=') + sizeof(char);
      if(!AdditionalHeader::get()->Load(ahbe_conf.c_str())){
        S3FS_PRN_EXIT("failed to load ahbe_conf file(%s).", ahbe_conf.c_str());
        return -1;
      }
      AdditionalHeader::get()->Dump();
      return 0;
    }
    if(0 == strcmp(arg, "noxmlns")){
      noxmlns = true;
      return 0;
    }
    if(0 == strcmp(arg, "nocopyapi")){
      nocopyapi = true;
      return 0;
    }
    if(0 == strcmp(arg, "norenameapi")){
      norenameapi = true;
      return 0;
    }
    if(0 == strcmp(arg, "complement_stat")){
      complement_stat = true;
      return 0;
    }
    if(0 == strcmp(arg, "notsup_compat_dir")){
      support_compat_dir = false;
      return 0;
    }
    if(0 == strcmp(arg, "enable_content_md5")){
      S3fsCurl::SetContentMd5(true);
      return 0;
    }
    if(0 == STR2NCMP(arg, "url=")){
      host = strchr(arg, '=') + sizeof(char);
      // strip the trailing '/', if any, off the end of the host
      // string
      size_t found, length;
      found  = host.find_last_of('/');
      length = host.length();
      while(found == (length - 1) && length > 0){
         host.erase(found);
         found  = host.find_last_of('/');
         length = host.length();
      }
      return 0;
    }
    if(0 == strcmp(arg, "sigv2")){
      S3fsCurl::SetSignatureV4(false);
      return 0;
    }
    if(0 == strcmp(arg, "createbucket")){
      create_bucket = true;
      return 0;
    }
    if(0 == STR2NCMP(arg, "endpoint=")){
      endpoint              = strchr(arg, '=') + sizeof(char);
      is_specified_endpoint = true;
      return 0;
    }
    if(0 == strcmp(arg, "use_path_request_style")){
      pathrequeststyle = true;
      return 0;
    }
    if(0 == STR2NCMP(arg, "noua")){
      S3fsCurl::SetUserAgentFlag(false);
      return 0;
    }
    if(0 == strcmp(arg, "use_xattr")){
      is_use_xattr = true;
      return 0;
    }else if(0 == STR2NCMP(arg, "use_xattr=")){
      const char* strflag = strchr(arg, '=') + sizeof(char);
      if(0 == strcmp(strflag, "1")){
        is_use_xattr = true;
      }else if(0 == strcmp(strflag, "0")){
        is_use_xattr = false;
      }else{
        S3FS_PRN_EXIT("option use_xattr has unknown parameter(%s).", strflag);
        return -1;
      }
      return 0;
    }
    if(0 == STR2NCMP(arg, "cipher_suites=")){
      cipher_suites = strchr(arg, '=') + sizeof(char);
      return 0;
    }
    //
    // debug option for s3fs
    //
    if(0 == STR2NCMP(arg, "dbglevel=")){
      const char* strlevel = strchr(arg, '=') + sizeof(char);
      if(0 == strcasecmp(strlevel, "silent") || 0 == strcasecmp(strlevel, "critical") || 0 == strcasecmp(strlevel, "crit")){
        set_s3fs_log_level(S3FS_LOG_CRIT);
      }else if(0 == strcasecmp(strlevel, "error") || 0 == strcasecmp(strlevel, "err")){
        set_s3fs_log_level(S3FS_LOG_ERR);
      }else if(0 == strcasecmp(strlevel, "wan") || 0 == strcasecmp(strlevel, "warn") || 0 == strcasecmp(strlevel, "warning")){
        set_s3fs_log_level(S3FS_LOG_WARN);
      }else if(0 == strcasecmp(strlevel, "inf") || 0 == strcasecmp(strlevel, "info") || 0 == strcasecmp(strlevel, "information")){
        set_s3fs_log_level(S3FS_LOG_INFO);
      }else if(0 == strcasecmp(strlevel, "dbg") || 0 == strcasecmp(strlevel, "debug")){
        set_s3fs_log_level(S3FS_LOG_DBG);
      }else{
        S3FS_PRN_EXIT("option dbglevel has unknown parameter(%s).", strlevel);
        return -1;
      }
      return 0;
    }
    //
    // debug option
    //
    // debug_level is S3FS_LOG_INFO, after second -d is passed to fuse.
    //
    if(0 == strcmp(arg, "-d") || 0 == strcmp(arg, "--debug")){
      if(!IS_S3FS_LOG_INFO() && !IS_S3FS_LOG_DBG()){
        set_s3fs_log_level(S3FS_LOG_INFO);
        return 0;
      }
      if(0 == strcmp(arg, "--debug")){
        // fuse doesn't understand "--debug", but it understands -d.
        // but we can't pass -d back to fuse.
        return 0;
      }
    }
    // "f2" is not used no more.
    // (set S3FS_LOG_DBG)
    if(0 == strcmp(arg, "f2")){
      set_s3fs_log_level(S3FS_LOG_DBG);
      return 0;
    }
    if(0 == strcmp(arg, "curldbg")){
      S3fsCurl::SetVerbose(true);
      return 0;
    }

    if(0 == STR2NCMP(arg, "accessKeyId=")){
      S3FS_PRN_EXIT("option accessKeyId is no longer supported.");
      return -1;
    }
    if(0 == STR2NCMP(arg, "secretAccessKey=")){
      S3FS_PRN_EXIT("option secretAccessKey is no longer supported.");
      return -1;
    }
    if(0 == strcmp(arg, "nohwscache")){
      nohwscache = true;
      return 0;
    }
    if(0 == strcmp(arg, "hwscache")){
      nohwscache = false;
      return 0;
    }
    if(0 == strcmp(arg, "cacheassert")){
      cache_assert = true;
      return 0;
    }
    if(0 == STR2NCMP(arg, "maxcachesize=")){
        off64_t maxCacheSize = strtol(strchr(arg, '=') + sizeof(char), NULL, 0);
        if (maxCacheSize > 0)
        {
            gMaxCacheMemSize = maxCacheSize;
        }
        S3FS_PRN_WARN("maxCacheSize = %ld", gMaxCacheMemSize);
        return 0;
      }
    if(0 == strcmp(arg, "obsfslog")){
        use_obsfs_log = true;
        S3FS_PRN_WARN("use_obsfs_log is true");
        return 0;
    }
    if(0 == STR2NCMP(arg, "resolveretrymax=")){
        off64_t retryCount = strtol(strchr(arg, '=') + sizeof(char), NULL, 0);
        if (retryCount > 0)
        {
            gCliCannotResolveRetryCount = retryCount;
        }
        S3FS_PRN_WARN("gCliCannotResolveRetryCount = %d", gCliCannotResolveRetryCount);
        return 0;
    }
    if(0 == strcmp(arg, "nocheckcrc")){
        gIsCheckCRC = false;
        S3FS_PRN_WARN("CheckCRC is false");
        return 0;
    }
    if (0 == strcmp(arg, "accesscheck")) {
        filter_check_access = true;
        S3FS_PRN_WARN("enable access check");
        return 0;
    }
  }
  return 1;
}

static int s3fs_utimens_hw_obs(const char* path, const struct timespec ts[2])
{
  int result;
  string strpath;
  string newpath;
  string nowcache;
  headers_t meta;
  struct stat stbuf;
  timeval begin_tv;
  s3fsStatisStart(UTIMENS_TOTAL, &begin_tv);

  S3FS_PRN_INFO("cmd start: [path=%s][mtime=%jd]", path, (intmax_t)(ts[1].tv_sec));

  if(0 == strcmp(path, "/")){
    S3FS_PRN_WARN("Could not change mtime for mount point.");
    return 0;
  }
  if(0 != (result = check_parent_object_access(path, X_OK))){
    S3FS_PRN_ERR("utime %s err, result = %d", path, result);
    return result;
  }
  if(0 != (result = check_object_access(path, W_OK, &stbuf, NULL))){
    if(0 != (result = check_object_owner(path, &stbuf))){
        S3FS_PRN_ERR("utime %s err, result = %d", strpath.c_str(), result);
        return result;
    }
  }

  strpath  = path;
  result = get_object_attribute(strpath.c_str(), NULL, &meta);
  if(0 != result){
    S3FS_PRN_ERR("utime %s err, result = %d", strpath.c_str(), result);
    return result;
  }

  meta["x-amz-meta-mtime"]         = str(ts[1].tv_sec);
  meta["x-amz-copy-source"]        = urlEncode(service_path + bucket + get_realpath(strpath.c_str()));
  meta["x-amz-metadata-directive"] = "REPLACE";

  if((result=put_headers(strpath.c_str(), meta, true, "")) != 0){
    S3FS_PRN_ERR("utime %s err, result = %d", strpath.c_str(), result);
    return -EIO;
  }

  s3fsStatisEnd(UTIMENS_TOTAL, &begin_tv,path);
  // clear attr stat cache
  IndexCache::getIndexCache()->setFilesizeOrClearGetAttrStat(strpath,0,true);            
  
  return 0;
}

void get_start_end_content(const char* path,const char* buf,size_t size,off_t offset)
{
    if (PRINT_LENGTH <= size){
        off64_t *pU64_buf = (off64_t *)buf;
        off64_t *pU64_buf_tail = (off64_t *)(buf+size-PRINT_LENGTH);
        S3FS_PRN_INFO("buf content [path=%s][size=%zu][offset=%jd][start=%lx][tail=%lx]", path, size, (intmax_t)offset, pU64_buf[0], pU64_buf_tail[0]);
    }
}
void s3fsStatisLogModeNotObsfs()        
{
    if (LOG_MODE_OBSFS != debug_log_mode)
    {
        g_LogModeNotObsfs++;
        g_backupLogMode = debug_log_mode;
    }
}

// copy cache stat to output para and set output para
void fillGetAttrStatInfoFromCacheEntry(tag_index_cache_entry_t* p_src_cache_entry, struct stat* pDestStat)
{
    memcpy(pDestStat,&(p_src_cache_entry->stGetAttrStat),sizeof(struct stat));
}

void afterCopyFromCacheEntryProcess(
    const char* path, long long* pInodeNo,long* pHeadLastResponseCode, tag_index_cache_entry_t* p_src_cache_entry, bool openflag)
{
    if (pInodeNo)
    {
        *pInodeNo = p_src_cache_entry->inodeNo;
    }
    if (pHeadLastResponseCode)
    {
        *pHeadLastResponseCode = 200;
    }
    if (openflag)
    {
        IndexCache::getIndexCache()->AddEntryOpenCnt(path);
    }
}


//if stGetAttrStat is valid
bool isCacheGetAttrStatValid(const char* path, tag_index_cache_entry_t* p_cache_entry)
{
    // try to get attr (getattr command may be caused by readdir)
    struct timespec *pGetAttrMs = &p_cache_entry->getAttrCacheSetTs;
    if (pGetAttrMs->tv_sec <= 0)
    {
        return false;
    }

    struct timespec nowTime;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &nowTime);
    
    long getAttrSetDiffMs = diff_in_ms(pGetAttrMs, &nowTime);

    int cfg_cache_attr_valid_ms = 0;
    if (HwsGetIntConfigValue(HWS_CFG_CACHE_ATTR_SWITCH_OPEN))
    {
        if (S_ISDIR(p_cache_entry->stGetAttrStat.st_mode))
        {
            cfg_cache_attr_valid_ms = HwsGetIntConfigValue(HWS_CFG_CACHE_ATTR_VALID_4_LIST_MS);
        }
        else
        {
            cfg_cache_attr_valid_ms = HwsGetIntConfigValue(HWS_CFG_CACHE_ATTR_VALID_MS);
        }
    }
    else
    {
        // If switch is close, attr need to be within list attr valid time
        cfg_cache_attr_valid_ms = HwsGetIntConfigValue(HWS_CFG_CACHE_ATTR_VALID_4_LIST_MS);
    }

    // If value is beyond the validity period, not use it.
    if (getAttrSetDiffMs >= cfg_cache_attr_valid_ms)
    {
        return false;
    }

    //only for statis return true count
    timeval begin_tv;
    s3fsStatisStart(CACHE_GET_ATTR_STAT_VALID, &begin_tv);
    S3FS_DATA_CACHE_PRN_LOG(
        "hit statCache,path=%s,attrDiff=%ld,cfg_ms=%d,filesize=%ld,mode=%04o",
        path, getAttrSetDiffMs, cfg_cache_attr_valid_ms,
        p_cache_entry->stGetAttrStat.st_size, p_cache_entry->stGetAttrStat.st_mode);
    s3fsStatisEnd(CACHE_GET_ATTR_STAT_VALID, &begin_tv);

    return true;

}

//if cache get attr stat valid and read offset beyond filesize
bool isReadBeyondCacheStatFileSize(const char* path,tag_index_cache_entry_t* 
        pIndexEntry,off_t read_offset)
{
    off64_t cacheFileSize = pIndexEntry->stGetAttrStat.st_size;
    
    S3FS_PRN_INFO("path=%s,cacheFilesize=%ld,readoff=%lu", 
        path,cacheFileSize,read_offset);

    //if cache get attr stat valid and read offset beyond filesize
    if (isCacheGetAttrStatValid(path,pIndexEntry)
        && read_offset >= cacheFileSize)
    {
        //only for statis return true count
        timeval begin_tv;
        s3fsStatisStart(READ_BEYOND_STAT_FILE_SIZE, &begin_tv);
        S3FS_DATA_CACHE_PRN_LOG("statCache,beyond size,path=%s,cacheFilesize=%ld,readoff=%lu", 
            path,cacheFileSize,read_offset);
        s3fsStatisEnd(READ_BEYOND_STAT_FILE_SIZE, &begin_tv);
        return true;
    }
    else
    {
        return false;
    }
    
}

//copy pSrcStat to cache entry and set time before add to entry map
void copyStatToCacheEntry(const char* path,
    tag_index_cache_entry_t* p_dest_cache_entry, struct stat* pSrcStat, CACHE_STAT_TYPE statType)
{
    memcpy(&(p_dest_cache_entry->stGetAttrStat),pSrcStat,sizeof(struct stat));
    clock_gettime(CLOCK_MONOTONIC_COARSE, &(p_dest_cache_entry->getAttrCacheSetTs));

    p_dest_cache_entry->statType = statType;
}

void copyGetAttrStatToCacheEntry(
    const char* path, tag_index_cache_entry_t* p_dest_cache_entry, struct stat* pSrcStat)
{
    if (0 == HwsGetIntConfigValue(HWS_CFG_CACHE_ATTR_SWITCH_OPEN))
    {
        return;
    }

    if (pSrcStat->st_size > 0)
    {
        copyStatToCacheEntry(path, p_dest_cache_entry, pSrcStat, STAT_TYPE_HEAD);
    }
    else
    {
        S3FS_PRN_INFO("path=%s,filesize zero,not copy",path);
    }
}


        
#if 1

static void init_index_cache_entry(tag_index_cache_entry_t &index_cache_entry)
{
    index_cache_entry.key         = "";
    index_cache_entry.dentryname  = "";
    index_cache_entry.inodeNo     = INVALID_INODE_NO;
    index_cache_entry.fsVersionId = "";

    index_cache_entry.firstWritFlag   = true;
    index_cache_entry.plogheadVersion = "";
    index_cache_entry.originName  = "";

    index_cache_entry.statType = STAT_TYPE_BUTT;

    memset(&index_cache_entry.stGetAttrStat, 0, sizeof(struct stat));    
    memset(&index_cache_entry.getAttrCacheSetTs, 0, sizeof(struct timespec));    
}

int GetIndexCacheEntryWithRetry(string path,tag_index_cache_entry_t* pIndexEntry)
{
    int          result = 0;

    bool hit_index = IndexCache::getIndexCache()->GetIndex(path, pIndexEntry);
    // if plogHeadVersion is empty(cache add by list), send head to osc
    if (hit_index && !pIndexEntry->plogheadVersion.empty())
    {
        return 0;
    }
    S3FS_PRN_INFO("[path=%s] getIndex failed and retry", path.c_str());
    result = get_object_attribute_with_open_flag_hw_obs(path.c_str(), NULL, NULL, NULL,
        false, pIndexEntry, NULL, false);
    if (0 != result)
    {
        S3FS_PRN_ERR("[path=%s] getIndex retry get attr failed", path.c_str());
    }
    return result;
}

static void  update_maxDhtViewVersion_from_response(const char* path, headers_t *response_headers)
{
    if (NULL == response_headers){
        S3FS_PRN_ERR("response_headers is NULL");
        return;
    }

    headers_t *header = response_headers;
    for(headers_t::iterator iter = header->begin(); iter != header->end(); ++iter){
        string key   = lower(iter->first);
        string value = iter->second;
        if(key == hws_s3fs_dht_version)
        {
            int dhtversion = get_dhtVersion(value.c_str());
            updateDHTViewVersionInfo(dhtversion);

            string maxDHTViewVersion = toString(getMaxDHTViewVersionId());
            S3FS_PRN_DBG("update dhtversion=[%d], maxdhtversion=[%s]", dhtversion, maxDHTViewVersion.c_str());
        }
        else
        {
            S3FS_PRN_DBG("write/read file [path=%s][reponseheadertag=%s][reponseheadervalue=%s]", path, key.c_str(), value.c_str());
        }
    }

    return;
}

/* add for object_file_gateway begin */
void adjust_filesize_with_write_cache(const char* path,
        long long inodeNo,struct stat* pstbuf)
{
    off64_t old_st_size = pstbuf->st_size;
    off64_t fileSize = 0;

	if(!nohwscache)
    {
        std::shared_ptr<HwsFdEntity> ent = HwsFdManager::GetInstance().Get(inodeNo);
        if (ent != nullptr)
		{
			fileSize = ent->GetFileSize();
			pstbuf->st_size = max(fileSize, pstbuf->st_size);
		}
        if (old_st_size != pstbuf->st_size)
        {
            S3FS_PRN_INFO("[path=%s][fileSize=%ld][st_size=%ld]old_st_size=%ld",
                SAFESTRPTR(path),fileSize,pstbuf->st_size,old_st_size);
        }
    }
}

/* add for object_file_gateway begin */
//output para:pstbuf,pheader,inodeNo,p_index_cache_entry,
//            headLastResponseCode(200 for ok)
static int get_object_attribute_with_open_flag_hw_obs(const char* path, struct stat* pstbuf,
    headers_t* pmeta, long long* inodeNo, bool openflag, tag_index_cache_entry_t* p_index_cache_entry, 
    long* headLastResponseCode, bool useIndexCache)
{
    //timeval object_tv;
    //s3fsStatisStart(GETATTR_OBJECT_TOTAL, &object_tv);
    int          result = -1;
    struct stat  tmpstbuf;
    struct stat* pstat = pstbuf ? pstbuf : &tmpstbuf;
    headers_t    tmpHead;
    headers_t*   pheader = pmeta ? pmeta : &tmpHead;
    string       strpath;
    S3fsCurl     s3fscurl(true);
    bool         forcedir = false;
    timeval begin_tv;

    S3FS_PRN_DBG("[path=%s]", path);

    if(!path || '\0' == path[0]){
      return -ENOENT;
    }

    memset(pstat, 0, sizeof(struct stat));
    if(0 == strcmp(path, "/") || 0 == strcmp(path, "."))
    {
      pstat->st_nlink = 1; // see fuse faq
      pstat->st_mode  = mp_mode;
      pstat->st_uid   = is_s3fs_uid ? s3fs_uid : mp_uid;
      pstat->st_gid   = is_s3fs_gid ? s3fs_gid : mp_gid;
	  pstat->st_mtime = mp_mtime;
	  pstat->st_size = mp_st_size;
      return 0;
    }

    /* firstly, find inodeNo from index cache */
    tag_index_cache_entry_t index_cache_entry;
    init_index_cache_entry(index_cache_entry);
    tag_index_cache_entry_t* p_cache_entry = p_index_cache_entry ? p_index_cache_entry : &index_cache_entry;
    // LRU cache key without / in the end
    strpath     = path;

    
    s3fsStatisStart(GETATTR_GETINDEX_DELAY, &begin_tv);
    bool get_ok = IndexCache::getIndexCache()->GetIndex(strpath, p_cache_entry);
    s3fsStatisEnd(GETATTR_GETINDEX_DELAY, &begin_tv);

    s3fsStatisStart(HEAD_DELAY, &begin_tv);
    if (get_ok)
    {
        if (useIndexCache)
        {
            //if pmeta is not null,not use cache stat because can not output pmeta
            if (NULL == pmeta && isCacheGetAttrStatValid(path,p_cache_entry))
            {
                fillGetAttrStatInfoFromCacheEntry(p_cache_entry, pstat);
                // do after process: copy inodeNo, set response code, add open count if need.
                afterCopyFromCacheEntryProcess(path, inodeNo, headLastResponseCode, p_cache_entry, openflag);

                return 0;
            }
        }

        result = s3fscurl.HeadRequest(strpath.c_str(), (*pheader), &(p_cache_entry->inodeNo), p_cache_entry->dentryname.c_str());
        if (-ENOENT == result)
        {
            /* may re-upload occurs, inode belongs to old file, use path to retry again */
            S3FS_PRN_WARN("[path=%s] may overwrite, so remove indexcache and getattr again without inode(%lld).",
                strpath.c_str(), p_cache_entry->inodeNo);

            IndexCache::getIndexCache()->DeleteIndex(strpath);
            result = s3fscurl.HeadRequest(strpath.c_str(), (*pheader));
        }
    }
    else
    {
        result = s3fscurl.HeadRequest(strpath.c_str(), (*pheader));
    }

    if (headLastResponseCode)
    {
        *headLastResponseCode = s3fscurl.GetLastResponseCode();
    }

    s3fsStatisEnd(HEAD_DELAY, &begin_tv);

    s3fsStatisStart(GETATTR_DESTROY_CUHANDLE, &begin_tv);
    s3fscurl.DestroyCurlHandle();
    s3fsStatisEnd(GETATTR_DESTROY_CUHANDLE, &begin_tv);
    if(0 != result)
    {
        S3FS_PRN_INFO("[path=%s] not exit", strpath.c_str());
        return -ENOENT;
    }

    // cache size is Zero -> only convert.
    if(!convert_header_to_stat(strpath.c_str(), (*pheader), pstat, forcedir))
    {
        S3FS_PRN_ERR("failed convert headers to stat[path=%s]", strpath.c_str());
        return -ENOENT;
    }

    /* decode response header and store shardkey and inodeno */
    init_index_cache_entry(*p_cache_entry);

    for(headers_t::iterator iter = pheader->begin(); iter != pheader->end(); ++iter)
    {
        string tag   = lower(iter->first);
        string value = iter->second;

        if(tag == hws_s3fs_shardkey)
        {
            p_cache_entry->dentryname= urlDecode(value);
            S3FS_PRN_DBG("get attr/open file [path=%s][dentryname=%s]", path, urlDecode(value).c_str());
        }
        else if(tag == hws_s3fs_inodeNo)
        {
            p_cache_entry->inodeNo = get_inodeNo(value.c_str());
            S3FS_PRN_DBG("get attr/open file [path=%s][inodeNo_str=%s][inodeNo=%llu]", path, value.c_str(), p_cache_entry->inodeNo);
        }
        else if(tag == Contentlength)
        {
            long filesize = get_size(value.c_str());
            if (0 != filesize)
            {
                p_cache_entry->firstWritFlag = false;
            }
            S3FS_PRN_DBG("get attr/open file [path=%s][ContentLength=%ld][firstWriteFlag=%d]", path, filesize, p_cache_entry->firstWritFlag);
        }
        else if(tag == hws_s3fs_plog_headversion)
        {
            p_cache_entry->plogheadVersion = value;
            S3FS_PRN_DBG("get attr/open file [path=%s][plogHeadversion=%s]", path, p_cache_entry->plogheadVersion.c_str());
        }
        else if(tag == hws_s3fs_version_id)
        {
            p_cache_entry->fsVersionId = value;
            S3FS_PRN_DBG("get attr/open file [path=%s][fsVersion=%s]", path, p_cache_entry->fsVersionId.c_str());
        }
        else if(tag == hws_s3fs_dht_version)
        {
            int dhtversion = get_dhtVersion(value.c_str());
            updateDHTViewVersionInfo(dhtversion);

            string maxDHTViewVersion = toString(getMaxDHTViewVersionId());
            S3FS_PRN_DBG("dhtversion=[%d], maxdhtversion=[%s]", dhtversion, maxDHTViewVersion.c_str());
        }
		else if(tag == hws_s3fs_origin_name)
		{
            p_cache_entry->originName= urlDecode(value);
			S3FS_PRN_DBG("get attr/open file [path=%s][originName=%s]", path, urlDecode(value).c_str());
		}
        else
        {
            ///TODO: shardkey-logictime and so on
            S3FS_PRN_DBG("get attr/open file [path=%s][reponseheadertag=%s][reponseheadervalue=%s]", path, tag.c_str(), value.c_str());
        }
    }

    if (INVALID_INODE_NO != p_cache_entry->inodeNo)
    {
        // Attention: dir cache key with out / in the end.
        string lruCacheKey = path;
        s3fsStatisStart(GETATTR_PUTINDEX_DELAY, &begin_tv);
        if (inodeNo)
        {
            *inodeNo = p_cache_entry->inodeNo;
        }
        pstat->st_ino = p_cache_entry->inodeNo;
        /*adjust file size with write page list*/
        adjust_filesize_with_write_cache(path, p_cache_entry->inodeNo,pstat);        
        //copy pstat to p_cache_entry
        copyGetAttrStatToCacheEntry(path,p_cache_entry,pstat);
        if (false == openflag)
        {
            result = IndexCache::getIndexCache()->PutIndexNotchangeOpenCnt(lruCacheKey, p_cache_entry);
        }
        else
        {
            result = IndexCache::getIndexCache()->PutIndexAddOpenCnt(lruCacheKey, p_cache_entry);
        }
        s3fsStatisEnd(GETATTR_PUTINDEX_DELAY, &begin_tv);
        if(0 != result)
        {
            S3FS_PRN_ERR("[path=%s] put meta to cache fail,result=%d",
                strpath.c_str(),result);
            return -ENOENT;
        }

        S3FS_PRN_INFO("get attr hws [path=%s] dentryname(%s) inodeno(%lld)size(%ld)",
                      path, p_cache_entry->dentryname.c_str(), 
                      p_cache_entry->inodeNo,pstat->st_size);
        return 0;
    }
    else
    {
        S3FS_PRN_ERR("get attr/open file [path=%s] fail without dentryname(%s) and inodeno(%lld) back",
                      path, p_cache_entry->dentryname.c_str(), p_cache_entry->inodeNo);
        return -ENOENT;
    }
}

void s3fs_set_obsfslog_mode()
{
    struct timespec now_ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);
    /*set obsfs log mod must delay 5 second after start*/
    if (diff_in_ms(&hws_s3fs_start_ts, &now_ts) < OBSFS_LOG_MODE_SET_DELAY_MS)
    {
        return;
    }

    if (use_obsfs_log && LOG_MODE_OBSFS != debug_log_mode)
    {
        S3FS_PRN_WARN("change logmode  to obsfslog");
        debug_log_mode = LOG_MODE_OBSFS;
    }

}

static int s3fs_getattr_hw_obs(const char* path, struct stat* pstbuf)
{
    /*use get attr function set obsfslog mode after 5 seconds after start*/
    s3fs_set_obsfslog_mode();

    int result;
	long long inodeNo = INVALID_INODE_NO;
    timeval begin_tv;
    s3fsStatisStart(GETATTR_DELAY, &begin_tv);

    if(0 != (result = check_object_access(path, F_OK, pstbuf, &inodeNo)))
    {
        s3fsStatisEnd(GETATTR_DELAY, &begin_tv);
        return result;
    }

    s3fsStatisEnd(GETATTR_DELAY, &begin_tv);
    timeval      end_tv;
    gettimeofday(&end_tv, NULL);
    unsigned long long diff = getDiffUs(&begin_tv,&end_tv);
    S3FS_INTF_PRN_LOG("cmd start: [path=%s],mode=%x,diff=%llu us", 
        path,(unsigned int)pstbuf->st_mode,diff);

    return result;
}

// common function for creation of a plain object
static int create_file_object_hw_obs(const char* path, mode_t mode, uid_t uid, gid_t gid, long long *inodeNo)
{
    S3FS_INTF_PRN_LOG("[path=%s][mode=%04o]", path, mode);

    headers_t meta;
    meta["Content-Type"]     = S3fsCurl::LookupMimeType(string(path));
    meta[hws_obs_meta_uid]   = str(uid);
    meta[hws_obs_meta_gid]   = str(gid);
    meta[hws_obs_meta_mode]  = str(mode);
    meta[hws_obs_meta_mtime] = str(time(NULL));

    S3fsCurl s3fscurl(true);
    timeval createZero_tv;
    s3fsStatisStart(CREATE_ZERO_FILE_DELAY, &createZero_tv);
    int result = s3fscurl.CreateZeroByteFileObject(path, meta);    // create zero byte object.
    s3fsStatisEnd(CREATE_ZERO_FILE_DELAY, &createZero_tv);
    if (0 != result)
    {
        S3FS_PRN_ERR("[result=%d],create file object failed [path=%s]", result, path);
        return result;
    }

    /* if create success, Cache index from response headers */
    headers_t response_headers;
    response_headers = *(s3fscurl.GetResponseHeaders());

    tag_index_cache_entry_t index_cache_entry;
    init_index_cache_entry(index_cache_entry);
    for(headers_t::iterator iter = response_headers.begin(); iter != response_headers.end(); ++iter)
    {
        string tag   = lower(iter->first);
        string value = iter->second;

        if(tag == hws_s3fs_shardkey)
        {
            index_cache_entry.dentryname = urlDecode(value);
            S3FS_PRN_DBG("create file [path=%s][%s=%s]", path, tag.c_str(), urlDecode(value).c_str());
        }
        else if(tag == hws_s3fs_inodeNo)
        {
            index_cache_entry.inodeNo = get_inodeNo(value.c_str());
            S3FS_PRN_DBG("create file [path=%s][%s.Str=%s][value=%llu]", path, tag.c_str(),
                                                                         value.c_str(),
                                                                         index_cache_entry.inodeNo);
        }
        else
        {
            ///TODO: shardkey-logictime and so on
            S3FS_PRN_DBG("create file [path=%s][reponseheadertag=%s][reponseheadervalue=%s]", path, tag.c_str(), value.c_str());
        }
    }

    if (!index_cache_entry.dentryname.empty() && (INVALID_INODE_NO != index_cache_entry.inodeNo))
    {
        string lruCacheKey = path;
        IndexCache::getIndexCache()->PutIndexAddOpenCnt(lruCacheKey, &index_cache_entry);
        *inodeNo = index_cache_entry.inodeNo;
        S3FS_PRN_DBG("create file [path=%s] success with inode(%llu)", path, index_cache_entry.inodeNo);
    }
    else
    {
        S3FS_PRN_ERR("create file [path=%s] no dentryname or inodeno=%llu",
            path,index_cache_entry.inodeNo);
        result = -1;
    }

    return result;
}

static int s3fs_create_hw_obs(const char* path, mode_t mode, struct fuse_file_info* fi)
{
    int result;
    struct fuse_context* pcxt;
    long long inodeNo = INVALID_INODE_NO;
    timeval begin_tv;
    S3FS_INTF_PRN_LOG("cmd start: [path=%s][mode=%04o][flags=%d]", path, mode, fi->flags);

    if(NULL == (pcxt = fuse_get_context())){
      return -EIO;
    }

    // check parent directory attribute.
    s3fsStatisStart(CREATE_FILE_TOTAL, &begin_tv);
    //s3fsStatisStart(CREATE_FILE_CHECK_ACCESS_DELAY, &begin_tv);
    if(0 != (result = check_parent_object_access(path, X_OK)))
    {
        return result;
    }

    result = check_object_access(path, W_OK, NULL, &inodeNo);
    if (0 == result)
    {
        S3FS_PRN_WARN("file exists, [path=%s]", path);
        fi->fh = inodeNo;
        if(!nohwscache)
        {
            HwsFdManager::GetInstance().Open(path, inodeNo, 0);
        }
        s3fsStatisEnd(CREATE_FILE_TOTAL, &begin_tv);
        return 0;
    }
    else if(-ENOENT == result)
    {
        if(0 != (result = check_parent_object_access(path, W_OK)))
        {
          return result;
        }
    }
    else
    {
        return result;
    }
    //s3fsStatisEnd(CREATE_FILE_CHECK_ACCESS_DELAY, &begin_tv);

    result = create_file_object_hw_obs(path, mode, pcxt->uid, pcxt->gid, &inodeNo);
    if(result != 0)
    {
        S3FS_PRN_ERR("create file failed [path=%s]", path);
        return result;
    }
    else
    {
        fi->fh = inodeNo;        //return fi->fh
        //S3FS_MALLOCTRIM(0);
        if(!nohwscache)
        {
            HwsFdManager::GetInstance().Open(path, inodeNo, 0);
        }
        s3fsStatisEnd(CREATE_FILE_TOTAL, &begin_tv);
        return 0;
    }
}


static int s3fs_open_hw_obs(const char* path, struct fuse_file_info* fi)
{
    int result = 0;
    long long inodeNo = INVALID_INODE_NO;
    struct stat st;
    S3FS_INTF_PRN_LOG("cmd start: [path=%s][flags=%d]", path, fi->flags);

    timeval begin_tv;
    s3fsStatisStart(OPEN_FILE_TOTAL, &begin_tv);

    int mask = (O_RDONLY != (fi->flags & O_ACCMODE) ? W_OK : R_OK);
    result = check_object_access_with_openflag(path, mask, &st, &inodeNo, true);
    if (-ENOENT == result)
    {
        S3FS_PRN_ERR("open file [path=%s] failed not exist", path);
        if(0 != (result = check_parent_object_access(path, W_OK)))
        {
            return result;
        }
    }
    else if (0 != result)
    {
        S3FS_PRN_ERR("open file [path=%s] failed without object access, result=%d", path, result);
        return result;
    }

    if((unsigned int)fi->flags & O_TRUNC)
    {
        result = s3fs_truncate(path,0);
        if (0 != result && -ENOENT != result)
        {
            S3FS_PRN_ERR("open file truncate fail [path=%s] result=%x",
                path,result);
            return result ;
        }
    }
    s3fsStatisEnd(OPEN_FILE_TOTAL, &begin_tv);

    fi->fh = inodeNo;                       //return fi->fh
    if(!nohwscache)
    {
        HwsFdManager::GetInstance().Open(path, inodeNo, static_cast<off64_t>(st.st_size));
    }
    S3FS_PRN_INFO("open file success [path=%s][inodeNo=%lld][size=%jd].", path, inodeNo, (intmax_t)(st.st_size));
    return result;
}

int s3fs_write_hw_obs_proc(const char* path,
                           const char* buf,
                           size_t size,
                           off_t offset)
{
    int writeResult;
    int getIndexCacheResult;
    string str_path = path;
    S3fsCurl s3fscurl(true);
    get_start_end_content(path, buf, size, offset);

    tag_index_cache_entry_t index_cache_entry;
    init_index_cache_entry(index_cache_entry);

    tagDataBuffer databuff;
    databuff.pcBuffer     = buf;
    databuff.ulBufferSize = size;
    databuff.offset       = 0;

    for(int i = 0; i < HWS_RETRY_WRITE_OBS_NUM; i++)
    {
        getIndexCacheResult = GetIndexCacheEntryWithRetry(str_path,&index_cache_entry);
        if (0 != getIndexCacheResult)
        {
            S3FS_PRN_ERR("[path=%s, offset=%ld, size=%zu], getIndex failed", path, (long)offset, size);
            return getIndexCacheResult;
        }
        writeResult = s3fscurl.WriteBytesToFileObject(str_path.c_str(), &databuff, offset, &index_cache_entry);
        if (0 == writeResult)
        {
            /*write success,so break*/
            break;
        }
        else
        {
            S3FS_PRN_ERR("write obs failed,result=%d,path=%s,offset=%ld,"
                "size=%zu,i=%d",
                writeResult, str_path.c_str(),(long)offset, size,i);
            /*sleep 4 seconds and retry*/
            sleep(4);
            //1.delete index cache 2.head again 3.retry write once more
            IndexCache::getIndexCache()->DeleteIndex(path);
        }
    }
    if (writeResult < 0)  /*retry all fail*/
    {
        S3FS_PRN_ERR("write obs retry fail,result=%d,path=%s,offset=%ld,"
            "size=%zu",
            writeResult, str_path.c_str(),(long)offset, size);
        return -1;
    }

    IndexCache::getIndexCache()->setFirstWriteFlag(str_path);

    update_maxDhtViewVersion_from_response(str_path.c_str(), s3fscurl.GetResponseHeaders());

    return static_cast<int>(size);
}

static int s3fs_write_hw_obs(const char* path,
                             const char* buf,
                             size_t size,
                             off_t offset,
                             struct fuse_file_info* fi)
{
    int result;
    get_start_end_content(path, buf, size, offset);
    string str_path = path;
    string dentryname = "";
    string inodeNoStr = toString(fi->fh);
    timeval begin_tv;
    s3fsStatisStart(WRITE_FILE_TOTAL, &begin_tv);
    S3fsCurl s3fscurl(true);

    tag_index_cache_entry_t index_cache_entry;
    init_index_cache_entry(index_cache_entry);

    bool get_ok = IndexCache::getIndexCache()->GetIndex(str_path, &index_cache_entry);
    if (get_ok)
    {
        dentryname = index_cache_entry.dentryname;
        if(fi->fh != (unsigned long long)index_cache_entry.inodeNo)
        {
            S3FS_PRN_ERR("fi->fh[%llu], cache inode[%lld] not match", (unsigned long long)fi->fh, index_cache_entry.inodeNo);
            return -1;
        }
    }

    S3FS_PRN_DBG("write file [path=%s],[inodeNO=%s],[dentryname=%s]", str_path.c_str(),
                                                                    inodeNoStr.c_str(),
                                                                    dentryname.c_str());

    if(nohwscache)
    {
        result = s3fs_write_hw_obs_proc(path, buf, size, offset);
    }
    else
    {
        std::shared_ptr<HwsFdEntity> ent = HwsFdManager::GetInstance().Get(fi->fh);
        if (ent == nullptr)
        {
            S3FS_PRN_ERR("s3fs_write_file GetInstance failed,[path=%s][size=%zu][offset=%jd]",
                str_path.c_str(),size,offset);
            return -1;
        }
        result = ent->Write(buf, offset, size, path);
    }

    if ((int)size > result)
    {
        S3FS_PRN_ERR("s3fs_write_file failed,[result=%d][path=%s][size=%zu][offset=%jd]",
                result,str_path.c_str(),size,offset);
        return -1;
    }
    
    timeval      end_tv;
    gettimeofday(&end_tv, NULL);
    unsigned long long diff = getDiffUs(&begin_tv,&end_tv);
    S3FS_INTF_PRN_LOG("cmd start: [path=%s][size=%zu][offset=%jd][fd=%llu][diff=%llu us]", path, size,
                                                                        (intmax_t)offset,
                                                                        (unsigned long long)(fi->fh),diff);

    s3fsStatisEnd(WRITE_FILE_TOTAL, &begin_tv,path);
    //set filesize to cache get attr stat after write succeed
    IndexCache::getIndexCache()->setFilesizeOrClearGetAttrStat(str_path,offset+size,false);
    return static_cast<int>(size);
}
/* add for object_file_gateway end */

int s3fs_read_hw_obs_proc(const char* path, char* buf, size_t size, off_t offset)
{
    int result = 0;
    S3fsCurl s3fscurl;

    tag_index_cache_entry_t index_cache_entry;
    init_index_cache_entry(index_cache_entry);

    std::string str_path = path;
    std::string dentryname = "";

    result = GetIndexCacheEntryWithRetry(str_path,&index_cache_entry);
    if (0 != result)
    {
        S3FS_PRN_ERR("[path=%s, offset=%ld, size=%zu], getIndex failed", path, (long)offset, size);
        return result;
    }
    else if (isReadBeyondCacheStatFileSize(path,&index_cache_entry,offset))
    {
        return 0;
    }
    
    if (!index_cache_entry.dentryname.empty())
    {
        dentryname = index_cache_entry.dentryname;
    }

    std::string inodeNoStr = toString(index_cache_entry.inodeNo);

    result = s3fscurl.ReadFromFileObject(path, buf, offset, size, dentryname.c_str(), inodeNoStr.c_str());
    if (404 == s3fscurl.GetLastResponseCode())
    {
        S3FS_PRN_WARN("[path=%s, offset=%ld, size=%zu, dentryname=%s, inode=%s] read return 404, try head it",
            path, (long)offset, size, dentryname.c_str(), inodeNoStr.c_str());
        IndexCache::getIndexCache()->DeleteIndex(toString(path));
        if(0 == GetIndexCacheEntryWithRetry(str_path,&index_cache_entry))
        {
            inodeNoStr = toString(index_cache_entry.inodeNo);
            dentryname = index_cache_entry.dentryname;
            result = s3fscurl.ReadFromFileObject(path, buf, offset, size, dentryname.c_str(), inodeNoStr.c_str());
        }
        else
        {
            S3FS_PRN_ERR("[path=%s, offset=%ld, size=%zu, dentryname=%s, inode=%s] read return 404, and try head failed",
                path, (long)offset, size, dentryname.c_str(), inodeNoStr.c_str());
            return -ENOENT;
        }
    }
    if (0 > result)
    {
        /*modify for object_file_gateway begin*/
        // for errorCode 416, Range Not Satisfiable
        // If offset is larger than file length, reading is considered complete.
        if (416 != s3fscurl.GetLastResponseCode())
        {
            S3FS_PRN_ERR("[path=%s, offset=%ld, size=%zu] read failed,ResCode=%ld",
                path, (long)offset, size,s3fscurl.GetLastResponseCode());
            return -EBUSY;
        }

        result = 0;
        /*modify for object_file_gateway end*/
    }

    update_maxDhtViewVersion_from_response(str_path.c_str(), s3fscurl.GetResponseHeaders());

    return static_cast<int>(result);
}

static int s3fs_read_hw_obs(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* fi)
{
    int read_ret_size = 0;
    timeval begin_tv;
    s3fsStatisStart(READ_FILE_TOTAL, &begin_tv);
    memset(buf, 0, size);
    if(nohwscache)
    {
        read_ret_size = s3fs_read_hw_obs_proc(path, buf, size, offset);
    }
    else
    {
        std::shared_ptr<HwsFdEntity> ent = HwsFdManager::GetInstance().Get(fi->fh);
        if (ent == nullptr)
        {
            S3FS_PRN_ERR("[path=%s],s3fs_read_file GetInstance failed,", path);
            return -EIO;
        }
        read_ret_size = ent->Read(buf, offset, size, path);
    }

    if (0 > read_ret_size)
    {
      S3FS_PRN_ERR("[path=%s] read failed,offset(%ld),size(%zu)",
            path,offset,size);
      return -EIO;
    }

    s3fsStatisEnd(READ_FILE_TOTAL, &begin_tv,path);
    timeval      end_tv;
    gettimeofday(&end_tv, NULL);
    unsigned long long diff = getDiffUs(&begin_tv,&end_tv);
    S3FS_INTF_PRN_LOG(
        "cmd start:[path=%s][size=%zu][offset=%jd][fd=%llu][retsize=%d][diff=%llu us]",
        path, size, (intmax_t)offset, (unsigned long long)(fi->fh),read_ret_size,diff);

    S3FS_PRN_DBG("[path=%s] read success", path);
    if (S3FS_LOG_INFO == debug_level)
    {
        string strMD5;
        strMD5 = s3fs_get_content_md5_hws_obs(buf, 0, size);
        S3FS_PRN_INFO("read data check:[path=%s][size=%zu][offset=%jd][fd=%llu],nohwscache=%d,check=%s",
            path, size, (intmax_t)offset, (unsigned long long)(fi->fh),nohwscache,strMD5.c_str());
    }
    return static_cast<int>(read_ret_size);
}

static int s3fs_flush_hw_obs(const char* path, struct fuse_file_info* fi)
{
  int result = 0;
  timeval begin_tv;
  s3fsStatisStart(FLUSH_TOTAL, &begin_tv);

  S3FS_INTF_PRN_LOG("cmd start: [path=%s][fd=%llu]", path, (unsigned long long)(fi->fh));

  if(!nohwscache)
  {
      std::shared_ptr<HwsFdEntity> ent = HwsFdManager::GetInstance().Get(fi->fh);
      if (ent == nullptr)
      {
          S3FS_PRN_ERR("[path=%s],s3fs_flush_file GetInstance failed,", path);
          return -1;
      }
      result = ent->Flush();
  }

  s3fsStatisEnd(FLUSH_TOTAL, &begin_tv,path);
  return result;
}

static int s3fs_release_hw_obs(const char* path, struct fuse_file_info* fi)
{
  timeval begin_tv;
  s3fsStatisStart(RELEASE_TOTAL, &begin_tv);

  S3FS_INTF_PRN_LOG("cmd start: [path=%s][fd=%llu]", path, (unsigned long long)(fi->fh));
  string tpath = path;
  IndexCache::getIndexCache()->PutIndexReduceOpenCnt(tpath);
  if(!nohwscache)
  {
    HwsFdManager::GetInstance().Close(fi->fh);
  }
  s3fsStatisEnd(RELEASE_TOTAL, &begin_tv,path);
  return 0;
}

static int s3fs_opendir_hw_obs(const char* path, struct fuse_file_info* fi)
{
  int result;
  int mask = (O_RDONLY != (fi->flags & O_ACCMODE) ? W_OK : R_OK) | X_OK;
  long long inodeNo = INVALID_INODE_NO;
  timeval begin_tv;
  s3fsStatisStart(OPENDIR_TOTAL, &begin_tv);

  S3FS_INTF_PRN_LOG("cmd start: [path=%s][flags=%d] debug_log_mode=%d,cacheassert=%d"
      ",logmodeNotObs=%d,backLogMode=%d",
      path, fi->flags,debug_log_mode,cache_assert,g_LogModeNotObsfs,g_backupLogMode);

  if(0 == (result = check_object_access(path, mask, NULL, &inodeNo))){
    result = check_parent_object_access(path, mask);
  }

  fi->fh = inodeNo;

  s3fsStatisEnd(OPENDIR_TOTAL, &begin_tv,path);
  return result;
}

/* file gateway modify begin */
static int readdir_get_ino(const char* path, struct fuse_file_info* fi, long long& inode_no)
{
  long long ino = fi->fh;
  
  if(INVALID_INODE_NO == ino){
    int result = check_object_access(path, X_OK, NULL, &ino);
    if(0 != result){
      return result;
    }
  }
  if(strcmp(path, "/") == 0){
    ino = gRootInodNo;
  }
  inode_no = ino;

  return 0;
}


static int s3fs_readdir_hw_obs(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* fi)
{
  long long ino;
  int result = readdir_get_ino(path, fi, ino);

  if(result != 0){
    return result;
  }

  char buffer[sizeof(int64_t)*8 + 1];
  sprintf(buffer, "%lld", ino);
  string dentryPrefix = std::string(buffer) + "/";

  S3FS_INTF_PRN_LOG("cmd start: [path=%s,ino=%lld, offset=%ld]", path, ino, offset);

  timeval begin_tv;

  // Search or create marker if it is not existed.
  string next_marker = "";
  off_t marker_pos = offset;
  result = marker_search_or_insert(ino, path, marker_pos, next_marker);
  if (result != 0) {
    return result;
  }
  S3FS_PRN_INFO1("[ino=%lld,path=%s,off=%ld,marker[%ld]=%s] succeeded to search marker.",
      ino, path, offset, marker_pos, next_marker.c_str());

  // In first page we need fill the two default items "." and ".." at first.
  PAGE_FILL_STATE fill_state = FILL_NONE;
  bool first_page = (0 == offset);
  if(first_page){
    filler(buf, ".", 0, marker_pos);
    filler(buf, "..", 0, marker_pos);
    fill_state = FILL_PART;
  }

  int list_max_key = HwsGetIntConfigValue(HWS_LIST_MAX_KEY);
  // Fill current page until the buffer is full or there is no more data from bucket.
  do {

    // Use S3HwObjList to sort files and directories.
    S3HwObjList head;

    s3fsStatisStart(LIST_BUCKET, &begin_tv);

    // Get one batch of list result
    result = list_bucket_hw_obs_with_optimization(path, "/", next_marker, ino, head, list_max_key);
	s3fsStatisEnd(LIST_BUCKET, &begin_tv, path);
	if(result != 0){
      S3FS_PRN_ERR("[ino=%lld,path=%s,off=%ld,marker[%ld]=%s] list_bucket_hw_obs returns error(%d).",
          ino, path, offset, marker_pos, next_marker.c_str(), result);
      (void)marker_remove(ino, path, marker_pos);
      return result;
    }

    S3FS_PRN_INFO1("[list end][path=%s,off=%ld,list_info=%s]",
      path, marker_pos, head.get_statistic().to_string().c_str());
   
    if(head.empty()){
      // there is no more data from obs, finish the list operation.
      break;
    }

    // fill list result to filler, and put stat to index cache
    add_list_result_to_filler(path, dentryPrefix, head, buf, marker_pos, filler, fill_state, next_marker);

  }while(fill_state != FILL_FULL);

  // Succeeded to stat objects:
  if(fill_state == FILL_NONE){
    // This page will be empty, so finish the relative marker.
    S3FS_PRN_INFO1("[ino=%lld,path=%s,off=%ld,marker[%ld]=%s] finish marker.",
        ino, path, offset, marker_pos, next_marker.c_str());
    (void)marker_remove(ino, path, marker_pos);
    return 0;
  }

  // not finished: update the relative marker.
  if(marker_update(ino, path, marker_pos, next_marker) == readdir_marker_map::npos){
    S3FS_PRN_ERR("[ino=%lld,path=%s,off=%ld,marker[%ld]=%s] failed to update marker.",
        ino, path, offset, marker_pos, next_marker.c_str());
    return -1;
  }

  S3FS_PRN_INFO1("[ino=%lld,path=%s,off=%ld,marker[%ld]=%s] succeeded to update marker.",
      ino, path, offset, marker_pos, next_marker.c_str());
  return 0;
}

static void add_list_result_to_filler(const char* path, string dentryPrefix, S3HwObjList& head, void* buf, off_t offset, fuse_fill_dir_t filler, PAGE_FILL_STATE& fill_state, string& next_marker)
{
    string strpath = path;
    if (strcmp(path, "/") != 0 && '/' != strpath[strpath.length() - 1]){
        strpath += "/";
    }

    bool is_full = false;
    for (auto it = head.begin(); it != head.end(); it++){
        is_full = filler(buf, it->first.c_str(), &(it->second), offset);
        if (is_full)
        {
            fill_state = FILL_FULL;
            break;
        }

        fill_state = FILL_PART;
        next_marker = it->first;

        // dispath, head path
        string dispath = strpath + it->first;

        // add to index cache
        tag_index_cache_entry_t index_cache_entry;
        init_index_cache_entry(index_cache_entry);

        index_cache_entry.inodeNo = it->second.st_ino;
        index_cache_entry.dentryname = dentryPrefix + it->first;

        std::shared_ptr<HwsFdEntity> ent = HwsFdManager::GetInstance().Get(index_cache_entry.inodeNo);
        if (ent != nullptr)
        {
            off64_t fileSize = ent->GetFileSize();
            index_cache_entry.stGetAttrStat.st_size = max(fileSize, it->second.st_size);
        }
        
        copyStatToCacheEntry(dispath.c_str(), &index_cache_entry, const_cast<struct stat*>(&(it->second)), STAT_TYPE_LIST);

        IndexCache::getIndexCache()->PutIndexNotchangeOpenCnt(dispath, &index_cache_entry);

        S3FS_PRN_INFO2("[offset=%ld,filename=%s,filemode=%o,filesize=%llu,uid=%ld,gid=%ld,mtime=%s]",
          offset, it->first.c_str(), it->second.st_mode, (long long)it->second.st_size, (long)it->second.st_uid, (long)it->second.st_gid, ctime(&it->second.st_mtime));

    }

    return;
}

static int create_directory_object_hw_obs(const char* path, mode_t mode, time_t time, uid_t uid, gid_t gid)
{
    int result;

    S3FS_INTF_PRN_LOG("[path=%s][mode=%04o][time=%jd][uid=%u][gid=%u]", path, mode, (intmax_t)time, (unsigned int)uid, (unsigned int)gid);

    if(!path || '\0' == path[0]){
      return -1;
    }

    // dir object for obs uniform without / in the end
    string tpath = path;
    //if('/' != tpath[tpath.length() - 1]){
    //  tpath += "/";
    //}

    headers_t meta;
    meta["Content-Type"]     = string("application/x-directory");
    meta[hws_obs_meta_uid]   = str(uid);
    meta[hws_obs_meta_gid]   = str(gid);
    meta[hws_obs_meta_mode]  = str(mode);
    meta[hws_obs_meta_mtime] = str(time);

    S3fsCurl s3fscurl;
    result = s3fscurl.CreateZeroByteDirObject(tpath.c_str(), meta);
    if (0 != result)
    {
        S3FS_PRN_ERR("[result=%d],mkdir failed [path=%s]", result, tpath.c_str());
        return result;
    }

    //create success then cache index which get from response headers
    headers_t response_headers;
    response_headers = *(s3fscurl.GetResponseHeaders());

    tag_index_cache_entry_t index_cache_entry;
    init_index_cache_entry(index_cache_entry);
    for(headers_t::iterator iter = response_headers.begin(); iter != response_headers.end(); ++iter)
    {
        string tag   = lower(iter->first);
        string value = iter->second;

        if(tag == hws_s3fs_shardkey)
        {
            index_cache_entry.dentryname = urlDecode(value);
            S3FS_PRN_DBG("mkdir [path=%s][%s=%s]", tpath.c_str(), tag.c_str(), urlDecode(value).c_str());
        }
        else if(tag == hws_s3fs_inodeNo)
        {
            index_cache_entry.inodeNo = get_inodeNo(value.c_str());
            S3FS_PRN_DBG("mkdir [path=%s][%s.Str=%s][value=%llu]", tpath.c_str(), tag.c_str(),
                                                                         value.c_str(),
                                                                         index_cache_entry.inodeNo);
        }
        else
        {
            S3FS_PRN_DBG("mkdir [path=%s][reponseheadertag=%s][reponseheadervalue=%s]",
                tpath.c_str(), tag.c_str(), value.c_str());
        }
    }

    if (!index_cache_entry.dentryname.empty() && (INVALID_INODE_NO != index_cache_entry.inodeNo))
    {
        // LRU cache key = path without / in the end.
        string lruCacheKey = path;
        IndexCache::getIndexCache()->PutIndexNotchangeOpenCnt(lruCacheKey, &index_cache_entry);
        S3FS_PRN_DBG("mkdir [path=%s] success with inode(%llu)", tpath.c_str(), index_cache_entry.inodeNo);
    }
    else
    {
        S3FS_PRN_ERR("[result=%d], mkdir [path=%s] without dentryname and inodeno back", result, path);
        result = -1;
    }

    return result;
}


static int s3fs_mkdir_hw_obs(const char* path, mode_t mode)
{
  int result;
  struct fuse_context* pcxt;
  timeval begin_tv;
  s3fsStatisStart(MAKEDIR_TOTAL, &begin_tv);
  
  mode |= S_IFDIR;
  S3FS_INTF_PRN_LOG("cmd start: [path=%s][mode=%04o]", path, mode);

  if(NULL == (pcxt = fuse_get_context())){
    return -EIO;
  }

  // check parent directory attribute.
  if(0 != (result = check_parent_object_access(path, W_OK | X_OK))){
    return result;
  }
  if(-ENOENT != (result = check_object_access(path, F_OK, NULL, NULL))){
    if(0 == result){
      result = -EEXIST;
    }
    return result;
  }

  result = create_directory_object_hw_obs(path, mode, time(NULL), pcxt->uid, pcxt->gid);
  s3fsStatisEnd(MAKEDIR_TOTAL, &begin_tv,path);

  return result;
}

static int s3fs_unlink_hw_obs(const char* path)
{
  int result;
  timeval begin_tv;
  s3fsStatisStart(UNLINK_TOTAL, &begin_tv);
  
  S3FS_PRN_INFO("cmd start: [path=%s]", path);

  // check dir
  if(0 != (result = check_parent_object_access(path, W_OK | X_OK))){
    return result;
  }

  // check the file is exist, update IndexCache
  if(0 != (result = check_object_access(path, F_OK, NULL, NULL))){
    return result;
  }

  if(!nohwscache)
  {
    tag_index_cache_entry_t index_cache_entry;
    init_index_cache_entry(index_cache_entry);
    string str_path = path;
    bool bHit = IndexCache::getIndexCache()->GetIndex(str_path, &index_cache_entry);

    if(!bHit)
    {
        if (0 != (result = get_object_attribute(path, NULL, NULL, &index_cache_entry)))
        {
            S3FS_PRN_WARN("get attr failed by unlink [path=%s]", path);
        }
    }

    if(INVALID_INODE_NO != index_cache_entry.inodeNo)
    {
      std::shared_ptr<HwsFdEntity> ent = HwsFdManager::GetInstance().Get(index_cache_entry.inodeNo);
      if (ent)
      {
        ent->Flush();
      }
    }
  }

  S3fsCurl s3fscurl;
  result = s3fscurl.DeleteRequest(path);
  if (0 == result)
  {
      IndexCache::getIndexCache()->DeleteIndex(string(path));
      S3FS_PRN_DBG("delete index cache by unlink [path=%s]", path);
  }
  S3FS_MALLOCTRIM(0);
  s3fsStatisEnd(UNLINK_TOTAL, &begin_tv,path);
  return result;

}

static int s3fs_rmdir_hw_obs(const char* path)
{
  int result;
  string strpath = path;
  struct stat stbuf;
  long long inodeNo = 0;
  headers_t pmeta;
  bool openflag = false;
  timeval begin_tv;
  s3fsStatisStart(RMDIR_TOTAL, &begin_tv);

  S3FS_INTF_PRN_LOG("cmd start: [path=%s]", path);

  if(0 != (result = check_parent_object_access(path, W_OK | X_OK))){
    return result;
  }

  if(0 == get_object_attribute_with_open_flag_hw_obs(path, &stbuf, &pmeta, &inodeNo, openflag))
  {
    if(!S_ISDIR(stbuf.st_mode)){
      S3FS_PRN_ERR("[path=%s] is not dir", path);
	  return -1;
    }
  }
  if(directory_empty(path,inodeNo) != 0){
    S3FS_PRN_ERR("[path=%s] is not empty", path);
    return -ENOTEMPTY;
  }

  S3fsCurl s3fscurl;
  result = s3fscurl.DeleteRequest(strpath.c_str());
  s3fscurl.DestroyCurlHandle();
  if (0 == result)
  {
      IndexCache::getIndexCache()->DeleteIndex(strpath);
      S3FS_PRN_DBG("delete index cache by rmdir [path=%s]", path);
  }
  S3FS_MALLOCTRIM(0);
  s3fsStatisEnd(RMDIR_TOTAL, &begin_tv,path);

  return result;
}

static int s3fs_mknod_hw_obs(const char *path, mode_t mode, dev_t rdev)
{
  int       result;
  struct fuse_context* pcxt;
  timeval begin_tv;
  s3fsStatisStart(MAKENODE_TOTAL, &begin_tv);

  S3FS_INTF_PRN_LOG("cmd start: [path=%s][mode=%04o][dev=%ju]", path, mode, (uintmax_t)rdev);

  if(NULL == (pcxt = fuse_get_context())){
    return -EIO;
  }

  long long inodeNo = INVALID_INODE_NO;
  if(0 != (result = create_file_object_hw_obs(path, mode, pcxt->uid, pcxt->gid, &inodeNo))){
    S3FS_PRN_ERR("could not create object for special file(result=%d).", result);
    return result;
  }
  s3fsStatisEnd(MAKENODE_TOTAL, &begin_tv,path);
  S3FS_PRN_INFO("mknod [path=%s][mode=%04o][dev=%ju], inode(%llu) ok.", path, mode, (uintmax_t)rdev, inodeNo);

  return result;
}

// [NOTICE]
// Assumption is a valid fd.
//
static int s3fs_fsync_hw_obs(const char* path, int datasync, struct fuse_file_info* fi)
{
  int result = 0;
  timeval begin_tv;
  s3fsStatisStart(FSYNC_TOTAL, &begin_tv);

  S3FS_INTF_PRN_LOG("cmd start: [path=%s][fd=%llu][datasync=%d]", path, (unsigned long long)(fi->fh), datasync);

  if(!nohwscache)
  {
      std::shared_ptr<HwsFdEntity> ent = HwsFdManager::GetInstance().Get(fi->fh);
      if (ent == nullptr)
      {
          S3FS_PRN_ERR("[path=%s],s3fs_fsync_file GetInstance failed,", path);
          return -1;
      }
      result = ent->Flush();
  }

  s3fsStatisEnd(FSYNC_TOTAL, &begin_tv,path);
  return result;
}

static void init_oper(struct fuse_operations &s3fs_oper)
{
    /* adatper to huawei obs private s3fs client */
    s3fs_oper.getattr   = s3fs_getattr_hw_obs;
    s3fs_oper.readlink  = s3fs_readlink;
    s3fs_oper.mknod     = s3fs_mknod_hw_obs;
    s3fs_oper.mkdir     = s3fs_mkdir_hw_obs;
    s3fs_oper.unlink    = s3fs_unlink_hw_obs;
    s3fs_oper.rmdir     = s3fs_rmdir_hw_obs;
    s3fs_oper.symlink   = s3fs_symlink;
    s3fs_oper.rename    = s3fs_rename;
    s3fs_oper.link      = s3fs_link;
    if(!nocopyapi){
      s3fs_oper.chmod   = s3fs_chmod;
      s3fs_oper.chown   = s3fs_chown_hw_obs;
      s3fs_oper.utimens = s3fs_utimens_hw_obs;
    }else{
      s3fs_oper.chmod   = s3fs_chmod_nocopy;
      s3fs_oper.chown   = s3fs_chown_nocopy;
      s3fs_oper.utimens = s3fs_utimens_nocopy;
    }
    s3fs_oper.truncate  = s3fs_truncate;
    s3fs_oper.open      = s3fs_open_hw_obs;
    s3fs_oper.read      = s3fs_read_hw_obs;
    s3fs_oper.write     = s3fs_write_hw_obs;
    s3fs_oper.statfs    = s3fs_statfs;
    s3fs_oper.flush     = s3fs_flush_hw_obs;
    s3fs_oper.fsync     = s3fs_fsync_hw_obs;
    s3fs_oper.release   = s3fs_release_hw_obs;
    s3fs_oper.opendir   = s3fs_opendir_hw_obs;
    s3fs_oper.readdir   = s3fs_readdir_hw_obs;
    s3fs_oper.init      = s3fs_init;
    s3fs_oper.destroy   = s3fs_destroy;
    s3fs_oper.access    = s3fs_access_hw_obs;
    s3fs_oper.create    = s3fs_create_hw_obs;
    // extended attributes
    if(is_use_xattr){
      s3fs_oper.setxattr    = s3fs_setxattr;
      s3fs_oper.getxattr    = s3fs_getxattr;
      s3fs_oper.listxattr   = s3fs_listxattr;
      s3fs_oper.removexattr = s3fs_removexattr;
    }
}

/* add for object_file_gateway end */

#endif
int main(int argc, char* argv[])
{
  int ch;
  int fuse_res;
  int option_index = 0;
  //int hw_s3fs_opt_index = argc;
  struct fuse_operations s3fs_oper;

  static const struct option long_opts[] = {
    {"help",    no_argument, NULL, 'h'},
    {"version", no_argument, 0,     0},
    {"debug",   no_argument, NULL, 'd'},
    {0, 0, 0, 0}
  };

  /*init obs fs log*/
  clock_gettime(CLOCK_MONOTONIC_COARSE, &hws_s3fs_start_ts);

  // init syslog(default CRIT)
  openlog("s3fs", LOG_PID | LOG_ODELAY | LOG_NOWAIT, LOG_USER);
  set_s3fs_log_level(debug_level);

  g_wholeCacheReadTasks.store(0);
  g_wholeCacheTotalMemSize.store(0);

  // init xml2
  xmlInitParser();
  LIBXML_TEST_VERSION

  // get program name - emulate basename
  size_t found = string::npos;
  program_name.assign(argv[0]);
  found = program_name.find_last_of("/");
  if(found != string::npos){
    program_name.replace(0, found+1, "");
  }

  while((ch = getopt_long(argc, argv, "dho:fsu", long_opts, &option_index)) != -1){
    switch(ch){
    case 0:
      if(strcmp(long_opts[option_index].name, "version") == 0){
        show_version();
        exit(EXIT_SUCCESS);
      }
      break;
    case 'h':
      show_help();
      exit(EXIT_SUCCESS);
    case 'o':
      break;
    case 'd':
      break;
    case 'f':
      debug_log_mode = LOG_MODE_FOREGROUND;
      break;
    case 's':
      break;
    case 'u':
      utility_mode = 1;
      break;
    default:
      S3FS_PRN_EXIT("getopt_long invalid ch=%d",ch);
      exit(EXIT_FAILURE);
    }
  }

  // Load SSE environment
  if(!S3fsCurl::LoadEnvSse()){
    S3FS_PRN_EXIT("something wrong about SSE environment.");
    exit(EXIT_FAILURE);
  }

  // clear this structure
  memset(&s3fs_oper, 0, sizeof(s3fs_oper));

  // This is the fuse-style parser for the arguments
  // after which the bucket name and mountpoint names
  // should have been set
  struct fuse_args custom_args = FUSE_ARGS_INIT(argc, argv);
  if(0 != fuse_opt_parse(&custom_args, NULL, NULL, my_fuse_opt_proc)){
    S3FS_PRN_EXIT("fuse_opt_parse fail");
    exit(EXIT_FAILURE);
  }

  // [NOTE]
  // exclusive option check here.
  //
  if(REDUCED_REDUNDANCY == S3fsCurl::GetStorageClass() && !S3fsCurl::IsSseDisable()){
    S3FS_PRN_EXIT("use_sse option could not be specified with storage class reduced_redundancy.");
    exit(EXIT_FAILURE);
  }
  if(!S3fsCurl::FinalCheckSse()){
    S3FS_PRN_EXIT("something wrong about SSE options.");
    exit(EXIT_FAILURE);
  }

  // The first plain argument is the bucket
  if(bucket.size() == 0){
    S3FS_PRN_EXIT("missing BUCKET argument.");
    show_usage();
    exit(EXIT_FAILURE);
  }

  // bucket names cannot contain upper case characters in virtual-hosted style
  if((!pathrequeststyle) && (lower(bucket) != bucket)){
    S3FS_PRN_EXIT("BUCKET %s, name not compatible with virtual-hosted style.", bucket.c_str());
    exit(EXIT_FAILURE);
  }

  // check bucket name for illegal characters
  found = bucket.find_first_of("/:\\;!@#$%^&*?|+=");
  if(found != string::npos){
    S3FS_PRN_EXIT("BUCKET %s -- bucket name contains an illegal character.", bucket.c_str());
    exit(EXIT_FAILURE);
  }

  // The second plain argument is the mountpoint
  // if the option was given, we all ready checked for a
  // readable, non-empty directory, this checks determines
  // if the mountpoint option was ever supplied
  if(utility_mode == 0){
    if(mountpoint.size() == 0){
      S3FS_PRN_EXIT("missing MOUNTPOINT argument.");
      show_usage();
      exit(EXIT_FAILURE);
    }
  }

  // error checking of command line arguments for compatibility
  if(S3fsCurl::IsPublicBucket() && S3fsCurl::IsSetAccessKeys()){
    S3FS_PRN_EXIT("specifying both public_bucket and the access keys options is invalid.");
    exit(EXIT_FAILURE);
  }
  if(passwd_file.size() > 0 && S3fsCurl::IsSetAccessKeys()){
    S3FS_PRN_EXIT("specifying both passwd_file and the access keys options is invalid.");
    exit(EXIT_FAILURE);
  }
  if(!S3fsCurl::IsPublicBucket() && !load_iamrole && !is_ecs){
    if(EXIT_SUCCESS != get_access_keys()){
      exit(EXIT_FAILURE);
    }
    if(!S3fsCurl::IsSetAccessKeys()){
      S3FS_PRN_EXIT("could not establish security credentials, check documentation.");
      exit(EXIT_FAILURE);
    }
    // More error checking on the access key pair can be done
    // like checking for appropriate lengths and characters
  }

  // check cache dir permission
  if(!FdManager::CheckCacheDirExist() || !FdManager::CheckCacheTopDir() || !CacheFileStat::CheckCacheFileStatTopDir()){
    S3FS_PRN_EXIT("could not allow cache directory permission, check permission of cache directories.");
    exit(EXIT_FAILURE);
  }

  // check IBM IAM requirements
  if(is_ibm_iam_auth){

    // check that default ACL is either public-read or private
    string defaultACL = S3fsCurl::GetDefaultAcl();
    if(defaultACL == "private"){
      // IBM's COS default ACL is private
      // set acl as empty string to avoid sending x-amz-acl header
      S3fsCurl::SetDefaultAcl("");
    }else if(defaultACL != "public-read"){
      S3FS_PRN_EXIT("can only use 'public-read' or 'private' ACL while using ibm_iam_auth");
      return -1;
    }

    if(create_bucket && !S3fsCurl::IsSetAccessKeyID()){
      S3FS_PRN_EXIT("missing service instance ID for bucket creation");
      return -1;
    }
  }

  // set user agent
  S3fsCurl::InitUserAgent();

  // There's room for more command line error checking

  // Check to see if the bucket name contains periods and https (SSL) is
  // being used. This is a known limitation:
  // http://docs.amazonwebservices.com/AmazonS3/latest/dev/
  // The Developers Guide suggests that either use HTTP of for us to write
  // our own certificate verification logic.
  // For now, this will be unsupported unless we get a request for it to
  // be supported. In that case, we have a couple of options:
  // - implement a command line option that bypasses the verify host
  //   but doesn't bypass verifying the certificate
  // - write our own host verification (this might be complex)
  // See issue #128strncasecmp
  /*
  if(1 == S3fsCurl::GetSslVerifyHostname()){
    found = bucket.find_first_of(".");
    if(found != string::npos){
      found = host.find("https:");
      if(found != string::npos){
        S3FS_PRN_EXIT("Using https and a bucket name with periods is unsupported.");
        exit(1);
      }
    }
  }
  */

  if(utility_mode){
    exit(s3fs_utility_mode());
  }

  // check free disk space
  FdManager::InitEnsureFreeDiskSpace();
  if(!FdManager::IsSafeDiskSpace(NULL, S3fsCurl::GetMultipartSize())){
    S3FS_PRN_EXIT("There is no enough disk space for used as cache(or temporary) directory by s3fs.");
    exit(EXIT_FAILURE);
  }

  init_oper(s3fs_oper);
  InitStatis();

  initDHTViewVersinoInfo();

  // set signal handler for debugging
  if(!set_s3fs_usr2_handler()){
    S3FS_PRN_EXIT("could not set signal handler for SIGUSR2.");
    clearStatis();
    exit(EXIT_FAILURE);
  }

  //check configure, add by g00422114
  HwsConfigure::GetInstance();

  // now passing things off to fuse, fuse will finish evaluating the command line args
  fuse_res = fuse_main(custom_args.argc, custom_args.argv, &s3fs_oper, NULL);
  fuse_opt_free_args(&custom_args);

  s3fs_destroy_global_ssl();

  destroyDHTViewVersinoInfo();

  // cleanup xml2
  xmlCleanupParser();
  S3FS_MALLOCTRIM(0);
  exit(fuse_res);
}

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: noet sw=4 ts=4 fdm=marker
* vim<600: noet sw=4 ts=4
*/
