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
#ifndef S3FS_S3FS_UTIL_H_
#define S3FS_S3FS_UTIL_H_

/* file gateway modify begin */
#include <string>
#include <sstream>
#include <list>

template<typename T> std::string toString(const T& t){
    std::ostringstream oss;  //创建一个格式化输出流
    oss<<t;             //把值传递如流中
    return oss.str();
}

/* file gateway modify begin */
class S3ObjListStatistic{
private:
  size_t size_;
  std::string first_;
  std::string last_;

public:
  S3ObjListStatistic() : size_(0),first_(""),last_("") {}
  ~S3ObjListStatistic() {}

  size_t get_size() const { return size_; }
  void set_size(size_t size) { size_ = size; }
  const std::string& get_first() const { return first_; }
  void set_first(const std::string& name) { first_ = name; }
  const std::string& get_last() const { return last_; }
  void set_last(const std::string& name) { last_ = name; }

  std::string to_string() {
    return std::string("(cnt:") + toString(size_)
                     + ",first:" + first_
                     + ",last:" + last_ + ")";
  }

  // increase the count and update first name or last name.
  void update(const std::string& name)
  {
    size_++;
    if(first_.empty()){
      first_ = name;
    }
    last_ = name;
  }

  // get the last name without tail '/'.
  std::string get_last_name() const {
    if(last_.empty() || ('/' != last_[last_.length()-1])){
      return last_;
    }
    return last_.substr(0, last_.length()-1);
  }
};

//
// Class
//
class S3ObjList
{
  private:
    // The map must keep the same sort rules with the OBS.
    // 1) The key name for direcotry isn't tailed by '/' and the default sort method (by dictionary) is OK.
    // 2) The value name for direcotry is tailed by '/'
    typedef std::map<std::string,std::string> s3obj_map_t;
    s3obj_map_t objects;

  public:
    S3ObjList() {}
    ~S3ObjList() {}

    s3obj_map_t::const_iterator begin(void) const {
      return objects.begin();
    }
    s3obj_map_t::const_iterator end(void) const {
      return objects.end();
    }
    bool empty() const { return objects.empty(); }
    size_t size() const { return objects.size(); }
    bool insert(const std::string name, bool is_dir);
    std::string get_last_name() const;
    S3ObjListStatistic get_statistic() const;
};
/* file gateway modify end */

class S3HwObjList
{
  private:
    typedef std::map<std::string, struct stat> hw_obj_map_t;
    hw_obj_map_t hw_obj_map;
  public:
    S3HwObjList() {}
    ~S3HwObjList() {}

    hw_obj_map_t::const_iterator begin(void) const {
      return hw_obj_map.begin();
    }
    hw_obj_map_t::const_iterator end(void) const {
      return hw_obj_map.end();
    }
    bool empty() const { return hw_obj_map.empty(); }
    size_t size() const { return hw_obj_map.size(); }
    bool insert(std::string& name, struct stat& statBuf);

    S3ObjListStatistic get_statistic() const;
};

class AutoLock
{
  private:
    pthread_mutex_t* auto_mutex;
    bool is_lock_acquired;

  public:
    explicit AutoLock(pthread_mutex_t* pmutex, bool no_wait = false);
    bool isLockAcquired() const;
    ~AutoLock();
};

//-------------------------------------------------------------------
// Functions
//-------------------------------------------------------------------
std::string get_realpath(const char *path);

std::string get_username(uid_t uid);
int is_uid_include_group(uid_t uid, gid_t gid);

std::string mydirname(const char* path);
std::string mydirname(std::string path);
std::string mybasename(const char* path);
std::string mybasename(std::string path);
int mkdirp(const std::string& path, mode_t mode);
std::string get_exist_directory_path(const std::string& path);
bool check_exist_dir_permission(const char* dirpath);
bool delete_files_in_dir(const char* dir, bool is_remove_own);

time_t get_mtime(const char *s);
time_t get_mtime(headers_t& meta, bool overcheck = true);
/* file gateway modify begin */
long long get_inodeNo(const char *s);
int get_dhtVersion(const char *s);
/* file gateway modify end */
off_t get_size(const char *s);
off_t get_size(headers_t& meta);
mode_t get_mode(const char *s);
mode_t get_mode(headers_t& meta, const char* path = NULL, bool checkdir = false, bool forcedir = false);
uid_t get_uid(const char *s);
uid_t get_uid(headers_t& meta);
gid_t get_gid(const char *s);
gid_t get_gid(headers_t& meta);
blkcnt_t get_blocks(off_t size);
time_t cvtIAMExpireStringToTime(const char* s);
time_t get_lastmodified(const char* s);
time_t get_lastmodified(headers_t& meta);
bool is_need_check_obj_detail(headers_t& meta);

void show_usage(void);
void show_help(void);
void show_version(void);


void initDHTViewVersinoInfo();
int getMaxDHTViewVersionId();
void updateDHTViewVersionInfo(int dhtversion);
void destroyDHTViewVersinoInfo();

#endif // S3FS_S3FS_UTIL_H_

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: noet sw=4 ts=4 fdm=marker
* vim<600: noet sw=4 ts=4
*/
