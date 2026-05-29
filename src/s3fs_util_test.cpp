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

#include <string>
#include <cstring>
#include <map>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "gtest.h"
#include "common.h"
#include "s3fs_util.h"
#include "string_util.h"

// Stub variables and functions required for linking
s3fs_log_level debug_level = S3FS_LOG_INFO;
const char* s3fs_log_nest[S3FS_LOG_NEST_MAX] = {"", "  ", "    ", "      "};
s3fs_log_mode debug_log_mode = LOG_MODE_FOREGROUND;

void IndexLogEntry(const uint64_t module, const char* chunk_id, const uint32_t log_level,
                   const char* log_file_name, const char* log_func_name, const uint32_t log_line,
                   const char* pszFormat, ...) {
  // Stub implementation
}

void s3fsStatisLogModeNotObsfs() {
  // Stub implementation
}

// Stub variables must match production values in hws_s3fs.cpp:149-152
const char* hws_obs_meta_mtime = "x-amz-meta-mtime";
const char* hws_obs_meta_uid = "x-amz-meta-uid";
const char* hws_obs_meta_gid = "x-amz-meta-gid";
const char* hws_obs_meta_mode = "x-amz-meta-mode";
bool complement_stat = false;
std::string program_name = "s3fs_util_test";

// Additional stub functions
const char* s3fs_crypt_lib_name() {
  return "openssl";
}

// Define INIT_DHTVIEW_VERSION_ID constant (same value as in s3fs_util.cpp)
#ifndef INIT_DHTVIEW_VERSION_ID
#define INIT_DHTVIEW_VERSION_ID (-1)
#endif

// Global variables from s3fs_util.cpp that are needed
extern std::string mount_prefix;
extern long long gRootInodNo;

using namespace std;

namespace {

class S3fsUtilTest : public ::testing::Test {
 protected:
  S3fsUtilTest() {
  }

  ~S3fsUtilTest() override {
  }

  void SetUp() override {
    // Reset mount_prefix before each test
    mount_prefix = "";
  }

  void TearDown() override {
  }
};

// Test S3ObjListStatistic class
TEST_F(S3fsUtilTest, S3ObjListStatistic_DefaultValues) {
  S3ObjListStatistic stat;
  EXPECT_EQ(stat.get_size(), 0);
  EXPECT_EQ(stat.get_first(), "");
  EXPECT_EQ(stat.get_last(), "");
}

TEST_F(S3fsUtilTest, S3ObjListStatistic_SetSize) {
  S3ObjListStatistic stat;
  stat.set_size(5);
  EXPECT_EQ(stat.get_size(), 5);
}

TEST_F(S3fsUtilTest, S3ObjListStatistic_SetFirst) {
  S3ObjListStatistic stat;
  stat.set_first("first_file");
  EXPECT_EQ(stat.get_first(), "first_file");
}

TEST_F(S3fsUtilTest, S3ObjListStatistic_SetLast) {
  S3ObjListStatistic stat;
  stat.set_last("last_file");
  EXPECT_EQ(stat.get_last(), "last_file");
}

TEST_F(S3fsUtilTest, S3ObjListStatistic_Update) {
  S3ObjListStatistic stat;
  stat.update("file1");
  EXPECT_EQ(stat.get_size(), 1);
  EXPECT_EQ(stat.get_first(), "file1");
  EXPECT_EQ(stat.get_last(), "file1");

  stat.update("file2");
  EXPECT_EQ(stat.get_size(), 2);
  EXPECT_EQ(stat.get_first(), "file1");
  EXPECT_EQ(stat.get_last(), "file2");

  stat.update("file3");
  EXPECT_EQ(stat.get_size(), 3);
  EXPECT_EQ(stat.get_first(), "file1");
  EXPECT_EQ(stat.get_last(), "file3");
}

TEST_F(S3fsUtilTest, S3ObjListStatistic_GetLastName) {
  S3ObjListStatistic stat;

  // Empty last name
  EXPECT_EQ(stat.get_last_name(), "");

  // Last name without trailing slash
  stat.set_last("file.txt");
  EXPECT_EQ(stat.get_last_name(), "file.txt");

  // Last name with trailing slash (directory)
  stat.set_last("directory/");
  EXPECT_EQ(stat.get_last_name(), "directory");
}

TEST_F(S3fsUtilTest, S3ObjListStatistic_ToString) {
  S3ObjListStatistic stat;
  stat.set_size(3);
  stat.set_first("first.txt");
  stat.set_last("last.txt");

  std::string str = stat.to_string();
  EXPECT_EQ(str, "(cnt:3,first:first.txt,last:last.txt)");
}

// Test S3ObjList class
TEST_F(S3fsUtilTest, S3ObjList_Empty) {
  S3ObjList objlist;
  EXPECT_TRUE(objlist.empty());
  EXPECT_EQ(objlist.size(), 0);
}

TEST_F(S3fsUtilTest, S3ObjList_InsertFile) {
  S3ObjList objlist;
  EXPECT_TRUE(objlist.insert("file.txt", false));
  EXPECT_FALSE(objlist.empty());
  EXPECT_EQ(objlist.size(), 1);
}

TEST_F(S3fsUtilTest, S3ObjList_InsertDirectory) {
  S3ObjList objlist;
  EXPECT_TRUE(objlist.insert("mydir/", true));
  EXPECT_FALSE(objlist.empty());
  EXPECT_EQ(objlist.size(), 1);
}

TEST_F(S3fsUtilTest, S3ObjList_InsertDirectoryWithSlashNormalization) {
  S3ObjList objlist;
  // Insert with trailing slash should normalize
  EXPECT_TRUE(objlist.insert("mydir/", false));
  EXPECT_EQ(objlist.size(), 1);

  // The insert function should normalize the directory name
  // and store it appropriately
  EXPECT_EQ(objlist.get_last_name(), "mydir");
}

TEST_F(S3fsUtilTest, S3ObjList_InsertEmptyName) {
  S3ObjList objlist;
  EXPECT_FALSE(objlist.insert("", false));
  EXPECT_TRUE(objlist.empty());
}

TEST_F(S3fsUtilTest, S3ObjList_GetLastName) {
  S3ObjList objlist;

  // Empty list
  EXPECT_EQ(objlist.get_last_name(), "");

  // Single file
  objlist.insert("file1.txt", false);
  EXPECT_EQ(objlist.get_last_name(), "file1.txt");

  // Multiple files - should return the last one in sorted order
  objlist.insert("file2.txt", false);
  objlist.insert("file3.txt", false);
  EXPECT_EQ(objlist.get_last_name(), "file3.txt");

  // Add a directory - alphabetically "dir" comes before "file1.txt", "file2.txt", "file3.txt"
  // So the last item is still "file3.txt"
  objlist.insert("dir/", true);
  EXPECT_EQ(objlist.get_last_name(), "file3.txt");

  // Add items that come after "file3.txt" alphabetically
  objlist.insert("zzz.txt", false);
  EXPECT_EQ(objlist.get_last_name(), "zzz.txt");

  // Add a directory that comes after "zzz.txt"
  objlist.insert("zzzdir/", true);
  EXPECT_EQ(objlist.get_last_name(), "zzzdir");
}

TEST_F(S3fsUtilTest, S3ObjList_GetStatistic) {
  S3ObjList objlist;
  objlist.insert("file1.txt", false);
  objlist.insert("file2.txt", false);
  objlist.insert("dir/", true);

  S3ObjListStatistic stat = objlist.get_statistic();
  EXPECT_EQ(stat.get_size(), 3);
  EXPECT_EQ(stat.get_first(), "dir/");  // First in alphabetical order
  EXPECT_EQ(stat.get_last(), "file2.txt");  // Last in alphabetical order
}

// Test S3HwObjList class
TEST_F(S3fsUtilTest, S3HwObjList_Empty) {
  S3HwObjList objlist;
  EXPECT_TRUE(objlist.empty());
  EXPECT_EQ(objlist.size(), 0);
}

TEST_F(S3fsUtilTest, S3HwObjList_Insert) {
  S3HwObjList objlist;
  std::string name = "test.txt";
  struct stat statBuf;
  memset(&statBuf, 0, sizeof(statBuf));
  statBuf.st_mode = S_IFREG | 0644;
  statBuf.st_size = 1024;

  EXPECT_TRUE(objlist.insert(name, statBuf));
  EXPECT_FALSE(objlist.empty());
  EXPECT_EQ(objlist.size(), 1);
}

TEST_F(S3fsUtilTest, S3HwObjList_InsertEmptyName) {
  S3HwObjList objlist;
  std::string name = "";
  struct stat statBuf;
  memset(&statBuf, 0, sizeof(statBuf));

  EXPECT_FALSE(objlist.insert(name, statBuf));
  EXPECT_TRUE(objlist.empty());
}

TEST_F(S3fsUtilTest, S3HwObjList_GetStatistic) {
  S3HwObjList objlist;

  std::string name1 = "file1.txt";
  std::string name2 = "file2.txt";
  struct stat statBuf1, statBuf2;
  memset(&statBuf1, 0, sizeof(statBuf1));
  memset(&statBuf2, 0, sizeof(statBuf2));

  objlist.insert(name1, statBuf1);
  objlist.insert(name2, statBuf2);

  S3ObjListStatistic stat = objlist.get_statistic();
  EXPECT_EQ(stat.get_size(), 2);
  EXPECT_EQ(stat.get_first(), "file1.txt");
  EXPECT_EQ(stat.get_last(), "file2.txt");
}

// Test AutoLock class
TEST_F(S3fsUtilTest, AutoLock_BasicLock) {
  pthread_mutex_t mutex;
  pthread_mutex_init(&mutex, NULL);

  {
    AutoLock lock(&mutex);
    EXPECT_TRUE(lock.isLockAcquired());
    // Lock should be released when lock goes out of scope
  }

  // Mutex should be unlocked now
  EXPECT_EQ(pthread_mutex_trylock(&mutex), 0);  // Should succeed
  pthread_mutex_unlock(&mutex);

  pthread_mutex_destroy(&mutex);
}

TEST_F(S3fsUtilTest, AutoLock_NoWait) {
  pthread_mutex_t mutex;
  pthread_mutex_init(&mutex, NULL);
  pthread_mutex_lock(&mutex);  // Lock it first

  AutoLock lock(&mutex, true);  // no_wait = true
  EXPECT_FALSE(lock.isLockAcquired());  // Should fail to acquire

  pthread_mutex_unlock(&mutex);
  pthread_mutex_destroy(&mutex);
}

TEST_F(S3fsUtilTest, AutoLock_DefaultWait) {
  // Verify AutoLock acquires lock with default (blocking) mode
  pthread_mutex_t mutex;
  pthread_mutex_init(&mutex, NULL);

  {
    AutoLock lock(&mutex);
    EXPECT_TRUE(lock.isLockAcquired());
  }

  // Mutex should be unlocked after AutoLock scope ends
  EXPECT_EQ(0, pthread_mutex_trylock(&mutex));
  pthread_mutex_unlock(&mutex);
  pthread_mutex_destroy(&mutex);
}

// Test get_realpath function
TEST_F(S3fsUtilTest, GetRealpath_NoMountPrefix) {
  mount_prefix = "";
  std::string result = get_realpath("/path/to/file");
  EXPECT_EQ(result, "/path/to/file");
}

TEST_F(S3fsUtilTest, GetRealpath_WithMountPrefix) {
  mount_prefix = "/mnt/obsfs";
  std::string result = get_realpath("/path/to/file");
  EXPECT_EQ(result, "/mnt/obsfs/path/to/file");
}

TEST_F(S3fsUtilTest, GetRealpath_EmptyPath) {
  mount_prefix = "/mnt";
  std::string result = get_realpath("");
  EXPECT_EQ(result, "/mnt");
}

TEST_F(S3fsUtilTest, GetRealpath_RootPath) {
  mount_prefix = "/mnt";
  std::string result = get_realpath("/");
  EXPECT_EQ(result, "/mnt/");
}

// Test mydirname function
TEST_F(S3fsUtilTest, Mydirname_SimplePath) {
  EXPECT_EQ(mydirname("/path/to/file.txt"), "/path/to");
}

TEST_F(S3fsUtilTest, Mydirname_RootPath) {
  EXPECT_EQ(mydirname("/"), "/");
}

TEST_F(S3fsUtilTest, Mydirname_CurrentDirectory) {
  EXPECT_EQ(mydirname("."), ".");
}

TEST_F(S3fsUtilTest, Mydirname_FileInRoot) {
  EXPECT_EQ(mydirname("/file.txt"), "/");
}

TEST_F(S3fsUtilTest, Mydirname_NullPath) {
  EXPECT_EQ(mydirname((char*)NULL), "");
}

TEST_F(S3fsUtilTest, Mydirname_EmptyPath) {
  EXPECT_EQ(mydirname(""), "");
}

TEST_F(S3fsUtilTest, Mydirname_StringVersion) {
  EXPECT_EQ(mydirname(std::string("/path/to/file.txt")), "/path/to");
}

// Test mybasename function
TEST_F(S3fsUtilTest, Mybasename_SimplePath) {
  EXPECT_EQ(mybasename("/path/to/file.txt"), "file.txt");
}

TEST_F(S3fsUtilTest, Mybasename_RootPath) {
  EXPECT_EQ(mybasename("/"), "/");
}

TEST_F(S3fsUtilTest, Mybasename_CurrentDirectory) {
  EXPECT_EQ(mybasename("."), ".");
}

TEST_F(S3fsUtilTest, Mybasename_FileInRoot) {
  EXPECT_EQ(mybasename("/file.txt"), "file.txt");
}

TEST_F(S3fsUtilTest, Mybasename_NullPath) {
  EXPECT_EQ(mybasename((char*)NULL), "");
}

TEST_F(S3fsUtilTest, Mybasename_EmptyPath) {
  EXPECT_EQ(mybasename(""), "");
}

TEST_F(S3fsUtilTest, Mybasename_StringVersion) {
  EXPECT_EQ(mybasename(std::string("/path/to/file.txt")), "file.txt");
}

TEST_F(S3fsUtilTest, Mybasename_DirectoryPath) {
  EXPECT_EQ(mybasename("/path/to/directory/"), "directory");
}

// Test toString template function
TEST_F(S3fsUtilTest, ToString_Integer) {
  EXPECT_EQ(toString(123), "123");
  EXPECT_EQ(toString(-456), "-456");
  EXPECT_EQ(toString(0), "0");
}

TEST_F(S3fsUtilTest, ToString_UnsignedInteger) {
  EXPECT_EQ(toString(123u), "123");
  EXPECT_EQ(toString(0u), "0");
}

TEST_F(S3fsUtilTest, ToString_Long) {
  EXPECT_EQ(toString(123456789L), "123456789");
}

TEST_F(S3fsUtilTest, ToString_String) {
  EXPECT_EQ(toString(std::string("hello")), "hello");
}

// Test mkdirp function
TEST_F(S3fsUtilTest, Mkdirp_SingleDirectory) {
  // Create a temporary directory for testing
  std::string test_base = "/tmp/obsfs_mkdirp_test_XXXXXX";
  char* temp_dir = strndup(test_base.c_str(), test_base.length());
  mkdtemp(temp_dir);
  std::string base_dir(temp_dir);
  free(temp_dir);

  // Test creating a single subdirectory
  std::string test_path = base_dir + "/subdir";
  EXPECT_EQ(mkdirp(test_path, 0755), 0);

  // Verify directory was created
  struct stat st;
  EXPECT_EQ(stat(test_path.c_str(), &st), 0);
  EXPECT_TRUE(S_ISDIR(st.st_mode));

  // Cleanup
  rmdir(test_path.c_str());
  rmdir(base_dir.c_str());
}

TEST_F(S3fsUtilTest, Mkdirp_NestedDirectories) {
  // Create a temporary directory for testing
  std::string test_base = "/tmp/obsfs_mkdirp_nested_XXXXXX";
  char* temp_dir = strndup(test_base.c_str(), test_base.length());
  mkdtemp(temp_dir);
  std::string base_dir(temp_dir);
  free(temp_dir);

  // Test creating nested directories
  std::string test_path = base_dir + "/level1/level2/level3";
  EXPECT_EQ(mkdirp(test_path, 0755), 0);

  // Verify all directories were created
  struct stat st;
  EXPECT_EQ(stat(test_path.c_str(), &st), 0);
  EXPECT_TRUE(S_ISDIR(st.st_mode));

  // Cleanup
  rmdir((base_dir + "/level1/level2/level3").c_str());
  rmdir((base_dir + "/level1/level2").c_str());
  rmdir((base_dir + "/level1").c_str());
  rmdir(base_dir.c_str());
}

TEST_F(S3fsUtilTest, Mkdirp_ExistingDirectory) {
  // Create a temporary directory for testing
  std::string test_base = "/tmp/obsfs_mkdirp_existing_XXXXXX";
  char* temp_dir = strndup(test_base.c_str(), test_base.length());
  mkdtemp(temp_dir);
  std::string base_dir(temp_dir);
  free(temp_dir);

  // Create a directory first
  std::string existing_dir = base_dir + "/existing";
  mkdir(existing_dir.c_str(), 0755);

  // Test mkdirp on existing directory - should succeed
  EXPECT_EQ(mkdirp(existing_dir, 0755), 0);

  // Cleanup
  rmdir(existing_dir.c_str());
  rmdir(base_dir.c_str());
}

TEST_F(S3fsUtilTest, Mkdirp_PartiallyExistingPath) {
  // Create a temporary directory for testing
  std::string test_base = "/tmp/obsfs_mkdirp_partial_XXXXXX";
  char* temp_dir = strndup(test_base.c_str(), test_base.length());
  mkdtemp(temp_dir);
  std::string base_dir(temp_dir);
  free(temp_dir);

  // Create first level directory
  std::string level1 = base_dir + "/level1";
  mkdir(level1.c_str(), 0755);

  // Test creating nested directories where first level already exists
  std::string test_path = level1 + "/level2/level3";
  EXPECT_EQ(mkdirp(test_path, 0755), 0);

  // Verify all directories were created
  struct stat st;
  EXPECT_EQ(stat(test_path.c_str(), &st), 0);
  EXPECT_TRUE(S_ISDIR(st.st_mode));

  // Cleanup
  rmdir((level1 + "/level2/level3").c_str());
  rmdir((level1 + "/level2").c_str());
  rmdir(level1.c_str());
  rmdir(base_dir.c_str());
}

// Test get_username function
TEST_F(S3fsUtilTest, GetUsername_RootUser) {
  std::string username = get_username(0);
  EXPECT_EQ(username, "root");
}

TEST_F(S3fsUtilTest, GetUsername_CurrentUser) {
  uid_t uid = getuid();
  std::string username = get_username(uid);
  EXPECT_FALSE(username.empty());
}

TEST_F(S3fsUtilTest, GetUsername_NonExistentUser) {
  // Use a very high UID that likely doesn't exist
  std::string username = get_username(999999);
  // Should return empty string for non-existent user
  EXPECT_TRUE(username.empty());
}

TEST_F(S3fsUtilTest, GetUsername_NoSuchUser) {
  // UID -1 cast to uid_t is a large number that won't exist
  std::string username = get_username(static_cast<uid_t>(-1));
  EXPECT_TRUE(username.empty());
}

// Test is_uid_include_group function
// NOTE: This function checks if a user's username is in a group's member list.
// It does NOT check primary group membership (which is handled by the kernel).
// It only checks secondary group memberships stored in /etc/group.

TEST_F(S3fsUtilTest, IsUidIncludeGroup_ValidInputs) {
  // Test with current user and primary group
  uid_t uid = getuid();
  gid_t gid = getgid();
  int result = is_uid_include_group(uid, gid);

  // Return: 1 (in group), 0 (not in group's secondary members list)
  // Primary group membership is not stored in /etc/group, so result is typically 0
  EXPECT_TRUE(result == 0 || result == 1)
      << "Expected 0 or 1, got " << result;
}

TEST_F(S3fsUtilTest, IsUidIncludeGroup_NonExistentGid) {
  // Test with a GID that likely doesn't exist
  gid_t non_existent_gid = 99999;
  uid_t uid = getuid();
  int result = is_uid_include_group(uid, non_existent_gid);

  // Group doesn't exist → getgrgid_r returns error → result is negative or 0
  EXPECT_LE(result, 0) << "Non-existent GID should return 0 or negative error";
}

TEST_F(S3fsUtilTest, IsUidIncludeGroup_NonExistentUid) {
  // Test with a UID that likely doesn't exist
  uid_t non_existent_uid = 99999;
  gid_t gid = getgid();
  int result = is_uid_include_group(non_existent_uid, gid);

  // UID doesn't exist → get_username returns "" → not found in group members → 0
  EXPECT_EQ(0, result) << "Non-existent UID should return 0";
}

TEST_F(S3fsUtilTest, IsUidIncludeGroup_RootUser) {
  // Test root user (UID 0) with root group (GID 0)
  int result = is_uid_include_group(0, 0);

  // Root's secondary membership in root group depends on /etc/group
  EXPECT_TRUE(result == 0 || result == 1)
      << "Expected 0 or 1, got " << result;
}

TEST_F(S3fsUtilTest, IsUidIncludeGroup_ErrorHandling) {
  // Test with current user's actual secondary groups
  uid_t uid = getuid();
  gid_t groups[64];
  int ngroups = getgroups(64, groups);

  if (ngroups > 0) {
    int result = is_uid_include_group(uid, groups[0]);
    EXPECT_TRUE(result == 0 || result == 1)
        << "Expected 0 or 1, got " << result;
  }
}

// Test get_blocks function
TEST_F(S3fsUtilTest, GetBlocks_ZeroSize) {
  blkcnt_t blocks = get_blocks(0);
  EXPECT_EQ(blocks, 1);  // 0 / 512 + 1 = 1
}

TEST_F(S3fsUtilTest, GetBlocks_SmallFile) {
  blkcnt_t blocks = get_blocks(512);
  EXPECT_EQ(blocks, 2);  // 512 / 512 + 1 = 2
}

TEST_F(S3fsUtilTest, GetBlocks_LargeFile) {
  blkcnt_t blocks = get_blocks(10240);
  EXPECT_EQ(blocks, 21);  // 10240 / 512 + 1 = 21
}

TEST_F(S3fsUtilTest, GetBlocks_OneByte) {
  blkcnt_t blocks = get_blocks(1);
  EXPECT_EQ(blocks, 1);  // 1 / 512 + 1 = 1
}

TEST_F(S3fsUtilTest, GetBlocks_ExactlyMultiple) {
  blkcnt_t blocks = get_blocks(512 * 10);
  EXPECT_EQ(blocks, 11);  // 5120 / 512 + 1 = 11
}

// Test cvtIAMExpireStringToTime function
TEST_F(S3fsUtilTest, CvtIAMExpireStringToTime_NullString) {
  time_t result = cvtIAMExpireStringToTime(NULL);
  EXPECT_EQ(result, 0L);
}

TEST_F(S3fsUtilTest, CvtIAMExpireStringToTime_ValidFormat) {
  // Test valid IAM expiration time format: YYYY-MM-DDTHH:MM:SS
  const char* valid_time = "2025-01-15T12:30:45";
  time_t result = cvtIAMExpireStringToTime(valid_time);
  EXPECT_GT(result, 0L);  // Should be a valid timestamp

  // Verify it's approximately correct (within 1 day of expected)
  // 2025-01-15 12:30:45 UTC should be around 1736947845
  EXPECT_NEAR(result, 1736947845L, 86400);
}

TEST_F(S3fsUtilTest, CvtIAMExpireStringToTime_Epoch) {
  // Test epoch time
  const char* epoch_time = "1970-01-01T00:00:00";
  time_t result = cvtIAMExpireStringToTime(epoch_time);
  EXPECT_EQ(result, 0L);
}

TEST_F(S3fsUtilTest, CvtIAMExpireStringToTime_FutureDate) {
  // Test a future date
  const char* future_time = "2030-12-31T23:59:59";
  time_t result = cvtIAMExpireStringToTime(future_time);
  EXPECT_GT(result, 1900000000L);  // Should be far in the future
}

// Test get_basename (C++ std::string version) function
// Function is declared in s3fs_util.h
TEST_F(S3fsUtilTest, GetBasename_String_EmptyPath) {
  std::string result = get_basename("");
  EXPECT_EQ(result, "");
}

TEST_F(S3fsUtilTest, GetBasename_String_RootPath) {
  std::string result = get_basename("/");
  EXPECT_EQ(result, "");
}

TEST_F(S3fsUtilTest, GetBasename_String_SimpleFile) {
  std::string result = get_basename("/path/to/file.txt");
  EXPECT_EQ(result, "file.txt");
}

TEST_F(S3fsUtilTest, GetBasename_String_FileInRoot) {
  std::string result = get_basename("/file.txt");
  EXPECT_EQ(result, "file.txt");
}

TEST_F(S3fsUtilTest, GetBasename_String_DirectoryWithTrailingSlash) {
  std::string result = get_basename("/path/to/directory/");
  EXPECT_EQ(result, "directory");
}

TEST_F(S3fsUtilTest, GetBasename_String_DirectoryWithoutTrailingSlash) {
  std::string result = get_basename("/path/to/directory");
  EXPECT_EQ(result, "directory");
}

TEST_F(S3fsUtilTest, GetBasename_String_NoPathSeparator) {
  std::string result = get_basename("filename.txt");
  EXPECT_EQ(result, "filename.txt");
}

TEST_F(S3fsUtilTest, GetBasename_String_MultipleSlashes) {
  std::string result = get_basename("/path/to//nested/file.txt");
  EXPECT_EQ(result, "file.txt");
}

TEST_F(S3fsUtilTest, GetBasename_String_DeepPath) {
  std::string result = get_basename("/a/b/c/d/e/f/g/h/file.txt");
  EXPECT_EQ(result, "file.txt");
}

TEST_F(S3fsUtilTest, GetBasename_String_PathWithDot) {
  std::string result = get_basename("/path/./file.txt");
  EXPECT_EQ(result, "file.txt");
}

// Test get_exist_directory_path function
TEST_F(S3fsUtilTest, GetExistDirectoryPath_RootPath) {
  std::string result = get_exist_directory_path("/");
  EXPECT_EQ(result, "/");
}

TEST_F(S3fsUtilTest, GetExistDirectoryPath_EmptyPath) {
  std::string result = get_exist_directory_path("");
  EXPECT_EQ(result, "/");
}

TEST_F(S3fsUtilTest, GetExistDirectoryPath_NonExistentPath) {
  // Use a path that likely doesn't exist
  std::string result = get_exist_directory_path("/nonexistent/path/to/nowhere");
  EXPECT_EQ(result, "/");  // Only root exists
}

TEST_F(S3fsUtilTest, GetExistDirectoryPath_TempPath) {
  // /tmp should exist on most systems
  std::string result = get_exist_directory_path("/tmp/obsfs_test/subdir");
  EXPECT_EQ(result, "/tmp");  // /tmp exists but /tmp/obsfs_test/subdir doesn't
}

TEST_F(S3fsUtilTest, GetExistDirectoryPath_EtcPath) {
  // /etc should exist on most systems
  std::string result = get_exist_directory_path("/etc/passwd");
  EXPECT_EQ(result, "/etc");  // /etc exists
}

TEST_F(S3fsUtilTest, GetExistDirectoryPath_MultipleExistingLevels) {
  // Test with a path where multiple levels exist
  // /var is a common directory that exists
  std::string result = get_exist_directory_path("/var/log/test/subdir");
  // At minimum, /var should exist. /var/log likely exists too.
  EXPECT_TRUE(result == "/var" || result == "/var/log");
}

TEST_F(S3fsUtilTest, GetExistDirectoryPath_ProcPath) {
  // /proc should exist on Linux
  std::string result = get_exist_directory_path("/proc/version");
  EXPECT_EQ(result, "/proc");
}

TEST_F(S3fsUtilTest, GetExistDirectoryPath_DevPath) {
  // /dev should exist on Linux
  std::string result = get_exist_directory_path("/dev/null/test");
  EXPECT_EQ(result, "/dev");
}

// Test get_inodeNo function
TEST_F(S3fsUtilTest, GetInodeNo_ValidNumber) {
  long long inode = get_inodeNo("12345678");
  EXPECT_EQ(inode, 12345678LL);
}

TEST_F(S3fsUtilTest, GetInodeNo_Zero) {
  long long inode = get_inodeNo("0");
  EXPECT_EQ(inode, 0LL);
}

TEST_F(S3fsUtilTest, GetInodeNo_NegativeNumber) {
  // s3fs_strtoofft doesn't support negative numbers, returns 0
  long long inode = get_inodeNo("-1");
  EXPECT_EQ(inode, 0LL);
}

// Test get_dhtVersion function
TEST_F(S3fsUtilTest, GetDhtVersion_ValidNumber) {
  int version = get_dhtVersion("123");
  EXPECT_EQ(version, 123);
}

TEST_F(S3fsUtilTest, GetDhtVersion_Zero) {
  int version = get_dhtVersion("0");
  EXPECT_EQ(version, 0);
}

TEST_F(S3fsUtilTest, GetDhtVersion_NegativeNumber) {
  // s3fs_strtoofft doesn't support negative numbers, returns 0
  int version = get_dhtVersion("-1");
  EXPECT_EQ(version, 0);
}

// Test get_size function with headers_t
TEST_F(S3fsUtilTest, GetSize_FromHeaders) {
  headers_t meta;
  meta["Content-Length"] = "1024";

  off_t size = get_size(meta);
  EXPECT_EQ(size, 1024);
}

TEST_F(S3fsUtilTest, GetSize_FromHeaders_Empty) {
  headers_t meta;

  off_t size = get_size(meta);
  EXPECT_EQ(size, 0);
}

TEST_F(S3fsUtilTest, GetSize_FromHeaders_MissingContentLength) {
  headers_t meta;
  meta["x-amz-meta-something"] = "value";

  off_t size = get_size(meta);
  EXPECT_EQ(size, 0);
}

// Test get_mtime function with headers_t
TEST_F(S3fsUtilTest, GetMtime_FromHeaders) {
  headers_t meta;
  meta["x-amz-meta-mtime"] = "1234567890";

  time_t mtime = get_mtime(meta, false);
  EXPECT_EQ(mtime, 1234567890L);
}

TEST_F(S3fsUtilTest, GetMtime_FromHeaders_NotFound) {
  headers_t meta;
  meta["Content-Type"] = "application/octet-stream";

  time_t mtime = get_mtime(meta, false);
  EXPECT_EQ(mtime, 0L);
}

// Test get_uid function with headers_t
TEST_F(S3fsUtilTest, GetUid_FromHeaders) {
  headers_t meta;
  meta["x-amz-meta-uid"] = "1000";

  uid_t uid = get_uid(meta);
  EXPECT_EQ(uid, 1000);
}

TEST_F(S3fsUtilTest, GetUid_FromHeaders_S3SyncFallback) {
  headers_t meta;
  // No x-amz-meta-uid, but has x-amz-meta-owner
  meta["x-amz-meta-owner"] = "2000";

  uid_t uid = get_uid(meta);
  EXPECT_EQ(uid, 2000);
}

TEST_F(S3fsUtilTest, GetUid_FromHeaders_NotFound) {
  // File bucket / unknown bucket: keeps historical return 0 fallback.
  // Object bucket switches to geteuid(); see GetUid_NotFound_ObjectBucket.
  bucket_type_t saved = g_bucket_type;
  g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
  headers_t meta;
  meta["Content-Type"] = "application/octet-stream";

  uid_t uid = get_uid(meta);
  EXPECT_EQ(uid, 0);
  g_bucket_type = saved;
}

// ISSUE2026051200001 C.2 review fix: object-bucket fallback uses geteuid().
TEST_F(S3fsUtilTest, GetUid_FromHeaders_NotFound_ObjectBucket) {
  bucket_type_t saved = g_bucket_type;
  g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
  headers_t meta;
  meta["Content-Type"] = "application/octet-stream";

  uid_t uid = get_uid(meta);
  EXPECT_EQ(uid, geteuid());   // mount user, not root
  g_bucket_type = saved;
}

// Test get_gid function with headers_t
TEST_F(S3fsUtilTest, GetGid_FromHeaders) {
  headers_t meta;
  meta["x-amz-meta-gid"] = "1000";

  gid_t gid = get_gid(meta);
  EXPECT_EQ(gid, 1000);
}

TEST_F(S3fsUtilTest, GetGid_FromHeaders_S3SyncFallback) {
  headers_t meta;
  // No x-amz-meta-gid, but has x-amz-meta-group
  meta["x-amz-meta-group"] = "2000";

  gid_t gid = get_gid(meta);
  EXPECT_EQ(gid, 2000);
}

TEST_F(S3fsUtilTest, GetGid_FromHeaders_NotFound) {
  // File bucket / unknown bucket: keeps historical return 0 fallback.
  bucket_type_t saved = g_bucket_type;
  g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
  headers_t meta;
  meta["Content-Type"] = "application/octet-stream";

  gid_t gid = get_gid(meta);
  EXPECT_EQ(gid, 0);
  g_bucket_type = saved;
}

// ISSUE2026051200001 C.2 review fix: object-bucket fallback uses getegid().
TEST_F(S3fsUtilTest, GetGid_FromHeaders_NotFound_ObjectBucket) {
  bucket_type_t saved = g_bucket_type;
  g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
  headers_t meta;
  meta["Content-Type"] = "application/octet-stream";

  gid_t gid = get_gid(meta);
  EXPECT_EQ(gid, getegid());   // mount group, not root
  g_bucket_type = saved;
}

// Test get_lastmodified function
TEST_F(S3fsUtilTest, GetLastModified_ValidFormat) {
  const char* time_str = "Wed, 15 Jan 2025 12:30:45 GMT";
  time_t result = get_lastmodified(time_str);
  EXPECT_GT(result, 0L);
}

TEST_F(S3fsUtilTest, GetLastModified_NullString) {
  time_t result = get_lastmodified(static_cast<const char*>(NULL));
  EXPECT_EQ(result, 0L);
}

TEST_F(S3fsUtilTest, GetLastModified_FromHeaders) {
  headers_t meta;
  meta["Last-Modified"] = "Wed, 15 Jan 2025 12:30:45 GMT";

  time_t result = get_lastmodified(meta);
  EXPECT_GT(result, 0L);
}

TEST_F(S3fsUtilTest, GetLastModified_FromHeaders_NotFound) {
  headers_t meta;
  meta["Content-Type"] = "application/octet-stream";

  time_t result = get_lastmodified(meta);
  EXPECT_EQ(result, 0L);
}

// Test get_mode function - basic behavior tests
TEST_F(S3fsUtilTest, GetMode_NullInput) {
  mode_t mode = get_mode((const char*)NULL);
  EXPECT_EQ(mode, 0);
}

// Test get_mode with hws_obs_meta_mode header - this covers line 512-513 in s3fs_util.cpp
TEST_F(S3fsUtilTest, GetMode_HwsObsMetaMode) {
  headers_t meta;
  // Use decimal 493 which equals octal 0755
  meta["x-amz-meta-mode"] = "493";
  meta["Content-Type"] = "application/octet-stream";

  mode_t mode = get_mode(meta, "/path/to/file", true, false);
  // When hws_obs_meta_mode is present, it should be used as base mode
  // Then get_refined_mode_by_file_type adds file type bits (S_IFREG for regular files)
  EXPECT_TRUE(S_ISREG(mode));  // Should be a regular file
  EXPECT_EQ(mode & 0777, 0755);  // Permission bits should be 0755 (octal) = 493 (decimal)
}

// Test get_mode with headers_t - directory content type
TEST_F(S3fsUtilTest, GetMode_DirectoryContentType) {
  headers_t meta;
  meta["Content-Type"] = "application/x-directory";

  mode_t mode = get_mode(meta, "/path/", true, false);
  EXPECT_TRUE(S_ISDIR(mode));
}

TEST_F(S3fsUtilTest, GetMode_ForceDir) {
  headers_t meta;

  mode_t mode = get_mode(meta, "/path/", true, true);
  EXPECT_TRUE(S_ISDIR(mode));
}

// ISSUE2026051200001 C.3 review fix: object-bucket fallback when no
// x-amz-meta-mode and no x-amz-meta-permissions → 0750 (dir) / 0640 (file).
// File bucket keeps mode=0 (refine adds only S_IFDIR/S_IFREG).
TEST_F(S3fsUtilTest, GetMode_NoModeHeader_ObjectBucket_Dir) {
  bucket_type_t saved = g_bucket_type;
  g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
  headers_t meta;
  meta["Content-Type"] = "application/x-directory";

  mode_t mode = get_mode(meta, "/some/dir/", true, true);
  EXPECT_TRUE(S_ISDIR(mode));
  EXPECT_EQ(mode & 0777, 0750);
  g_bucket_type = saved;
}

TEST_F(S3fsUtilTest, GetMode_NoModeHeader_ObjectBucket_File) {
  bucket_type_t saved = g_bucket_type;
  g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
  headers_t meta;
  meta["Content-Type"] = "application/octet-stream";

  mode_t mode = get_mode(meta, "/some/file.txt", true, false);
  EXPECT_TRUE(S_ISREG(mode));
  EXPECT_EQ(mode & 0777, 0640);
  g_bucket_type = saved;
}

TEST_F(S3fsUtilTest, GetMode_NoModeHeader_FileBucket_KeepsZero) {
  // File bucket: fallback unchanged. mode bits stay 0 (refine adds S_IFREG only).
  bucket_type_t saved_bucket = g_bucket_type;
  bool saved_complement = complement_stat;
  g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
  complement_stat = false;
  headers_t meta;
  meta["Content-Type"] = "application/octet-stream";

  mode_t mode = get_mode(meta, "/some/file.txt", true, false);
  EXPECT_TRUE(S_ISREG(mode));
  EXPECT_EQ(mode & 0777, 0);   // file bucket keeps historical 0
  g_bucket_type = saved_bucket;
  complement_stat = saved_complement;
}

// Test get_mode with complement_stat enabled
TEST_F(S3fsUtilTest, GetMode_ComplementStat) {
  complement_stat = true;  // Enable complement stat mode
  headers_t meta;
  meta["Content-Length"] = "0";
  meta["Content-Type"] = "text/plain";

  // Path ending with / and small size should be treated as directory
  mode_t mode = get_mode(meta, "/path/", true, false);
  EXPECT_TRUE(S_ISDIR(mode));

  complement_stat = false;  // Reset
}

// Test is_need_check_obj_detail function
TEST_F(S3fsUtilTest, IsNeedCheckObjDetail_NonZeroSize) {
  headers_t meta;
  meta["Content-Length"] = "1024";

  bool result = is_need_check_obj_detail(meta);
  EXPECT_FALSE(result);  // Non-zero size means no need to check
}

TEST_F(S3fsUtilTest, IsNeedCheckObjDetail_HasMetaInfo) {
  headers_t meta;
  meta["Content-Length"] = "0";
  meta["x-amz-meta-mode"] = "0755";  // production header name

  bool result = is_need_check_obj_detail(meta);
  EXPECT_FALSE(result);  // Has meta info, no need to check
}

TEST_F(S3fsUtilTest, IsNeedCheckObjDetail_S3SyncMeta) {
  headers_t meta;
  meta["Content-Length"] = "0";
  meta["x-amz-meta-owner"] = "1000";

  bool result = is_need_check_obj_detail(meta);
  EXPECT_FALSE(result);  // Has s3sync meta, no need to check
}

TEST_F(S3fsUtilTest, IsNeedCheckObjDetail_DirectoryContentType) {
  headers_t meta;
  meta["Content-Length"] = "0";
  meta["Content-Type"] = "application/x-directory";

  bool result = is_need_check_obj_detail(meta);
  EXPECT_FALSE(result);  // Is directory, no need to check
}

TEST_F(S3fsUtilTest, IsNeedCheckObjDetail_NoContentType) {
  headers_t meta;
  meta["Content-Length"] = "0";

  bool result = is_need_check_obj_detail(meta);
  EXPECT_FALSE(result);  // No content type, no need to check
}

TEST_F(S3fsUtilTest, IsNeedCheckObjDetail_NeedsCheck) {
  headers_t meta;
  meta["Content-Length"] = "0";
  meta["Content-Type"] = "application/octet-stream";

  bool result = is_need_check_obj_detail(meta);
  EXPECT_TRUE(result);  // Zero size + octet-stream type needs checking
}

// Test check_exist_dir_permission function
TEST_F(S3fsUtilTest, CheckExistDirPermission_NullPath) {
  bool result = check_exist_dir_permission(NULL);
  EXPECT_FALSE(result);
}

TEST_F(S3fsUtilTest, CheckExistDirPermission_EmptyPath) {
  bool result = check_exist_dir_permission("");
  EXPECT_FALSE(result);
}

TEST_F(S3fsUtilTest, CheckExistDirPermission_NonExistent) {
  // Use a path that likely doesn't exist
  bool result = check_exist_dir_permission("/nonexistent/path/12345");
  EXPECT_TRUE(result);  // Non-existent is OK (will be created)
}

TEST_F(S3fsUtilTest, CheckExistDirPermission_TempDirectory) {
  // /tmp should exist and be accessible
  bool result = check_exist_dir_permission("/tmp");
  EXPECT_TRUE(result);
}

TEST_F(S3fsUtilTest, CheckExistDirPermission_FileNotDirectory) {
  // Test with a file path, not a directory
  // Create a temporary file
  std::string test_file = "/tmp/obsfs_perm_test_XXXXXX";
  int fd = mkstemp(&test_file[0]);
  if (fd >= 0) {
    close(fd);

    bool result = check_exist_dir_permission(test_file.c_str());
    EXPECT_FALSE(result);  // Path exists but is not a directory

    unlink(test_file.c_str());
  }
}

// Test delete_files_in_dir function
TEST_F(S3fsUtilTest, DeleteFilesInDir_EmptyDirectory) {
  // Create a temporary empty directory
  std::string test_base = "/tmp/obsfs_delete_test_XXXXXX";
  char* temp_dir = strndup(test_base.c_str(), test_base.length());
  mkdtemp(temp_dir);
  std::string test_dir(temp_dir);
  free(temp_dir);

  // Test deleting files in empty directory (not removing the directory itself)
  EXPECT_TRUE(delete_files_in_dir(test_dir.c_str(), false));

  // Directory should still exist
  struct stat st;
  EXPECT_EQ(stat(test_dir.c_str(), &st), 0);
  EXPECT_TRUE(S_ISDIR(st.st_mode));

  // Cleanup
  rmdir(test_dir.c_str());
}

TEST_F(S3fsUtilTest, DeleteFilesInDir_DirectoryWithFiles) {
  // Create a temporary directory
  std::string test_base = "/tmp/obsfs_delete_files_test_XXXXXX";
  char* temp_dir = strndup(test_base.c_str(), test_base.length());
  mkdtemp(temp_dir);
  std::string test_dir(temp_dir);
  free(temp_dir);

  // Create some test files
  std::string file1 = test_dir + "/file1.txt";
  std::string file2 = test_dir + "/file2.txt";

  int fd1 = open(file1.c_str(), O_WRONLY | O_CREAT, 0644);
  if (fd1 >= 0) {
    write(fd1, "test", 4);
    close(fd1);
  }

  int fd2 = open(file2.c_str(), O_WRONLY | O_CREAT, 0644);
  if (fd2 >= 0) {
    write(fd2, "test", 4);
    close(fd2);
  }

  // Delete files in directory (not removing the directory itself)
  EXPECT_TRUE(delete_files_in_dir(test_dir.c_str(), false));

  // Directory should still exist
  struct stat st;
  EXPECT_EQ(stat(test_dir.c_str(), &st), 0);

  // Files should be deleted
  EXPECT_NE(stat(file1.c_str(), &st), 0);  // Should not exist
  EXPECT_NE(stat(file2.c_str(), &st), 0);  // Should not exist

  // Cleanup
  rmdir(test_dir.c_str());
}

TEST_F(S3fsUtilTest, DeleteFilesInDir_RemoveOwnDirectory) {
  // Create a temporary directory
  std::string test_base = "/tmp/obsfs_delete_remove_test_XXXXXX";
  char* temp_dir = strndup(test_base.c_str(), test_base.length());
  mkdtemp(temp_dir);
  std::string test_dir(temp_dir);
  free(temp_dir);

  // Delete files and remove the directory itself
  EXPECT_TRUE(delete_files_in_dir(test_dir.c_str(), true));

  // Directory should be removed
  struct stat st;
  EXPECT_NE(stat(test_dir.c_str(), &st), 0);  // Should not exist
}

TEST_F(S3fsUtilTest, DeleteFilesInDir_NestedDirectories) {
  // Create a temporary directory
  std::string test_base = "/tmp/obsfs_delete_nested_test_XXXXXX";
  char* temp_dir = strndup(test_base.c_str(), test_base.length());
  mkdtemp(temp_dir);
  std::string test_dir(temp_dir);
  free(temp_dir);

  // Create nested directories and files
  std::string subdir = test_dir + "/subdir";
  mkdir(subdir.c_str(), 0755);

  std::string file1 = test_dir + "/file1.txt";
  std::string file2 = subdir + "/file2.txt";

  int fd1 = open(file1.c_str(), O_WRONLY | O_CREAT, 0644);
  if (fd1 >= 0) {
    write(fd1, "test", 4);
    close(fd1);
  }

  int fd2 = open(file2.c_str(), O_WRONLY | O_CREAT, 0644);
  if (fd2 >= 0) {
    write(fd2, "test", 4);
    close(fd2);
  }

  // Delete files and remove the directory itself
  EXPECT_TRUE(delete_files_in_dir(test_dir.c_str(), true));

  // All should be removed
  struct stat st;
  EXPECT_NE(stat(test_dir.c_str(), &st), 0);  // Should not exist
}

TEST_F(S3fsUtilTest, DeleteFilesInDir_NonExistentDirectory) {
  // Test with non-existent directory
  EXPECT_FALSE(delete_files_in_dir("/nonexistent/path/obsfs_test_12345", false));
}

// Test get_refined_mode_by_file_type function
TEST_F(S3fsUtilTest, GetRefinedModeByFileType_BasicFile) {
  headers_t meta;
  meta["Content-Type"] = "application/octet-stream";
  mode_t mode = 0644;

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/to/file.txt", false, true, false);
  EXPECT_TRUE(S_ISREG(result));
  EXPECT_EQ(result & 0777, 0644);
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_DirectoryContentType) {
  headers_t meta;
  meta["Content-Type"] = "application/x-directory";
  mode_t mode = 0755;

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/to/dir/", false, true, false);
  EXPECT_TRUE(S_ISDIR(result));
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_PathEndsWithSlash_OctetStream) {
  headers_t meta;
  meta["Content-Type"] = "binary/octet-stream";
  mode_t mode = 0644;

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/to/dir/", false, true, false);
  EXPECT_TRUE(S_ISDIR(result));
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_PathEndsWithSlash_ApplicationOctetStream) {
  headers_t meta;
  meta["Content-Type"] = "application/octet-stream";
  mode_t mode = 0644;

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/to/dir/", false, true, false);
  EXPECT_TRUE(S_ISDIR(result));
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_PathEndsWithSlash_TextPlain_ZeroSize) {
  complement_stat = true;
  headers_t meta;
  meta["Content-Type"] = "text/plain";
  meta["Content-Length"] = "0";
  mode_t mode = 0644;

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/to/dir/", false, true, false);
  EXPECT_TRUE(S_ISDIR(result));
  complement_stat = false;
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_PathEndsWithSlash_TextPlain_SizeOne) {
  complement_stat = true;
  headers_t meta;
  meta["Content-Type"] = "text/plain";
  meta["Content-Length"] = "1";
  mode_t mode = 0644;

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/to/dir/", false, true, false);
  EXPECT_TRUE(S_ISDIR(result));
  complement_stat = false;
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_PathEndsWithSlash_TextPlain_LargerSize) {
  complement_stat = true;
  headers_t meta;
  meta["Content-Type"] = "text/plain";
  meta["Content-Length"] = "100";
  mode_t mode = 0644;

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/to/dir/", false, true, false);
  EXPECT_TRUE(S_ISREG(result));
  complement_stat = false;
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_NoContentType) {
  headers_t meta;
  mode_t mode = 0644;

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/to/file.txt", false, true, false);
  EXPECT_TRUE(S_ISREG(result));
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_ForceDir) {
  headers_t meta;
  meta["Content-Type"] = "text/plain";
  mode_t mode = 0644;

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/some/path", false, true, true);
  EXPECT_TRUE(S_ISDIR(result));
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_ComplementStat_NoPermission) {
  complement_stat = true;
  headers_t meta;
  meta["Content-Type"] = "application/octet-stream";
  mode_t mode = 0;  // No permission bits set

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/to/file", false, true, false);
  EXPECT_TRUE(S_ISREG(result));
  EXPECT_TRUE(result & S_IRUSR);  // Should have read permission
  complement_stat = false;
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_ComplementStat_DirectoryNoPermission) {
  complement_stat = true;
  headers_t meta;
  meta["Content-Type"] = "application/x-directory";
  mode_t mode = 0;  // No permission bits set

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/to/dir/", false, true, false);
  EXPECT_TRUE(S_ISDIR(result));
  EXPECT_TRUE(result & S_IRUSR);  // Should have read permission
  EXPECT_TRUE(result & S_IXUSR);  // Directory should have execute permission
  complement_stat = false;
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_ContentTypeWithCharset) {
  headers_t meta;
  meta["Content-Type"] = "text/plain; charset=utf-8";
  mode_t mode = 0644;

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/to/file.txt", false, true, false);
  EXPECT_TRUE(S_ISREG(result));
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_S3Sync_ModeWithFileTypeBits) {
  headers_t meta;
  // S_IFDIR | 0755 has file type bits set (mode & S_IFMT != 0),
  // so the function returns early at line 530 without entering the s3sync path.
  mode_t mode = S_IFDIR | 0755;

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/", true, false, false);
  // Early return: mode already has file type bits → preserved unchanged
  EXPECT_TRUE(S_ISDIR(result));
  EXPECT_EQ(result & 0777, 0755);
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_S3Sync_CutFlags) {
  headers_t meta;
  // Mode WITHOUT file type bits (mode & S_IFMT == 0) → enters s3sync path.
  // isS3sync=true, checkdir=false → clears S_IFDIR and S_IFREG from mode (lines 580-581).
  // Since mode=0755 has no file type bits, result is unchanged: 0755 with no S_IFMT.
  mode_t mode = 0755;

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/", true, false, false);
  EXPECT_EQ(0u, result & S_IFMT);   // No file type bits after s3sync cut
  EXPECT_EQ(0755, result & 07777);   // Permission bits preserved
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_PreservesSpecialFileType) {
  headers_t meta;
  meta["Content-Type"] = "application/octet-stream";
  mode_t mode = S_IFIFO | 0644;  // FIFO pipe

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/to/pipe", false, true, false);
  // Special file types (FIFO, SOCKET, CHR, BLK, LNK) should be preserved
  EXPECT_TRUE(S_ISFIFO(result));
}

TEST_F(S3fsUtilTest, GetRefinedModeByFileType_SocketFile) {
  headers_t meta;
  meta["Content-Type"] = "application/octet-stream";
  mode_t mode = S_IFSOCK | 0644;  // Socket

  mode_t result = get_refined_mode_by_file_type(mode, meta, "/path/to/socket", false, true, false);
  // Socket type should be preserved
  EXPECT_TRUE(S_ISSOCK(result));
}

// Test DHT View Version Info functions
TEST_F(S3fsUtilTest, InitDHTViewVersionInfo) {
  // Initialize DHT view version info
  initDHTViewVersinoInfo();

  // After initialization, max version should be INIT_DHTVIEW_VERSION_ID (-1)
  EXPECT_EQ(getMaxDHTViewVersionId(), INIT_DHTVIEW_VERSION_ID);

  // Clean up
  destroyDHTViewVersinoInfo();
}

TEST_F(S3fsUtilTest, UpdateDHTViewVersionId) {
  initDHTViewVersinoInfo();

  // Update DHT view version info
  updateDHTViewVersionInfo(10);
  EXPECT_EQ(getMaxDHTViewVersionId(), 10);

  updateDHTViewVersionInfo(20);
  EXPECT_EQ(getMaxDHTViewVersionId(), 20);

  destroyDHTViewVersinoInfo();
}

TEST_F(S3fsUtilTest, UpdateDHTViewVersionId_NoChangeForLower) {
  initDHTViewVersinoInfo();

  // Update with higher version
  updateDHTViewVersionInfo(100);
  EXPECT_EQ(getMaxDHTViewVersionId(), 100);

  // Try to update with lower version (should not change max)
  updateDHTViewVersionInfo(50);
  EXPECT_EQ(getMaxDHTViewVersionId(), 100);

  destroyDHTViewVersinoInfo();
}

TEST_F(S3fsUtilTest, DestroyDHTViewVersionInfo) {
  initDHTViewVersinoInfo();

  // Update some values
  updateDHTViewVersionInfo(5);

  // Destroy should reset values
  destroyDHTViewVersinoInfo();
  EXPECT_EQ(getMaxDHTViewVersionId(), INIT_DHTVIEW_VERSION_ID);
}

// Test generate_virtual_inode function
TEST_F(S3fsUtilTest, GenerateVirtualInode_RootPath) {
  ino_t inode = generate_virtual_inode("/");
  EXPECT_GT(inode, 0);
}

TEST_F(S3fsUtilTest, GenerateVirtualInode_SimplePath) {
  ino_t inode1 = generate_virtual_inode("/file.txt");
  ino_t inode2 = generate_virtual_inode("/file.txt");

  // Same path should generate same inode
  EXPECT_EQ(inode1, inode2);
  EXPECT_GT(inode1, 0);
}

TEST_F(S3fsUtilTest, GenerateVirtualInode_DifferentPaths) {
  ino_t inode1 = generate_virtual_inode("/file1.txt");
  ino_t inode2 = generate_virtual_inode("/file2.txt");

  // Different paths should generate different inodes (very likely)
  EXPECT_NE(inode1, inode2);
  EXPECT_GT(inode1, 0);
  EXPECT_GT(inode2, 0);
}

TEST_F(S3fsUtilTest, GenerateVirtualInode_NestedPath) {
  ino_t inode = generate_virtual_inode("/path/to/nested/file.txt");
  EXPECT_GT(inode, 0);
}

TEST_F(S3fsUtilTest, GenerateVirtualInode_Consistency) {
  const char* path = "/test/consistency/path.txt";

  // Generate multiple times, should be consistent
  ino_t inode1 = generate_virtual_inode(path);
  ino_t inode2 = generate_virtual_inode(path);
  ino_t inode3 = generate_virtual_inode(path);

  EXPECT_EQ(inode1, inode2);
  EXPECT_EQ(inode2, inode3);
}

TEST_F(S3fsUtilTest, GenerateVirtualInode_SimilarPaths) {
  ino_t inode1 = generate_virtual_inode("/file.txt");
  ino_t inode2 = generate_virtual_inode("/file.txt/");
  ino_t inode3 = generate_virtual_inode("file.txt");

  // Even similar paths should generate different inodes
  EXPECT_NE(inode1, inode2);
  EXPECT_NE(inode1, inode3);
}

TEST_F(S3fsUtilTest, GenerateVirtualInode_LongPath) {
  std::string long_path = "/very/long/path/to/some/deeply/nested/directory/with/many/levels/file.txt";
  ino_t inode = generate_virtual_inode(long_path.c_str());
  EXPECT_GT(inode, 0);
}

TEST_F(S3fsUtilTest, GenerateVirtualInode_SpecialCharacters) {
  ino_t inode = generate_virtual_inode("/path/with-special_chars_123.txt");
  EXPECT_GT(inode, 0);
}

TEST_F(S3fsUtilTest, GenerateVirtualInode_Distribution) {
  // Generate inodes for multiple paths and check they're distributed
  std::set<ino_t> inodes;
  for (int i = 0; i < 100; i++) {
    std::string path = "/file" + std::to_string(i) + ".txt";
    inodes.insert(generate_virtual_inode(path.c_str()));
  }

  // Should have 100 unique inodes (or very close to it)
  EXPECT_GE(inodes.size(), 95);
}

// Test get_mtime with overcheck parameter
TEST_F(S3fsUtilTest, GetMtime_OvercheckWithLastModified) {
  headers_t meta;
  // No x-amz-meta-mtime, but has Last-Modified
  meta["Last-Modified"] = "Wed, 15 Jan 2025 12:30:45 GMT";

  time_t mtime = get_mtime(meta, true);  // overcheck = true
  EXPECT_GT(mtime, 0L);  // Should use Last-Modified
}

TEST_F(S3fsUtilTest, GetMtime_OvercheckNoMeta) {
  headers_t meta;
  // No mtime or Last-Modified

  time_t mtime = get_mtime(meta, true);  // overcheck = true
  EXPECT_EQ(mtime, 0L);
}

TEST_F(S3fsUtilTest, GetMtime_NoOvercheckNoMeta) {
  headers_t meta;
  // No x-amz-meta-mtime

  time_t mtime = get_mtime(meta, false);  // overcheck = false
  EXPECT_EQ(mtime, 0L);  // Should not check Last-Modified
}

// Test show_usage, show_help, show_version functions
// These print to stdout. We redirect stdout and verify non-empty output.
TEST_F(S3fsUtilTest, ShowUsage_ProducesOutput) {
  testing::internal::CaptureStdout();
  show_usage();
  std::string output = testing::internal::GetCapturedStdout();
  EXPECT_FALSE(output.empty()) << "show_usage should produce output";
}

TEST_F(S3fsUtilTest, ShowHelp_ProducesOutput) {
  testing::internal::CaptureStdout();
  show_help();
  std::string output = testing::internal::GetCapturedStdout();
  EXPECT_FALSE(output.empty()) << "show_help should produce output";
}

TEST_F(S3fsUtilTest, ShowVersion_ProducesOutput) {
  testing::internal::CaptureStdout();
  show_version();
  std::string output = testing::internal::GetCapturedStdout();
  EXPECT_FALSE(output.empty()) << "show_version should produce output";
}

// Additional tests for get_basename (C++ std::string version)
// Note: GetBasename_String_RootPath (line ~685) already tests get_basename("/") == ""
TEST_F(S3fsUtilTest, GetBasename_String_DoubleSlash) {
  std::string result = get_basename("//");
  EXPECT_EQ(result, "");
}

TEST_F(S3fsUtilTest, GetBasename_String_WhitespacePath) {
  std::string result = get_basename("/path/with spaces/file name.txt");
  EXPECT_EQ(result, "file name.txt");
}

TEST_F(S3fsUtilTest, GetBasename_String_UnicodePath) {
  std::string result = get_basename("/path/文件.txt");
  EXPECT_EQ(result, "文件.txt");
}

TEST_F(S3fsUtilTest, GetBasename_String_PathWithDots) {
  std::string result = get_basename("/path/to/../file.txt");
  EXPECT_EQ(result, "file.txt");
}

TEST_F(S3fsUtilTest, GetBasename_String_PathWithMultipleSlashes) {
  std::string result = get_basename("/path///to//file.txt");
  EXPECT_EQ(result, "file.txt");
}

TEST_F(S3fsUtilTest, GetBasename_String_EndsWithMultipleSlashes) {
  // get_basename only removes one trailing slash, so this case
  // results in an empty basename because the remaining "//" causes
  // find_last_of to return the position before "dir"
  std::string result = get_basename("/path/to/dir///");
  EXPECT_EQ(result, "");
}

TEST_F(S3fsUtilTest, GetBasename_String_EndsWithMultipleSlashesSingleDir) {
  // Test with path ending in "/" but only one directory level
  std::string result = get_basename("/dir//");
  EXPECT_EQ(result, "");
}

TEST_F(S3fsUtilTest, GetBasename_String_EndsWithSingleSlash) {
  // Test with single trailing slash - normal case
  std::string result = get_basename("/path/to/dir/");
  EXPECT_EQ(result, "dir");
}

// More edge cases for generate_virtual_inode
TEST_F(S3fsUtilTest, GenerateVirtualInode_EmptyPath) {
  ino_t inode = generate_virtual_inode("");
  // Empty path should still generate a valid inode
  EXPECT_GT(inode, 0);
}

TEST_F(S3fsUtilTest, GenerateVirtualInode_SingleCharPath) {
  ino_t inode = generate_virtual_inode("a");
  EXPECT_GT(inode, 0);
}

TEST_F(S3fsUtilTest, GenerateVirtualInode_AllSameChar) {
  ino_t inode1 = generate_virtual_inode("aaaaaaa");
  ino_t inode2 = generate_virtual_inode("/a/a/a/a/a/a");
  // Different paths should generate different inodes
  EXPECT_NE(inode1, inode2);
}

TEST_F(S3fsUtilTest, GenerateVirtualInode_CaseSensitive) {
  ino_t inode1 = generate_virtual_inode("/path/File.txt");
  ino_t inode2 = generate_virtual_inode("/path/file.txt");
  // Case sensitive paths should generate different inodes
  EXPECT_NE(inode1, inode2);
}

TEST_F(S3fsUtilTest, GenerateVirtualInode_HashCollisionUnlikely) {
  // Generate 1000 random paths and check for collisions
  std::set<ino_t> inodes;
  for (int i = 0; i < 1000; i++) {
    std::string path = "/random/path/" + std::to_string(i) + "/file.dat";
    inodes.insert(generate_virtual_inode(path.c_str()));
  }
  // FNV-1a hash should have very low collision rate
  // For 1000 items, we expect close to 1000 unique values
  EXPECT_GE(inodes.size(), 995);
}

// Additional tests for improved coverage
// Test AutoLock with edge cases
TEST_F(S3fsUtilTest, AutoLock_MultipleLocksSameThread) {
  pthread_mutex_t mutex;
  pthread_mutex_init(&mutex, NULL);

  {
    AutoLock lock1(&mutex);
    EXPECT_TRUE(lock1.isLockAcquired());

    // Recursive lock on same mutex (non-recursive mutex)
    AutoLock lock2(&mutex, true);  // no_wait = true
    EXPECT_FALSE(lock2.isLockAcquired());  // Should fail
  }

  pthread_mutex_destroy(&mutex);
}

// Test S3ObjList with edge cases
TEST_F(S3fsUtilTest, S3ObjList_InsertSameFileTwice) {
  S3ObjList objlist;
  EXPECT_TRUE(objlist.insert("file.txt", false));
  // Note: insert may return true even for duplicates depending on implementation
  // The important thing is size should not increase
  objlist.insert("file.txt", false);
  EXPECT_EQ(objlist.size(), 1);
}

TEST_F(S3fsUtilTest, S3ObjList_InsertManyFiles) {
  S3ObjList objlist;
  for (int i = 0; i < 100; i++) {
    std::string file = "file" + std::to_string(i) + ".txt";
    EXPECT_TRUE(objlist.insert(file, false));
  }
  EXPECT_EQ(objlist.size(), 100);
}

TEST_F(S3fsUtilTest, S3ObjList_InsertMixedFilesAndDirs) {
  S3ObjList objlist;
  EXPECT_TRUE(objlist.insert("file1.txt", false));
  EXPECT_TRUE(objlist.insert("dir1/", true));
  EXPECT_TRUE(objlist.insert("file2.txt", false));
  EXPECT_TRUE(objlist.insert("dir2/", true));

  EXPECT_EQ(objlist.size(), 4);
}

// Test S3HwObjList with stat structures
TEST_F(S3fsUtilTest, S3HwObjList_DifferentFileTypes) {
  S3HwObjList objlist;

  std::string name = "test.txt";
  struct stat statBuf;
  memset(&statBuf, 0, sizeof(statBuf));

  // Test regular file
  statBuf.st_mode = S_IFREG | 0644;
  EXPECT_TRUE(objlist.insert(name, statBuf));

  // Test directory
  name = "directory";
  statBuf.st_mode = S_IFDIR | 0755;
  EXPECT_TRUE(objlist.insert(name, statBuf));

  EXPECT_EQ(objlist.size(), 2);
}

TEST_F(S3fsUtilTest, S3HwObjList_LargeFile) {
  S3HwObjList objlist;

  std::string name = "large_file.bin";
  struct stat statBuf;
  memset(&statBuf, 0, sizeof(statBuf));
  statBuf.st_mode = S_IFREG | 0644;
  statBuf.st_size = 1024LL * 1024 * 1024;  // 1GB

  EXPECT_TRUE(objlist.insert(name, statBuf));
  EXPECT_EQ(objlist.size(), 1);
}

TEST_F(S3fsUtilTest, S3HwObjList_WithTimestamps) {
  S3HwObjList objlist;

  std::string name = "timestamped.txt";
  struct stat statBuf;
  memset(&statBuf, 0, sizeof(statBuf));
  statBuf.st_mode = S_IFREG | 0644;
  statBuf.st_mtime = 1234567890;
  statBuf.st_atime = 1234567891;
  statBuf.st_ctime = 1234567892;

  EXPECT_TRUE(objlist.insert(name, statBuf));
  EXPECT_EQ(objlist.size(), 1);
}

// Test get_realpath with complex paths
TEST_F(S3fsUtilTest, GetRealpath_ComplexNestedPath) {
  mount_prefix = "/mnt/obsfs";
  std::string result = get_realpath("/a/b/c/d/e/f/g/file.txt");
  EXPECT_EQ(result, "/mnt/obsfs/a/b/c/d/e/f/g/file.txt");
}

TEST_F(S3fsUtilTest, GetRealpath_WithSpecialChars) {
  mount_prefix = "";
  std::string result = get_realpath("/path/with-dashes_and_underscores/file name.txt");
  EXPECT_EQ(result, "/path/with-dashes_and_underscores/file name.txt");
}

// Test mydirname and mybasename together
TEST_F(S3fsUtilTest, MydirnameMybasename_CompletePath) {
  std::string path = "/path/to/file.txt";
  std::string dir = mydirname(path);
  std::string base = mybasename(path);

  EXPECT_EQ(dir, "/path/to");
  EXPECT_EQ(base, "file.txt");
}

TEST_F(S3fsUtilTest, MydirnameMybasename_RootFile) {
  std::string path = "/file.txt";
  std::string dir = mydirname(path);
  std::string base = mybasename(path);

  EXPECT_EQ(dir, "/");
  EXPECT_EQ(base, "file.txt");
}

TEST_F(S3fsUtilTest, MydirnameMybasename_NestedDirectory) {
  std::string path = "/a/b/c/d/";
  std::string dir = mydirname(path);
  std::string base = mybasename(path);

  EXPECT_EQ(dir, "/a/b/c");
  EXPECT_EQ(base, "d");
}

// Test get_exist_directory_path behavior
TEST_F(S3fsUtilTest, GetExistDirectoryPath_Root) {
  std::string result = get_exist_directory_path("/");
  EXPECT_EQ(result, "/");  // Root always exists
}

TEST_F(S3fsUtilTest, GetExistDirectoryPath_NonExistent) {
  std::string result = get_exist_directory_path("/nonexistent/path/12345");
  EXPECT_EQ(result, "/");  // Only root exists
}

TEST_F(S3fsUtilTest, GetExistDirectoryPath_Depth) {
  std::string result = get_exist_directory_path("/tmp/this/does/not/exist/path");
  EXPECT_EQ(result, "/tmp");  // /tmp should exist on most systems
}

// Test is_uid_include_group edge cases
TEST_F(S3fsUtilTest, IsUidIncludeGroup_RootGroup) {
  uid_t uid = geteuid();
  gid_t gid = 0;  // root group

  // This test verifies the function can be called with various group IDs
  // The actual result depends on the user's group membership
  int result = is_uid_include_group(uid, gid);
  // Result should be 0 (not in group) or 1 (in group)
  EXPECT_GE(result, 0);
  EXPECT_LE(result, 1);
}

TEST_F(S3fsUtilTest, IsUidIncludeGroup_SameGidAsUid) {
  uid_t uid = 1000;
  gid_t gid = 1000;

  int result = is_uid_include_group(uid, gid);
  // Result should be 0 (not in group) or 1 (in group)
  EXPECT_GE(result, 0);
  EXPECT_LE(result, 1);
}

// Test S3ObjListStatistic edge cases
TEST_F(S3fsUtilTest, S3ObjListStatistic_EmptyList) {
  S3ObjListStatistic stat;
  EXPECT_EQ(stat.get_size(), 0);
  EXPECT_EQ(stat.get_first(), "");
  EXPECT_EQ(stat.get_last(), "");
  EXPECT_EQ(stat.get_last_name(), "");
}

TEST_F(S3fsUtilTest, S3ObjListStatistic_SingleItem) {
  S3ObjListStatistic stat;
  stat.set_first("file.txt");
  stat.set_last("file.txt");
  stat.set_size(1);

  EXPECT_EQ(stat.get_first(), "file.txt");
  EXPECT_EQ(stat.get_last(), "file.txt");
  EXPECT_EQ(stat.get_last_name(), "file.txt");
  EXPECT_EQ(stat.get_size(), 1);
}

TEST_F(S3fsUtilTest, S3ObjListStatistic_DirectoryItem) {
  S3ObjListStatistic stat;
  stat.set_last("mydir/");
  EXPECT_EQ(stat.get_last_name(), "mydir");
}

// Test AutoLock RAII behavior
TEST_F(S3fsUtilTest, AutoLock_RAII_Cleanup) {
  pthread_mutex_t mutex;
  pthread_mutex_init(&mutex, NULL);

  EXPECT_EQ(pthread_mutex_trylock(&mutex), 0);  // Should be locked
  pthread_mutex_unlock(&mutex);

  {
    AutoLock lock(&mutex);
    EXPECT_TRUE(lock.isLockAcquired());
    // Lock is automatically released when lock goes out of scope
  }

  // Mutex should be unlockable now
  EXPECT_EQ(pthread_mutex_trylock(&mutex), 0);
  pthread_mutex_unlock(&mutex);

  pthread_mutex_destroy(&mutex);
}

// Test path handling with mount_prefix
TEST_F(S3fsUtilTest, MountPrefixWithPathOperations) {
  mount_prefix = "/bucket";

  // get_realpath just concatenates mount_prefix and path
  EXPECT_EQ(get_realpath("/file.txt"), "/bucket/file.txt");
  EXPECT_EQ(get_realpath("/path/file.txt"), "/bucket/path/file.txt");
  EXPECT_EQ(get_realpath("/"), "/bucket/");
}

TEST_F(S3fsUtilTest, MountPrefixEmpty) {
  mount_prefix = "";

  // Without mount prefix, paths should be unchanged
  EXPECT_EQ(get_realpath("/file.txt"), "/file.txt");
  EXPECT_EQ(get_realpath("/"), "/");
}

// Test path utility functions consistency
TEST_F(S3fsUtilTest, PathUtilityFunctions_Consistency) {
  std::string path = "/path/to/file.txt";

  std::string dir = mydirname(path);
  std::string base = mybasename(path);
  std::string base_get = get_basename(path);

  EXPECT_EQ(dir, "/path/to");
  EXPECT_EQ(base, "file.txt");
  EXPECT_EQ(base_get, "file.txt");
}

TEST_F(S3fsUtilTest, PathUtilityFunctions_Root) {
  std::string path = "/";

  std::string dir = mydirname(path);
  std::string base = mybasename(path);
  std::string base_get = get_basename(path);

  EXPECT_EQ(dir, "/");
  EXPECT_EQ(base, "/");
  EXPECT_EQ(base_get, "");
}

// Additional tests for edge cases in s3fs_util.cpp
// Test path handling with edge cases
TEST_F(S3fsUtilTest, GetRealpath_EmptyString) {
  std::string result = get_realpath("");
  // Empty path should still concatenate with mount_prefix
  EXPECT_EQ(result, mount_prefix);
}

TEST_F(S3fsUtilTest, GetRealpath_WithMountPrefixAndEmptyPath) {
  mount_prefix = "/bucket";
  std::string result = get_realpath("");
  EXPECT_EQ(result, "/bucket");
}

// Test dirname and basename with additional edge cases
TEST_F(S3fsUtilTest, Mydirname_SingleComponent) {
  std::string result = mydirname("file.txt");
  // dirname of a single component (no slashes) returns "."
  EXPECT_EQ(result, ".");
}

TEST_F(S3fsUtilTest, Mybasename_SingleComponent) {
  std::string result = mybasename("file.txt");
  EXPECT_EQ(result, "file.txt");
}

TEST_F(S3fsUtilTest, Mydirname_TrailingSlash) {
  std::string result = mydirname("/path/to/dir/");
  // dirname with trailing slash
  EXPECT_EQ(result, "/path/to");
}

TEST_F(S3fsUtilTest, Mybasename_TrailingSlash) {
  std::string result = mybasename("/path/to/dir/");
  // basename with trailing slash returns "dir"
  EXPECT_EQ(result, "dir");
}

// Test get_basename with various inputs
TEST_F(S3fsUtilTest, GetBasename_EmptyPath) {
  std::string result = get_basename("");
  EXPECT_EQ(result, "");
}

TEST_F(S3fsUtilTest, GetBasename_OnlyFilename) {
  std::string result = get_basename("file.txt");
  EXPECT_EQ(result, "file.txt");
}

TEST_F(S3fsUtilTest, GetBasename_WithDirectory) {
  std::string result = get_basename("/path/to/file.txt");
  EXPECT_EQ(result, "file.txt");
}

TEST_F(S3fsUtilTest, GetBasename_RootPath) {
  std::string result = get_basename("/");
  EXPECT_EQ(result, "");
}

TEST_F(S3fsUtilTest, GetBasename_MultipleSlashes) {
  std::string result = get_basename("/path///to//file.txt");
  EXPECT_EQ(result, "file.txt");
}

// Test S3ObjList with edge cases
TEST_F(S3fsUtilTest, S3ObjList_InsertMultipleFiles) {
  S3ObjList objlist;

  EXPECT_TRUE(objlist.insert("file1.txt", false));
  EXPECT_TRUE(objlist.insert("file2.txt", false));
  EXPECT_TRUE(objlist.insert("dir1/", true));

  EXPECT_EQ(objlist.size(), 3);
}

TEST_F(S3fsUtilTest, S3ObjList_InsertSameFileMultipleTimes) {
  S3ObjList objlist;

  EXPECT_TRUE(objlist.insert("file.txt", false));
  // Inserting same file again should overwrite (map behavior)
  EXPECT_TRUE(objlist.insert("file.txt", false));

  EXPECT_EQ(objlist.size(), 1);
}

TEST_F(S3fsUtilTest, S3ObjList_GetLastName_Empty) {
  S3ObjList objlist;
  EXPECT_EQ(objlist.get_last_name(), "");
}

// Test S3HwObjList with stat structures
TEST_F(S3fsUtilTest, S3HwObjList_InsertWithStat) {
  S3HwObjList objlist;
  struct stat st;
  memset(&st, 0, sizeof(st));
  st.st_mode = S_IFREG | 0644;
  st.st_size = 1024;
  st.st_mtime = 1234567890;

  std::string name = "test.txt";
  EXPECT_TRUE(objlist.insert(name, st));

  EXPECT_EQ(objlist.size(), 1);
}

// ─── is_ip_address_endpoint tests ───────────────────────────
TEST_F(S3fsUtilTest, IsIpEndpoint_IPv4_Plain) {
  EXPECT_TRUE(is_ip_address_endpoint("http://192.168.1.100"));
  EXPECT_TRUE(is_ip_address_endpoint("https://10.0.0.1"));
  EXPECT_TRUE(is_ip_address_endpoint("http://127.0.0.1"));
  EXPECT_TRUE(is_ip_address_endpoint("http://1.2.3.4"));
  EXPECT_TRUE(is_ip_address_endpoint("https://255.255.255.255"));
}

TEST_F(S3fsUtilTest, IsIpEndpoint_IPv4_WithPort) {
  EXPECT_TRUE(is_ip_address_endpoint("http://192.168.1.100:8080"));
  EXPECT_TRUE(is_ip_address_endpoint("https://10.0.0.1:443"));
  EXPECT_TRUE(is_ip_address_endpoint("http://127.0.0.1:15000"));
}

TEST_F(S3fsUtilTest, IsIpEndpoint_IPv4_WithPath) {
  EXPECT_TRUE(is_ip_address_endpoint("http://1.2.3.4/path"));
  EXPECT_TRUE(is_ip_address_endpoint("https://10.0.0.1:8080/api/v1"));
}

TEST_F(S3fsUtilTest, IsIpEndpoint_Domain) {
  EXPECT_FALSE(is_ip_address_endpoint("https://obs.cn-north-4.myhuaweicloud.com"));
  EXPECT_FALSE(is_ip_address_endpoint("https://s3.amazonaws.com"));
  EXPECT_FALSE(is_ip_address_endpoint("http://localhost"));
  EXPECT_FALSE(is_ip_address_endpoint("http://localhost:8080"));
  EXPECT_FALSE(is_ip_address_endpoint("https://my-obs.example.com:443"));
}

TEST_F(S3fsUtilTest, IsIpEndpoint_NotIpButNumeric) {
  // Domains that look numeric but aren't valid IPv4
  EXPECT_FALSE(is_ip_address_endpoint("https://10-0-0-1.cloud.internal"));
  EXPECT_FALSE(is_ip_address_endpoint("http://123.com"));
  EXPECT_FALSE(is_ip_address_endpoint("http://1.2.3"));       // only 3 octets
  EXPECT_FALSE(is_ip_address_endpoint("http://1.2.3.4.5"));   // 5 octets
}

TEST_F(S3fsUtilTest, IsIpEndpoint_InvalidOctet) {
  EXPECT_FALSE(is_ip_address_endpoint("http://999.0.0.1"));
  EXPECT_FALSE(is_ip_address_endpoint("http://1.2.3.256"));
}

TEST_F(S3fsUtilTest, IsIpEndpoint_EdgeCases) {
  EXPECT_FALSE(is_ip_address_endpoint(""));
  EXPECT_FALSE(is_ip_address_endpoint("http://"));
  EXPECT_TRUE(is_ip_address_endpoint("http://0.0.0.0"));
  EXPECT_TRUE(is_ip_address_endpoint("0.0.0.0"));             // no scheme
}

TEST_F(S3fsUtilTest, IsIpEndpoint_NoScheme) {
  EXPECT_TRUE(is_ip_address_endpoint("192.168.1.1"));
  EXPECT_TRUE(is_ip_address_endpoint("192.168.1.1:8080"));
  EXPECT_FALSE(is_ip_address_endpoint("obs.myhuaweicloud.com"));
}

// ─── get_parent_path tests ────────────────────────────────────────────────

TEST_F(S3fsUtilTest, GetParentPath_Root) {
  EXPECT_EQ(get_parent_path("/"), "");
}

TEST_F(S3fsUtilTest, GetParentPath_Empty) {
  EXPECT_EQ(get_parent_path(""), "");
}

TEST_F(S3fsUtilTest, GetParentPath_SingleLevel) {
  EXPECT_EQ(get_parent_path("/file.txt"), "/");
}

TEST_F(S3fsUtilTest, GetParentPath_TwoLevels) {
  EXPECT_EQ(get_parent_path("/dir/file.txt"), "/dir");
}

TEST_F(S3fsUtilTest, GetParentPath_DeepPath) {
  EXPECT_EQ(get_parent_path("/a/b/c/d/file.txt"), "/a/b/c/d");
}

TEST_F(S3fsUtilTest, GetParentPath_TrailingSlash) {
  EXPECT_EQ(get_parent_path("/dir/subdir/"), "/dir");
}

TEST_F(S3fsUtilTest, GetParentPath_Directory) {
  EXPECT_EQ(get_parent_path("/path/to/directory"), "/path/to");
}

TEST_F(S3fsUtilTest, GetParentPath_NoSlash) {
  // Path with no slashes returns "/" (line 1127)
  EXPECT_EQ(get_parent_path("file.txt"), "/");
  EXPECT_EQ(get_parent_path("filename"), "/");
  EXPECT_EQ(get_parent_path("relative_path"), "/");
}

TEST_F(S3fsUtilTest, GetParentPath_SlashOnly) {
  EXPECT_EQ(get_parent_path("/"), "");
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
