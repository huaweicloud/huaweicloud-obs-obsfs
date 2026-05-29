/*
 * s3fs - FUSE-based file system backed by Amazon S3
 *
 * Unit tests for s3fs_util path-related functionality
 *
 * This test file focuses on testing path manipulation functions
 * to improve code coverage for s3fs_util.cpp
 */

#include "gtest.h"
#include "common.h"
#include "s3fs_util.h"
#include "string_util.h"
#include <string>
#include <cstring>

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

// Must match production values in hws_s3fs.cpp:149-152
const char* hws_obs_meta_mtime = "x-amz-meta-mtime";
const char* hws_obs_meta_uid = "x-amz-meta-uid";
const char* hws_obs_meta_gid = "x-amz-meta-gid";
const char* hws_obs_meta_mode = "x-amz-meta-mode";
bool complement_stat = false;
std::string program_name = "s3fs_util_path_test";

const char* s3fs_crypt_lib_name() {
  return "openssl";
}

// Reference mount_prefix from s3fs_util.cpp
extern std::string mount_prefix;

using namespace std;

namespace {

class S3fsUtilPathTest : public ::testing::Test {
 protected:
  S3fsUtilPathTest() = default;

  ~S3fsUtilPathTest() override = default;

  void SetUp() override {
    mount_prefix = "";
  }

  void TearDown() override {
    mount_prefix = "";
  }
};

// Test get_basename function
TEST_F(S3fsUtilPathTest, GetBasename_SimplePath) {
  string path = "/path/to/file.txt";
  string result = get_basename(path);
  EXPECT_EQ(result, "file.txt");
}

TEST_F(S3fsUtilPathTest, GetBasename_RootPath) {
  string path = "/";
  string result = get_basename(path);
  EXPECT_TRUE(result.empty() || result == "/");
}

TEST_F(S3fsUtilPathTest, GetBasename_NoDirectory) {
  string path = "file.txt";
  string result = get_basename(path);
  EXPECT_EQ(result, "file.txt");
}

TEST_F(S3fsUtilPathTest, GetBasename_TrailingSlash) {
  string path = "/path/to/dir/";
  string result = get_basename(path);
  EXPECT_TRUE(result.empty() || result == "dir");
}

TEST_F(S3fsUtilPathTest, GetBasename_NestedPath) {
  string path = "/a/b/c/d/e/file.txt";
  string result = get_basename(path);
  EXPECT_EQ(result, "file.txt");
}

TEST_F(S3fsUtilPathTest, GetBasename_HiddenFile) {
  string path = "/path/to/.hidden";
  string result = get_basename(path);
  EXPECT_EQ(result, ".hidden");
}

TEST_F(S3fsUtilPathTest, GetBasename_FileWithExtension) {
  string path = "/path/to/archive.tar.gz";
  string result = get_basename(path);
  EXPECT_EQ(result, "archive.tar.gz");
}

// Test path manipulation with mount_prefix
TEST_F(S3fsUtilPathTest, PathWithMountPrefix) {
  mount_prefix = "/mnt/s3";

  string path = "/mnt/s3/path/to/file.txt";
  string result = get_basename(path);

  // basename should still return just the filename
  EXPECT_EQ(result, "file.txt");
}

TEST_F(S3fsUtilPathTest, PathWithEmptyMountPrefix) {
  mount_prefix = "";

  string path = "/path/to/file.txt";
  string result = get_basename(path);

  EXPECT_EQ(result, "file.txt");
}

// Test edge cases
TEST_F(S3fsUtilPathTest, GetBasename_EmptyPath) {
  string path = "";
  string result = get_basename(path);
  EXPECT_TRUE(result.empty());
}

TEST_F(S3fsUtilPathTest, GetBasename_DotPath) {
  string path = ".";
  string result = get_basename(path);
  EXPECT_EQ(result, ".");
}

TEST_F(S3fsUtilPathTest, GetBasename_DoubleDotPath) {
  string path = "..";
  string result = get_basename(path);
  EXPECT_EQ(result, "..");
}

TEST_F(S3fsUtilPathTest, GetBasename_DotDirectory) {
  string path = "/path/to/./file.txt";
  string result = get_basename(path);
  EXPECT_EQ(result, "file.txt");
}

TEST_F(S3fsUtilPathTest, GetBasename_DoubleDotDirectory) {
  string path = "/path/to/../file.txt";
  string result = get_basename(path);
  EXPECT_EQ(result, "file.txt");
}

// Test directory-only paths
TEST_F(S3fsUtilPathTest, GetBasename_DirectoryOnly) {
  string path = "/path/to/directory";
  string result = get_basename(path);
  EXPECT_EQ(result, "directory");
}

TEST_F(S3fsUtilPathTest, GetBasename_MultipleSlashes) {
  string path = "/path//to///file.txt";
  string result = get_basename(path);
  EXPECT_EQ(result, "file.txt");
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
