/*
 * Unit tests for new features
 * Tests for:
 * - str_to_off_t() function
 * - build_standard_metadata() function
 *
 * This file is designed to link with minimal dependencies
 */

#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#include <sys/stat.h>
#include <limits.h>

#include "gtest.h"
#include "common.h"
#include "string_util.h"
#include "s3fs_util.h"
#include "s3fs_operations.h"

// Stub variables required for linking
s3fs_log_level debug_level = S3FS_LOG_INFO;
const char* s3fs_log_nest[S3FS_LOG_NEST_MAX] = {"", "  ", "    ", "      "};
s3fs_log_mode debug_log_mode = LOG_MODE_FOREGROUND;

// Global variables for hws_obs_meta constants
// Must match production values in hws_s3fs.cpp:149-152
const char* hws_obs_meta_uid = "x-amz-meta-uid";
const char* hws_obs_meta_gid = "x-amz-meta-gid";
const char* hws_obs_meta_mode = "x-amz-meta-mode";
const char* hws_obs_meta_mtime = "x-amz-meta-mtime";

// Stub functions
void IndexLogEntry(const uint64_t module, const char* chunk_id, const uint32_t log_level,
                   const char* log_file_name, const char* log_func_name, const uint32_t log_line,
                   const char* pszFormat, ...) {
  // Stub implementation
}

void s3fsStatisLogModeNotObsfs() {
  // Stub implementation
}

// Stub for program_name
std::string program_name = "obsfs";

using namespace std;

namespace {

class NewFeaturesTest : public ::testing::Test {
 protected:
  NewFeaturesTest() {
  }

  ~NewFeaturesTest() override {
  }

  void SetUp() override {
  }

  void TearDown() override {
  }
};

// ============================================================================
// Tests for str_to_off_t()
// ============================================================================

TEST_F(NewFeaturesTest, StrToOffT_ValidPositiveNumber) {
  EXPECT_EQ(str_to_off_t("12345"), 12345);
  EXPECT_EQ(str_to_off_t("0"), 0);
  EXPECT_EQ(str_to_off_t("1"), 1);
  EXPECT_EQ(str_to_off_t("999999"), 999999);
}

TEST_F(NewFeaturesTest, StrToOffT_ValidNegativeNumber) {
  EXPECT_EQ(str_to_off_t("-1"), -1);
  EXPECT_EQ(str_to_off_t("-100"), -100);
  EXPECT_EQ(str_to_off_t("-99999"), -99999);
}

TEST_F(NewFeaturesTest, StrToOffT_NullPointer) {
  EXPECT_EQ(str_to_off_t(NULL), 0);
}

TEST_F(NewFeaturesTest, StrToOffT_EmptyString) {
  EXPECT_EQ(str_to_off_t(""), 0);
}

TEST_F(NewFeaturesTest, StrToOffT_OnlyNullTerminator) {
  EXPECT_EQ(str_to_off_t("\0"), 0);
}

TEST_F(NewFeaturesTest, StrToOffT_LargeNumber) {
  off_t large = 999999999;
  EXPECT_EQ(str_to_off_t("999999999"), large);
}

TEST_F(NewFeaturesTest, StrToOffT_MaxOffT) {
  // Use LLONG_MAX as a reasonable maximum for testing
  char buf[32];
  snprintf(buf, sizeof(buf), "%lld", LLONG_MAX);
  off_t result = str_to_off_t(buf);
  EXPECT_GT(result, 0);
}

TEST_F(NewFeaturesTest, StrToOffT_WithWhitespace) {
  // strtol skips leading whitespace
  EXPECT_EQ(str_to_off_t(" 123"), 123);
  EXPECT_EQ(str_to_off_t("\t456"), 456);
  EXPECT_EQ(str_to_off_t("\n789"), 789);
}

// ============================================================================
// Tests for build_standard_metadata()
// ============================================================================

TEST_F(NewFeaturesTest, BuildStandardMetadata_BasicFile) {
  headers_t meta;
  mode_t mode = 0644;
  string content_type = "text/plain";

  build_standard_metadata(meta, mode, content_type, false);

  // Verify Content-Type is set
  EXPECT_EQ(meta["Content-Type"], "text/plain");

  // Verify UID is set to current user
  uid_t expected_uid = getuid();
  EXPECT_EQ(meta[hws_obs_meta_uid], std::to_string(expected_uid));

  // Verify GID is set to current group
  gid_t expected_gid = getgid();
  EXPECT_EQ(meta[hws_obs_meta_gid], std::to_string(expected_gid));

  // Verify mode is set correctly
  EXPECT_EQ(meta[hws_obs_meta_mode], "0644");

  // Verify mtime is set (should be a valid timestamp string)
  EXPECT_FALSE(meta[hws_obs_meta_mtime].empty());
}

TEST_F(NewFeaturesTest, BuildStandardMetadata_Directory) {
  headers_t meta;
  mode_t mode = 0755;
  string content_type = "application/x-directory";

  build_standard_metadata(meta, mode, content_type, false);

  EXPECT_EQ(meta["Content-Type"], "application/x-directory");
  EXPECT_EQ(meta[hws_obs_meta_mode], "0755");
}

TEST_F(NewFeaturesTest, BuildStandardMetadata_NoContentType) {
  headers_t meta;
  mode_t mode = 0600;

  // Call with empty content type
  build_standard_metadata(meta, mode, "", false);

  // Content-Type should not be set if empty string provided
  bool has_content_type = (meta.find("Content-Type") != meta.end());
  if (has_content_type) {
    EXPECT_TRUE(meta["Content-Type"].empty());
  }

  // Other fields should still be set
  EXPECT_FALSE(meta[hws_obs_meta_uid].empty());
  EXPECT_FALSE(meta[hws_obs_meta_gid].empty());
  EXPECT_EQ(meta[hws_obs_meta_mode], "0600");
}

TEST_F(NewFeaturesTest, BuildStandardMetadata_PreserveModeFalse) {
  headers_t meta;
  meta[hws_obs_meta_mode] = "0000";  // Pre-existing mode

  mode_t new_mode = 0644;

  build_standard_metadata(meta, new_mode, "", false);

  // Mode should be replaced with new value
  EXPECT_EQ(meta[hws_obs_meta_mode], "0644");
}

TEST_F(NewFeaturesTest, BuildStandardMetadata_PreserveModeTrue) {
  headers_t meta;
  string original_mode = "0755";
  meta[hws_obs_meta_mode] = original_mode;

  mode_t new_mode = 0644;

  build_standard_metadata(meta, new_mode, "", true);

  // Mode should be preserved from original value
  EXPECT_EQ(meta[hws_obs_meta_mode], original_mode);
}

TEST_F(NewFeaturesTest, BuildStandardMetadata_PreserveModeWithEmptyOriginal) {
  headers_t meta;
  // No pre-existing mode

  mode_t new_mode = 0644;

  build_standard_metadata(meta, new_mode, "", true);

  // Mode should be set to new value since nothing to preserve
  EXPECT_EQ(meta[hws_obs_meta_mode], "0644");
}

TEST_F(NewFeaturesTest, BuildStandardMetadata_DifferentModes) {
  headers_t meta;

  // Test 0755 (rwxr-xr-x)
  build_standard_metadata(meta, 0755, "", false);
  EXPECT_EQ(meta[hws_obs_meta_mode], "0755");

  // Test 0600 (rw-------)
  meta.clear();
  build_standard_metadata(meta, 0600, "", false);
  EXPECT_EQ(meta[hws_obs_meta_mode], "0600");

  // Test 0000 (no permissions)
  meta.clear();
  build_standard_metadata(meta, 0000, "", false);
  EXPECT_EQ(meta[hws_obs_meta_mode], "0000");

  // Test 0777 (rwxrwxrwx)
  meta.clear();
  build_standard_metadata(meta, 0777, "", false);
  EXPECT_EQ(meta[hws_obs_meta_mode], "0777");
}

TEST_F(NewFeaturesTest, BuildStandardMetadata_ClearsExistingMetadata) {
  headers_t meta;
  meta["x-amz-meta-some-field"] = "some-value";
  meta["Content-Type"] = "old-type";
  meta[hws_obs_meta_uid] = "999";

  build_standard_metadata(meta, 0644, "new-type", false);

  // Old custom fields should be cleared
  EXPECT_TRUE(meta.find("x-amz-meta-some-field") == meta.end());

  // New values should be set
  EXPECT_EQ(meta["Content-Type"], "new-type");
  EXPECT_EQ(meta[hws_obs_meta_uid], std::to_string(getuid()));
}

TEST_F(NewFeaturesTest, BuildStandardMetadata_TimestampReasonable) {
  headers_t meta;
  time_t before = time(NULL);

  build_standard_metadata(meta, 0644, "", false);

  time_t after = time(NULL);

  // Parse the timestamp
  long mtime = strtol(meta[hws_obs_meta_mtime].c_str(), NULL, 10);

  // Timestamp should be between before and after (with 2 second tolerance)
  EXPECT_GE(mtime, before - 2);
  EXPECT_LE(mtime, after + 2);
}

TEST_F(NewFeaturesTest, BuildStandardMetadata_DefaultContentTypeParam) {
  headers_t meta;
  mode_t mode = 0644;

  // Test calling with default parameter (content_type = "")
  // Note: This tests the function signature with default parameter
  build_standard_metadata(meta, mode, std::string(), false);

  EXPECT_EQ(meta[hws_obs_meta_mode], "0644");
  EXPECT_FALSE(meta[hws_obs_meta_uid].empty());
}

// ============================================================================
// Integration tests - metadata building for different scenarios
// ============================================================================

TEST_F(NewFeaturesTest, Integration_FileMetadataCreation) {
  headers_t meta;

  // Simulate creating a text file
  build_standard_metadata(meta, 0644, "text/plain", false);

  EXPECT_EQ(meta["Content-Type"], "text/plain");
  EXPECT_EQ(meta[hws_obs_meta_mode], "0644");
  EXPECT_FALSE(meta[hws_obs_meta_mtime].empty());
}

TEST_F(NewFeaturesTest, Integration_DirectoryMetadataCreation) {
  headers_t meta;

  // Simulate creating a directory
  build_standard_metadata(meta, 0755, "application/x-directory", false);

  EXPECT_EQ(meta["Content-Type"], "application/x-directory");
  EXPECT_EQ(meta[hws_obs_meta_mode], "0755");
}

TEST_F(NewFeaturesTest, Integration_TruncatePreservesMode) {
  headers_t meta;
  meta[hws_obs_meta_mode] = "0755";

  // Simulate truncate operation (should preserve mode)
  build_standard_metadata(meta, 0, "application/octet-stream", true);

  EXPECT_EQ(meta[hws_obs_meta_mode], "0755");
  EXPECT_EQ(meta["Content-Type"], "application/octet-stream");
}

}  // namespace
