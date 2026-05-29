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

/*
 * Unit tests for addhead (additional HTTP header) functionality
 *
 * This test links the actual addhead.cpp source code to get proper coverage.
 * It provides stub implementations for required dependencies.
 */

#include "gtest.h"
#include <string>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <regex.h>
#include <vector>
#include <map>
#include <curl/curl.h>

// Include the actual header we're testing
extern "C" {
#include "common.h"
}

#include "addhead.h"
#include "hws_fs_util.h"

//-------------------------------------------------------------------
// Stub implementations for dependencies
//-------------------------------------------------------------------

// Note: debug_level, s3fs_log_nest, IndexLogEntry, s3fsStatisLogModeNotObsfs,
// verifyPath, and curl_slist_sort_insert are already defined in test_stubs.cpp
// and will be linked from there.
// AdditionalHeader::singleton is defined in addhead.cpp and will be linked from addhead.o.

//-------------------------------------------------------------------
// Test Helper Functions
//-------------------------------------------------------------------

// Helper to create a temporary config file for testing
static std::string create_temp_config_file(const char* content) {
    char temp_file[] = "/tmp/addhead_test_XXXXXX";
    int fd = mkstemp(temp_file);
    if (fd < 0) {
        return "";
    }

    if (content) {
        write(fd, content, strlen(content));
    }
    close(fd);

    return std::string(temp_file);
}

// Helper to remove temporary config file
static void remove_temp_config_file(const std::string& path) {
    if (!path.empty()) {
        unlink(path.c_str());
    }
}

//-------------------------------------------------------------------
// Test: AdditionalHeader::Load - File operations
//-------------------------------------------------------------------

TEST(AddHeadTest, Load_NullFile) {
    AdditionalHeader* ah = AdditionalHeader::get();

    // Test loading with NULL file path
    bool result = ah->Load(nullptr);

    EXPECT_FALSE(result);
}

TEST(AddHeadTest, Load_NonExistentFile) {
    AdditionalHeader* ah = AdditionalHeader::get();

    // Test loading a non-existent file
    // verifyPath stub will succeed but the file open will fail
    bool result = ah->Load("/nonexistent/path/to/file.conf");

    EXPECT_FALSE(result);
}

TEST(AddHeadTest, Load_EmptyFile) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Create empty temp file
    std::string temp_file = create_temp_config_file("");
    ASSERT_FALSE(temp_file.empty());

    // Load empty file - should succeed
    bool result = ah->Load(temp_file.c_str());

    EXPECT_TRUE(result);

    // Clean up
    ah->Unload();
    remove_temp_config_file(temp_file);
}

TEST(AddHeadTest, Load_CommentLinesOnly) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    const char* content = "# This is a comment\n"
                          "# Another comment\n"
                          "#   Comment with leading space\n";

    std::string temp_file = create_temp_config_file(content);
    ASSERT_FALSE(temp_file.empty());

    bool result = ah->Load(temp_file.c_str());

    EXPECT_TRUE(result);

    ah->Unload();
    remove_temp_config_file(temp_file);
}

TEST(AddHeadTest, Load_ValidSuffixHeader) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    const char* content = ".txt x-amz-meta-content-type text/plain\n";

    std::string temp_file = create_temp_config_file(content);
    ASSERT_FALSE(temp_file.empty());

    bool result = ah->Load(temp_file.c_str());

    EXPECT_TRUE(result);

    ah->Unload();
    remove_temp_config_file(temp_file);
}

TEST(AddHeadTest, Load_MultipleHeaders) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    const char* content = ".txt x-amz-meta-type text\n"
                          ".jpg x-amz-meta-type image\n"
                          ".pdf x-amz-meta-type document\n";

    std::string temp_file = create_temp_config_file(content);
    ASSERT_FALSE(temp_file.empty());

    bool result = ah->Load(temp_file.c_str());

    EXPECT_TRUE(result);

    ah->Unload();
    remove_temp_config_file(temp_file);
}

TEST(AddHeadTest, Load_RegexPattern) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    const char* content = "reg:.*\\.txt$ x-amz-meta-type text\n";

    std::string temp_file = create_temp_config_file(content);
    ASSERT_FALSE(temp_file.empty());

    bool result = ah->Load(temp_file.c_str());

    EXPECT_TRUE(result);

    ah->Unload();
    remove_temp_config_file(temp_file);
}

TEST(AddHeadTest, Load_InvalidRegexPattern) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Invalid regex - missing closing bracket
    const char* content = "reg:.*\\.txt$[ x-amz-meta-type text\n";

    std::string temp_file = create_temp_config_file(content);
    ASSERT_FALSE(temp_file.empty());

    // Should still return true (skips invalid lines)
    bool result = ah->Load(temp_file.c_str());

    EXPECT_TRUE(result);

    ah->Unload();
    remove_temp_config_file(temp_file);
}

TEST(AddHeadTest, Load_EmptyKeyWithHeader) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Line starting with space - empty key, but has header
    // The code allows empty key when head is not empty
    const char* content = " x-amz-meta-key value\n";

    std::string temp_file = create_temp_config_file(content);
    ASSERT_FALSE(temp_file.empty());

    // Should succeed - empty key is allowed when head is not empty
    bool result = ah->Load(temp_file.c_str());

    EXPECT_TRUE(result);

    ah->Unload();
    remove_temp_config_file(temp_file);
}

TEST(AddHeadTest, Load_NoHeaderValue) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Key with no header value (should fail format check)
    const char* content = ".txt\n";

    std::string temp_file = create_temp_config_file(content);
    ASSERT_FALSE(temp_file.empty());

    bool result = ah->Load(temp_file.c_str());

    EXPECT_FALSE(result);

    ah->Unload();
    remove_temp_config_file(temp_file);
}

TEST(AddHeadTest, Load_HeaderWithValueOnly) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Key with header but no value (should succeed - value can be empty)
    const char* content = ".txt x-amz-meta-key\n";

    std::string temp_file = create_temp_config_file(content);
    ASSERT_FALSE(temp_file.empty());

    bool result = ah->Load(temp_file.c_str());

    EXPECT_TRUE(result);

    ah->Unload();
    remove_temp_config_file(temp_file);
}

//-------------------------------------------------------------------
// Test: AdditionalHeader::AddHeader - headers_t version
//-------------------------------------------------------------------

TEST(AddHeadTest, AddHeader_NotEnabled) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();  // Make sure it's not enabled

    headers_t meta;

    // When not enabled, should return true without modifying meta
    bool result = ah->AddHeader(meta, "/path/to/file.txt");

    EXPECT_TRUE(result);
    EXPECT_TRUE(meta.empty());
}

TEST(AddHeadTest, AddHeader_NullPath) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Load something to enable it
    const char* content = ".txt x-amz-meta-type text\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    headers_t meta;

    // Null path should return false
    bool result = ah->AddHeader(meta, nullptr);

    EXPECT_FALSE(result);
    EXPECT_TRUE(meta.empty());

    ah->Unload();
}

TEST(AddHeadTest, AddHeader_SuffixMatch) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Load a suffix header
    const char* content = ".txt x-amz-meta-type text\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    headers_t meta;

    // Test matching path
    bool result = ah->AddHeader(meta, "/path/to/file.txt");

    EXPECT_TRUE(result);
    EXPECT_EQ(meta.size(), static_cast<size_t>(1));
    EXPECT_EQ(meta["x-amz-meta-type"], "text");

    ah->Unload();
}

TEST(AddHeadTest, AddHeader_SuffixNoMatch) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Load a suffix header
    const char* content = ".txt x-amz-meta-type text\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    headers_t meta;

    // Test non-matching path
    bool result = ah->AddHeader(meta, "/path/to/file.jpg");

    EXPECT_TRUE(result);
    EXPECT_TRUE(meta.empty());

    ah->Unload();
}

TEST(AddHeadTest, AddHeader_RegexMatch) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Load a regex header
    const char* content = "reg:^/important/.*\\.txt$ x-amz-meta-priority high\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    headers_t meta;

    // Test matching path
    bool result = ah->AddHeader(meta, "/important/document.txt");

    EXPECT_TRUE(result);
    EXPECT_EQ(meta.size(), static_cast<size_t>(1));
    EXPECT_EQ(meta["x-amz-meta-priority"], "high");

    ah->Unload();
}

TEST(AddHeadTest, AddHeader_RegexNoMatch) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Load a regex header
    const char* content = "reg:^/important/.*\\.txt$ x-amz-meta-priority high\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    headers_t meta;

    // Test non-matching path
    bool result = ah->AddHeader(meta, "/other/document.txt");

    EXPECT_TRUE(result);
    EXPECT_TRUE(meta.empty());

    ah->Unload();
}

TEST(AddHeadTest, AddHeader_MultipleHeadersMatch) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Load multiple headers
    const char* content = ".txt x-amz-meta-type text\n"
                          "reg:^/secret/.* x-amz-meta-encrypted true\n"
                          ".txt x-amz-meta-extension txt\n";

    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    headers_t meta;

    // Path that matches both suffix and regex
    bool result = ah->AddHeader(meta, "/secret/notes.txt");

    EXPECT_TRUE(result);
    EXPECT_EQ(meta.size(), static_cast<size_t>(3));
    EXPECT_EQ(meta["x-amz-meta-type"], "text");
    EXPECT_EQ(meta["x-amz-meta-encrypted"], "true");
    EXPECT_EQ(meta["x-amz-meta-extension"], "txt");

    ah->Unload();
}

TEST(AddHeadTest, AddHeader_EmptyBaseString) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Load header with empty base string (line starts with space)
    // Empty basestring is allowed - it will match any path
    const char* content = " x-amz-meta-default value\n";

    std::string temp_file = create_temp_config_file(content);
    ASSERT_FALSE(temp_file.empty());

    // Load should succeed - empty key is allowed
    bool result = ah->Load(temp_file.c_str());

    EXPECT_TRUE(result);

    // Empty basestring matches all paths (when basestring.length() < pathlength is false)
    // The condition: if(0 == paddhead->basestring.length() || 0 == strcmp(...))
    // When basestring.length() == 0, the first condition is true, so it matches
    headers_t meta;
    ah->AddHeader(meta, "/any/path.txt");

    EXPECT_EQ(meta.size(), static_cast<size_t>(1));
    EXPECT_EQ(meta["x-amz-meta-default"], "value");

    ah->Unload();
    remove_temp_config_file(temp_file);
}

//-------------------------------------------------------------------
// Test: AdditionalHeader::AddHeader - curl_slist version
//-------------------------------------------------------------------

TEST(AddHeadTest, AddHeaderCurlList_NotEnabled) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    struct curl_slist* list = nullptr;

    // When not enabled, should return list unchanged
    struct curl_slist* result = ah->AddHeader(list, "/path/to/file.txt");

    EXPECT_EQ(result, (struct curl_slist*)nullptr);

    curl_slist_free_all(result);
}

TEST(AddHeadTest, AddHeaderCurlList_NullPath) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Load something to enable
    const char* content = ".txt x-amz-meta-type text\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    struct curl_slist* list = nullptr;

    // Null path should return list unchanged
    struct curl_slist* result = ah->AddHeader(list, nullptr);

    EXPECT_EQ(result, (struct curl_slist*)nullptr);

    curl_slist_free_all(result);
    ah->Unload();
}

TEST(AddHeadTest, AddHeaderCurlList_AppendsHeaders) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    const char* content = ".txt x-amz-meta-type text\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    struct curl_slist* list = nullptr;

    // Add matching path
    list = ah->AddHeader(list, "/path/to/file.txt");

    EXPECT_NE(list, (struct curl_slist*)nullptr);
    EXPECT_NE(list->data, (char*)nullptr);

    curl_slist_free_all(list);
    ah->Unload();
}

//-------------------------------------------------------------------
// Test: AdditionalHeader::Unload
//-------------------------------------------------------------------

TEST(AddHeadTest, Unload_ClearsHeaders) {
    AdditionalHeader* ah = AdditionalHeader::get();

    // Load something
    const char* content = ".txt x-amz-meta-type text\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    // Unload
    ah->Unload();

    // After unload, AddHeader should not modify meta (not enabled)
    headers_t meta;
    bool result = ah->AddHeader(meta, "/path/to/file.txt");

    EXPECT_TRUE(result);
    EXPECT_TRUE(meta.empty());
}

TEST(AddHeadTest, Unload_Idempotent) {
    AdditionalHeader* ah = AdditionalHeader::get();

    // Unload multiple times - should not crash
    ah->Unload();
    ah->Unload();
    ah->Unload();

    SUCCEED();
}

//-------------------------------------------------------------------
// Test: AdditionalHeader::Dump
//-------------------------------------------------------------------

TEST(AddHeadTest, Dump_NotEnabled) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Dump when not enabled should return true
    bool result = ah->Dump();

    EXPECT_TRUE(result);
}

TEST(AddHeadTest, Dump_WithHeaders) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    const char* content = ".txt x-amz-meta-type text\n"
                          ".jpg x-amz-meta-type image\n";

    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    // Dump should succeed
    bool result = ah->Dump();

    EXPECT_TRUE(result);

    ah->Unload();
}

//-------------------------------------------------------------------
// Test: Edge cases and special scenarios
//-------------------------------------------------------------------

TEST(AddHeadTest, Load_LongHeaderValue) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Very long header value
    std::string long_value(1000, 'X');
    std::string content = ".txt x-amz-meta-long " + long_value + "\n";

    std::string temp_file = create_temp_config_file(content.c_str());
    ASSERT_FALSE(temp_file.empty());

    bool result = ah->Load(temp_file.c_str());

    EXPECT_TRUE(result);

    ah->Unload();
    remove_temp_config_file(temp_file);
}

TEST(AddHeadTest, AddHeader_PathLongerThanBase) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    const char* content = ".txt x-amz-meta-type text\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    headers_t meta;

    // Path exactly matching the suffix
    bool result = ah->AddHeader(meta, ".txt");

    EXPECT_TRUE(result);

    ah->Unload();
}

TEST(AddHeadTest, Load_MixedValidAndInvalidLines) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    const char* content = "# Comment\n"
                          ".txt x-amz-meta-type text\n"
                          "\n"
                          "reg:.*\\.jpg$ x-amz-meta-image yes\n"
                          "   x-amz-meta-default value\n";  // Empty key is allowed

    std::string temp_file = create_temp_config_file(content);
    ASSERT_FALSE(temp_file.empty());

    // Should succeed - empty key is allowed
    bool result = ah->Load(temp_file.c_str());

    EXPECT_TRUE(result);

    ah->Unload();
    remove_temp_config_file(temp_file);
}

TEST(AddHeadTest, AddHeader_MultipleSameKey) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Load multiple entries with the same header key
    const char* content = ".txt x-amz-meta-custom value1\n"
                          ".jpg x-amz-meta-custom value2\n";

    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    headers_t meta;

    // Add headers - second one should win
    bool result = ah->AddHeader(meta, "/file.txt.jpg");

    EXPECT_TRUE(result);
    EXPECT_EQ(meta.size(), static_cast<size_t>(1));
    EXPECT_EQ(meta["x-amz-meta-custom"], "value2");

    ah->Unload();
}

// Test for line 117: empty key AND empty header (should be skipped with continue)
TEST(AddHeadTest, Load_EmptyKeyAndEmptyHeader) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Line with only whitespace - both key and head are empty
    // This triggers the continue at line 117
    const char* content = "   \n.txt x-amz-meta-key value\n";

    std::string temp_file = create_temp_config_file(content);
    ASSERT_FALSE(temp_file.empty());

    // Should succeed - empty line is skipped
    bool result = ah->Load(temp_file.c_str());

    EXPECT_TRUE(result);

    ah->Unload();
    remove_temp_config_file(temp_file);
}

// Test for line 128: regex key without pattern (reg: without actual regex)
TEST(AddHeadTest, Load_RegexKeyWithoutPattern) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // "reg:" without any actual regex pattern - triggers line 128 error path
    // The code: if(key.size() <= strlen(ADD_HEAD_REGEX)) continues
    const char* content = "reg: x-amz-meta-key value\n.txt x-amz-meta-other val\n";

    std::string temp_file = create_temp_config_file(content);
    ASSERT_FALSE(temp_file.empty());

    // Should succeed - invalid regex line is skipped
    bool result = ah->Load(temp_file.c_str());

    EXPECT_TRUE(result);

    ah->Unload();
    remove_temp_config_file(temp_file);
}

// Test for line 128 edge case: exactly "reg:" (4 chars)
TEST(AddHeadTest, Load_RegexKeyExactlyRegPrefix) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Exactly "reg:" with no pattern - key.size() == 4, strlen("reg:") == 4
    // This triggers key.size() <= strlen(ADD_HEAD_REGEX) to be true
    const char* content = "REG: x-amz-meta-test value\n";

    std::string temp_file = create_temp_config_file(content);
    ASSERT_FALSE(temp_file.empty());

    bool result = ah->Load(temp_file.c_str());

    // Should succeed - line is skipped but file is valid
    EXPECT_TRUE(result);

    ah->Unload();
    remove_temp_config_file(temp_file);
}

// Test for verifyPath failure: Load with non-existent directory path
// This tests line 79-80 where verifyPath fails when file doesn't exist and shouldExist=true
TEST(AddHeadTest, Load_VerifyPathFailure) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Use a path in a non-existent directory - verifyPath with shouldExist=true will fail
    // because realpath() fails when the file doesn't exist
    bool result = ah->Load("/nonexistent_dir_12345/file.conf");

    // Should fail because verifyPath fails on non-existent file with shouldExist=true
    EXPECT_FALSE(result);

    ah->Unload();
}

// Test AddHeader when path length equals basestring length
// This tests line 218-219 edge case where pathlength == basestring.length()
TEST(AddHeadTest, AddHeader_PathEqualsBaseStringLength) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    const char* content = ".txt x-amz-meta-type text\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    headers_t meta;

    // Path that is exactly ".txt" - pathlength (4) == basestring.length() (4)
    // This tests the else branch where pathlength is NOT greater than basestring.length()
    bool result = ah->AddHeader(meta, ".txt");

    EXPECT_TRUE(result);
    // Should NOT match because the condition is: basestring.length() < pathlength
    // When equal, the suffix match is not performed
    EXPECT_TRUE(meta.empty());

    ah->Unload();
}

// Test AddHeader with regex pattern that matches
// This exercises the regex path (line 209-215) more thoroughly
TEST(AddHeadTest, AddHeader_RegexMatchComplex) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Complex regex pattern matching multiple file types
    const char* content = "reg:.*\\.(cpp|hpp|h|c)$ x-amz-meta-source-code true\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    headers_t meta;

    // Test matching .cpp file
    bool result = ah->AddHeader(meta, "/src/main.cpp");
    EXPECT_TRUE(result);
    EXPECT_EQ(meta["x-amz-meta-source-code"], "true");

    meta.clear();

    // Test matching .h file
    result = ah->AddHeader(meta, "/include/header.h");
    EXPECT_TRUE(result);
    EXPECT_EQ(meta["x-amz-meta-source-code"], "true");

    meta.clear();

    // Test non-matching .txt file
    result = ah->AddHeader(meta, "/docs/readme.txt");
    EXPECT_TRUE(result);
    EXPECT_TRUE(meta.empty());

    ah->Unload();
}

// Test Dump with debug level enabled
// This tests the Dump function's debug output path (line 247-279)
TEST(AddHeadTest, Dump_WithDebugLevel) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    const char* content = ".txt x-amz-meta-type text\n"
                          "reg:^/special/.* x-amz-meta-special yes\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    // Save old debug level
    s3fs_log_level old_level = debug_level;

    // Set debug level to DBG to enable Dump output
    debug_level = S3FS_LOG_DBG;

    // Dump should succeed and produce output
    bool result = ah->Dump();
    EXPECT_TRUE(result);

    // Restore debug level
    debug_level = old_level;

    ah->Unload();
}

// Test Dump with regex entry to cover regex output path in Dump
TEST(AddHeadTest, Dump_WithRegexEntry) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Load a config with regex pattern
    const char* content = "reg:.*\\.log$ x-amz-meta-logfile true\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    // Save old debug level
    s3fs_log_level old_level = debug_level;
    debug_level = S3FS_LOG_DBG;

    // Dump should output regex type info
    bool result = ah->Dump();
    EXPECT_TRUE(result);

    // Restore debug level
    debug_level = old_level;

    ah->Unload();
}

// Test AddHeader curl_slist version with multiple headers
TEST(AddHeadTest, AddHeaderCurlList_MultipleHeaders) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    const char* content = ".txt x-amz-meta-type text\n"
                          ".log x-amz-meta-log true\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    struct curl_slist* list = nullptr;

    // Add headers for a path that matches both (.txt.log)
    list = ah->AddHeader(list, "/file.txt.log");

    // Should have added headers
    EXPECT_NE(list, (struct curl_slist*)nullptr);

    // Count headers in list
    int count = 0;
    struct curl_slist* item = list;
    while (item) {
        count++;
        item = item->next;
    }
    EXPECT_GE(count, 1);

    curl_slist_free_all(list);
    ah->Unload();
}

// Test Load with value containing spaces
TEST(AddHeadTest, Load_ValueWithSpaces) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Value with spaces - tests line 110 value extraction
    const char* content = ".txt x-amz-meta-desc This is a text file\n";
    std::string temp_file = create_temp_config_file(content);
    ASSERT_FALSE(temp_file.empty());

    bool result = ah->Load(temp_file.c_str());
    EXPECT_TRUE(result);

    // Verify the value was captured correctly
    headers_t meta;
    ah->AddHeader(meta, "/file.txt");
    EXPECT_EQ(meta["x-amz-meta-desc"], "This is a text file");

    ah->Unload();
    remove_temp_config_file(temp_file);
}

// Test AddHeader with empty basestring matching any path
TEST(AddHeadTest, AddHeader_EmptyBaseStringMatchesAll) {
    AdditionalHeader* ah = AdditionalHeader::get();
    ah->Unload();

    // Load with empty key (matches all paths)
    const char* content = " x-amz-meta-global global-value\n";
    std::string temp_file = create_temp_config_file(content);
    ah->Load(temp_file.c_str());
    remove_temp_config_file(temp_file);

    headers_t meta;

    // Should match any path
    bool result = ah->AddHeader(meta, "/any/random/path.xyz");
    EXPECT_TRUE(result);
    EXPECT_EQ(meta["x-amz-meta-global"], "global-value");

    ah->Unload();
}

// Main function to run the tests
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
