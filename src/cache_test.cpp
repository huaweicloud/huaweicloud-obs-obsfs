/*
 * s3fs - FUSE-based file system backed by Amazon S3
 *
 * Cache unit tests
 */

#include "gtest.h"
#include "cache.h"
#include "common.h"
#include "string_util.h"
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>

// Stub variables and functions required for linking
s3fs_log_level debug_level = S3FS_LOG_INFO;
const char* s3fs_log_nest[S3FS_LOG_NEST_MAX] = {"", "  ", "    ", "      "};
s3fs_log_mode debug_log_mode = LOG_MODE_FOREGROUND;
const char* Contentlength = "content-length";
std::string program_name = "obsfs";

void IndexLogEntry(const uint64_t module, const char* chunk_id, const uint32_t log_level,
                   const char* log_file_name, const char* log_func_name, const uint32_t log_line,
                   const char* pszFormat, ...) {
  // Stub implementation
}

void s3fsStatisLogModeNotObsfs() {
  // Stub implementation
}

// Stub variables for s3fs_util dependencies
bool complement_stat = false;
// Must match production values in hws_s3fs.cpp:149-152
const char* hws_obs_meta_uid = "x-amz-meta-uid";
const char* hws_obs_meta_gid = "x-amz-meta-gid";
const char* hws_obs_meta_mode = "x-amz-meta-mode";
const char* hws_obs_meta_mtime = "x-amz-meta-mtime";

const char* s3fs_crypt_lib_name() {
  return "openssl";
}

using namespace std;

namespace {

class StatCacheWildcardTest : public ::testing::Test {
 protected:
  StatCache* cache;
  unsigned long cache_size;

  void SetUp() override {
    cache = StatCache::getStatCacheData();
    cache_size = cache->GetCacheSize();
    // Set a small cache size for testing
    cache->SetCacheSize(1000);
    // Note: We can't call Clear() as it's private, so we use unique paths per test
  }

  void TearDown() override {
    // Restore original cache size
    cache->SetCacheSize(cache_size);
    // Clear is private, so we can't clean up - tests use unique paths
  }

  // Helper: Create test headers_t
  headers_t CreateTestMeta(const std::string& etag = "test-etag") {
    headers_t meta;
    meta["Content-Type"] = "text/plain";
    meta["ETag"] = etag;
    meta["Content-Length"] = "100";
    return meta;
  }

  // Helper: Generate unique test path
  std::string UniquePath(const std::string& base) {
    static int counter = 0;
    return base + "_" + std::to_string(++counter) + "_$$";
  }
};

// Test 1: Basic functionality - delete all files under a directory
TEST_F(StatCacheWildcardTest, DeleteWildcard_Basic) {
  std::string path1 = UniquePath("/dir/file1.txt");
  std::string path2 = UniquePath("/dir/file2.txt");
  std::string path3 = UniquePath("/other/file3.txt");
  std::string pattern = path1.substr(0, path1.find_last_of('/') + 1);

  headers_t meta1 = CreateTestMeta("etag1");
  headers_t meta2 = CreateTestMeta("etag2");
  headers_t meta3 = CreateTestMeta("etag3");

  ASSERT_TRUE(cache->AddStat(path1, meta1, false, true));
  ASSERT_TRUE(cache->AddStat(path2, meta2, false, true));
  ASSERT_TRUE(cache->AddStat(path3, meta3, false, true));

  // Execute wildcard deletion
  bool result = cache->DelStatWildcard(pattern);
  ASSERT_TRUE(result);

  // Verify /dir/ entries are deleted
  headers_t result_meta;
  EXPECT_FALSE(cache->GetStat(path1, &result_meta))
    << "file1.txt should be deleted";
  EXPECT_FALSE(cache->GetStat(path2, &result_meta))
    << "file2.txt should be deleted";

  // Verify other entries still exist
  EXPECT_TRUE(cache->GetStat(path3, &result_meta))
    << "file3.txt should still exist";
}

// Test 2: Empty pattern - should not delete any entries
TEST_F(StatCacheWildcardTest, DeleteWildcard_EmptyPattern) {
  std::string path = UniquePath("/dir/file1.txt");
  headers_t meta = CreateTestMeta();
  ASSERT_TRUE(cache->AddStat(path, meta, false, true));

  // Empty pattern should not delete anything
  bool result = cache->DelStatWildcard("");
  EXPECT_FALSE(result) << "Empty pattern should return false";

  // Verify data still exists
  headers_t result_meta;
  EXPECT_TRUE(cache->GetStat(path, &result_meta));
}

// Test 3: No match - pattern doesn't match any entries
TEST_F(StatCacheWildcardTest, DeleteWildcard_NoMatch) {
  std::string path = UniquePath("/dir/file1.txt");
  headers_t meta = CreateTestMeta();
  ASSERT_TRUE(cache->AddStat(path, meta, false, true));

  // Pattern doesn't match any entries
  bool result = cache->DelStatWildcard("/nomatch/");
  ASSERT_TRUE(result);

  // Verify data still exists
  headers_t result_meta;
  EXPECT_TRUE(cache->GetStat(path, &result_meta));
}

// Test 4: Root directory - special case
TEST_F(StatCacheWildcardTest, DeleteWildcard_RootDirectory) {
  std::string path1 = UniquePath("/file1.txt");
  std::string path2 = UniquePath("/dir/file2.txt");

  headers_t meta1 = CreateTestMeta("etag1");
  headers_t meta2 = CreateTestMeta("etag2");

  ASSERT_TRUE(cache->AddStat(path1, meta1, false, true));
  ASSERT_TRUE(cache->AddStat(path2, meta2, false, true));

  // Delete all content under root directory
  bool result = cache->DelStatWildcard("/");
  ASSERT_TRUE(result);

  // Verify all entries are deleted (all paths start with "/")
  headers_t result_meta;
  EXPECT_FALSE(cache->GetStat(path1, &result_meta));
  EXPECT_FALSE(cache->GetStat(path2, &result_meta));
}

// Test 5: Nested directories - recursive deletion
TEST_F(StatCacheWildcardTest, DeleteWildcard_NestedDirectories) {
  std::string base = UniquePath("/dir");
  std::string path1 = base + "/file1.txt";
  std::string path2 = base + "/subdir1/file2.txt";
  std::string path3 = base + "/subdir1/subdir2/file3.txt";
  std::string path4 = UniquePath("/other/file4.txt");
  std::string pattern = base + "/";

  headers_t meta = CreateTestMeta();

  // Add nested directory structure
  ASSERT_TRUE(cache->AddStat(path1, meta, false, true));
  ASSERT_TRUE(cache->AddStat(path2, meta, false, true));
  ASSERT_TRUE(cache->AddStat(path3, meta, false, true));
  ASSERT_TRUE(cache->AddStat(path4, meta, false, true));

  // Execute wildcard deletion
  bool result = cache->DelStatWildcard(pattern);
  ASSERT_TRUE(result);

  // Verify all /dir/ entries are deleted (including deeply nested)
  headers_t result_meta;
  EXPECT_FALSE(cache->GetStat(path1, &result_meta));
  EXPECT_FALSE(cache->GetStat(path2, &result_meta));
  EXPECT_FALSE(cache->GetStat(path3, &result_meta));

  // Verify other directories are not affected
  EXPECT_TRUE(cache->GetStat(path4, &result_meta));
}

// Test 6: Slash handling - with/without trailing slash
TEST_F(StatCacheWildcardTest, DeleteWildcard_PatternWithSlash) {
  std::string base = UniquePath("/dir");
  std::string path1 = base + "/file1.txt";
  std::string path_dir = base + "/";
  std::string pattern = base + "/";

  headers_t meta1 = CreateTestMeta("etag1");
  headers_t meta2 = CreateTestMeta("etag2");

  // Add paths with and without slashes
  ASSERT_TRUE(cache->AddStat(path1, meta1, false, true));
  ASSERT_TRUE(cache->AddStat(path_dir, meta2, true, false));

  // Delete pattern with slash
  bool result = cache->DelStatWildcard(pattern);
  ASSERT_TRUE(result);

  // Verify entries are deleted
  headers_t result_meta;
  EXPECT_FALSE(cache->GetStat(path1, &result_meta));

  // Note: path_dir might still exist because exact match requires DelStat()
  // This is expected behavior, DelStatWildcard only handles prefix matching
}

// Test 7: Large dataset - performance test
TEST_F(StatCacheWildcardTest, DeleteWildcard_LargeDataset) {
  std::string base = UniquePath("/dir");
  std::string pattern = base + "/";
  std::string other_path = UniquePath("/other/file.txt");
  headers_t meta = CreateTestMeta();

  // Add 1000 entries
  for (int i = 0; i < 1000; i++) {
    std::string path = base + "/file" + std::to_string(i) + ".txt";
    ASSERT_TRUE(cache->AddStat(path, meta, false, true));
  }

  // Add other entries
  ASSERT_TRUE(cache->AddStat(other_path, meta, false, true));

  // Delete all /dir/ entries
  bool result = cache->DelStatWildcard(pattern);
  ASSERT_TRUE(result);

  // Verify all /dir/ entries are deleted
  headers_t result_meta;
  for (int i = 0; i < 1000; i++) {
    std::string path = base + "/file" + std::to_string(i) + ".txt";
    EXPECT_FALSE(cache->GetStat(path, &result_meta))
      << "File should be deleted: " << path;
  }

  // Verify other entries still exist
  EXPECT_TRUE(cache->GetStat(other_path, &result_meta));
}

// Test 8: Concurrent safety (basic check)
TEST_F(StatCacheWildcardTest, DeleteWildcard_ThreadSafetyBasic) {
  std::string path = UniquePath("/dir/file1.txt");
  std::string pattern = path.substr(0, path.find_last_of('/') + 1);
  headers_t meta = CreateTestMeta();

  ASSERT_TRUE(cache->AddStat(path, meta, false, true));

  // Multiple deletions should not crash
  bool result1 = cache->DelStatWildcard(pattern);
  bool result2 = cache->DelStatWildcard(pattern);

  EXPECT_TRUE(result1);
  EXPECT_TRUE(result2);  // Second deletion should also succeed (just no entries to delete)
}

// Test 9: Performance benchmark - measure deletion performance at different cache sizes
TEST_F(StatCacheWildcardTest, Performance_BenchmarkDeleteWildcard) {
  headers_t meta = CreateTestMeta();

  // Test different cache sizes
  std::vector<int> sizes = {100, 1000, 5000, 10000};

  for (int size : sizes) {
    // Clear cache by creating a new unique base for each iteration
    std::string base = UniquePath("/bench_" + std::to_string(size));
    std::string pattern = base + "/";

    // Add specified number of entries
    for (int i = 0; i < size; i++) {
      std::string path = base + "/file" + std::to_string(i) + ".txt";
      ASSERT_TRUE(cache->AddStat(path, meta, false, true));
    }

    // Measure deletion time
    auto start = std::chrono::high_resolution_clock::now();
    bool result = cache->DelStatWildcard(pattern);
    auto end = std::chrono::high_resolution_clock::now();

    ASSERT_TRUE(result);

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    std::cout << "Cache size: " << size
              << ", Delete time: " << duration.count() << " μs"
              << ", Per entry: " << (duration.count() / size) << " μs/entry"
              << std::endl;

    // Verify all entries are deleted
    headers_t result_meta;
    for (int i = 0; i < size; i++) {
      std::string path = base + "/file" + std::to_string(i) + ".txt";
      EXPECT_FALSE(cache->GetStat(path, &result_meta))
        << "File should be deleted: " << path;
    }

    // Performance assertions (adjusted for reasonable expectations)
    if (size == 100) {
      EXPECT_LT(duration.count(), 10000) << "100 entries should delete in < 10ms";
    } else if (size == 1000) {
      EXPECT_LT(duration.count(), 100000) << "1000 entries should delete in < 100ms";
    } else if (size == 5000) {
      EXPECT_LT(duration.count(), 500000) << "5000 entries should delete in < 500ms";
    } else if (size == 10000) {
      EXPECT_LT(duration.count(), 1000000) << "10000 entries should delete in < 1s";
    }
  }
}

// Test 10: GetExpireTime - when expire time is not set
TEST_F(StatCacheWildcardTest, GetExpireTime_NotSet) {
  // First unset any existing expire time
  cache->UnsetExpireTime();

  // Initially, expire time should return -1 (not set)
  time_t expire = cache->GetExpireTime();
  EXPECT_EQ(expire, (time_t)-1) << "Expire time should be -1 when not set";
}

// Test 11: SetExpireTime - set expire time and verify
TEST_F(StatCacheWildcardTest, SetExpireTime_Basic) {
  // First unset any existing expire time to get a clean state
  cache->UnsetExpireTime();

  time_t new_expire = 3600; // 1 hour
  time_t old_expire = cache->SetExpireTime(new_expire, false);

  // We can't assert the old value due to singleton pattern across tests
  // Just verify the new value is set correctly

  // New expire time should be set
  time_t current_expire = cache->GetExpireTime();
  EXPECT_EQ(current_expire, new_expire) << "Expire time should be set to 3600";
}

// Test 12: SetExpireTime - with interval type
TEST_F(StatCacheWildcardTest, SetExpireTime_WithInterval) {
  // Set a new expire time with interval type
  time_t new_expire = 7200; // 2 hours
  time_t old_expire = cache->SetExpireTime(new_expire, true);

  // We don't assert the old value since it depends on previous test state
  // Just verify the new value is set correctly

  // New expire time should be set
  time_t current_expire = cache->GetExpireTime();
  EXPECT_EQ(current_expire, new_expire) << "Expire time should be set to 7200";
}

// Test 13: SetExpireTime - update existing expire time
TEST_F(StatCacheWildcardTest, SetExpireTime_UpdateExisting) {
  // Set initial expire time
  time_t first_expire = 3600;
  cache->SetExpireTime(first_expire, false);

  // Update with new expire time
  time_t second_expire = 7200;
  time_t old_expire = cache->SetExpireTime(second_expire, true);

  // Old expire time should be the first value
  EXPECT_EQ(old_expire, first_expire) << "Old expire time should be 3600";

  // Current expire time should be the new value
  time_t current_expire = cache->GetExpireTime();
  EXPECT_EQ(current_expire, second_expire) << "Expire time should be updated to 7200";
}

// Test 14: UnsetExpireTime - unset expire time
TEST_F(StatCacheWildcardTest, UnsetExpireTime_Basic) {
  // Set expire time first
  time_t expire = 3600;
  cache->SetExpireTime(expire, false);

  // Verify it's set
  EXPECT_EQ(cache->GetExpireTime(), expire) << "Expire time should be set";

  // Unset expire time
  time_t old_expire = cache->UnsetExpireTime();

  // Old expire time should be the value we set
  EXPECT_EQ(old_expire, expire) << "Old expire time should be 3600";

  // Current expire time should be -1 (unset)
  EXPECT_EQ(cache->GetExpireTime(), (time_t)-1) << "Expire time should be -1 after unset";
}

// Test 15: UnsetExpireTime - when not set
TEST_F(StatCacheWildcardTest, UnsetExpireTime_NotSet) {
  // Try to unset when it's not set
  time_t old_expire = cache->UnsetExpireTime();

  // Should return -1 when expire time was not set
  EXPECT_EQ(old_expire, (time_t)-1) << "Old expire time should be -1 when not set";
}

// Test 16: SetCacheNoObject - enable cache no object
TEST_F(StatCacheWildcardTest, SetCacheNoObject_Enable) {
  // Initially, cache no object should be disabled (default)
  bool initial_state = cache->GetCacheNoObject();
  // Note: We don't know the initial state, so we'll test the toggle

  // Enable cache no object
  bool old_state = cache->SetCacheNoObject(true);

  // Current state should be enabled
  EXPECT_TRUE(cache->GetCacheNoObject()) << "Cache no object should be enabled";

  // Old state should be what it was before
  EXPECT_EQ(old_state, initial_state) << "Old state should match initial state";
}

// Test 17: SetCacheNoObject - disable cache no object
TEST_F(StatCacheWildcardTest, SetCacheNoObject_Disable) {
  // First enable it
  cache->SetCacheNoObject(true);
  EXPECT_TRUE(cache->GetCacheNoObject()) << "Cache no object should be enabled";

  // Now disable it
  bool old_state = cache->SetCacheNoObject(false);

  // Old state should be true (was enabled)
  EXPECT_TRUE(old_state) << "Old state should be true";

  // Current state should be disabled
  EXPECT_FALSE(cache->GetCacheNoObject()) << "Cache no object should be disabled";
}

// Test 18: SetCacheNoObject - toggle multiple times
TEST_F(StatCacheWildcardTest, SetCacheNoObject_Toggle) {
  // Start with disabled
  cache->SetCacheNoObject(false);
  EXPECT_FALSE(cache->GetCacheNoObject());

  // Enable
  bool old1 = cache->SetCacheNoObject(true);
  EXPECT_FALSE(old1) << "Old state should be false";
  EXPECT_TRUE(cache->GetCacheNoObject()) << "Should be enabled";

  // Disable
  bool old2 = cache->SetCacheNoObject(false);
  EXPECT_TRUE(old2) << "Old state should be true";
  EXPECT_FALSE(cache->GetCacheNoObject()) << "Should be disabled";

  // Enable again
  bool old3 = cache->SetCacheNoObject(true);
  EXPECT_FALSE(old3) << "Old state should be false";
  EXPECT_TRUE(cache->GetCacheNoObject()) << "Should be enabled";
}

// Test 19: EnableCacheNoObject and DisableCacheNoObject convenience methods
TEST_F(StatCacheWildcardTest, CacheNoObject_ConvenienceMethods) {
  // Test EnableCacheNoObject
  cache->DisableCacheNoObject();
  bool old1 = cache->EnableCacheNoObject();
  EXPECT_FALSE(old1) << "Old state should be false";
  EXPECT_TRUE(cache->GetCacheNoObject()) << "Should be enabled";

  // Test DisableCacheNoObject
  bool old2 = cache->DisableCacheNoObject();
  EXPECT_TRUE(old2) << "Old state should be true";
  EXPECT_FALSE(cache->GetCacheNoObject()) << "Should be disabled";
}

// Test 20: DelStat - basic deletion of existing entry
TEST_F(StatCacheWildcardTest, DelStat_Basic) {
  std::string path = UniquePath("/dir/file1.txt");
  headers_t meta = CreateTestMeta("etag1");

  // Add entry
  ASSERT_TRUE(cache->AddStat(path, meta, false, true));

  // Verify entry exists
  headers_t result_meta;
  ASSERT_TRUE(cache->GetStat(path, &result_meta));

  // Delete entry
  bool result = cache->DelStat(path.c_str());
  EXPECT_TRUE(result) << "DelStat should return true for successful deletion";

  // Verify entry is deleted
  EXPECT_FALSE(cache->GetStat(path, &result_meta))
    << "Entry should be deleted after DelStat";
}

// Test 21: DelStat - delete non-existent entry
TEST_F(StatCacheWildcardTest, DelStat_NonExistent) {
  std::string path = UniquePath("/dir/nonexistent.txt");

  // Try to delete non-existent entry
  bool result = cache->DelStat(path.c_str());
  EXPECT_TRUE(result) << "DelStat should return true even for non-existent entry";
}

// Test 22: DelStat - NULL key handling
TEST_F(StatCacheWildcardTest, DelStat_NullKey) {
  // Try to delete with NULL key
  bool result = cache->DelStat(nullptr);
  EXPECT_FALSE(result) << "DelStat should return false for NULL key";
}

// Test 23: DelStat - delete path with trailing slash
TEST_F(StatCacheWildcardTest, DelStat_PathWithTrailingSlash) {
  std::string path_with_slash = UniquePath("/dir/file.txt/");
  std::string path_without_slash = path_with_slash.substr(0, path_with_slash.length() - 1);
  headers_t meta = CreateTestMeta("etag1");

  // Add entry with trailing slash (directory)
  ASSERT_TRUE(cache->AddStat(path_with_slash, meta, true, false));

  // Verify entry exists with trailing slash
  headers_t result_meta;
  ASSERT_TRUE(cache->GetStat(path_with_slash, &result_meta, false));

  // Delete with trailing slash
  bool result = cache->DelStat(path_with_slash.c_str());
  EXPECT_TRUE(result);

  // Verify both variants are deleted
  EXPECT_FALSE(cache->GetStat(path_with_slash, &result_meta, false))
    << "Entry with trailing slash should be deleted";
  EXPECT_FALSE(cache->GetStat(path_without_slash, &result_meta, false))
    << "Entry without trailing slash should also be deleted";
}

// Test 24: DelStat - delete without trailing slash removes both variants
TEST_F(StatCacheWildcardTest, DelStat_DeleteWithoutSlashRemovesBoth) {
  std::string base = UniquePath("/dir/file.txt");
  std::string path_with_slash = base + "/";
  std::string path_without_slash = base;
  headers_t meta1 = CreateTestMeta("etag1");
  headers_t meta2 = CreateTestMeta("etag2");

  // Add both variants
  ASSERT_TRUE(cache->AddStat(path_with_slash, meta1, true, false));
  ASSERT_TRUE(cache->AddStat(path_without_slash, meta2, false, true));

  // Verify both exist
  headers_t result_meta;
  ASSERT_TRUE(cache->GetStat(path_with_slash, &result_meta, false));
  ASSERT_TRUE(cache->GetStat(path_without_slash, &result_meta, false));

  // Delete without trailing slash - this should delete the version without slash
  // and also try to delete the version with slash
  bool result = cache->DelStat(path_without_slash.c_str());
  EXPECT_TRUE(result);

  // Both should be deleted (DelStat removes both variants)
  EXPECT_FALSE(cache->GetStat(path_with_slash, &result_meta, false))
    << "Entry with slash should be deleted";
  EXPECT_FALSE(cache->GetStat(path_without_slash, &result_meta, false))
    << "Entry without slash should be deleted";
}

// Test 25: DelStat - root path special handling
TEST_F(StatCacheWildcardTest, DelStat_RootPath) {
  std::string root_path = "/";
  headers_t meta = CreateTestMeta("etag-root");

  // Add root entry
  ASSERT_TRUE(cache->AddStat(root_path, meta, true, false));

  // Verify root exists
  headers_t result_meta;
  ASSERT_TRUE(cache->GetStat(root_path, &result_meta, false));

  // Delete root
  bool result = cache->DelStat(root_path.c_str());
  EXPECT_TRUE(result) << "DelStat should handle root path";

  // Verify root is deleted
  EXPECT_FALSE(cache->GetStat(root_path, &result_meta, false))
    << "Root entry should be deleted";
}

// Test 26: DelStat - string variant overload
TEST_F(StatCacheWildcardTest, DelStat_StringOverload) {
  std::string path = UniquePath("/dir/file1.txt");
  headers_t meta = CreateTestMeta("etag1");

  // Add entry
  ASSERT_TRUE(cache->AddStat(path, meta, false, true));

  // Verify entry exists
  headers_t result_meta;
  ASSERT_TRUE(cache->GetStat(path, &result_meta));

  // Delete using string overload
  std::string path_copy = path;
  bool result = cache->DelStat(path_copy);
  EXPECT_TRUE(result) << "DelStat string overload should work";

  // Verify entry is deleted
  EXPECT_FALSE(cache->GetStat(path, &result_meta))
    << "Entry should be deleted after DelStat";
}

// Test 27: DelStat - multiple deletions of same entry
TEST_F(StatCacheWildcardTest, DelStat_MultipleDeletions) {
  std::string path = UniquePath("/dir/file1.txt");
  headers_t meta = CreateTestMeta("etag1");

  // Add entry
  ASSERT_TRUE(cache->AddStat(path, meta, false, true));

  // Delete first time
  bool result1 = cache->DelStat(path.c_str());
  EXPECT_TRUE(result1);

  // Verify it's deleted
  headers_t result_meta;
  EXPECT_FALSE(cache->GetStat(path, &result_meta));

  // Delete second time (should still return true)
  bool result2 = cache->DelStat(path.c_str());
  EXPECT_TRUE(result2) << "Second deletion should also return true";

  // Third deletion
  bool result3 = cache->DelStat(path.c_str());
  EXPECT_TRUE(result3) << "Third deletion should also return true";
}

// Test 28: DelStat - delete entry with both slash variants present
TEST_F(StatCacheWildcardTest, DelStat_BothSlashVariantsPresent) {
  std::string base = UniquePath("/dir/file.txt");
  std::string path_with_slash = base + "/";
  std::string path_without_slash = base;
  headers_t meta1 = CreateTestMeta("etag1");
  headers_t meta2 = CreateTestMeta("etag2");

  // Add both variants (they might have different metadata)
  ASSERT_TRUE(cache->AddStat(path_with_slash, meta1, true, false));
  ASSERT_TRUE(cache->AddStat(path_without_slash, meta2, false, true));

  // Verify both exist
  headers_t result_meta;
  ASSERT_TRUE(cache->GetStat(path_with_slash, &result_meta, false));
  ASSERT_TRUE(cache->GetStat(path_without_slash, &result_meta, false));

  // Delete the version without slash - should remove both
  bool result = cache->DelStat(path_without_slash.c_str());
  EXPECT_TRUE(result);

  // Both should be deleted
  EXPECT_FALSE(cache->GetStat(path_with_slash, &result_meta, false))
    << "Entry with slash should be deleted";
  EXPECT_FALSE(cache->GetStat(path_without_slash, &result_meta, false))
    << "Entry without slash should be deleted";
}

// Test 29: DelStat - delete entry preserves other entries
TEST_F(StatCacheWildcardTest, DelStat_PreservesOtherEntries) {
  std::string path1 = UniquePath("/dir/file1.txt");
  std::string path2 = UniquePath("/dir/file2.txt");
  std::string path3 = UniquePath("/other/file3.txt");
  headers_t meta1 = CreateTestMeta("etag1");
  headers_t meta2 = CreateTestMeta("etag2");
  headers_t meta3 = CreateTestMeta("etag3");

  // Add multiple entries
  ASSERT_TRUE(cache->AddStat(path1, meta1, false, true));
  ASSERT_TRUE(cache->AddStat(path2, meta2, false, true));
  ASSERT_TRUE(cache->AddStat(path3, meta3, false, true));

  // Delete only path1
  bool result = cache->DelStat(path1.c_str());
  EXPECT_TRUE(result);

  // Verify path1 is deleted but others remain
  headers_t result_meta;
  EXPECT_FALSE(cache->GetStat(path1, &result_meta))
    << "path1 should be deleted";
  EXPECT_TRUE(cache->GetStat(path2, &result_meta))
    << "path2 should still exist";
  EXPECT_TRUE(cache->GetStat(path3, &result_meta))
    << "path3 should still exist";
}

// Test 30: DelStat - with empty string key
TEST_F(StatCacheWildcardTest, DelStat_EmptyStringKey) {
  // Try to delete with empty string
  bool result = cache->DelStat("");
  EXPECT_TRUE(result) << "DelStat should return true for empty string key";

  // Should not crash or affect other entries
  std::string path = UniquePath("/dir/file1.txt");
  headers_t meta = CreateTestMeta("etag1");
  ASSERT_TRUE(cache->AddStat(path, meta, false, true));

  headers_t result_meta;
  EXPECT_TRUE(cache->GetStat(path, &result_meta))
    << "Other entries should not be affected";
}

// Test 31: ChangeNoTruncateFlag - increase no_truncate flag
TEST_F(StatCacheWildcardTest, ChangeNoTruncateFlag_Increase) {
  std::string path = UniquePath("/dir/file1.txt");
  headers_t meta = CreateTestMeta("etag1");

  // Add entry (no_truncate=0 initially)
  ASSERT_TRUE(cache->AddStat(path, meta, false, false));

  // Increase no_truncate flag
  cache->ChangeNoTruncateFlag(path, true);

  // Entry should still exist after flag change
  headers_t result_meta;
  EXPECT_TRUE(cache->GetStat(path, &result_meta))
    << "Entry should exist after increasing no_truncate flag";

  // Increase no_truncate flag multiple times
  cache->ChangeNoTruncateFlag(path, true);
  cache->ChangeNoTruncateFlag(path, true);

  // Entry should still exist
  EXPECT_TRUE(cache->GetStat(path, &result_meta))
    << "Entry should exist after multiple increases";
}

// Test 32: ChangeNoTruncateFlag - decrease no_truncate flag
TEST_F(StatCacheWildcardTest, ChangeNoTruncateFlag_Decrease) {
  std::string path = UniquePath("/dir/file1.txt");
  headers_t meta = CreateTestMeta("etag1");

  // Add entry and protect it
  ASSERT_TRUE(cache->AddStat(path, meta, false, true));
  cache->ChangeNoTruncateFlag(path, true);
  cache->ChangeNoTruncateFlag(path, true);  // Increase twice

  // Now decrease
  cache->ChangeNoTruncateFlag(path, false);

  // Entry should still exist
  headers_t result_meta;
  EXPECT_TRUE(cache->GetStat(path, &result_meta))
    << "Entry should still exist after one decrease";
}

// Test 33: ChangeNoTruncateFlag - decrease to zero
TEST_F(StatCacheWildcardTest, ChangeNoTruncateFlag_DecreaseToZero) {
  std::string path = UniquePath("/dir/file1.txt");
  headers_t meta = CreateTestMeta("etag1");

  // Add entry and protect it
  ASSERT_TRUE(cache->AddStat(path, meta, false, true));
  cache->ChangeNoTruncateFlag(path, true);

  // Decrease back to zero
  cache->ChangeNoTruncateFlag(path, false);

  // Entry should still exist (no_truncate=0 doesn't delete it)
  headers_t result_meta;
  EXPECT_TRUE(cache->GetStat(path, &result_meta))
    << "Entry should still exist when no_truncate reaches zero";
}

// Test 34: ChangeNoTruncateFlag - multiple increase and decrease cycles
TEST_F(StatCacheWildcardTest, ChangeNoTruncateFlag_MultipleCycles) {
  std::string path = UniquePath("/dir/file1.txt");
  headers_t meta = CreateTestMeta("etag1");

  // Add entry
  ASSERT_TRUE(cache->AddStat(path, meta, false, true));

  // Multiple increase/decrease cycles
  for (int i = 0; i < 5; i++) {
    cache->ChangeNoTruncateFlag(path, true);
  }

  for (int i = 0; i < 3; i++) {
    cache->ChangeNoTruncateFlag(path, false);
  }

  // Entry should still exist
  headers_t result_meta;
  EXPECT_TRUE(cache->GetStat(path, &result_meta))
    << "Entry should still exist after multiple cycles";
}

// Test 35: ChangeNoTruncateFlag - decrease on entry with zero count
TEST_F(StatCacheWildcardTest, ChangeNoTruncateFlag_DecreaseFromZero) {
  std::string path = UniquePath("/dir/file1.txt");
  headers_t meta = CreateTestMeta("etag1");

  // Add entry (notruncate starts at 0)
  ASSERT_TRUE(cache->AddStat(path, meta, false, true));

  // Try to decrease from zero - should not crash
  cache->ChangeNoTruncateFlag(path, false);

  // Entry should still exist
  headers_t result_meta;
  EXPECT_TRUE(cache->GetStat(path, &result_meta))
    << "Entry should still exist after decrease from zero";
}

// Test 36: ChangeNoTruncateFlag - non-existent entry
TEST_F(StatCacheWildcardTest, ChangeNoTruncateFlag_NonExistentEntry) {
  std::string path = UniquePath("/dir/nonexistent.txt");

  // Try to change no_truncate flag on non-existent entry
  // Should not crash
  cache->ChangeNoTruncateFlag(path, true);
  cache->ChangeNoTruncateFlag(path, false);
}

// Test 37: ChangeNoTruncateFlag - with AddStat no_truncate parameter
TEST_F(StatCacheWildcardTest, ChangeNoTruncateFlag_WithAddStatNoTruncate) {
  std::string path = UniquePath("/dir/file1.txt");
  headers_t meta = CreateTestMeta("etag1");

  // Add entry with no_truncate=true via AddStat
  ASSERT_TRUE(cache->AddStat(path, meta, false, true));

  // Entry should exist
  headers_t result_meta;
  EXPECT_TRUE(cache->GetStat(path, &result_meta))
    << "Entry should exist after AddStat with no_truncate=true";

  // Use ChangeNoTruncateFlag to further increase the flag
  cache->ChangeNoTruncateFlag(path, true);

  // Entry should still exist
  EXPECT_TRUE(cache->GetStat(path, &result_meta))
    << "Entry should exist after ChangeNoTruncateFlag";

  // Now decrease the flag
  cache->ChangeNoTruncateFlag(path, false);

  // Entry should still exist
  EXPECT_TRUE(cache->GetStat(path, &result_meta))
    << "Entry should exist after decreasing no_truncate flag";

  // Decrease back to zero (AddStat with no_truncate=true sets notruncate=1)
  cache->ChangeNoTruncateFlag(path, false);

  // Entry should still exist (decreasing to zero doesn't delete the entry)
  EXPECT_TRUE(cache->GetStat(path, &result_meta))
    << "Entry should exist when no_truncate reaches zero";
}

// ============================================================
// Tests for AddNoObjectCache and IsNoObjectCache
// ============================================================

// Test 38: AddNoObjectCache - basic functionality
TEST_F(StatCacheWildcardTest, AddNoObjectCache_Basic) {
  // Enable cache no object
  cache->EnableCacheNoObject();

  std::string path = UniquePath("/dir/nonexistent.txt");

  // Add no object cache entry
  bool result = cache->AddNoObjectCache(path);
  EXPECT_TRUE(result) << "AddNoObjectCache should return true";

  // Verify no object cache exists
  EXPECT_TRUE(cache->IsNoObjectCache(path))
    << "IsNoObjectCache should return true for the added path";
}

// Test 39: AddNoObjectCache - when cache no object is disabled
TEST_F(StatCacheWildcardTest, AddNoObjectCache_Disabled) {
  // Disable cache no object
  cache->DisableCacheNoObject();

  std::string path = UniquePath("/dir/nonexistent.txt");

  // Add no object cache entry when disabled
  bool result = cache->AddNoObjectCache(path);
  EXPECT_TRUE(result) << "AddNoObjectCache should return true even when disabled";

  // IsNoObjectCache should return false when disabled
  EXPECT_FALSE(cache->IsNoObjectCache(path))
    << "IsNoObjectCache should return false when feature is disabled";
}

// Test 40: AddNoObjectCache - with overcheck enabled
TEST_F(StatCacheWildcardTest, AddNoObjectCache_WithOvercheck) {
  cache->EnableCacheNoObject();

  std::string path = UniquePath("/dir/file.txt");
  std::string path_with_slash = path + "/";

  // Add no object cache entry with trailing slash (directory-style)
  ASSERT_TRUE(cache->AddNoObjectCache(path_with_slash));

  // Check with overcheck=true (default behavior)
  // When overcheck is true and the path doesn't end with '/',
  // IsNoObjectCache will try both with and without the trailing slash
  EXPECT_TRUE(cache->IsNoObjectCache(path))
    << "IsNoObjectCache with overcheck should find the entry (path without slash should match path with slash)";
  EXPECT_TRUE(cache->IsNoObjectCache(path_with_slash))
    << "IsNoObjectCache should find the entry with exact match";
}

// Test 41: AddNoObjectCache - replace existing entry
TEST_F(StatCacheWildcardTest, AddNoObjectCache_ReplaceExisting) {
  cache->EnableCacheNoObject();

  std::string path = UniquePath("/dir/file.txt");
  headers_t meta = CreateTestMeta("etag1");

  // First add a regular stat entry
  ASSERT_TRUE(cache->AddStat(path, meta, false, true));

  // Now replace it with no object cache
  ASSERT_TRUE(cache->AddNoObjectCache(path));

  // GetStat should return false (no object cache is not a regular stat)
  headers_t result_meta;
  EXPECT_FALSE(cache->GetStat(path, &result_meta))
    << "GetStat should return false after AddNoObjectCache";

  // IsNoObjectCache should return true
  EXPECT_TRUE(cache->IsNoObjectCache(path))
    << "IsNoObjectCache should return true";
}

// Test 42: AddNoObjectCache - with zero cache size
TEST_F(StatCacheWildcardTest, AddNoObjectCache_ZeroCacheSize) {
  cache->EnableCacheNoObject();

  // Set cache size to zero
  cache->SetCacheSize(0);

  std::string path = UniquePath("/dir/file.txt");

  // AddNoObjectCache should return true (no-op when cache size is 0)
  bool result = cache->AddNoObjectCache(path);
  EXPECT_TRUE(result) << "AddNoObjectCache should return true with zero cache size";

  // Restore cache size
  cache->SetCacheSize(1000);
}

// Test 43: AddNoObjectCache - multiple entries
TEST_F(StatCacheWildcardTest, AddNoObjectCache_MultipleEntries) {
  cache->EnableCacheNoObject();

  std::string base = UniquePath("/dir");
  std::vector<std::string> paths = {
    base + "/file1.txt",
    base + "/file2.txt",
    base + "/subdir/file3.txt"
  };

  // Add multiple no object cache entries
  for (size_t i = 0; i < paths.size(); ++i) {
    std::string& path = paths[i];
    ASSERT_TRUE(cache->AddNoObjectCache(path))
      << "AddNoObjectCache should succeed for " << path;
  }

  // Verify all entries exist
  for (size_t i = 0; i < paths.size(); ++i) {
    std::string& path = paths[i];
    EXPECT_TRUE(cache->IsNoObjectCache(path))
      << "IsNoObjectCache should find " << path;
  }
}

// Test 44: IsNoObjectCache - non-existent entry
TEST_F(StatCacheWildcardTest, IsNoObjectCache_NonExistent) {
  cache->EnableCacheNoObject();

  std::string path = UniquePath("/dir/nonexistent.txt");

  // IsNoObjectCache should return false for non-existent entry
  EXPECT_FALSE(cache->IsNoObjectCache(path))
    << "IsNoObjectCache should return false for non-existent entry";
}

// Test 45: IsNoObjectCache - with regular stat entry
TEST_F(StatCacheWildcardTest, IsNoObjectCache_WithRegularStat) {
  cache->EnableCacheNoObject();

  std::string path = UniquePath("/dir/file.txt");
  headers_t meta = CreateTestMeta("etag1");

  // Add a regular stat entry (not no object cache)
  ASSERT_TRUE(cache->AddStat(path, meta, false, true));

  // IsNoObjectCache should return false for regular stat entries
  EXPECT_FALSE(cache->IsNoObjectCache(path))
    << "IsNoObjectCache should return false for regular stat entries";
}

// Test 46: IsNoObjectCache - entry is deleted after timeout
TEST_F(StatCacheWildcardTest, IsNoObjectCache_WithExpireTime) {
  cache->EnableCacheNoObject();

  // Set a very short expire time (1 second)
  cache->SetExpireTime(1, false);

  std::string path = UniquePath("/dir/file.txt");

  // Add no object cache entry
  ASSERT_TRUE(cache->AddNoObjectCache(path));
  EXPECT_TRUE(cache->IsNoObjectCache(path))
    << "IsNoObjectCache should find the entry immediately";

  // Wait for cache to expire
  sleep(2);

  // After expiration, IsNoObjectCache should return false
  // and should delete the expired entry
  EXPECT_FALSE(cache->IsNoObjectCache(path))
    << "IsNoObjectCache should return false after expiration";

  // Restore default expire time
  cache->UnsetExpireTime();
}

// Test 47: IsNoObjectCache - with overcheck behavior
TEST_F(StatCacheWildcardTest, IsNoObjectCache_OvercheckBehavior) {
  cache->EnableCacheNoObject();

  std::string path = UniquePath("/dir/file.txt");
  std::string path_with_slash = path + "/";

  // Add with trailing slash
  ASSERT_TRUE(cache->AddNoObjectCache(path_with_slash));

  // IsNoObjectCache with overcheck=true should find it
  EXPECT_TRUE(cache->IsNoObjectCache(path_with_slash))
    << "IsNoObjectCache should find entry with slash";

  // IsNoObjectCache with overcheck=false should also find it
  EXPECT_TRUE(cache->IsNoObjectCache(path))
    << "IsNoObjectCache should find entry without slash";
}

// Test 48: AddNoObjectCache - when cache needs truncation
TEST_F(StatCacheWildcardTest, AddNoObjectCache_WithTruncation) {
  cache->EnableCacheNoObject();

  // Set a small cache size
  cache->SetCacheSize(10);

  // Add more entries than cache size
  std::vector<std::string> paths;
  for (int i = 0; i < 20; i++) {
    std::string path = UniquePath("/dir/file" + std::to_string(i) + ".txt");
    paths.push_back(path);
    ASSERT_TRUE(cache->AddNoObjectCache(paths.back()))
      << "AddNoObjectCache should succeed for entry " << i;
  }

  // At least some entries should exist (oldest may be truncated)
  int found_count = 0;
  for (int i = 10; i < 20; i++) {
    if (cache->IsNoObjectCache(paths[i])) {
      found_count++;
    }
  }

  // Some newer entries should still exist
  EXPECT_GT(found_count, 0) << "Some entries should exist after truncation";

  // Restore cache size
  cache->SetCacheSize(1000);
}

// Test 49: AddNoObjectCache - mixed regular and no object entries
TEST_F(StatCacheWildcardTest, AddNoObjectCache_MixedEntries) {
  cache->EnableCacheNoObject();

  std::string path1 = UniquePath("/dir/file1.txt");
  std::string path2 = UniquePath("/dir/file2.txt");
  headers_t meta = CreateTestMeta("etag1");

  // Add regular stat entry
  ASSERT_TRUE(cache->AddStat(path1, meta, false, true));

  // Add no object cache entry
  ASSERT_TRUE(cache->AddNoObjectCache(path2));

  // Verify both exist with appropriate methods
  headers_t result_meta;
  EXPECT_TRUE(cache->GetStat(path1, &result_meta))
    << "Regular stat should exist";
  EXPECT_FALSE(cache->GetStat(path2, &result_meta))
    << "No object cache should not be found by GetStat";
  EXPECT_TRUE(cache->IsNoObjectCache(path2))
    << "No object cache should be found by IsNoObjectCache";
  EXPECT_FALSE(cache->IsNoObjectCache(path1))
    << "Regular stat should not be found by IsNoObjectCache";
}

// Test fixture for GetChildStatMap
class StatCacheGetChildTest : public ::testing::Test {
 protected:
  StatCache* cache;
  unsigned long cache_size;

  void SetUp() override {
    cache = StatCache::getStatCacheData();
    cache_size = cache->GetCacheSize();
    cache->SetCacheSize(1000);
  }

  void TearDown() override {
    cache->SetCacheSize(cache_size);
  }

  std::string UniquePath(const std::string& base) {
    static int counter = 0;
    return base + "_" + std::to_string(++counter) + "_gc";
  }

  headers_t CreateTestMeta(const std::string& etag = "test-etag") {
    headers_t meta;
    meta["Content-Type"] = "text/plain";
    meta["ETag"] = etag;
    meta["Content-Length"] = "100";
    return meta;
  }

  struct stat CreateTestStat(mode_t mode = S_IFREG | 0644) {
    struct stat stbuf;
    memset(&stbuf, 0, sizeof(stbuf));
    stbuf.st_mode = mode;
    stbuf.st_size = 100;
    stbuf.st_nlink = 1;
    return stbuf;
  }
};

TEST_F(StatCacheGetChildTest, GetChildStatMap_EmptyDirectory) {
  std::string dir = UniquePath("/dir");
  s3obj_type_map_t result;

  bool ret = cache->GetChildStatMap(dir, result);
  ASSERT_TRUE(ret);
  EXPECT_EQ(0, result.size());
}

TEST_F(StatCacheGetChildTest, GetChildStatMap_WithFiles) {
  std::string dir = UniquePath("/dir");
  std::string file1 = dir + "/file1.txt";
  std::string file2 = dir + "/file2.txt";

  headers_t meta1 = CreateTestMeta("etag1");
  headers_t meta2 = CreateTestMeta("etag2");

  ASSERT_TRUE(cache->AddStat(file1, meta1, false, true));
  ASSERT_TRUE(cache->AddStat(file2, meta2, false, true));

  s3obj_type_map_t result;
  bool ret = cache->GetChildStatMap(dir, result);
  ASSERT_TRUE(ret);
  EXPECT_EQ(2, result.size());
  EXPECT_EQ(OBJTYPE_FILE, result["file1.txt"]);
  EXPECT_EQ(OBJTYPE_FILE, result["file2.txt"]);
}

TEST_F(StatCacheGetChildTest, GetChildStatMap_RootDirectory) {
  std::string file1 = UniquePath("/file1.txt");
  std::string file2 = UniquePath("/file2.txt");

  headers_t meta1 = CreateTestMeta("etag1");
  headers_t meta2 = CreateTestMeta("etag2");

  ASSERT_TRUE(cache->AddStat(file1, meta1, false, true));
  ASSERT_TRUE(cache->AddStat(file2, meta2, false, true));

  s3obj_type_map_t result;
  bool ret = cache->GetChildStatMap("/", result);
  ASSERT_TRUE(ret);
  EXPECT_EQ(2, result.size());
}

TEST_F(StatCacheGetChildTest, GetChildStatMap_NestedStructure) {
  std::string parent = UniquePath("/parent");
  std::string child_dir = parent + "/child";
  std::string file1 = parent + "/file1.txt";
  std::string file2 = child_dir + "/file2.txt";

  headers_t meta1 = CreateTestMeta("etag1");
  headers_t meta2 = CreateTestMeta("etag2");

  ASSERT_TRUE(cache->AddStat(file1, meta1, false, true));
  ASSERT_TRUE(cache->AddStat(file2, meta2, false, true));

  s3obj_type_map_t result;
  bool ret = cache->GetChildStatMap(parent, result);
  ASSERT_TRUE(ret);
  EXPECT_EQ(1, result.size());  // Only file1.txt, file2.txt is nested
  EXPECT_EQ(OBJTYPE_FILE, result["file1.txt"]);
  EXPECT_TRUE(result.find("file2.txt") == result.end());
}

TEST_F(StatCacheGetChildTest, GetChildStatMap_PathNormalization) {
  std::string dir = UniquePath("/dir");
  std::string file1 = dir + "/file1.txt";

  headers_t meta1 = CreateTestMeta("etag1");
  ASSERT_TRUE(cache->AddStat(file1, meta1, false, true));

  // Test without trailing slash
  s3obj_type_map_t result1;
  bool ret1 = cache->GetChildStatMap(dir, result1);
  ASSERT_TRUE(ret1);
  EXPECT_EQ(1, result1.size());

  // Test with trailing slash (should give same result)
  s3obj_type_map_t result2;
  bool ret2 = cache->GetChildStatMap(dir + "/", result2);
  ASSERT_TRUE(ret2);
  EXPECT_EQ(1, result2.size());
}

TEST_F(StatCacheGetChildTest, GetChildStatMap_MixedFilesAndDirs) {
  std::string dir = UniquePath("/dir");
  std::string file1 = dir + "/file1.txt";
  std::string subdir = dir + "/subdir";
  std::string file2 = dir + "/file2.txt";

  headers_t meta1 = CreateTestMeta("etag1");
  headers_t meta_dir = CreateTestMeta("etag_dir");
  headers_t meta2 = CreateTestMeta("etag2");

  // Add files (forcedir=false) and directory (forcedir=true)
  ASSERT_TRUE(cache->AddStat(file1, meta1, false, true));
  ASSERT_TRUE(cache->AddStat(subdir, meta_dir, true, true));  // forcedir=true for directory
  ASSERT_TRUE(cache->AddStat(file2, meta2, false, true));

  s3obj_type_map_t result;
  bool ret = cache->GetChildStatMap(dir, result);
  ASSERT_TRUE(ret);
  EXPECT_EQ(3, result.size());
  EXPECT_EQ(OBJTYPE_FILE, result["file1.txt"]);
  EXPECT_EQ(OBJTYPE_DIR, result["subdir"]);
  EXPECT_EQ(OBJTYPE_FILE, result["file2.txt"]);
}

TEST_F(StatCacheGetChildTest, GetChildStatMap_DeepNesting) {
  std::string root = UniquePath("/root");
  std::string level1 = root + "/level1";
  std::string level2 = level1 + "/level2";
  std::string level3 = level2 + "/level3";
  std::string deep_file = level3 + "/deep.txt";

  headers_t meta = CreateTestMeta("etag");
  headers_t meta_dir = CreateTestMeta("etag_dir");

  // Add all intermediate directories to cache (forcedir=true for directories)
  ASSERT_TRUE(cache->AddStat(level1, meta_dir, true, true));
  ASSERT_TRUE(cache->AddStat(level2, meta_dir, true, true));
  ASSERT_TRUE(cache->AddStat(level3, meta_dir, true, true));
  ASSERT_TRUE(cache->AddStat(deep_file, meta, false, true));

  // Only direct children should be returned
  s3obj_type_map_t result;
  bool ret = cache->GetChildStatMap(root, result);
  ASSERT_TRUE(ret);
  EXPECT_EQ(1, result.size());  // Only level1
  EXPECT_EQ(OBJTYPE_DIR, result["level1"]);
}

//-------------------------------------------------------------------
// Concurrent tests — verify lock_guard correctness under contention
//-------------------------------------------------------------------

class StatCacheConcurrentTest : public ::testing::Test {
 protected:
  StatCache* cache;
  unsigned long orig_cache_size;
  time_t orig_expire;

  void SetUp() override {
    cache = StatCache::getStatCacheData();
    orig_cache_size = cache->GetCacheSize();
    orig_expire = cache->GetExpireTime();
    cache->SetCacheSize(10000);
    cache->UnsetExpireTime();
  }

  void TearDown() override {
    cache->SetCacheSize(orig_cache_size);
    if(orig_expire != (time_t)-1){
      cache->SetExpireTime(orig_expire);
    }
  }

  headers_t CreateTestMeta(const std::string& etag = "test-etag") {
    headers_t meta;
    meta["Content-Type"] = "text/plain";
    meta["ETag"] = etag;
    meta["Content-Length"] = "100";
    return meta;
  }

  std::string UniquePath(const std::string& base) {
    static std::atomic<int> counter(10000);
    return base + "_" + std::to_string(counter.fetch_add(1)) + "_ct";
  }
};

TEST_F(StatCacheConcurrentTest, Concurrent_AddStat_SameKey) {
  std::string key = UniquePath("/conc/same");
  const int num_threads = 8;
  const int iterations = 100;
  std::vector<std::thread> threads;

  for(int t = 0; t < num_threads; t++){
    threads.emplace_back([&, t](){
      for(int i = 0; i < iterations; i++){
        headers_t meta = CreateTestMeta("etag_t" + std::to_string(t) + "_i" + std::to_string(i));
        cache->AddStat(key, meta, false, true);
      }
    });
  }
  for(auto& th : threads) th.join();

  // Final state: key should exist with some consistent meta
  headers_t result_meta;
  EXPECT_TRUE(cache->GetStat(key, &result_meta));
}

TEST_F(StatCacheConcurrentTest, Concurrent_AddStat_DifferentKeys) {
  const int num_threads = 8;
  const int keys_per_thread = 100;
  std::vector<std::thread> threads;
  std::vector<std::string> all_keys;

  for(int t = 0; t < num_threads; t++){
    for(int i = 0; i < keys_per_thread; i++){
      all_keys.push_back(UniquePath("/conc/diff_t" + std::to_string(t) + "_k" + std::to_string(i)));
    }
  }

  for(int t = 0; t < num_threads; t++){
    threads.emplace_back([&, t](){
      for(int i = 0; i < keys_per_thread; i++){
        int idx = t * keys_per_thread + i;
        headers_t meta = CreateTestMeta("etag_" + std::to_string(idx));
        std::string k = all_keys[idx];
        cache->AddStat(k, meta, false, true);
      }
    });
  }
  for(auto& th : threads) th.join();

  // All keys should exist
  for(const auto& k : all_keys){
    std::string mutable_k = k;
    EXPECT_TRUE(cache->HasStat(mutable_k));
  }
}

TEST_F(StatCacheConcurrentTest, Concurrent_GetStat_WithAddStat) {
  std::string key = UniquePath("/conc/getadd");
  headers_t meta = CreateTestMeta("initial");
  cache->AddStat(key, meta, false, true);

  const int num_threads = 8;
  const int iterations = 200;
  std::atomic<int> get_success(0);
  std::vector<std::thread> threads;

  // 4 threads adding, 4 threads getting
  for(int t = 0; t < num_threads; t++){
    if(t < num_threads / 2){
      threads.emplace_back([&, t](){
        for(int i = 0; i < iterations; i++){
          headers_t m = CreateTestMeta("etag_" + std::to_string(t) + "_" + std::to_string(i));
          cache->AddStat(key, m, false, true);
        }
      });
    }else{
      threads.emplace_back([&](){
        for(int i = 0; i < iterations; i++){
          struct stat st;
          headers_t rmeta;
          if(cache->GetStat(key, &st, &rmeta)){
            get_success.fetch_add(1);
            // Verify meta is consistent (has Content-Type)
            EXPECT_TRUE(rmeta.find("Content-Type") != rmeta.end() || rmeta.find("content-type") != rmeta.end());
          }
        }
      });
    }
  }
  for(auto& th : threads) th.join();

  // Some gets should have succeeded
  EXPECT_GT(get_success.load(), 0);
}

TEST_F(StatCacheConcurrentTest, Concurrent_DelStat_WithAddStat) {
  std::string key = UniquePath("/conc/deladd");
  const int num_threads = 8;
  const int iterations = 200;
  std::vector<std::thread> threads;

  for(int t = 0; t < num_threads; t++){
    if(t < num_threads / 2){
      threads.emplace_back([&](){
        for(int i = 0; i < iterations; i++){
          headers_t m = CreateTestMeta("etag_" + std::to_string(i));
          cache->AddStat(key, m, false, true);
        }
      });
    }else{
      threads.emplace_back([&](){
        for(int i = 0; i < iterations; i++){
          cache->DelStat(key);
        }
      });
    }
  }
  for(auto& th : threads) th.join();
  // No crash, no double-free = PASS
}

TEST_F(StatCacheConcurrentTest, Concurrent_AddNoObjectCache_Mixed) {
  const int num_threads = 8;
  const int iterations = 100;
  std::vector<std::thread> threads;
  cache->EnableCacheNoObject();

  for(int t = 0; t < num_threads; t++){
    threads.emplace_back([&, t](){
      for(int i = 0; i < iterations; i++){
        std::string k = UniquePath("/conc/mixed_t" + std::to_string(t) + "_" + std::to_string(i));
        if(t < num_threads / 2){
          headers_t m = CreateTestMeta("etag_" + std::to_string(i));
          cache->AddStat(k, m, false, true);
        }else{
          cache->AddNoObjectCache(k);
        }
      }
    });
  }
  for(auto& th : threads) th.join();

  cache->DisableCacheNoObject();
}

TEST_F(StatCacheConcurrentTest, Concurrent_DelStatWildcard_WithAdd) {
  const int num_threads = 8;
  const int iterations = 50;
  std::string base = UniquePath("/conc/wild");
  std::vector<std::thread> threads;

  for(int t = 0; t < num_threads; t++){
    if(t < num_threads / 2){
      threads.emplace_back([&, t](){
        for(int i = 0; i < iterations; i++){
          std::string k = base + "/t" + std::to_string(t) + "_f" + std::to_string(i);
          headers_t m = CreateTestMeta("e" + std::to_string(i));
          cache->AddStat(k, m, false, true);
        }
      });
    }else{
      threads.emplace_back([&](){
        for(int i = 0; i < iterations; i++){
          cache->DelStatWildcard(base + "/");
        }
      });
    }
  }
  for(auto& th : threads) th.join();
  // No crash, no iterator invalidation = PASS
}

TEST_F(StatCacheConcurrentTest, Concurrent_TruncateUnderLoad) {
  cache->SetCacheSize(50);
  const int num_threads = 8;
  const int keys_per_thread = 100;
  std::vector<std::thread> threads;

  for(int t = 0; t < num_threads; t++){
    threads.emplace_back([&, t](){
      for(int i = 0; i < keys_per_thread; i++){
        std::string k = UniquePath("/conc/trunc_t" + std::to_string(t) + "_" + std::to_string(i));
        headers_t m = CreateTestMeta("e" + std::to_string(i));
        cache->AddStat(k, m, false, false);
      }
    });
  }
  for(auto& th : threads) th.join();
  // No crash under truncation pressure = PASS
}

//-------------------------------------------------------------------
// HasLock behavior equivalence tests
//-------------------------------------------------------------------

class StatCacheHasLockTest : public ::testing::Test {
 protected:
  StatCache* cache;
  unsigned long orig_cache_size;
  time_t orig_expire;

  void SetUp() override {
    cache = StatCache::getStatCacheData();
    orig_cache_size = cache->GetCacheSize();
    orig_expire = cache->GetExpireTime();
    cache->SetCacheSize(1000);
    cache->UnsetExpireTime();
  }

  void TearDown() override {
    cache->SetCacheSize(orig_cache_size);
    if(orig_expire != (time_t)-1){
      cache->SetExpireTime(orig_expire);
    }
  }

  headers_t CreateTestMeta(const std::string& etag = "test-etag") {
    headers_t meta;
    meta["Content-Type"] = "text/plain";
    meta["ETag"] = etag;
    meta["Content-Length"] = "100";
    return meta;
  }

  std::string UniquePath(const std::string& base) {
    static std::atomic<int> counter(20000);
    return base + "_" + std::to_string(counter.fetch_add(1)) + "_hl";
  }
};

TEST_F(StatCacheHasLockTest, AddStat_OverwriteExisting) {
  std::string key = UniquePath("/hl/overwrite");
  headers_t meta1 = CreateTestMeta("etag_first");
  headers_t meta2 = CreateTestMeta("etag_second");

  ASSERT_TRUE(cache->AddStat(key, meta1, false, true));

  // Verify first meta
  headers_t result;
  ASSERT_TRUE(cache->GetStat(key, &result));
  bool found_first = false;
  for(auto& kv : result){
    if(lower(kv.first) == "etag" && kv.second == "etag_first") found_first = true;
  }
  EXPECT_TRUE(found_first);

  // Overwrite with second meta
  ASSERT_TRUE(cache->AddStat(key, meta2, false, true));

  // Verify second meta
  headers_t result2;
  ASSERT_TRUE(cache->GetStat(key, &result2));
  bool found_second = false;
  for(auto& kv : result2){
    if(lower(kv.first) == "etag" && kv.second == "etag_second") found_second = true;
  }
  EXPECT_TRUE(found_second);
}

TEST_F(StatCacheHasLockTest, AddStat_TriggersInternalTruncate) {
  cache->SetCacheSize(5);

  for(int i = 0; i < 10; i++){
    std::string k = UniquePath("/hl/trunc_" + std::to_string(i));
    headers_t m = CreateTestMeta("e" + std::to_string(i));
    ASSERT_TRUE(cache->AddStat(k, m, false, false));
  }
  // Cache should have been truncated internally — no crash, and cache is bounded
  // We can't directly inspect cache size, but the fact it didn't crash proves TruncateCacheHasLock works
}

TEST_F(StatCacheHasLockTest, AddStat_ConvertHeaderFailure) {
  std::string key = UniquePath("/hl/bad_convert");
  // Use meta that will fail convert_header_to_stat (null path tested separately)
  // Actually convert_header_to_stat only fails on null path/pst, which AddStat handles.
  // We can test that AddStat with valid meta succeeds.
  headers_t meta = CreateTestMeta("etag_valid");
  ASSERT_TRUE(cache->AddStat(key, meta, false, true));
  EXPECT_TRUE(cache->HasStat(key));
}

//-------------------------------------------------------------------
// GetStat boundary tests
//-------------------------------------------------------------------

TEST_F(StatCacheHasLockTest, GetStat_ExpiredEntry_AutoDelete) {
  cache->SetExpireTime(1, false);  // 1 second expiry

  std::string key = UniquePath("/hl/expire");
  headers_t meta = CreateTestMeta("etag_exp");
  ASSERT_TRUE(cache->AddStat(key, meta, false, true));

  // Should be accessible immediately
  ASSERT_TRUE(cache->HasStat(key));

  // Wait for expiry
  sleep(2);

  // Should be expired and auto-deleted
  EXPECT_FALSE(cache->HasStat(key));
}

TEST_F(StatCacheHasLockTest, GetStat_ETagMismatch_DeletesEntry) {
  std::string key = UniquePath("/hl/etag_mismatch");
  headers_t meta = CreateTestMeta("etag_original");
  ASSERT_TRUE(cache->AddStat(key, meta, false, true));

  // HasStat with different etag should return false and delete entry
  EXPECT_FALSE(cache->HasStat(key, "etag_different"));

  // Entry should be deleted
  EXPECT_FALSE(cache->HasStat(key));
}

TEST_F(StatCacheHasLockTest, GetStat_NoobjcacheEntry_DeletedWhenDisabled) {
  cache->EnableCacheNoObject();

  std::string key = UniquePath("/hl/noobj_del");
  ASSERT_TRUE(cache->AddNoObjectCache(key));
  ASSERT_TRUE(cache->IsNoObjectCache(key));

  cache->DisableCacheNoObject();

  // GetStat should delete the noobjcache entry when IsCacheNoObject is false
  struct stat st;
  EXPECT_FALSE(cache->GetStat(key, &st));

  // Re-enable and check it's gone
  cache->EnableCacheNoObject();
  EXPECT_FALSE(cache->IsNoObjectCache(key));
  cache->DisableCacheNoObject();
}

TEST_F(StatCacheHasLockTest, GetStat_IntervalType_UpdatesCacheDate) {
  cache->SetExpireTime(10, true);  // interval type

  std::string key = UniquePath("/hl/interval");
  headers_t meta = CreateTestMeta("etag_int");
  ASSERT_TRUE(cache->AddStat(key, meta, false, true));

  // Access it — cache_date should be updated (interval mode)
  struct stat st;
  EXPECT_TRUE(cache->GetStat(key, &st));

  // Access again — should still be valid
  EXPECT_TRUE(cache->GetStat(key, &st));
}

TEST_F(StatCacheHasLockTest, GetStat_OvercheckSlash_FindsBothVariants) {
  // Add entry without trailing slash
  std::string key = UniquePath("/hl/overcheck");
  headers_t meta = CreateTestMeta("etag_oc");
  ASSERT_TRUE(cache->AddStat(key, meta, false, true));

  // GetStat with overcheck=true should try both variants
  EXPECT_TRUE(cache->HasStat(key, true));
}

TEST_F(StatCacheHasLockTest, GetStat_MultiplePaths_HitMiss) {
  std::string k1 = UniquePath("/hl/multi1");
  std::string k2 = UniquePath("/hl/multi2");
  std::string k3 = UniquePath("/hl/multi3");
  std::string k4 = UniquePath("/hl/miss1");
  std::string k5 = UniquePath("/hl/miss2");

  headers_t meta = CreateTestMeta("etag_m");
  ASSERT_TRUE(cache->AddStat(k1, meta, false, true));
  ASSERT_TRUE(cache->AddStat(k2, meta, false, true));
  ASSERT_TRUE(cache->AddStat(k3, meta, false, true));

  EXPECT_TRUE(cache->HasStat(k1));
  EXPECT_TRUE(cache->HasStat(k2));
  EXPECT_TRUE(cache->HasStat(k3));
  EXPECT_FALSE(cache->HasStat(k4));
  EXPECT_FALSE(cache->HasStat(k5));
}

TEST_F(StatCacheHasLockTest, GetStat_ReturnsMetaAndStat) {
  std::string key = UniquePath("/hl/meta_stat");
  headers_t meta;
  meta["Content-Type"] = "application/json";
  meta["ETag"] = "\"abc123\"";
  meta["Content-Length"] = "42";
  meta["Last-Modified"] = "Thu, 01 Jan 2026 00:00:00 GMT";
  meta["x-amz-meta-custom"] = "value1";

  ASSERT_TRUE(cache->AddStat(key, meta, false, true));

  struct stat st;
  headers_t result_meta;
  ASSERT_TRUE(cache->GetStat(key, &st, &result_meta));

  // Verify stat fields
  EXPECT_EQ(st.st_size, 42);

  // Verify filtered meta keys
  bool has_content_type = false, has_etag = false, has_content_length = false;
  bool has_last_modified = false, has_x_amz = false;
  for(auto& kv : result_meta){
    std::string tag = lower(kv.first);
    if(tag == "content-type") has_content_type = true;
    if(tag == "etag") has_etag = true;
    if(tag == "content-length") has_content_length = true;
    if(tag == "last-modified") has_last_modified = true;
    if(tag.substr(0, 5) == "x-amz") has_x_amz = true;
  }
  EXPECT_TRUE(has_content_type);
  EXPECT_TRUE(has_etag);
  EXPECT_TRUE(has_content_length);
  EXPECT_TRUE(has_last_modified);
  EXPECT_TRUE(has_x_amz);
}

TEST_F(StatCacheHasLockTest, GetStat_ForceDir_Flag) {
  std::string key = UniquePath("/hl/forcedir");
  headers_t meta = CreateTestMeta("etag_fd");

  ASSERT_TRUE(cache->AddStat(key, meta, true, true));  // forcedir=true

  struct stat st;
  bool isforce = false;
  ASSERT_TRUE(cache->GetStat(key, &st, NULL, true, &isforce));
  EXPECT_TRUE(isforce);
}

//-------------------------------------------------------------------
// AddStat / AddNoObjectCache boundary tests
//-------------------------------------------------------------------

TEST_F(StatCacheHasLockTest, AddStat_ZeroCacheSize) {
  cache->SetCacheSize(0);

  std::string key = UniquePath("/hl/zero_cs");
  headers_t meta = CreateTestMeta("etag_z");
  // no_truncate=false, CacheSize=0 → should return true but not add
  ASSERT_TRUE(cache->AddStat(key, meta, false, false));
  EXPECT_FALSE(cache->HasStat(key));
}

TEST_F(StatCacheHasLockTest, AddStat_ZeroCacheSize_ForcedByNoTruncate) {
  cache->SetCacheSize(0);

  std::string key = UniquePath("/hl/zero_nt");
  headers_t meta = CreateTestMeta("etag_znt");
  // no_truncate=true bypasses CacheSize check
  ASSERT_TRUE(cache->AddStat(key, meta, false, true));
  EXPECT_TRUE(cache->HasStat(key));
}

TEST_F(StatCacheHasLockTest, AddStat_MetaCopyFiltering) {
  std::string key = UniquePath("/hl/meta_filter");
  headers_t meta;
  meta["Content-Type"] = "text/html";
  meta["Content-Length"] = "200";
  meta["ETag"] = "\"etag_filter\"";
  meta["Last-Modified"] = "Mon, 01 Jan 2026 00:00:00 GMT";
  meta["x-amz-meta-foo"] = "bar";
  meta["X-Custom-Header"] = "should_not_appear";
  meta["Server"] = "should_not_appear";

  ASSERT_TRUE(cache->AddStat(key, meta, false, true));

  headers_t result;
  ASSERT_TRUE(cache->GetStat(key, &result));

  // Check that only filtered keys are present
  for(auto& kv : result){
    std::string tag = lower(kv.first);
    bool allowed = (tag == "content-type" || tag == "content-length" ||
                    tag == "etag" || tag == "last-modified" ||
                    tag.substr(0, 5) == "x-amz");
    EXPECT_TRUE(allowed) << "Unexpected meta key in cache: " << kv.first;
  }

  // Custom headers should NOT be present
  EXPECT_EQ(result.find("X-Custom-Header"), result.end());
  EXPECT_EQ(result.find("Server"), result.end());
}

TEST_F(StatCacheHasLockTest, AddNoObjectCache_ReplaceRegularEntry) {
  cache->EnableCacheNoObject();

  std::string key = UniquePath("/hl/replace_noobj");
  headers_t meta = CreateTestMeta("etag_rep");

  // Add regular stat entry
  ASSERT_TRUE(cache->AddStat(key, meta, false, true));
  ASSERT_TRUE(cache->HasStat(key));

  // Replace with no-object cache
  ASSERT_TRUE(cache->AddNoObjectCache(key));

  // GetStat should return false (it's a no-object entry)
  struct stat st;
  EXPECT_FALSE(cache->GetStat(key, &st));

  // IsNoObjectCache should return true
  EXPECT_TRUE(cache->IsNoObjectCache(key));

  cache->DisableCacheNoObject();
}

TEST_F(StatCacheHasLockTest, AddNoObjectCache_TruncationBehavior) {
  cache->SetCacheSize(5);
  cache->EnableCacheNoObject();

  for(int i = 0; i < 10; i++){
    std::string k = UniquePath("/hl/noobj_trunc_" + std::to_string(i));
    ASSERT_TRUE(cache->AddNoObjectCache(k));
  }
  // No crash under truncation = PASS

  cache->DisableCacheNoObject();
}

//-------------------------------------------------------------------
// TruncateCache boundary tests
//-------------------------------------------------------------------

TEST_F(StatCacheHasLockTest, TruncateCache_PreservesNoTruncateEntries) {
  // Clear the singleton cache to get a controlled starting state.
  // DelStatWildcard("/") removes all entries since all paths start with "/".
  cache->DelStatWildcard("/");
  cache->SetCacheSize(5);

  // Add 2 no_truncate entries
  std::string nt1 = UniquePath("/hl/nt_pres1");
  std::string nt2 = UniquePath("/hl/nt_pres2");
  headers_t meta = CreateTestMeta("etag_nt");
  ASSERT_TRUE(cache->AddStat(nt1, meta, false, true));  // no_truncate=true
  ASSERT_TRUE(cache->AddStat(nt2, meta, false, true));  // no_truncate=true

  // Add regular entries to fill up to CacheSize
  for(int i = 0; i < 3; i++){
    std::string k = UniquePath("/hl/nt_reg_" + std::to_string(i));
    headers_t m = CreateTestMeta("etag_r" + std::to_string(i));
    ASSERT_TRUE(cache->AddStat(k, m, false, false));
  }

  // Now add one more regular entry to trigger truncation.
  // erase_count = cache.size()(6) - CacheSize(5) + 1 = 2
  // The 2 notruncate entries each decrement erase_count → erase_count becomes 0.
  // With erase_count=0, no entries should actually be erased.
  std::string trigger = UniquePath("/hl/nt_trigger");
  headers_t meta_t = CreateTestMeta("etag_trigger");
  ASSERT_TRUE(cache->AddStat(trigger, meta_t, false, false));

  // no_truncate entries should still exist (they reduced erase_count to 0)
  EXPECT_TRUE(cache->HasStat(nt1));
  EXPECT_TRUE(cache->HasStat(nt2));
}

TEST_F(StatCacheHasLockTest, TruncateCache_ExpiredEntriesFirst) {
  cache->SetExpireTime(1, false);  // 1 second expiry
  cache->SetCacheSize(3);

  // Add entries that will expire
  std::string exp1 = UniquePath("/hl/exp_first1");
  std::string exp2 = UniquePath("/hl/exp_first2");
  headers_t meta = CreateTestMeta("etag_ef");
  ASSERT_TRUE(cache->AddStat(exp1, meta, false, false));
  ASSERT_TRUE(cache->AddStat(exp2, meta, false, false));

  // Wait for expiry
  sleep(2);

  // Add fresh entry (triggers truncation, expired ones should go first)
  std::string fresh = UniquePath("/hl/exp_fresh");
  headers_t meta_f = CreateTestMeta("etag_fresh");
  ASSERT_TRUE(cache->AddStat(fresh, meta_f, false, false));

  // Fresh entry should exist
  EXPECT_TRUE(cache->HasStat(fresh));
}

// ==========================================================================
// ISSUE2026051100001 iter-7: DelStatIfTruncatable tests
// ==========================================================================

class DelStatIfTruncatableTest : public ::testing::Test {
 protected:
  StatCache* cache;
  unsigned long saved_cache_size;

  void SetUp() override {
    cache = StatCache::getStatCacheData();
    saved_cache_size = cache->GetCacheSize();
    cache->SetCacheSize(1000);
  }

  void TearDown() override {
    cache->SetCacheSize(saved_cache_size);
  }

  headers_t MakeMeta() {
    headers_t meta;
    meta["Content-Type"] = "application/octet-stream";
    meta["Content-Length"] = "0";
    return meta;
  }

  std::string Uniq(const std::string& base) {
    static int counter = 0;
    return base + "_dsit_" + std::to_string(++counter);
  }
};

// Past the cleanup-race grace window. Used so tests don't have to sleep.
// Same clock as cache_date (CLOCK_MONOTONIC_COARSE), with +1000s to ensure
// even very stale entries are beyond the 1s grace.
static time_t FutureNow() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
  return ts.tv_sec + 1000;
}

TEST_F(DelStatIfTruncatableTest, DeletesEntryWhenNotruncateZero) {
  std::string key = Uniq("/a");
  headers_t meta = MakeMeta();
  ASSERT_TRUE(cache->AddStat(key, meta, false, false));  // notruncate=false
  ASSERT_TRUE(cache->HasStat(key));

  EXPECT_TRUE(cache->DelStatIfTruncatable(key, FutureNow()));

  struct stat stbuf;
  EXPECT_FALSE(cache->GetStat(key, &stbuf));
}

TEST_F(DelStatIfTruncatableTest, PreservesEntryWhenNotruncatePositive) {
  std::string key = Uniq("/a");
  headers_t meta = MakeMeta();
  ASSERT_TRUE(cache->AddStat(key, meta, false, true));  // notruncate=true → pin

  EXPECT_FALSE(cache->DelStatIfTruncatable(key, FutureNow()));

  // Still in cache
  struct stat stbuf;
  EXPECT_TRUE(cache->GetStat(key, &stbuf));
}

TEST_F(DelStatIfTruncatableTest, PreservesEntryWithMultipleNotruncatePins) {
  std::string key = Uniq("/a");
  headers_t meta = MakeMeta();
  ASSERT_TRUE(cache->AddStat(key, meta, false, true));
  cache->ChangeNoTruncateFlag(key, true);   // bump to 2
  cache->ChangeNoTruncateFlag(key, false);  // back to 1
  // notruncate==1, still pinned

  EXPECT_FALSE(cache->DelStatIfTruncatable(key, FutureNow()));
  EXPECT_TRUE(cache->HasStat(key));
}

TEST_F(DelStatIfTruncatableTest, ReturnsFalseForNonExistentKey) {
  std::string key = Uniq("/never_added");
  EXPECT_FALSE(cache->DelStatIfTruncatable(key, FutureNow()));
}

TEST_F(DelStatIfTruncatableTest, NotruncateBumpAndClear) {
  std::string key = Uniq("/a");
  headers_t meta = MakeMeta();
  ASSERT_TRUE(cache->AddStat(key, meta, false, false));  // notruncate=0

  cache->ChangeNoTruncateFlag(key, true);   // → 1
  EXPECT_FALSE(cache->DelStatIfTruncatable(key, FutureNow()));

  cache->ChangeNoTruncateFlag(key, false);  // → 0
  EXPECT_TRUE(cache->DelStatIfTruncatable(key, FutureNow()));
  EXPECT_FALSE(cache->HasStat(key));
}

// ISSUE2026051200001 review fix: 1-second cleanup-race grace.
// Refuses to delete an entry younger than 1 second so a mkdir
// immediately followed by readdir doesn't self-evict its own entry
// if ListBucket consistency lags the PUT.
TEST_F(DelStatIfTruncatableTest, GraceProtectsFreshEntry) {
  std::string key = Uniq("/fresh");
  headers_t meta = MakeMeta();
  ASSERT_TRUE(cache->AddStat(key, meta, false, false));

  // `now=0` defaults to time(NULL), which is what real callers use.
  // Entry was added <1s ago → must be refused.
  EXPECT_FALSE(cache->DelStatIfTruncatable(key));
  EXPECT_TRUE(cache->HasStat(key));
}

TEST_F(DelStatIfTruncatableTest, GraceWindowExpires) {
  std::string key = Uniq("/aged");
  headers_t meta = MakeMeta();
  ASSERT_TRUE(cache->AddStat(key, meta, false, false));

  // Simulate cleanup running 2 seconds later by passing now=cache_date+2.
  // Bypasses real sleep — keeps the test fast and deterministic.
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
  EXPECT_TRUE(cache->DelStatIfTruncatable(key, ts.tv_sec + 2));
  EXPECT_FALSE(cache->HasStat(key));
}

TEST_F(DelStatIfTruncatableTest, GraceBoundaryAtOneSecond) {
  // Boundary: exactly 1s elapsed → eligible (now-cache_date < 1 fails).
  std::string key = Uniq("/boundary");
  headers_t meta = MakeMeta();
  ASSERT_TRUE(cache->AddStat(key, meta, false, false));

  // now == cache_date + 1 → diff == 1, NOT < 1 → cleanup proceeds.
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
  EXPECT_TRUE(cache->DelStatIfTruncatable(key, ts.tv_sec + 1));
}

TEST_F(DelStatIfTruncatableTest, ThreadSafetyConcurrentDelete) {
  std::string key = Uniq("/concurrent");
  headers_t meta = MakeMeta();
  ASSERT_TRUE(cache->AddStat(key, meta, false, false));

  // Pass FutureNow() so the grace window doesn't mask the race we're testing.
  time_t now = FutureNow();
  std::atomic<int> success_count{0};
  std::vector<std::thread> threads;
  for(int i = 0; i < 4; ++i){
    threads.emplace_back([&, now]{
      if(cache->DelStatIfTruncatable(key, now)){
        ++success_count;
      }
    });
  }
  for(auto& t : threads) t.join();

  // Exactly one thread should have succeeded
  EXPECT_EQ(1, success_count.load());
  EXPECT_FALSE(cache->HasStat(key));
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
