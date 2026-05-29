/*
 * hws_index_cache_test.cpp - Unit tests for IndexCache
 */

#include "gtest.h"
#include "hws_index_cache.h"
#include "common.h"
#include <type_traits>

// Stub variables and functions required for linking
s3fs_log_level debug_level = S3FS_LOG_INFO;
const char* s3fs_log_nest[S3FS_LOG_NEST_MAX] = {"", "  ", "    ", "      "};
s3fs_log_mode debug_log_mode = LOG_MODE_FOREGROUND;

std::string program_name = "obsfs";
bool complement_stat = false;
const char* hws_obs_meta_uid = "x-hws-fs-uid";
const char* hws_obs_meta_gid = "x-hws-fs-gid";
const char* hws_obs_meta_mode = "x-hws-fs-mode";
const char* hws_obs_meta_mtime = "x-hws-fs-mtime";
bool cache_assert = false;
int gMetaCacheSize = 20000;
bucket_type_t g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;

void IndexLogEntry(const uint64_t module, const char* chunk_id, const uint32_t log_level,
                   const char* log_file_name, const char* log_func_name, const uint32_t log_line,
                   const char* pszFormat, ...) {
  // Stub implementation
}

void s3fsStatisLogModeNotObsfs() {
  // Stub implementation
}

const char* s3fs_crypt_lib_name() {
  return "openssl";
}

bool nohwscache = false;

using namespace std;

namespace {

class IndexCacheWildcardTest : public ::testing::Test {
 protected:
  IndexCache* cache;

  void SetUp() override {
    cache = IndexCache::getIndexCache();
    // Clear cache if possible, or use unique test keys
  }

  void TearDown() override {
    // Clean up test entries
    cache->DeleteIndex("/test_wildcard_file1.txt");
    cache->DeleteIndex("/test_wildcard_file2.txt");
    cache->DeleteIndex("/test_wildcard_subdir/file3.txt");
    cache->DeleteIndex("/other_file4.txt");
    cache->DeleteIndex("/test_set_first_write.txt");
    cache->DeleteIndex("/test_add_open_cnt.txt");
    cache->DeleteIndex("/test_put_add_open_cnt.txt");
    cache->DeleteIndex("/test_reduce_open_cnt.txt");
    cache->DeleteIndex("/test_set_filesize.txt");
    cache->DeleteIndex("/test_clear_stat.txt");
    cache->DeleteIndex("/test_smaller_size.txt");
    cache->DeleteIndex("/test_set_filesize_and_clear_time.txt");
  }

  // Helper: 创建测试用的 index cache entry
  tag_index_cache_entry_t CreateTestEntry(const std::string& key) {
    tag_index_cache_entry_t entry;
    entry.key = key;
    entry.dentryname = get_basename(key);  // Use get_basename to set dentryname correctly
    entry.inodeNo = generate_virtual_inode(key.c_str());
    entry.firstWritFlag = false;
    entry.statType = STAT_TYPE_HEAD;
    return entry;
  }
};

// 测试 1: 基本功能 - 删除目录下的所有文件
TEST_F(IndexCacheWildcardTest, DeleteWildcard_Basic) {
  // 添加测试数据
  tag_index_cache_entry_t entry1 = CreateTestEntry("/test_wildcard_file1.txt");
  tag_index_cache_entry_t entry2 = CreateTestEntry("/test_wildcard_file2.txt");
  tag_index_cache_entry_t entry3 = CreateTestEntry("/other_file4.txt");

  cache->PutIndexNotchangeOpenCnt("/test_wildcard_file1.txt", &entry1);
  cache->PutIndexNotchangeOpenCnt("/test_wildcard_file2.txt", &entry2);
  cache->PutIndexNotchangeOpenCnt("/other_file4.txt", &entry3);

  // 执行通配符删除
  cache->DeleteIndexWildcard("/test_wildcard_");

  // 验证 /test_wildcard_ 下的条目已删除
  tag_index_cache_entry_t result;
  EXPECT_FALSE(cache->GetIndex("/test_wildcard_file1.txt", &result))
    << "file1.txt should be deleted";
  EXPECT_FALSE(cache->GetIndex("/test_wildcard_file2.txt", &result))
    << "file2.txt should be deleted";

  // 验证其他条目仍在
  EXPECT_TRUE(cache->GetIndex("/other_file4.txt", &result))
    << "other_file4.txt should still exist";
}

// 测试 2: 空模式 - 不应崩溃
TEST_F(IndexCacheWildcardTest, DeleteWildcard_EmptyPattern) {
  tag_index_cache_entry_t entry = CreateTestEntry("/test_file.txt");
  cache->PutIndexNotchangeOpenCnt("/test_file.txt", &entry);

  // 空模式不应崩溃
  cache->DeleteIndexWildcard("");

  // 验证数据仍在
  tag_index_cache_entry_t result;
  EXPECT_TRUE(cache->GetIndex("/test_file.txt", &result));
}

// 测试 3: 嵌套目录 - 递归删除
TEST_F(IndexCacheWildcardTest, DeleteWildcard_NestedDirectories) {
  tag_index_cache_entry_t entry1 = CreateTestEntry("/test_wildcard_file1.txt");
  tag_index_cache_entry_t entry2 = CreateTestEntry("/test_wildcard_subdir/file3.txt");

  cache->PutIndexNotchangeOpenCnt("/test_wildcard_file1.txt", &entry1);
  cache->PutIndexNotchangeOpenCnt("/test_wildcard_subdir/file3.txt", &entry2);

  // 执行通配符删除
  cache->DeleteIndexWildcard("/test_wildcard_");

  // 验证所有 /test_wildcard_ 下的条目都被删除 (包括深层嵌套)
  tag_index_cache_entry_t result;
  EXPECT_FALSE(cache->GetIndex("/test_wildcard_file1.txt", &result));
  EXPECT_FALSE(cache->GetIndex("/test_wildcard_subdir/file3.txt", &result));
}

// 测试 4: setFirstWriteFlag - 设置首次写入标志
TEST_F(IndexCacheWildcardTest, SetFirstWriteFlag_Success) {
  tag_index_cache_entry_t entry = CreateTestEntry("/test_set_first_write.txt");
  entry.firstWritFlag = true;  // Set to true initially

  cache->PutIndexNotchangeOpenCnt("/test_set_first_write.txt", &entry);

  // Call setFirstWriteFlag
  int result = cache->setFirstWriteFlag("/test_set_first_write.txt");
  EXPECT_EQ(0, result);

  // Verify the flag is now false
  tag_index_cache_entry_t result_entry;
  ASSERT_TRUE(cache->GetIndex("/test_set_first_write.txt", &result_entry));
  EXPECT_FALSE(result_entry.firstWritFlag);
}

// 测试 5: setFirstWriteFlag - 对不存在的路径
TEST_F(IndexCacheWildcardTest, SetFirstWriteFlag_NonExistentPath) {
  // Call setFirstWriteFlag on non-existent path - should not crash
  int result = cache->setFirstWriteFlag("/non_existent_path.txt");
  EXPECT_EQ(0, result);  // Function returns 0 even if node doesn't exist
}

// 测试 6: AddEntryOpenCnt - 增加打开计数
TEST_F(IndexCacheWildcardTest, AddEntryOpenCnt_Success) {
  tag_index_cache_entry_t entry = CreateTestEntry("/test_add_open_cnt.txt");
  cache->PutIndexNotchangeOpenCnt("/test_add_open_cnt.txt", &entry);

  // Add open count
  cache->AddEntryOpenCnt("/test_add_open_cnt.txt");

  // Verify the entry still exists (open count incremented internally)
  tag_index_cache_entry_t result;
  EXPECT_TRUE(cache->GetIndex("/test_add_open_cnt.txt", &result));
}

// 测试 7: AddEntryOpenCnt - 对不存在的路径
TEST_F(IndexCacheWildcardTest, AddEntryOpenCnt_NonExistentPath) {
  // Call AddEntryOpenCnt on non-existent path - should not crash
  cache->AddEntryOpenCnt("/non_existent_path.txt");

  // Verify no entry was created
  tag_index_cache_entry_t result;
  EXPECT_FALSE(cache->GetIndex("/non_existent_path.txt", &result));
}

// 测试 8: PutIndexAddOpenCnt - 添加索引并增加打开计数
TEST_F(IndexCacheWildcardTest, PutIndexAddOpenCnt_InsertNew) {
  tag_index_cache_entry_t entry = CreateTestEntry("/test_put_add_open_cnt.txt");

  // Put with ADD_OPEN_CNT
  int result = cache->PutIndexAddOpenCnt("/test_put_add_open_cnt.txt", &entry);
  EXPECT_EQ(0, result);

  // Verify entry was added
  tag_index_cache_entry_t result_entry;
  EXPECT_TRUE(cache->GetIndex("/test_put_add_open_cnt.txt", &result_entry));
}

// 测试 9: PutIndexReduceOpenCnt - 减少打开计数
TEST_F(IndexCacheWildcardTest, PutIndexReduceOpenCnt_Success) {
  tag_index_cache_entry_t entry = CreateTestEntry("/test_reduce_open_cnt.txt");

  // First add with ADD_OPEN_CNT
  cache->PutIndexAddOpenCnt("/test_reduce_open_cnt.txt", &entry);

  // Then reduce with REDUCE_OPEN_CNT
  int result = cache->PutIndexReduceOpenCnt("/test_reduce_open_cnt.txt");
  EXPECT_EQ(0, result);

  // Verify entry still exists
  tag_index_cache_entry_t result_entry;
  EXPECT_TRUE(cache->GetIndex("/test_reduce_open_cnt.txt", &result_entry));
}

// 测试 10: PutIndexReduceOpenCnt - 对不存在的路径
TEST_F(IndexCacheWildcardTest, PutIndexReduceOpenCnt_NonExistentPath) {
  // Call PutIndexReduceOpenCnt on non-existent path - should return error
  int result = cache->PutIndexReduceOpenCnt("/non_existent_path.txt");
  EXPECT_EQ(-1, result);  // Function returns -1 when node doesn't exist
}

// 测试 11: setFilesizeOrClearGetAttrStat - 设置文件大小
TEST_F(IndexCacheWildcardTest, SetFilesizeOrClearGetAttrStat_SetSize) {
  tag_index_cache_entry_t entry = CreateTestEntry("/test_set_filesize.txt");
  entry.stGetAttrStat.st_size = 100;
  cache->PutIndexNotchangeOpenCnt("/test_set_filesize.txt", &entry);

  // Set larger file size
  int result = cache->setFilesizeOrClearGetAttrStat("/test_set_filesize.txt",
                                                     200,  // Larger size
                                                     false,  // Don't clear
                                                     STAT_TYPE_HEAD);
  EXPECT_EQ(0, result);

  // Verify size was updated
  tag_index_cache_entry_t result_entry;
  ASSERT_TRUE(cache->GetIndex("/test_set_filesize.txt", &result_entry));
  EXPECT_EQ(200, result_entry.stGetAttrStat.st_size);
}

// 测试 12: setFilesizeOrClearGetAttrStat - 清除GetAttrStat
TEST_F(IndexCacheWildcardTest, SetFilesizeOrClearGetAttrStat_ClearStat) {
  tag_index_cache_entry_t entry = CreateTestEntry("/test_clear_stat.txt");
  entry.stGetAttrStat.st_size = 100;
  entry.stGetAttrStat.st_mtime = 12345;
  cache->PutIndexNotchangeOpenCnt("/test_clear_stat.txt", &entry);

  // Clear stat
  int result = cache->setFilesizeOrClearGetAttrStat("/test_clear_stat.txt",
                                                     200,
                                                     true,  // Clear stat
                                                     STAT_TYPE_HEAD);
  EXPECT_EQ(0, result);

  // Verify stat was cleared
  tag_index_cache_entry_t result_entry;
  ASSERT_TRUE(cache->GetIndex("/test_clear_stat.txt", &result_entry));
  EXPECT_EQ(0, result_entry.stGetAttrStat.st_size);
  EXPECT_EQ(0, result_entry.stGetAttrStat.st_mtime);
}

// 测试 13: setFilesizeOrClearGetAttrStat - 设置较小的文件大小（不应更新）
TEST_F(IndexCacheWildcardTest, SetFilesizeOrClearGetAttrStat_SmallerSize) {
  tag_index_cache_entry_t entry = CreateTestEntry("/test_smaller_size.txt");
  entry.stGetAttrStat.st_size = 200;
  cache->PutIndexNotchangeOpenCnt("/test_smaller_size.txt", &entry);

  // Set smaller file size - should not update
  int result = cache->setFilesizeOrClearGetAttrStat("/test_smaller_size.txt",
                                                     100,  // Smaller size
                                                     false,
                                                     STAT_TYPE_HEAD);
  EXPECT_EQ(0, result);

  // Verify size was NOT updated (stays at 200)
  tag_index_cache_entry_t result_entry;
  ASSERT_TRUE(cache->GetIndex("/test_smaller_size.txt", &result_entry));
  EXPECT_EQ(200, result_entry.stGetAttrStat.st_size);
}

// 测试 14: setFilesizeOrClearGetAttrStat - 对不存在的路径
TEST_F(IndexCacheWildcardTest, SetFilesizeOrClearGetAttrStat_NonExistentPath) {
  // Call on non-existent path - should not crash
  int result = cache->setFilesizeOrClearGetAttrStat("/non_existent_path.txt",
                                                     100,
                                                     false,
                                                     STAT_TYPE_HEAD);
  EXPECT_EQ(0, result);  // Function returns 0 even if node doesn't exist
}

// 测试 15: setFilesizeAndClearIndexCacheTime - 设置文件大小并清除缓存时间
TEST_F(IndexCacheWildcardTest, SetFilesizeAndClearIndexCacheTime_Success) {
  tag_index_cache_entry_t entry = CreateTestEntry("/test_set_filesize_and_clear_time.txt");
  entry.stGetAttrStat.st_size = 100;
  entry.stGetAttrStat.st_mtime = 12345;
  entry.getAttrCacheSetTs.tv_sec = 999;
  entry.getAttrCacheSetTs.tv_nsec = 999;
  cache->PutIndexNotchangeOpenCnt("/test_set_filesize_and_clear_time.txt", &entry);

  // Set file size and clear cache time
  int result = cache->setFilesizeAndClearIndexCacheTime("/test_set_filesize_and_clear_time.txt",
                                                         200);  // New size
  EXPECT_EQ(0, result);

  // Verify size was updated and time fields were set
  tag_index_cache_entry_t result_entry;
  ASSERT_TRUE(cache->GetIndex("/test_set_filesize_and_clear_time.txt", &result_entry));
  EXPECT_EQ(200, result_entry.stGetAttrStat.st_size);
  EXPECT_GT(result_entry.stGetAttrStat.st_mtime, 12345);  // mtime should be updated to current time
  EXPECT_EQ(0, result_entry.getAttrCacheSetTs.tv_sec);  // getAttrCacheSetTs should be cleared
  EXPECT_EQ(0, result_entry.getAttrCacheSetTs.tv_nsec);
}

// 测试 16: setFilesizeAndClearIndexCacheTime - 对不存在的路径
TEST_F(IndexCacheWildcardTest, SetFilesizeAndClearIndexCacheTime_NonExistentPath) {
  // Call on non-existent path - should not crash and return 0
  int result = cache->setFilesizeAndClearIndexCacheTime("/non_existent_path.txt", 100);
  EXPECT_EQ(0, result);  // Function returns 0 even if node doesn't exist
}

// 测试 17: ReplaceIndex - 基本功能测试
// ReplaceIndex requires openCnt > 0 on source to create destination entry
TEST_F(IndexCacheWildcardTest, ReplaceIndex_Success) {
  // 创建源条目，使用 PutIndexAddOpenCnt 以增加 openCnt
  tag_index_cache_entry_t srcEntry = CreateTestEntry("/test_replace_src.txt");
  srcEntry.stGetAttrStat.st_size = 1000;
  cache->PutIndexAddOpenCnt("/test_replace_src.txt", &srcEntry);  // 使用 AddOpenCnt 以确保 openCnt > 0

  // 创建目标条目
  tag_index_cache_entry_t destEntry = CreateTestEntry("/test_replace_dest.txt");

  // 执行替换
  cache->ReplaceIndex("/test_replace_src.txt", "/test_replace_dest.txt", &destEntry);

  // 验证源条目被删除
  tag_index_cache_entry_t result;
  EXPECT_FALSE(cache->GetIndex("/test_replace_src.txt", &result))
    << "Source entry should be deleted after ReplaceIndex";

  // 验证目标条目存在 (因为源条目的 openCnt > 0)
  EXPECT_TRUE(cache->GetIndex("/test_replace_dest.txt", &result))
    << "Destination entry should exist after ReplaceIndex";

  // 清理
  cache->DeleteIndex("/test_replace_dest.txt");
}

// 测试 18: ReplaceIndex - 空路径处理
TEST_F(IndexCacheWildcardTest, ReplaceIndex_EmptyPath) {
  tag_index_cache_entry_t entry = CreateTestEntry("/test_replace_empty.txt");
  cache->PutIndexNotchangeOpenCnt("/test_replace_empty.txt", &entry);

  // 使用空路径调用 - 不应崩溃
  tag_index_cache_entry_t newEntry = CreateTestEntry("");
  cache->ReplaceIndex("", "/test_replace_new.txt", &newEntry);

  // 清理
  cache->DeleteIndex("/test_replace_empty.txt");
}

// 测试 19: DeleteIndex - 基本删除功能
TEST_F(IndexCacheWildcardTest, DeleteIndex_Success) {
  // 添加测试条目
  tag_index_cache_entry_t entry = CreateTestEntry("/test_delete_basic.txt");
  cache->PutIndexNotchangeOpenCnt("/test_delete_basic.txt", &entry);

  // 验证条目存在
  tag_index_cache_entry_t result;
  EXPECT_TRUE(cache->GetIndex("/test_delete_basic.txt", &result));

  // 删除条目
  cache->DeleteIndex("/test_delete_basic.txt");

  // 验证条目被删除
  EXPECT_FALSE(cache->GetIndex("/test_delete_basic.txt", &result));
}

// 测试 20: DeleteIndex - 删除不存在的条目
TEST_F(IndexCacheWildcardTest, DeleteIndex_NonExistent) {
  // 删除不存在的条目 - 不应崩溃
  cache->DeleteIndex("/non_existent_entry.txt");
}

// 测试 21: GetIndex - 获取不存在的条目
TEST_F(IndexCacheWildcardTest, GetIndex_NonExistent) {
  tag_index_cache_entry_t result;
  EXPECT_FALSE(cache->GetIndex("/completely_non_existent.txt", &result));
}

// 测试 22: PutIndexNotchangeOpenCnt - 更新现有条目
TEST_F(IndexCacheWildcardTest, PutIndexNotchangeOpenCnt_Update) {
  // 首先添加条目
  tag_index_cache_entry_t entry1 = CreateTestEntry("/test_update_entry.txt");
  entry1.stGetAttrStat.st_size = 100;
  cache->PutIndexNotchangeOpenCnt("/test_update_entry.txt", &entry1);

  // 更新条目
  tag_index_cache_entry_t entry2 = CreateTestEntry("/test_update_entry.txt");
  entry2.stGetAttrStat.st_size = 200;
  int result = cache->PutIndexNotchangeOpenCnt("/test_update_entry.txt", &entry2);
  EXPECT_EQ(0, result);

  // 验证更新成功
  tag_index_cache_entry_t resultEntry;
  ASSERT_TRUE(cache->GetIndex("/test_update_entry.txt", &resultEntry));
  EXPECT_EQ(200, resultEntry.stGetAttrStat.st_size);

  // 清理
  cache->DeleteIndex("/test_update_entry.txt");
}

// 测试 23: getIndexCache - 单例模式测试
TEST_F(IndexCacheWildcardTest, GetIndexCache_Singleton) {
  IndexCache* cache1 = IndexCache::getIndexCache();
  IndexCache* cache2 = IndexCache::getIndexCache();

  // 验证返回相同的实例
  EXPECT_EQ(cache1, cache2);
}

// 测试 24: 多条目操作测试
TEST_F(IndexCacheWildcardTest, MultipleEntries_Operations) {
  // 添加多个条目
  for (int i = 0; i < 10; i++) {
    std::string key = "/test_multi_" + std::to_string(i) + ".txt";
    tag_index_cache_entry_t entry = CreateTestEntry(key);
    entry.stGetAttrStat.st_size = i * 100;
    cache->PutIndexNotchangeOpenCnt(key, &entry);
  }

  // 验证所有条目存在
  for (int i = 0; i < 10; i++) {
    std::string key = "/test_multi_" + std::to_string(i) + ".txt";
    tag_index_cache_entry_t result;
    EXPECT_TRUE(cache->GetIndex(key, &result)) << "Entry " << key << " should exist";
    EXPECT_EQ(i * 100, result.stGetAttrStat.st_size);
  }

  // 清理
  for (int i = 0; i < 10; i++) {
    std::string key = "/test_multi_" + std::to_string(i) + ".txt";
    cache->DeleteIndex(key);
  }
}

// 测试 25: PutIndexAddOpenCnt - 更新现有条目
TEST_F(IndexCacheWildcardTest, PutIndexAddOpenCnt_Update) {
  // 首先添加条目
  tag_index_cache_entry_t entry1 = CreateTestEntry("/test_put_add_update.txt");
  entry1.stGetAttrStat.st_size = 100;
  cache->PutIndexNotchangeOpenCnt("/test_put_add_update.txt", &entry1);

  // 再次调用 PutIndexAddOpenCnt 更新
  tag_index_cache_entry_t entry2 = CreateTestEntry("/test_put_add_update.txt");
  entry2.stGetAttrStat.st_size = 200;
  int result = cache->PutIndexAddOpenCnt("/test_put_add_update.txt", &entry2);
  EXPECT_EQ(0, result);

  // 验证条目存在
  tag_index_cache_entry_t resultEntry;
  EXPECT_TRUE(cache->GetIndex("/test_put_add_update.txt", &resultEntry));

  // 清理
  cache->DeleteIndex("/test_put_add_update.txt");
}

// ============================================================
// openCnt boundary tests
// ============================================================

class OpenCntTest : public ::testing::Test {
 protected:
  IndexCache* cache;

  void SetUp() override {
    cache = IndexCache::getIndexCache();
  }

  void TearDown() override {
    // Clean up all test entries
    cache->DeleteIndex("/oc_init.txt");
    cache->DeleteIndex("/oc_multi.txt");
    cache->DeleteIndex("/oc_reduce_zero.txt");
    cache->DeleteIndex("/oc_reduce_negative.txt");
    cache->DeleteIndex("/oc_nochange.txt");
    cache->DeleteIndex("/oc_lru_evict.txt");
    cache->DeleteIndex("/oc_replace_accum_src.txt");
    cache->DeleteIndex("/oc_replace_accum_dest.txt");
    cache->DeleteIndex("/oc_replace_zero_src.txt");
    cache->DeleteIndex("/oc_replace_zero_dest.txt");
    cache->DeleteIndex("/oc_add_then_put.txt");
    cache->DeleteIndex("/oc_reuse.txt");
  }

  tag_index_cache_entry_t CreateEntry(const std::string& key) {
    tag_index_cache_entry_t entry;
    entry.key = key;
    entry.dentryname = get_basename(key);
    entry.inodeNo = generate_virtual_inode(key.c_str());
    entry.firstWritFlag = false;
    entry.statType = STAT_TYPE_HEAD;
    return entry;
  }
};

// openCnt starts at 0 after Node default initialization
TEST_F(OpenCntTest, InitialValueIsZero) {
  tag_index_cache_entry_t entry = CreateEntry("/oc_init.txt");
  cache->PutIndexNotchangeOpenCnt("/oc_init.txt", &entry);

  // Reduce should bring openCnt to -1 (error path), proving it started at 0
  // After reduce from 0, putNodeToList sees negative → erases from hashmap, puts to lru
  int rc = cache->PutIndexReduceOpenCnt("/oc_init.txt");
  EXPECT_EQ(0, rc);

  // Entry is erased from hashmap due to negative openCnt error path
  tag_index_cache_entry_t result;
  EXPECT_FALSE(cache->GetIndex("/oc_init.txt", &result))
    << "Entry should be removed from hashmap after openCnt goes negative";
}

// Multiple AddOpenCnt increments
TEST_F(OpenCntTest, MultipleIncrements) {
  tag_index_cache_entry_t entry = CreateEntry("/oc_multi.txt");
  cache->PutIndexNotchangeOpenCnt("/oc_multi.txt", &entry);

  // Increment openCnt 3 times
  cache->AddEntryOpenCnt("/oc_multi.txt");
  cache->AddEntryOpenCnt("/oc_multi.txt");
  cache->AddEntryOpenCnt("/oc_multi.txt");

  // Reduce 3 times → back to 0
  EXPECT_EQ(0, cache->PutIndexReduceOpenCnt("/oc_multi.txt"));
  EXPECT_EQ(0, cache->PutIndexReduceOpenCnt("/oc_multi.txt"));
  EXPECT_EQ(0, cache->PutIndexReduceOpenCnt("/oc_multi.txt"));

  // Entry should still exist at openCnt=0 (lru list)
  tag_index_cache_entry_t result;
  EXPECT_TRUE(cache->GetIndex("/oc_multi.txt", &result));
}

// Reduce openCnt from 1 to 0 moves node from openflag list to lru list
TEST_F(OpenCntTest, ReduceToZero_MovesToLruList) {
  tag_index_cache_entry_t entry = CreateEntry("/oc_reduce_zero.txt");
  // PutIndexAddOpenCnt → openCnt=1, placed on openflag_head
  cache->PutIndexAddOpenCnt("/oc_reduce_zero.txt", &entry);

  // Reduce → openCnt=0, should move to lru_head
  EXPECT_EQ(0, cache->PutIndexReduceOpenCnt("/oc_reduce_zero.txt"));

  // Entry should still be accessible
  tag_index_cache_entry_t result;
  EXPECT_TRUE(cache->GetIndex("/oc_reduce_zero.txt", &result));
}

// Reduce openCnt below zero triggers error path: erase from hashmap + move to lru
TEST_F(OpenCntTest, ReduceBelowZero_ErasedFromHashmap) {
  tag_index_cache_entry_t entry = CreateEntry("/oc_reduce_negative.txt");
  cache->PutIndexNotchangeOpenCnt("/oc_reduce_negative.txt", &entry);
  // openCnt is 0

  // Reduce → openCnt=-1, error path in putNodeToList erases from hashmap
  EXPECT_EQ(0, cache->PutIndexReduceOpenCnt("/oc_reduce_negative.txt"));

  tag_index_cache_entry_t result;
  EXPECT_FALSE(cache->GetIndex("/oc_reduce_negative.txt", &result))
    << "Negative openCnt entry should be erased from hashmap";
}

// NOCHANGE_OPEN_CNT preserves current openCnt value
TEST_F(OpenCntTest, NoChange_PreservesValue) {
  tag_index_cache_entry_t entry = CreateEntry("/oc_nochange.txt");

  // Add with openCnt=1
  cache->PutIndexAddOpenCnt("/oc_nochange.txt", &entry);

  // Update with NOCHANGE → openCnt stays 1
  entry.stGetAttrStat.st_size = 999;
  cache->PutIndexNotchangeOpenCnt("/oc_nochange.txt", &entry);

  // Reduce once → openCnt=0, entry still exists
  EXPECT_EQ(0, cache->PutIndexReduceOpenCnt("/oc_nochange.txt"));
  tag_index_cache_entry_t result;
  EXPECT_TRUE(cache->GetIndex("/oc_nochange.txt", &result));
  EXPECT_EQ(999, result.stGetAttrStat.st_size);
}

// ReplaceIndex accumulates openCnt when dest exists with same inode
TEST_F(OpenCntTest, ReplaceIndex_AccumulatesOpenCnt) {
  tag_index_cache_entry_t srcEntry = CreateEntry("/oc_replace_accum_src.txt");
  srcEntry.inodeNo = 12345;
  cache->PutIndexAddOpenCnt("/oc_replace_accum_src.txt", &srcEntry);  // openCnt=1

  tag_index_cache_entry_t destEntry = CreateEntry("/oc_replace_accum_dest.txt");
  destEntry.inodeNo = 12345;  // same inode
  cache->PutIndexAddOpenCnt("/oc_replace_accum_dest.txt", &destEntry);  // openCnt=1

  // Replace: dest.openCnt += src.openCnt → dest.openCnt=2
  cache->ReplaceIndex("/oc_replace_accum_src.txt", "/oc_replace_accum_dest.txt", &destEntry);

  // Source gone
  tag_index_cache_entry_t result;
  EXPECT_FALSE(cache->GetIndex("/oc_replace_accum_src.txt", &result));

  // Dest exists, needs 2 reduces to reach 0
  EXPECT_TRUE(cache->GetIndex("/oc_replace_accum_dest.txt", &result));
  EXPECT_EQ(0, cache->PutIndexReduceOpenCnt("/oc_replace_accum_dest.txt"));
  EXPECT_EQ(0, cache->PutIndexReduceOpenCnt("/oc_replace_accum_dest.txt"));
  EXPECT_TRUE(cache->GetIndex("/oc_replace_accum_dest.txt", &result));
}

// ReplaceIndex with src openCnt=0: dest not created when dest doesn't exist
TEST_F(OpenCntTest, ReplaceIndex_ZeroOpenCnt_NoDestCreated) {
  tag_index_cache_entry_t srcEntry = CreateEntry("/oc_replace_zero_src.txt");
  cache->PutIndexNotchangeOpenCnt("/oc_replace_zero_src.txt", &srcEntry);  // openCnt=0

  tag_index_cache_entry_t destEntry = CreateEntry("/oc_replace_zero_dest.txt");

  // Replace: src.openCnt=0, dest doesn't exist → dest NOT created (line 490 check)
  cache->ReplaceIndex("/oc_replace_zero_src.txt", "/oc_replace_zero_dest.txt", &destEntry);

  // Source removed
  tag_index_cache_entry_t result;
  EXPECT_FALSE(cache->GetIndex("/oc_replace_zero_src.txt", &result));

  // Dest should NOT be created (openCnt=0 means no open fds to preserve)
  EXPECT_FALSE(cache->GetIndex("/oc_replace_zero_dest.txt", &result))
    << "Dest should not be created when src openCnt is 0";
}

// AddEntryOpenCnt then PutIndexAddOpenCnt accumulates
TEST_F(OpenCntTest, AddEntry_ThenPutAdd_Accumulates) {
  tag_index_cache_entry_t entry = CreateEntry("/oc_add_then_put.txt");
  cache->PutIndexNotchangeOpenCnt("/oc_add_then_put.txt", &entry);  // openCnt=0

  cache->AddEntryOpenCnt("/oc_add_then_put.txt");  // openCnt=1

  entry.stGetAttrStat.st_size = 500;
  cache->PutIndexAddOpenCnt("/oc_add_then_put.txt", &entry);  // openCnt=2

  // Need 2 reduces to get back to 0
  EXPECT_EQ(0, cache->PutIndexReduceOpenCnt("/oc_add_then_put.txt"));
  EXPECT_EQ(0, cache->PutIndexReduceOpenCnt("/oc_add_then_put.txt"));

  tag_index_cache_entry_t result;
  EXPECT_TRUE(cache->GetIndex("/oc_add_then_put.txt", &result));
  EXPECT_EQ(500, result.stGetAttrStat.st_size);
}

// Node reuse: after delete, node goes back to lru with openCnt=0, can be reused
TEST_F(OpenCntTest, NodeReuse_OpenCntResetOnEviction) {
  tag_index_cache_entry_t entry = CreateEntry("/oc_reuse.txt");
  cache->PutIndexAddOpenCnt("/oc_reuse.txt", &entry);  // openCnt=1

  // Delete: node goes back to lru tail
  cache->DeleteIndex("/oc_reuse.txt");

  // Re-insert same key with NOCHANGE (openCnt should be fresh=0, not leftover=1)
  tag_index_cache_entry_t entry2 = CreateEntry("/oc_reuse.txt");
  cache->PutIndexNotchangeOpenCnt("/oc_reuse.txt", &entry2);

  // If openCnt was properly reset, reduce from 0 → negative → erased from hashmap
  EXPECT_EQ(0, cache->PutIndexReduceOpenCnt("/oc_reuse.txt"));
  tag_index_cache_entry_t result;
  EXPECT_FALSE(cache->GetIndex("/oc_reuse.txt", &result))
    << "After reuse, openCnt should be 0, so reduce makes it negative → erased";
}

// ========================================================================
// ISSUE2026040300002: resizeMetaCacheCapacity extend array leak fix tests
// ========================================================================

class ResizeMetaCacheTest : public ::testing::Test {
 protected:
  IndexCache* cache;

  void SetUp() override {
    cache = IndexCache::getIndexCache();
  }

  void TearDown() override {
    cache->DeleteIndex("/resize_test_1.txt");
    cache->DeleteIndex("/resize_test_2.txt");
  }

  tag_index_cache_entry_t CreateEntry(const std::string& key) {
    tag_index_cache_entry_t entry;
    entry.key = key;
    entry.dentryname = get_basename(key);
    entry.inodeNo = generate_virtual_inode(key.c_str());
    entry.firstWritFlag = false;
    entry.statType = STAT_TYPE_HEAD;
    return entry;
  }
};

// After resizeMetaCacheCapacity, the extended nodes should be usable in the cache.
// The extend_arrays_ vector saves the pointer for later cleanup in destructor.
TEST_F(ResizeMetaCacheTest, ResizeMetaCacheCapacity_ExtendArraySaved) {
  int old_size = gMetaCacheSize;
  size_t new_capacity = old_size + 100;

  // Expand the cache
  cache->resizeMetaCacheCapacity(new_capacity);
  EXPECT_EQ((int)new_capacity, gMetaCacheSize);

  // Verify the cache still works after expansion: put + get
  tag_index_cache_entry_t entry1 = CreateEntry("/resize_test_1.txt");
  entry1.stGetAttrStat.st_size = 777;
  cache->PutIndexNotchangeOpenCnt("/resize_test_1.txt", &entry1);

  tag_index_cache_entry_t result;
  ASSERT_TRUE(cache->GetIndex("/resize_test_1.txt", &result));
  EXPECT_EQ(777, result.stGetAttrStat.st_size);
}

// When the requested capacity is smaller than current, resizeMetaCacheCapacity
// should return immediately without allocating any new arrays.
TEST_F(ResizeMetaCacheTest, ResizeMetaCacheCapacity_DecreaseNoop) {
  int old_size = gMetaCacheSize;

  // Try to shrink -- should be a no-op
  cache->resizeMetaCacheCapacity(old_size - 10);
  EXPECT_EQ(old_size, gMetaCacheSize);

  // Verify cache still works
  tag_index_cache_entry_t entry2 = CreateEntry("/resize_test_2.txt");
  entry2.stGetAttrStat.st_size = 888;
  cache->PutIndexNotchangeOpenCnt("/resize_test_2.txt", &entry2);

  tag_index_cache_entry_t result;
  ASSERT_TRUE(cache->GetIndex("/resize_test_2.txt", &result));
  EXPECT_EQ(888, result.stGetAttrStat.st_size);
}

// ========================================================================
// Task D: Non-copyable class compile-time check (ISSUE2026040300001)
// ========================================================================

TEST(NonCopyable, IndexCache_IsNotCopyable) {
    EXPECT_FALSE(std::is_copy_constructible<IndexCache>::value);
    EXPECT_FALSE(std::is_copy_assignable<IndexCache>::value);
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
