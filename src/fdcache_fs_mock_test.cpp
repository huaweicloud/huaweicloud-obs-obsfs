// fdcache_fs_mock_test.cpp
// Tests fdcache.cpp file system error paths using fs_mock infrastructure
//
// IMPORTANT: NO MODIFICATION to fdcache.cpp required.
// Tests link against existing fdcache.o and use --wrap flags for interception.
//
// Quality Standards:
// - Each test has meaningful assertions
// - No duplicate tests with different errno (same code path)
// - Only realistic error scenarios

#include "gtest.h"
#include "fs_mock.h"
#include "common.h"
#include "fdcache.h"
#include <string>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>

using namespace std;

// External stubs (provided by test_stubs.o)
extern "C" {
void IndexLogEntry(const uint64_t, const char*, const uint32_t,
                   const char*, const char*, const uint32_t,
                   const char*, ...);
}
void s3fsStatisLogModeNotObsfs();

// ============================================================
// Test Fixtures
// ============================================================

class CacheFileStatFsMockTest : public ::testing::Test {
protected:
    ScopedFsMock scoped_mock;
    string tmpdir;

    void SetUp() override {
        tmpdir = "/tmp/fdcache_fsmock_" + to_string(getpid());
        mkdir(tmpdir.c_str(), 0755);
        FdManager::SetCacheDir(tmpdir.c_str());
    }
    void TearDown() override {
        FdManager::SetCacheDir("");
        FsMockController::Instance().SetMockEnabled(false);
        string cmd = "rm -rf " + tmpdir;
        (void)system(cmd.c_str());
    }
};

// ============================================================
// CacheFileStat Open Error Tests (representative errno only)
// ============================================================

// Purpose: Verify CacheFileStat handles open() failure with ENOSPC
// Expected: GetFd() returns -1
// Why: Disk full is a real production scenario
TEST_F(CacheFileStatFsMockTest, OpenFailureEnospcReturnsInvalidFd) {
    FsMockController::Instance().SetOpenResult(-1, ENOSPC);
    CacheFileStat stat("/test/open_enospc");
    EXPECT_EQ(stat.GetFd(), -1);
}

// Purpose: Verify CacheFileStat handles open() failure with EACCES
// Expected: GetFd() returns -1
// Why: Permission denied is common in multi-user environments
TEST_F(CacheFileStatFsMockTest, OpenFailureEaccesReturnsInvalidFd) {
    FsMockController::Instance().SetOpenResult(-1, EACCES);
    CacheFileStat stat("/test/open_eacces");
    EXPECT_EQ(stat.GetFd(), -1);
}

// Purpose: Verify CacheFileStat handles open() failure with ENOENT
// Expected: GetFd() returns -1
// Why: Cache directory missing indicates configuration error
TEST_F(CacheFileStatFsMockTest, OpenFailureEnoentReturnsInvalidFd) {
    FsMockController::Instance().SetOpenResult(-1, ENOENT);
    CacheFileStat stat("/test/open_enoent");
    EXPECT_EQ(stat.GetFd(), -1);
}

// ============================================================
// CacheFileStat Delete Error Tests
// ============================================================

// Purpose: Verify DeleteCacheFileStat handles unlink() failure
// Expected: Returns false when unlink fails
// Why: File system errors during cleanup
TEST_F(CacheFileStatFsMockTest, DeleteCacheFileStatUnlinkFailureReturnsFalse) {
    // Create stat file first (with mock disabled)
    FsMockController::Instance().SetMockEnabled(false);
    {
        CacheFileStat stat("/test/unlink_fail");
    }
    FsMockController::Instance().SetMockEnabled(true);
    FsMockController::Instance().SetUnlinkResult(-1, EACCES);
    
    bool result = CacheFileStat::DeleteCacheFileStat("/test/unlink_fail");
    EXPECT_FALSE(result);
}

// Purpose: Verify DeleteCacheFileStat success path
// Expected: Returns true when unlink succeeds
// Why: Happy path verification
TEST_F(CacheFileStatFsMockTest, DeleteCacheFileStatSuccessReturnsTrue) {
    // Create stat file (with mock disabled)
    FsMockController::Instance().SetMockEnabled(false);
    {
        CacheFileStat stat("/test/delete_ok");
    }
    FsMockController::Instance().SetMockEnabled(true);
    FsMockController::Instance().SetUnlinkResult(0, 0);
    
    bool result = CacheFileStat::DeleteCacheFileStat("/test/delete_ok");
    EXPECT_TRUE(result);
}

// ============================================================
// CacheFileStat Rename Error Tests
// ============================================================

// Purpose: Verify RenameCacheFileStat handles link() failure
// Expected: Returns false when link fails
// Why: Disk full or permission error during rename
TEST_F(CacheFileStatFsMockTest, RenameCacheFileStatLinkFailureReturnsFalse) {
    // Create source stat file (with mock disabled)
    FsMockController::Instance().SetMockEnabled(false);
    {
        CacheFileStat stat("/test/rename_src");
    }
    FsMockController::Instance().SetMockEnabled(true);

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = S_IFREG | 0644;
    FsMockController::Instance().SetStatResult(0, st, 0);
    FsMockController::Instance().SetStatResult(0, st, 0);
    FsMockController::Instance().SetLinkResult(-1, ENOSPC);

    bool result = CacheFileStat::RenameCacheFileStat("/test/rename_src", "/test/rename_dst");
    EXPECT_FALSE(result);
}

// Purpose: Verify RenameCacheFileStat returns true when source doesn't exist
// Expected: Returns true (nothing to rename)
// Why: Idempotent behavior - rename of non-existent file is success
TEST_F(CacheFileStatFsMockTest, RenameCacheFileStatSourceNotExistReturnsTrue) {
    struct stat st;
    FsMockController::Instance().SetStatResult(-1, st, ENOENT);
    FsMockController::Instance().SetStatResult(-1, st, ENOENT);

    bool result = CacheFileStat::RenameCacheFileStat("/test/noexist", "/test/newpath");
    EXPECT_TRUE(result);
}

// Purpose: Verify RenameCacheFileStat success path
// Expected: Returns true when all operations succeed
// Why: Happy path verification
TEST_F(CacheFileStatFsMockTest, RenameCacheFileStatSuccessReturnsTrue) {
    // Create source stat file (with mock disabled)
    FsMockController::Instance().SetMockEnabled(false);
    {
        CacheFileStat stat("/test/rename_ok");
    }
    FsMockController::Instance().SetMockEnabled(true);

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = S_IFREG | 0644;
    FsMockController::Instance().SetStatResult(0, st, 0);
    FsMockController::Instance().SetStatResult(0, st, 0);
    FsMockController::Instance().SetLinkResult(0, 0);
    FsMockController::Instance().SetUnlinkResult(0, 0);

    bool result = CacheFileStat::RenameCacheFileStat("/test/rename_ok", "/test/rename_new");
    EXPECT_TRUE(result);
}

// Purpose: Verify RenameCacheFileStat handles unlink of old destination failure
// Expected: Returns false when unlink fails
// Why: Error during cleanup of old destination file
TEST_F(CacheFileStatFsMockTest, RenameCacheFileStatUnlinkOldFailReturnsFalse) {
    // Create source and destination stat files (with mock disabled)
    FsMockController::Instance().SetMockEnabled(false);
    {
        CacheFileStat stat_src("/test/rename_src2");
        CacheFileStat stat_dst("/test/rename_dst2");
    }
    FsMockController::Instance().SetMockEnabled(true);

    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = S_IFREG | 0644;
    FsMockController::Instance().SetStatResult(0, st, 0);
    FsMockController::Instance().SetStatResult(0, st, 0);
    FsMockController::Instance().SetStatResult(0, st, 0);
    FsMockController::Instance().SetLinkResult(0, 0);
    FsMockController::Instance().SetUnlinkResult(-1, EACCES);

    bool result = CacheFileStat::RenameCacheFileStat("/test/rename_src2", "/test/rename_dst2");
    EXPECT_FALSE(result);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
