/*
 * s3fs - FUSE-based file system backed by Amazon S3
 *
 * fdcache linkage unit tests
 *
 * Tests fdcache.cpp utility functions that can be tested without complex mocking.
 * Links against the actual fdcache.o to get proper coverage.
 */

#include "gtest.h"
#include "common.h"
#include "fdcache.h"
#include <string>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>

using namespace std;

// Stubs for external dependencies
// test_stubs.cpp provides: bucket, debug_level, debug_log_mode, s3fs_log_nest, IndexLogEntry, s3fsStatisLogModeNotObsfs

extern "C" {
// Forward declaration for IndexLogEntry (provided by test_stubs.cpp and obsfs_log.o)
void IndexLogEntry(const uint64_t, const char*, const uint32_t,
                   const char*, const char*, const uint32_t,
                   const char*, ...);
}
// Forward declaration for s3fsStatisLogModeNotObsfs (provided by test_stubs.cpp)
void s3fsStatisLogModeNotObsfs();

// Stub for FdManager cache_dir (needed by CacheFileStat)
// FdManager is defined in fdcache.h, we just need to provide the static member initialization
// This will be provided by test_stubs.o

// Test fixture for fdcache tests
class FdcacheLinkageTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Cache directory is set by test_stubs.o
    }

    void TearDown() override {
        // Clean up
    }
};

// ============================================================
// Tests for CacheFileStat class
// ============================================================

TEST_F(FdcacheLinkageTest, CacheFileStat_DefaultConstructor) {
    CacheFileStat stat;

    // Default constructor creates stat with fd = -1 (not open)
    EXPECT_EQ(stat.GetFd(), -1);
}

TEST_F(FdcacheLinkageTest, CacheFileStat_ConstructorWithPath) {
    CacheFileStat stat("/test/path");

    // Constructor with path should set the path
    // Note: GetFd will be -1 since we didn't create actual files
    EXPECT_EQ(stat.GetFd(), -1);
}

TEST_F(FdcacheLinkageTest, CacheFileStat_ConstructorWithNullPath) {
    CacheFileStat stat(nullptr);

    EXPECT_EQ(stat.GetFd(), -1);
}

TEST_F(FdcacheLinkageTest, CacheFileStat_SetPath_ValidPath) {
    CacheFileStat stat;

    bool success = stat.SetPath("/new/path", false);

    // SetPath should succeed (we pass is_open=false to avoid actual file operations)
    EXPECT_TRUE(success);
    EXPECT_EQ(stat.GetFd(), -1);
}

TEST_F(FdcacheLinkageTest, CacheFileStat_SetPath_NullPath) {
    CacheFileStat stat("/original/path");

    bool success = stat.SetPath(nullptr, false);

    // Setting null path should fail
    EXPECT_FALSE(success);
}

TEST_F(FdcacheLinkageTest, CacheFileStat_SetPath_EmptyPath) {
    CacheFileStat stat;

    bool success = stat.SetPath("", false);

    // Setting empty path should fail
    EXPECT_FALSE(success);
}

TEST_F(FdcacheLinkageTest, CacheFileStat_Release) {
    CacheFileStat stat("/test/path");

    // Release should succeed even if not open
    bool success = stat.Release();

    EXPECT_TRUE(success);
    EXPECT_EQ(stat.GetFd(), -1);
}

// ============================================================
// Tests for fdpage struct (actual linkage test)
// ============================================================

TEST_F(FdcacheLinkageTest, FdPage_StructAlignment) {
    // Test that fdpage struct has correct layout
    fdpage page;

    // Verify default values
    EXPECT_EQ(page.offset, 0);
    EXPECT_EQ(page.bytes, static_cast<size_t>(0));
    EXPECT_FALSE(page.loaded);
}

TEST_F(FdcacheLinkageTest, FdPage_NextEndMethods) {
    fdpage page(100, 500);

    // Test next() method
    EXPECT_EQ(page.next(), 600);

    // Test end() method
    EXPECT_EQ(page.end(), 599);
}

TEST_F(FdcacheLinkageTest, FdPage_ZeroBytes) {
    fdpage page(100, 0);

    EXPECT_EQ(page.offset, 100);
    EXPECT_EQ(page.bytes, static_cast<size_t>(0));
    EXPECT_EQ(page.next(), 100);
    EXPECT_EQ(page.end(), 0);
}

// ============================================================
// Tests for PageList class methods
// ============================================================

TEST_F(FdcacheLinkageTest, PageList_DefaultConstructor) {
    PageList plist;

    // Default constructor should create empty list
    EXPECT_EQ(plist.Size(), static_cast<size_t>(0));
}

TEST_F(FdcacheLinkageTest, PageList_ConstructorWithSize) {
    PageList plist(1024);

    // Constructor with size should initialize list
    EXPECT_GT(plist.Size(), static_cast<size_t>(0));
}

TEST_F(FdcacheLinkageTest, PageList_ConstructorWithSizeAndLoaded) {
    PageList plist(1024, true);

    // Constructor with size and loaded flag
    EXPECT_GT(plist.Size(), static_cast<size_t>(0));
}

TEST_F(FdcacheLinkageTest, PageList_Init) {
    PageList plist;

    bool success = plist.Init(2048, false);

    EXPECT_TRUE(success);
    EXPECT_GT(plist.Size(), static_cast<size_t>(0));
}

TEST_F(FdcacheLinkageTest, PageList_InitZeroSize) {
    PageList plist(100);

    bool success = plist.Init(0, false);

    EXPECT_TRUE(success);
}

TEST_F(FdcacheLinkageTest, PageList_SizeAfterInit) {
    PageList plist;

    plist.Init(4096, false);
    size_t size = plist.Size();

    // Size should be consistent with initialization
    EXPECT_GT(size, static_cast<size_t>(0));
}

TEST_F(FdcacheLinkageTest, PageList_IsPageLoaded_EmptyList) {
    PageList plist;

    // Empty list should return false for page loaded
    bool is_loaded = plist.IsPageLoaded(0, 100);

    EXPECT_FALSE(is_loaded);
}

TEST_F(FdcacheLinkageTest, PageList_IsPageLoaded_WithInit) {
    PageList plist(1024, true);  // Initialize with loaded=true

    // Check if page is loaded at various positions
    bool is_loaded = plist.IsPageLoaded(0, 100);

    // With loaded=true, pages should be loaded
    EXPECT_TRUE(is_loaded);
}

TEST_F(FdcacheLinkageTest, PageList_IsPageLoaded_NotLoaded) {
    PageList plist(1024, false);  // Initialize with loaded=false

    // Check if page is loaded
    bool is_loaded = plist.IsPageLoaded(0, 100);

    // With loaded=false, pages should not be loaded
    EXPECT_FALSE(is_loaded);
}

TEST_F(FdcacheLinkageTest, PageList_SetPageLoadedStatus) {
    PageList plist(1024, false);

    // Set page as loaded
    bool success = plist.SetPageLoadedStatus(0, 100, true, false);

    EXPECT_TRUE(success);
}

TEST_F(FdcacheLinkageTest, PageList_SetPageLoadedStatus_MultiplePages) {
    PageList plist(4096, false);

    // Set multiple pages as loaded
    bool success1 = plist.SetPageLoadedStatus(0, 100, true, false);
    bool success2 = plist.SetPageLoadedStatus(100, 100, true, false);
    bool success3 = plist.SetPageLoadedStatus(200, 100, true, false);

    EXPECT_TRUE(success1);
    EXPECT_TRUE(success2);
    EXPECT_TRUE(success3);
}

TEST_F(FdcacheLinkageTest, PageList_SetPageLoadedStatus_WithCompress) {
    PageList plist(1024, false);

    // Set page as loaded with compress
    bool success = plist.SetPageLoadedStatus(0, 100, true, true);

    EXPECT_TRUE(success);
}

TEST_F(FdcacheLinkageTest, PageList_Resize) {
    PageList plist(1024);

    // Resize to larger size
    bool success = plist.Resize(2048, false);

    EXPECT_TRUE(success);
}

TEST_F(FdcacheLinkageTest, PageList_ResizeToSmaller) {
    PageList plist(2048);

    // Resize to smaller size
    bool success = plist.Resize(1024, false);

    EXPECT_TRUE(success);
}

TEST_F(FdcacheLinkageTest, PageList_ResizeToZero) {
    PageList plist(1024);

    // Resize to zero
    bool success = plist.Resize(0, false);

    EXPECT_TRUE(success);
}

TEST_F(FdcacheLinkageTest, PageList_FindUnloadedPage_EmptyList) {
    PageList plist;
    off_t start = 0;
    size_t size = 0;

    // Find unloaded page in default-constructed list
    // Note: PageList(0, false) creates a page with 0 bytes that is not loaded
    // So FindUnloadedPage will find this unloaded page
    bool found = plist.FindUnloadedPage(0, start, size);

    // The list has one page (0 bytes, not loaded), so it should be found
    EXPECT_TRUE(found);
    EXPECT_EQ(start, 0);
    EXPECT_EQ(size, 0);
}

TEST_F(FdcacheLinkageTest, PageList_GetTotalUnloadedPageSize_EmptyList) {
    PageList plist;

    // Get total unloaded page size from empty list
    size_t size = plist.GetTotalUnloadedPageSize(0, 100);

    EXPECT_EQ(size, static_cast<size_t>(0));
}

TEST_F(FdcacheLinkageTest, PageList_Dump) {
    PageList plist(1024);

    // Dump should not crash
    plist.Dump();

    // If we get here, the test passed
    SUCCEED();
}

// ============================================================
// Main function
// ============================================================

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: noet sw=4 ts=4 fdm=marker
* vim<600: noet sw=4 ts=4
*/
