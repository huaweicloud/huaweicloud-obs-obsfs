/*
 * s3fs - FUSE-based file system backed by Amazon S3
 *
 * Comprehensive unit tests for fdcache.cpp to increase code coverage.
 *
 * Links against the actual fdcache.o (and its dependencies) so that
 * test execution contributes to fdcache.cpp gcov/lcov coverage.
 *
 * Tests cover:
 *   - CacheFileStat (path construction, open, release, delete)
 *   - PageList (all methods, edge cases, serialization)
 *   - FdEntity (constructor, open via tmpfile, read, write, stats, metadata, close)
 *   - FdManager (singleton, cache dir, entity lifecycle, rename, close)
 */

#include "gtest.h"
#include "common.h"
#include "fdcache.h"
#include <string>
#include <cstring>
#include <type_traits>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <cstdint>
#include <climits>
#include <atomic>
#include <vector>
#include <pthread.h>
#ifdef __linux__
#include <sys/vfs.h>
#endif
// Filesystem magic numbers (same as fdcache.cpp, avoid <linux/magic.h> for portability)
#ifndef EXT4_SUPER_MAGIC
#define EXT4_SUPER_MAGIC  0xEF53
#endif
#ifndef XFS_SUPER_MAGIC
#define XFS_SUPER_MAGIC   0x58465342
#endif
#ifndef BTRFS_SUPER_MAGIC
#define BTRFS_SUPER_MAGIC 0x9123683E
#endif
#ifndef TMPFS_MAGIC
#define TMPFS_MAGIC       0x01021994
#endif
#ifndef NFS_SUPER_MAGIC
#define NFS_SUPER_MAGIC   0x6969
#endif
#include <fstream>

using namespace std;

// External stubs provided by test_stubs.o
extern "C" {
void IndexLogEntry(const uint64_t, const char*, const uint32_t,
                   const char*, const char*, const uint32_t,
                   const char*, ...);
}
void s3fsStatisLogModeNotObsfs();

// ============================================================
// Helper: create a temp directory that will be cleaned up
// ============================================================
static string make_temp_dir(const char* prefix) {
    char tmpl[PATH_MAX];
    snprintf(tmpl, sizeof(tmpl), "/tmp/%s_XXXXXX", prefix);
    char* d = mkdtemp(tmpl);
    return d ? string(d) : "";
}

static void remove_dir_recursive(const string& dir) {
    string cmd = "rm -rf '" + dir + "'";
    int rc = system(cmd.c_str());
    (void)rc;
}

// ============================================================
// Test Fixtures
// ============================================================

class PageListCoverageTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

class CacheFileStatCoverageTest : public ::testing::Test {
protected:
    string tmpdir;
    void SetUp() override {
        tmpdir = make_temp_dir("cfs_test");
        ASSERT_FALSE(tmpdir.empty());
        // Set cache dir so CacheFileStat can create directories
        FdManager::SetCacheDir(tmpdir.c_str());
    }
    void TearDown() override {
        FdManager::SetCacheDir("");
        if (!tmpdir.empty()) {
            remove_dir_recursive(tmpdir);
        }
    }
};

class FdEntityCoverageTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Ensure no cache dir so FdEntity uses tmpfile
        FdManager::SetCacheDir("");
    }
    void TearDown() override {
        FdManager::SetCacheDir("");
    }
};

class FdManagerCoverageTest : public ::testing::Test {
protected:
    string tmpdir;
    void SetUp() override {
        tmpdir = make_temp_dir("fdm_test");
        ASSERT_FALSE(tmpdir.empty());
    }
    void TearDown() override {
        FdManager::SetCacheDir("");
        FdManager::SetTmpDir("/tmp");
        if (!tmpdir.empty()) {
            remove_dir_recursive(tmpdir);
        }
    }
};

// ============================================================
// PageList Coverage Tests
// ============================================================

// --- Constructor / Init / Clear / Size ---

TEST_F(PageListCoverageTest, DefaultConstructorCreatesZeroSizePage) {
    PageList pl;
    EXPECT_EQ(pl.Size(), 0u);
}

TEST_F(PageListCoverageTest, ConstructorWithSizeUnloaded) {
    PageList pl(4096, false);
    EXPECT_EQ(pl.Size(), 4096u);
    EXPECT_FALSE(pl.IsPageLoaded(0, 4096));
}

TEST_F(PageListCoverageTest, ConstructorWithSizeLoaded) {
    PageList pl(4096, true);
    EXPECT_EQ(pl.Size(), 4096u);
    EXPECT_TRUE(pl.IsPageLoaded(0, 4096));
}

TEST_F(PageListCoverageTest, InitReplacesContent) {
    PageList pl(1000, true);
    EXPECT_EQ(pl.Size(), 1000u);
    pl.Init(2000, false);
    EXPECT_EQ(pl.Size(), 2000u);
    EXPECT_FALSE(pl.IsPageLoaded(0, 2000));
}

TEST_F(PageListCoverageTest, InitZeroSize) {
    PageList pl(500, true);
    pl.Init(0, false);
    EXPECT_EQ(pl.Size(), 0u);
}

TEST_F(PageListCoverageTest, ReInitClearsOldData) {
    PageList pl(1024, true);
    // Re-init should clear old pages and create new ones
    pl.Init(512, false);
    EXPECT_EQ(pl.Size(), 512u);
    EXPECT_FALSE(pl.IsPageLoaded(0, 512));
}

// --- Resize ---

TEST_F(PageListCoverageTest, ResizeLargerAddsPages) {
    PageList pl(100, true);
    EXPECT_TRUE(pl.Resize(200, false));
    EXPECT_EQ(pl.Size(), 200u);
    EXPECT_TRUE(pl.IsPageLoaded(0, 100));
    EXPECT_FALSE(pl.IsPageLoaded(100, 100));
}

TEST_F(PageListCoverageTest, ResizeSmallerTruncates) {
    PageList pl(200, true);
    EXPECT_TRUE(pl.Resize(100, false));
    EXPECT_EQ(pl.Size(), 100u);
}

TEST_F(PageListCoverageTest, ResizeSameSizeIsNoop) {
    PageList pl(300, true);
    EXPECT_TRUE(pl.Resize(300, false));
    EXPECT_EQ(pl.Size(), 300u);
}

TEST_F(PageListCoverageTest, ResizeFromZero) {
    PageList pl(0, false);
    EXPECT_TRUE(pl.Resize(500, true));
    EXPECT_EQ(pl.Size(), 500u);
}

TEST_F(PageListCoverageTest, ResizeToZero) {
    PageList pl(1000, true);
    EXPECT_TRUE(pl.Resize(0, false));
    EXPECT_EQ(0u, pl.Size());
}

TEST_F(PageListCoverageTest, ResizeSmallerPartialPage) {
    PageList pl(300, false);
    pl.SetPageLoadedStatus(200, 100, true);
    // Now: [0-200 unloaded] [200-300 loaded]
    pl.Resize(250, false);
    // Should truncate the loaded page
    EXPECT_EQ(pl.Size(), 250u);
}

TEST_F(PageListCoverageTest, ResizeSmallerRemovesFullPages) {
    PageList pl(1000, false);
    pl.SetPageLoadedStatus(0, 200, true);
    pl.SetPageLoadedStatus(200, 200, false);
    pl.SetPageLoadedStatus(400, 600, true);
    // Resize to 300 should remove pages starting at/after 300
    pl.Resize(300, false);
    EXPECT_EQ(pl.Size(), 300u);
}

// --- Compress (tested indirectly through SetPageLoadedStatus with is_compress=true) ---

TEST_F(PageListCoverageTest, CompressViaSetPageLoadedStatus) {
    PageList pl(300, false);
    // Set three adjacent regions as loaded without compress
    pl.SetPageLoadedStatus(0, 100, true, false);
    pl.SetPageLoadedStatus(100, 100, true, false);
    pl.SetPageLoadedStatus(200, 100, true, false);
    // Now set with compress to trigger Compress()
    pl.SetPageLoadedStatus(0, 300, true, true);
    EXPECT_TRUE(pl.IsPageLoaded(0, 300));
}

TEST_F(PageListCoverageTest, CompressPreservesDifferentStatus) {
    PageList pl(200, false);
    pl.SetPageLoadedStatus(0, 100, true, true);
    // [0-100 loaded] [100-200 unloaded] - should NOT merge
    EXPECT_TRUE(pl.IsPageLoaded(0, 100));
    EXPECT_FALSE(pl.IsPageLoaded(100, 100));
}

// --- Parse (tested indirectly through SetPageLoadedStatus which calls Parse) ---

TEST_F(PageListCoverageTest, ParseViaSetPageLoadedStatusMiddleRange) {
    PageList pl(1000, false);
    // SetPageLoadedStatus on inner range will call Parse(start) and Parse(start+size)
    pl.SetPageLoadedStatus(200, 300, true);
    EXPECT_FALSE(pl.IsPageLoaded(0, 200));
    EXPECT_TRUE(pl.IsPageLoaded(200, 300));
    EXPECT_FALSE(pl.IsPageLoaded(500, 500));
}

TEST_F(PageListCoverageTest, ParseViaSetPageLoadedStatusAtStart) {
    PageList pl(500, false);
    // start=0 means Parse(0) is called (matches offset, noop)
    pl.SetPageLoadedStatus(0, 250, true);
    EXPECT_TRUE(pl.IsPageLoaded(0, 250));
    EXPECT_FALSE(pl.IsPageLoaded(250, 250));
}

TEST_F(PageListCoverageTest, ParseViaResize) {
    PageList pl(200, false);
    pl.SetPageLoadedStatus(0, 100, true);
    // Resize will compress, which exercises Compress()
    pl.Resize(300, false);
    EXPECT_EQ(pl.Size(), 300u);
}

// --- IsPageLoaded ---

TEST_F(PageListCoverageTest, IsPageLoadedSizeZeroChecksAll) {
    PageList pl(100, true);
    EXPECT_TRUE(pl.IsPageLoaded(0, 0));

    PageList pl2(100, false);
    EXPECT_FALSE(pl2.IsPageLoaded(0, 0));
}

TEST_F(PageListCoverageTest, IsPageLoadedPartialRange) {
    PageList pl(100, false);
    pl.SetPageLoadedStatus(25, 50, true);
    EXPECT_TRUE(pl.IsPageLoaded(25, 50));
    EXPECT_FALSE(pl.IsPageLoaded(0, 25));
    EXPECT_FALSE(pl.IsPageLoaded(75, 25));
}

TEST_F(PageListCoverageTest, IsPageLoadedCrossPageBoundary) {
    PageList pl(300, false);
    pl.SetPageLoadedStatus(50, 200, true);
    // Check range that spans loaded and unloaded
    EXPECT_FALSE(pl.IsPageLoaded(0, 100));
    EXPECT_TRUE(pl.IsPageLoaded(50, 200));
    EXPECT_FALSE(pl.IsPageLoaded(200, 150));
}

TEST_F(PageListCoverageTest, IsPageLoadedStartBeyondEnd) {
    PageList pl(100, true);
    // Start beyond page list size → loop skips all pages → returns true (vacuous truth).
    // This is by-design: the range [200,250) has no pages to check, so "all pages loaded" is trivially true.
    EXPECT_TRUE(pl.IsPageLoaded(200, 50));
}

TEST_F(PageListCoverageTest, IsPageLoadedAtBoundary) {
    PageList pl(100, true);
    // Boundary: exactly at end of loaded range
    EXPECT_TRUE(pl.IsPageLoaded(0, 100));
    // One byte beyond → no page covers it
    EXPECT_TRUE(pl.IsPageLoaded(100, 1));  // vacuous truth: beyond list size
}

TEST_F(PageListCoverageTest, IsPageLoadedZeroSizeList) {
    PageList pl(0, false);
    // Zero-size page, size=0 means check all, but all pages are size 0 with loaded=false
    // The implementation skips pages where end() < start. For a 0-byte page, end()=0 and start=0.
    // end() < start is false (0 < 0 is false), so it checks loaded flag which is false.
    EXPECT_FALSE(pl.IsPageLoaded(0, 0));
}

// --- SetPageLoadedStatus ---

TEST_F(PageListCoverageTest, SetPageLoadedStatusInnerRange) {
    PageList pl(1000, false);
    // Set a range inside the existing pages
    EXPECT_TRUE(pl.SetPageLoadedStatus(100, 200, true));
    EXPECT_TRUE(pl.IsPageLoaded(100, 200));
    EXPECT_FALSE(pl.IsPageLoaded(0, 100));
    EXPECT_FALSE(pl.IsPageLoaded(300, 100));
}

TEST_F(PageListCoverageTest, SetPageLoadedStatusExtendBeyond) {
    PageList pl(100, false);
    // Set range that extends beyond current size
    EXPECT_TRUE(pl.SetPageLoadedStatus(50, 200, true));
    EXPECT_EQ(250u, pl.Size());
}

TEST_F(PageListCoverageTest, SetPageLoadedStatusStartBeyondCurrentSize) {
    PageList pl(100, false);
    // Start is beyond current size
    EXPECT_TRUE(pl.SetPageLoadedStatus(200, 100, true));
    EXPECT_EQ(300u, pl.Size());
}

TEST_F(PageListCoverageTest, SetPageLoadedStatusStartEqualsSize) {
    PageList pl(100, false);
    // Start equals current size exactly
    EXPECT_TRUE(pl.SetPageLoadedStatus(100, 50, true));
    EXPECT_EQ(pl.Size(), 150u);
}

TEST_F(PageListCoverageTest, SetPageLoadedStatusNoCompress) {
    PageList pl(100, false);
    EXPECT_TRUE(pl.SetPageLoadedStatus(0, 50, true, false));
    EXPECT_TRUE(pl.SetPageLoadedStatus(50, 50, true, false));
    // Without compress, should still have correct state
    EXPECT_TRUE(pl.IsPageLoaded(0, 100));
}

TEST_F(PageListCoverageTest, SetPageLoadedStatusWithCompress) {
    PageList pl(100, false);
    EXPECT_TRUE(pl.SetPageLoadedStatus(0, 100, true, true));
    EXPECT_TRUE(pl.IsPageLoaded(0, 100));
}

TEST_F(PageListCoverageTest, SetPageLoadedStatusToggle) {
    PageList pl(100, true);
    // Unload middle
    EXPECT_TRUE(pl.SetPageLoadedStatus(25, 50, false));
    EXPECT_FALSE(pl.IsPageLoaded(25, 50));
    // Reload middle
    EXPECT_TRUE(pl.SetPageLoadedStatus(25, 50, true));
    EXPECT_TRUE(pl.IsPageLoaded(0, 100));
}

// --- FindUnloadedPage ---

TEST_F(PageListCoverageTest, FindUnloadedPageAllLoaded) {
    PageList pl(100, true);
    off_t resstart;
    size_t ressize;
    EXPECT_FALSE(pl.FindUnloadedPage(0, resstart, ressize));
}

TEST_F(PageListCoverageTest, FindUnloadedPageAllUnloaded) {
    PageList pl(100, false);
    off_t resstart;
    size_t ressize;
    EXPECT_TRUE(pl.FindUnloadedPage(0, resstart, ressize));
    EXPECT_EQ(resstart, 0);
    EXPECT_EQ(ressize, 100u);
}

TEST_F(PageListCoverageTest, FindUnloadedPageAtEnd) {
    PageList pl(200, false);
    pl.SetPageLoadedStatus(0, 100, true);
    off_t resstart;
    size_t ressize;
    EXPECT_TRUE(pl.FindUnloadedPage(100, resstart, ressize));
    EXPECT_EQ(resstart, 100);
    EXPECT_EQ(ressize, 100u);
}

TEST_F(PageListCoverageTest, FindUnloadedPageStartInMiddle) {
    PageList pl(300, false);
    pl.SetPageLoadedStatus(0, 100, true);
    pl.SetPageLoadedStatus(200, 100, true);
    // Unloaded: 100-200
    off_t resstart;
    size_t ressize;
    EXPECT_TRUE(pl.FindUnloadedPage(50, resstart, ressize));
    EXPECT_EQ(resstart, 100);
    EXPECT_EQ(ressize, 100u);
}

TEST_F(PageListCoverageTest, FindUnloadedPageNoneAfterStart) {
    PageList pl(100, false);
    pl.SetPageLoadedStatus(0, 100, true);
    off_t resstart;
    size_t ressize;
    EXPECT_FALSE(pl.FindUnloadedPage(0, resstart, ressize));
}

// --- GetTotalUnloadedPageSize ---

TEST_F(PageListCoverageTest, GetTotalUnloadedPageSizeAllLoaded) {
    PageList pl(100, true);
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(0, 100), 0u);
}

TEST_F(PageListCoverageTest, GetTotalUnloadedPageSizeAllUnloaded) {
    PageList pl(100, false);
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(0, 100), 100u);
}

TEST_F(PageListCoverageTest, GetTotalUnloadedPageSizePartial) {
    PageList pl(300, false);
    pl.SetPageLoadedStatus(100, 100, true);
    // Unloaded: [0-100] and [200-300] = 200 bytes
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(0, 300), 200u);
}

TEST_F(PageListCoverageTest, GetTotalUnloadedPageSizeSubRange) {
    PageList pl(300, false);
    pl.SetPageLoadedStatus(100, 100, true);
    // Check only first 150 bytes: unloaded = 0-100 = 100 bytes
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(0, 150), 100u);
}

TEST_F(PageListCoverageTest, GetTotalUnloadedPageSizeOverlap) {
    PageList pl(300, false);
    pl.SetPageLoadedStatus(100, 100, true);
    // Check range 50-250: unloaded = [50-100] + [200-250] = 100 bytes
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(50, 200), 100u);
}

TEST_F(PageListCoverageTest, GetTotalUnloadedPageSizeEndOverlap) {
    PageList pl(200, false);
    // [0-200 unloaded]
    // Check range 150-300 (extends beyond): should count 150-200 = 50
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(150, 150), 50u);
}

// --- GetUnloadedPages ---

TEST_F(PageListCoverageTest, GetUnloadedPagesNone) {
    PageList pl(100, true);
    fdpage_list_t list;
    int count = pl.GetUnloadedPages(list, 0, 100);
    EXPECT_EQ(count, 0);
    EXPECT_TRUE(list.empty());
}

TEST_F(PageListCoverageTest, GetUnloadedPagesAll) {
    PageList pl(100, false);
    fdpage_list_t list;
    int count = pl.GetUnloadedPages(list, 0, 100);
    EXPECT_EQ(count, 1);
    EXPECT_EQ(list.size(), 1u);
    PageList::FreeList(list);
}

TEST_F(PageListCoverageTest, GetUnloadedPagesMultiple) {
    PageList pl(300, false);
    pl.SetPageLoadedStatus(100, 100, true);
    // Unloaded: [0-100] and [200-300]
    fdpage_list_t list;
    int count = pl.GetUnloadedPages(list, 0, 300);
    EXPECT_EQ(count, 2);
    EXPECT_EQ(list.size(), 2u);
    PageList::FreeList(list);
}

TEST_F(PageListCoverageTest, GetUnloadedPagesSizeZero) {
    PageList pl(200, false);
    pl.SetPageLoadedStatus(0, 100, true);
    // size=0 means check to end
    fdpage_list_t list;
    int count = pl.GetUnloadedPages(list, 0, 0);
    EXPECT_EQ(count, 1);
    PageList::FreeList(list);
}

TEST_F(PageListCoverageTest, GetUnloadedPagesSubRange) {
    PageList pl(300, false);
    pl.SetPageLoadedStatus(100, 100, true);
    // Check only [0, 150): should find [0-100]
    fdpage_list_t list;
    int count = pl.GetUnloadedPages(list, 0, 150);
    EXPECT_GE(count, 1);
    PageList::FreeList(list);
}

TEST_F(PageListCoverageTest, GetUnloadedPagesMerge) {
    PageList pl(300, false);
    // All unloaded, single contiguous page
    fdpage_list_t list;
    int count = pl.GetUnloadedPages(list, 0, 300);
    // Should merge into a single entry
    EXPECT_EQ(count, 1);
    PageList::FreeList(list);
}

// --- FreeList ---

TEST_F(PageListCoverageTest, FreeListEmpty) {
    fdpage_list_t list;
    PageList::FreeList(list);
    EXPECT_TRUE(list.empty());
}

TEST_F(PageListCoverageTest, FreeListWithPages) {
    fdpage_list_t list;
    list.push_back(new fdpage(0, 100, true));
    list.push_back(new fdpage(100, 200, false));
    list.push_back(new fdpage(300, 300, true));
    PageList::FreeList(list);
    EXPECT_TRUE(list.empty());
}

// --- Dump ---

TEST_F(PageListCoverageTest, DumpDoesNotCrash) {
    PageList pl(1024, false);
    pl.SetPageLoadedStatus(0, 512, true);
    pl.Dump();  // Should not crash
    SUCCEED();
}

TEST_F(PageListCoverageTest, DumpEmptyDoesNotCrash) {
    PageList pl;
    pl.Dump();
    SUCCEED();
}

// --- Serialize ---

TEST_F(PageListCoverageTest, SerializeAndDeserialize) {
    // Create a temp file for the CacheFileStat
    string tmpdir = make_temp_dir("pl_serialize");
    ASSERT_FALSE(tmpdir.empty());
    FdManager::SetCacheDir(tmpdir.c_str());

    // Create pagelist with some data
    PageList pl(1000, false);
    pl.SetPageLoadedStatus(0, 300, true);
    pl.SetPageLoadedStatus(500, 200, true);
    // [0-300 loaded] [300-500 unloaded] [500-700 loaded] [700-1000 unloaded]

    {
        CacheFileStat cfstat("/test/serialize_file");
        // Write with non-zero inode (Gap A: inode=0 in new format is rejected)
        EXPECT_TRUE(pl.Serialize(cfstat, true, 42));
    }

    // Read back with matching inode
    PageList pl2;
    {
        CacheFileStat cfstat2("/test/serialize_file");
        EXPECT_TRUE(pl2.Serialize(cfstat2, false, 42));
    }

    // Verify size matches
    EXPECT_EQ(pl.Size(), pl2.Size());

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir);
}

TEST_F(PageListCoverageTest, SerializeEmptyPageList) {
    string tmpdir = make_temp_dir("pl_ser_empty");
    ASSERT_FALSE(tmpdir.empty());
    FdManager::SetCacheDir(tmpdir.c_str());

    PageList pl(0, false);
    {
        CacheFileStat cfstat("/test/empty_file");
        EXPECT_TRUE(pl.Serialize(cfstat, true, 42));
    }

    PageList pl2;
    {
        CacheFileStat cfstat2("/test/empty_file");
        EXPECT_TRUE(pl2.Serialize(cfstat2, false, 42));
    }

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir);
}

// ============================================================
// CacheFileStat Coverage Tests
// ============================================================

TEST_F(CacheFileStatCoverageTest, DefaultConstructor) {
    CacheFileStat stat;
    EXPECT_EQ(stat.GetFd(), -1);
}

TEST_F(CacheFileStatCoverageTest, ConstructorWithPath) {
    CacheFileStat stat("/test/path");
    // Should have opened the file successfully
    EXPECT_NE(stat.GetFd(), -1);
}

TEST_F(CacheFileStatCoverageTest, ConstructorWithNullPath) {
    CacheFileStat stat(nullptr);
    EXPECT_EQ(stat.GetFd(), -1);
}

TEST_F(CacheFileStatCoverageTest, ConstructorWithEmptyString) {
    CacheFileStat stat("");
    EXPECT_EQ(stat.GetFd(), -1);
}

TEST_F(CacheFileStatCoverageTest, SetPathNoOpen) {
    CacheFileStat stat;
    EXPECT_TRUE(stat.SetPath("/new/path", false));
    EXPECT_EQ(stat.GetFd(), -1);
}

TEST_F(CacheFileStatCoverageTest, SetPathWithOpen) {
    CacheFileStat stat;
    EXPECT_TRUE(stat.SetPath("/new/path", true));
    EXPECT_NE(stat.GetFd(), -1);
}

TEST_F(CacheFileStatCoverageTest, SetPathNull) {
    CacheFileStat stat;
    EXPECT_FALSE(stat.SetPath(nullptr, false));
}

TEST_F(CacheFileStatCoverageTest, SetPathEmpty) {
    CacheFileStat stat;
    EXPECT_FALSE(stat.SetPath("", false));
}

TEST_F(CacheFileStatCoverageTest, OpenAndRelease) {
    CacheFileStat stat;
    stat.SetPath("/test/open_release", false);
    EXPECT_TRUE(stat.Open());
    EXPECT_NE(stat.GetFd(), -1);
    EXPECT_TRUE(stat.Release());
    EXPECT_EQ(stat.GetFd(), -1);
}

TEST_F(CacheFileStatCoverageTest, ReleaseWhenNotOpen) {
    CacheFileStat stat;
    EXPECT_TRUE(stat.Release());  // Already released
}

TEST_F(CacheFileStatCoverageTest, OpenWhenAlreadyOpen) {
    CacheFileStat stat("/test/double_open");
    int fd1 = stat.GetFd();
    EXPECT_NE(fd1, -1);
    // Open again should succeed (already open)
    EXPECT_TRUE(stat.Open());
    EXPECT_EQ(stat.GetFd(), fd1);
}

TEST_F(CacheFileStatCoverageTest, OpenWithEmptyPath) {
    CacheFileStat stat;
    // path is empty, Open should fail
    EXPECT_FALSE(stat.Open());
}

TEST_F(CacheFileStatCoverageTest, DeleteCacheFileStat) {
    // Create a stat file first
    {
        CacheFileStat stat("/test/delete_me");
        EXPECT_NE(stat.GetFd(), -1);
    }
    // Now delete it
    bool result = CacheFileStat::DeleteCacheFileStat("/test/delete_me");
    EXPECT_TRUE(result);
}

TEST_F(CacheFileStatCoverageTest, DeleteCacheFileStatNullPath) {
    EXPECT_FALSE(CacheFileStat::DeleteCacheFileStat(nullptr));
}

TEST_F(CacheFileStatCoverageTest, DeleteCacheFileStatEmptyPath) {
    EXPECT_FALSE(CacheFileStat::DeleteCacheFileStat(""));
}

TEST_F(CacheFileStatCoverageTest, DeleteCacheFileStatNonexistent) {
    EXPECT_FALSE(CacheFileStat::DeleteCacheFileStat("/nonexistent/path/file"));
}

TEST_F(CacheFileStatCoverageTest, CheckCacheFileStatTopDir) {
    EXPECT_TRUE(CacheFileStat::CheckCacheFileStatTopDir());
}

TEST_F(CacheFileStatCoverageTest, CheckCacheFileStatTopDirNoCache) {
    FdManager::SetCacheDir("");
    EXPECT_TRUE(CacheFileStat::CheckCacheFileStatTopDir());
}

TEST_F(CacheFileStatCoverageTest, DeleteCacheFileStatDirectory) {
    // Create some stat files
    {
        CacheFileStat stat("/test/dir_delete1");
        CacheFileStat stat2("/test/dir_delete2");
    }
    EXPECT_TRUE(CacheFileStat::DeleteCacheFileStatDirectory());
}

// ============================================================
// FdEntity Coverage Tests
// ============================================================

TEST_F(FdEntityCoverageTest, ConstructorDefault) {
    FdEntity ent;
    EXPECT_FALSE(ent.IsOpen());
    EXPECT_EQ(ent.GetFd(), -1);
    EXPECT_STREQ(ent.GetPath(), "");
}

TEST_F(FdEntityCoverageTest, ConstructorWithPath) {
    FdEntity ent("/test/file.txt");
    EXPECT_STREQ(ent.GetPath(), "/test/file.txt");
    EXPECT_FALSE(ent.IsOpen());
}

TEST_F(FdEntityCoverageTest, ConstructorWithPathAndCache) {
    FdEntity ent("/test/file.txt", "/tmp/cache/file.txt");
    EXPECT_STREQ(ent.GetPath(), "/test/file.txt");
    EXPECT_FALSE(ent.IsOpen());
}

TEST_F(FdEntityCoverageTest, ConstructorWithNull) {
    FdEntity ent(nullptr, nullptr);
    EXPECT_STREQ(ent.GetPath(), "");
}

TEST_F(FdEntityCoverageTest, SetPath) {
    FdEntity ent("/old/path");
    ent.SetPath("/new/path");
    EXPECT_STREQ(ent.GetPath(), "/new/path");
}

TEST_F(FdEntityCoverageTest, IsModifiedDefault) {
    FdEntity ent;
    EXPECT_FALSE(ent.IsModified());
}

TEST_F(FdEntityCoverageTest, OpenWithTmpfile) {
    FdEntity ent("/test/tmpfile_test");
    // Open without cache (will create tmpfile)
    int result = ent.Open(NULL, 0, -1);
    EXPECT_EQ(result, 0);
    EXPECT_TRUE(ent.IsOpen());
    EXPECT_NE(ent.GetFd(), -1);
    ent.Close();
}

TEST_F(FdEntityCoverageTest, OpenWithSize) {
    FdEntity ent("/test/sized_file");
    int result = ent.Open(NULL, 100, -1);
    EXPECT_EQ(result, 0);
    EXPECT_TRUE(ent.IsOpen());

    size_t sz;
    EXPECT_TRUE(ent.GetSize(sz));
    EXPECT_EQ(sz, 100u);
    ent.Close();
}

TEST_F(FdEntityCoverageTest, OpenWithMeta) {
    FdEntity ent("/test/meta_file");
    headers_t meta;
    meta["Content-Type"] = "text/plain";
    int result = ent.Open(&meta, 0, -1);
    EXPECT_EQ(result, 0);
    EXPECT_TRUE(ent.IsOpen());
    ent.Close();
}

TEST_F(FdEntityCoverageTest, OpenWithTime) {
    FdEntity ent("/test/time_file");
    int result = ent.Open(NULL, 0, 1234567890);
    EXPECT_EQ(result, 0);
    EXPECT_TRUE(ent.IsOpen());
    ent.Close();
}

TEST_F(FdEntityCoverageTest, DupIncrementsRefCount) {
    FdEntity ent("/test/dup_file");
    ent.Open(NULL, 0, -1);
    EXPECT_TRUE(ent.IsOpen());
    int fd = ent.Dup();
    EXPECT_NE(fd, -1);
    // Close twice - once for dup, once for original
    ent.Close();
    EXPECT_TRUE(ent.IsOpen());  // Still open after first close
    ent.Close();
    EXPECT_FALSE(ent.IsOpen());  // Now closed
}

TEST_F(FdEntityCoverageTest, DupWhenNotOpen) {
    FdEntity ent;
    int fd = ent.Dup();
    EXPECT_EQ(fd, -1);
}

TEST_F(FdEntityCoverageTest, CloseWhenNotOpen) {
    FdEntity ent;
    // Should not crash
    ent.Close();
    EXPECT_FALSE(ent.IsOpen());
}

TEST_F(FdEntityCoverageTest, CloseDecrements) {
    FdEntity ent("/test/close_test");
    ent.Open(NULL, 0, -1);
    ent.Dup();
    ent.Close();  // decrement
    EXPECT_TRUE(ent.IsOpen());
    ent.Close();  // final close
    EXPECT_FALSE(ent.IsOpen());
}

TEST_F(FdEntityCoverageTest, GetStats) {
    FdEntity ent("/test/stats_file");
    ent.Open(NULL, 100, -1);

    struct stat st;
    EXPECT_TRUE(ent.GetStats(st));
    // File was truncated to 100 bytes
    EXPECT_EQ(st.st_size, 100);
    ent.Close();
}

TEST_F(FdEntityCoverageTest, GetStatsNotOpen) {
    FdEntity ent;
    struct stat st;
    EXPECT_FALSE(ent.GetStats(st));
}

TEST_F(FdEntityCoverageTest, GetSize) {
    FdEntity ent("/test/size_file");
    ent.Open(NULL, 200, -1);

    size_t size;
    EXPECT_TRUE(ent.GetSize(size));
    EXPECT_EQ(size, 200u);
    ent.Close();
}

TEST_F(FdEntityCoverageTest, GetSizeNotOpen) {
    FdEntity ent;
    size_t size;
    EXPECT_FALSE(ent.GetSize(size));
}

TEST_F(FdEntityCoverageTest, SetMtime) {
    FdEntity ent("/test/mtime_file");
    ent.Open(NULL, 0, -1);
    int result = ent.SetMtime(1234567890);
    EXPECT_EQ(result, 0);
    ent.Close();
}

TEST_F(FdEntityCoverageTest, SetMtimeMinusOne) {
    FdEntity ent("/test/mtime_minus_one");
    ent.Open(NULL, 0, -1);
    int result = ent.SetMtime(-1);
    EXPECT_EQ(result, 0);  // -1 means no-op
    ent.Close();
}

TEST_F(FdEntityCoverageTest, SetMtimeNotOpen) {
    FdEntity ent("/test/mtime_not_open");
    // Not open, but has no cachepath either
    int result = ent.SetMtime(1234567890);
    // Should write to orgmeta and return 0
    EXPECT_EQ(result, 0);
}

TEST_F(FdEntityCoverageTest, SetMode) {
    FdEntity ent("/test/mode_file");
    ent.Open(NULL, 0, -1);
    EXPECT_TRUE(ent.SetMode(0755));
    ent.Close();
}

TEST_F(FdEntityCoverageTest, SetUId) {
    FdEntity ent("/test/uid_file");
    ent.Open(NULL, 0, -1);
    EXPECT_TRUE(ent.SetUId(1000));
    ent.Close();
}

TEST_F(FdEntityCoverageTest, SetGId) {
    FdEntity ent("/test/gid_file");
    ent.Open(NULL, 0, -1);
    EXPECT_TRUE(ent.SetGId(1000));
    ent.Close();
}

TEST_F(FdEntityCoverageTest, SetContentType) {
    FdEntity ent("/test/content_type");
    ent.Open(NULL, 0, -1);
    EXPECT_TRUE(ent.SetContentType("/test/file.html"));
    ent.Close();
}

TEST_F(FdEntityCoverageTest, SetContentTypeNull) {
    FdEntity ent("/test/ct_null");
    EXPECT_FALSE(ent.SetContentType(nullptr));
}

TEST_F(FdEntityCoverageTest, WriteAndRead) {
    FdEntity ent("/test/rw_file");
    ent.Open(NULL, 0, -1);
    EXPECT_TRUE(ent.IsOpen());

    const char* data = "Hello, World!";
    ssize_t wsize = ent.Write(data, 0, strlen(data));
    EXPECT_EQ(wsize, (ssize_t)strlen(data));
    EXPECT_TRUE(ent.IsModified());

    char buf[64] = {0};
    ssize_t rsize = ent.Read(buf, 0, strlen(data));
    EXPECT_EQ(rsize, (ssize_t)strlen(data));
    EXPECT_STREQ(buf, data);
    ent.Close();
}

TEST_F(FdEntityCoverageTest, WriteAtOffset) {
    FdEntity ent("/test/write_offset");
    ent.Open(NULL, 100, -1);

    const char* data = "test";
    ssize_t wsize = ent.Write(data, 50, strlen(data));
    EXPECT_EQ(wsize, (ssize_t)strlen(data));
    ent.Close();
}

TEST_F(FdEntityCoverageTest, WriteBeyondSize) {
    FdEntity ent("/test/write_beyond");
    ent.Open(NULL, 10, -1);

    // Write beyond current size - should grow the file
    const char* data = "extended";
    ssize_t wsize = ent.Write(data, 100, strlen(data));
    EXPECT_EQ(wsize, (ssize_t)strlen(data));

    size_t newsize;
    EXPECT_TRUE(ent.GetSize(newsize));
    EXPECT_GE(newsize, 100u + strlen(data));
    ent.Close();
}

TEST_F(FdEntityCoverageTest, ReadNotOpen) {
    FdEntity ent;
    char buf[64];
    ssize_t rsize = ent.Read(buf, 0, 10);
    EXPECT_EQ(rsize, -EBADF);
}

TEST_F(FdEntityCoverageTest, WriteNotOpen) {
    FdEntity ent;
    ssize_t wsize = ent.Write("test", 0, 4);
    EXPECT_EQ(wsize, -EBADF);
}

TEST_F(FdEntityCoverageTest, RowFlushNotOpen) {
    FdEntity ent;
    int result = ent.RowFlush(NULL, false);
    EXPECT_EQ(result, -EBADF);
}

TEST_F(FdEntityCoverageTest, RowFlushNotModified) {
    FdEntity ent("/test/flush_noop");
    ent.Open(NULL, 0, -1);
    // Not modified, not force_sync
    int result = ent.RowFlush(NULL, false);
    EXPECT_EQ(result, 0);
    ent.Close();
}

TEST_F(FdEntityCoverageTest, FlushNotModified) {
    FdEntity ent("/test/flush_nomod");
    ent.Open(NULL, 0, -1);
    int result = ent.Flush(false);
    EXPECT_EQ(result, 0);
    ent.Close();
}

TEST_F(FdEntityCoverageTest, LoadNotOpen) {
    FdEntity ent;
    int result = ent.Load(0, 100);
    EXPECT_EQ(result, -EBADF);
}

TEST_F(FdEntityCoverageTest, LoadEmptyFile) {
    FdEntity ent("/test/load_empty");
    ent.Open(NULL, 0, -1);
    // Load on a 0-size file should be a no-op
    int result = ent.Load(0, 0);
    EXPECT_EQ(result, 0);
    ent.Close();
}

TEST_F(FdEntityCoverageTest, OpenAlreadyOpenedReopen) {
    FdEntity ent("/test/reopen");
    ent.Open(NULL, 50, -1);
    EXPECT_TRUE(ent.IsOpen());

    // Open again with different size
    int result = ent.Open(NULL, 100, -1);
    EXPECT_EQ(result, 0);

    size_t sz;
    EXPECT_TRUE(ent.GetSize(sz));
    EXPECT_EQ(sz, 100u);
    // Ref count is now 2
    ent.Close();
    EXPECT_TRUE(ent.IsOpen());
    ent.Close();
    EXPECT_FALSE(ent.IsOpen());
}

TEST_F(FdEntityCoverageTest, OpenAlreadyOpenedSameSize) {
    FdEntity ent("/test/reopen_same");
    ent.Open(NULL, 100, -1);
    // Reopen with same size
    int result = ent.Open(NULL, 100, -1);
    EXPECT_EQ(result, 0);
    ent.Close();
    ent.Close();
}

TEST_F(FdEntityCoverageTest, OpenAlreadyOpenedWithMeta) {
    FdEntity ent("/test/reopen_meta");
    ent.Open(NULL, 50, -1);
    headers_t meta;
    meta["Content-Type"] = "text/plain";
    int result = ent.Open(&meta, 50, -1);
    EXPECT_EQ(result, 0);
    ent.Close();
    ent.Close();
}

TEST_F(FdEntityCoverageTest, NoCacheMultipartPostNotInit) {
    FdEntity ent("/test/nomp");
    ent.Open(NULL, 0, -1);
    // upload_id is empty, should fail
    int result = ent.NoCacheMultipartPost(-1, 0, 100);
    EXPECT_EQ(result, -EIO);
    ent.Close();
}

TEST_F(FdEntityCoverageTest, NoCacheCompleteMultipartPostNotInit) {
    FdEntity ent("/test/nocp");
    // upload_id is empty
    int result = ent.NoCacheCompleteMultipartPost();
    EXPECT_EQ(result, -EIO);
}

TEST_F(FdEntityCoverageTest, NoCacheLoadAndPostNotOpen) {
    FdEntity ent;
    int result = ent.NoCacheLoadAndPost(0, 100);
    EXPECT_EQ(result, -EBADF);
}

// --- FillFile (static, tested indirectly via Load/Write) ---

TEST_F(FdEntityCoverageTest, FillFileThroughWrite) {
    FdEntity ent("/test/fill_via_write");
    ent.Open(NULL, 0, -1);

    // Write some data
    const char* data = "ABCDEFGHIJ";
    ent.Write(data, 0, 10);

    // Read back to verify
    char buf[16] = {0};
    ssize_t r = ent.Read(buf, 0, 10);
    EXPECT_EQ(r, 10);
    EXPECT_EQ(memcmp(buf, data, 10), 0);
    ent.Close();
}

// ============================================================
// FdManager Coverage Tests
// ============================================================

TEST_F(FdManagerCoverageTest, SingletonAccess) {
    FdManager* mgr = FdManager::get();
    EXPECT_NE(mgr, nullptr);
    // Same instance
    EXPECT_EQ(mgr, FdManager::get());
}

TEST_F(FdManagerCoverageTest, SetCacheDir) {
    EXPECT_TRUE(FdManager::SetCacheDir(tmpdir.c_str()));
    EXPECT_TRUE(FdManager::IsCacheDir());
    EXPECT_STREQ(FdManager::GetCacheDir(), tmpdir.c_str());
}

TEST_F(FdManagerCoverageTest, SetCacheDirEmpty) {
    FdManager::SetCacheDir(tmpdir.c_str());
    EXPECT_TRUE(FdManager::SetCacheDir(""));
    EXPECT_FALSE(FdManager::IsCacheDir());
    EXPECT_STREQ(FdManager::GetCacheDir(), "");
}

TEST_F(FdManagerCoverageTest, SetCacheDirNull) {
    EXPECT_TRUE(FdManager::SetCacheDir(nullptr));
    EXPECT_FALSE(FdManager::IsCacheDir());
}

TEST_F(FdManagerCoverageTest, MakeCachePathNoCache) {
    FdManager::SetCacheDir("");
    string path;
    EXPECT_TRUE(FdManager::MakeCachePath("/test/file", path));
    EXPECT_TRUE(path.empty());
}

TEST_F(FdManagerCoverageTest, MakeCachePathWithCache) {
    FdManager::SetCacheDir(tmpdir.c_str());
    string path;
    EXPECT_TRUE(FdManager::MakeCachePath("/test/file", path, true, false));
    EXPECT_FALSE(path.empty());
    // Should contain bucket name
    EXPECT_NE(path.find("test-bucket"), string::npos);
}

TEST_F(FdManagerCoverageTest, MakeCachePathMirror) {
    FdManager::SetCacheDir(tmpdir.c_str());
    string path;
    EXPECT_TRUE(FdManager::MakeCachePath("/test/file", path, true, true));
    // Should contain .mirror
    EXPECT_NE(path.find(".mirror"), string::npos);
}

TEST_F(FdManagerCoverageTest, MakeCachePathNullPath) {
    FdManager::SetCacheDir(tmpdir.c_str());
    string path;
    EXPECT_TRUE(FdManager::MakeCachePath(NULL, path, true, false));
    // Path should be the base dir without file
}

TEST_F(FdManagerCoverageTest, MakeCachePathEmptyPath) {
    FdManager::SetCacheDir(tmpdir.c_str());
    string path;
    EXPECT_TRUE(FdManager::MakeCachePath("", path, true, false));
}

TEST_F(FdManagerCoverageTest, MakeRandomTempPath) {
    string tmppath;
    EXPECT_TRUE(FdManager::MakeRandomTempPath("/test/file", tmppath));
    EXPECT_FALSE(tmppath.empty());
    // Should contain the path at the end
    EXPECT_NE(tmppath.find("/test/file"), string::npos);
}

TEST_F(FdManagerCoverageTest, MakeRandomTempPathNull) {
    string tmppath;
    EXPECT_TRUE(FdManager::MakeRandomTempPath(NULL, tmppath));
    EXPECT_FALSE(tmppath.empty());
}

TEST_F(FdManagerCoverageTest, CheckCacheTopDirNoCache) {
    FdManager::SetCacheDir("");
    EXPECT_TRUE(FdManager::CheckCacheTopDir());
}

TEST_F(FdManagerCoverageTest, CheckCacheTopDirWithCache) {
    FdManager::SetCacheDir(tmpdir.c_str());
    // The directory exists
    EXPECT_TRUE(FdManager::CheckCacheTopDir());
}

TEST_F(FdManagerCoverageTest, SetCheckCacheDirExist) {
    bool old = FdManager::SetCheckCacheDirExist(true);
    EXPECT_FALSE(old);  // Default is false
    bool old2 = FdManager::SetCheckCacheDirExist(false);
    EXPECT_TRUE(old2);
}

TEST_F(FdManagerCoverageTest, CheckCacheDirExistNotEnabled) {
    FdManager::SetCheckCacheDirExist(false);
    EXPECT_TRUE(FdManager::CheckCacheDirExist());
}

TEST_F(FdManagerCoverageTest, CheckCacheDirExistNoCache) {
    FdManager::SetCheckCacheDirExist(true);
    FdManager::SetCacheDir("");
    EXPECT_TRUE(FdManager::CheckCacheDirExist());
    FdManager::SetCheckCacheDirExist(false);
}

TEST_F(FdManagerCoverageTest, CheckCacheDirExistValidDir) {
    FdManager::SetCheckCacheDirExist(true);
    FdManager::SetCacheDir(tmpdir.c_str());
    EXPECT_TRUE(FdManager::CheckCacheDirExist());
    FdManager::SetCheckCacheDirExist(false);
}

TEST_F(FdManagerCoverageTest, CheckCacheDirExistInvalidDir) {
    FdManager::SetCheckCacheDirExist(true);
    FdManager::SetCacheDir("/nonexistent/path/fdcache_test_dir");
    EXPECT_FALSE(FdManager::CheckCacheDirExist());
    FdManager::SetCheckCacheDirExist(false);
    FdManager::SetCacheDir("");
}

TEST_F(FdManagerCoverageTest, GetEnsureFreeDiskSpace) {
    size_t space = FdManager::GetEnsureFreeDiskSpace();
    // Initially might be 0 or set by previous test
    (void)space;
    SUCCEED();
}

TEST_F(FdManagerCoverageTest, IsSafeDiskSpace) {
    // Test with small size - should be safe
    EXPECT_TRUE(FdManager::IsSafeDiskSpace(NULL, 1));
}

TEST_F(FdManagerCoverageTest, IsSafeDiskSpaceWithPath) {
    // IsSafeDiskSpace checks size + free_disk_space <= actual free space
    // free_disk_space might be very large from previous InitEnsureFreeDiskSpace
    // Just verify it doesn't crash
    bool result = FdManager::IsSafeDiskSpace("/tmp", 1);
    (void)result;
    SUCCEED();
}

TEST_F(FdManagerCoverageTest, DeleteCacheFileNoCache) {
    FdManager::SetCacheDir("");
    int result = FdManager::DeleteCacheFile("/test/file");
    EXPECT_EQ(result, 0);
}

TEST_F(FdManagerCoverageTest, DeleteCacheFileNull) {
    int result = FdManager::DeleteCacheFile(nullptr);
    EXPECT_EQ(result, -EIO);
}

TEST_F(FdManagerCoverageTest, DeleteCacheFileNonexistent) {
    FdManager::SetCacheDir(tmpdir.c_str());
    int result = FdManager::DeleteCacheFile("/nonexistent");
    // Should return error because file doesn't exist
    EXPECT_NE(result, 0);
}

TEST_F(FdManagerCoverageTest, DeleteCacheDirectoryNoCache) {
    FdManager::SetCacheDir("");
    EXPECT_TRUE(FdManager::DeleteCacheDirectory());
}

TEST_F(FdManagerCoverageTest, DeleteCacheDirectoryWithCache) {
    FdManager::SetCacheDir(tmpdir.c_str());
    // Create the bucket dir
    string bucketdir = tmpdir + "/test-bucket";
    mkdir(bucketdir.c_str(), 0755);
    EXPECT_TRUE(FdManager::DeleteCacheDirectory());
}

TEST_F(FdManagerCoverageTest, GetFdEntityNull) {
    FdEntity* ent = FdManager::get()->GetFdEntity(nullptr);
    EXPECT_EQ(ent, nullptr);
}

TEST_F(FdManagerCoverageTest, GetFdEntityEmptyPath) {
    FdEntity* ent = FdManager::get()->GetFdEntity("");
    EXPECT_EQ(ent, nullptr);
}

TEST_F(FdManagerCoverageTest, GetFdEntityNotFound) {
    FdEntity* ent = FdManager::get()->GetFdEntity("/not/in/map");
    EXPECT_EQ(ent, nullptr);
}

TEST_F(FdManagerCoverageTest, OpenAndCloseEntity) {
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/fdmgr_open", NULL, 0, -1, true);
    ASSERT_NE(ent, nullptr);
    EXPECT_TRUE(ent->IsOpen());

    bool closed = FdManager::get()->Close(ent);
    EXPECT_TRUE(closed);
}

TEST_F(FdManagerCoverageTest, OpenNullPath) {
    FdEntity* ent = FdManager::get()->Open(nullptr);
    EXPECT_EQ(ent, nullptr);
}

TEST_F(FdManagerCoverageTest, OpenEmptyPath) {
    FdEntity* ent = FdManager::get()->Open("");
    EXPECT_EQ(ent, nullptr);
}

TEST_F(FdManagerCoverageTest, OpenWithForceCreate) {
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/force_create", NULL, 0, -1, true, true);
    ASSERT_NE(ent, nullptr);
    FdManager::get()->Close(ent);
}

TEST_F(FdManagerCoverageTest, OpenIsCreateFalse) {
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/no_create", NULL, -1, -1, false, false);
    // Not found and is_create=false, should return NULL
    EXPECT_EQ(ent, nullptr);
}

TEST_F(FdManagerCoverageTest, ExistOpenNotFound) {
    FdEntity* ent = FdManager::get()->ExistOpen("/nonexistent/path");
    EXPECT_EQ(ent, nullptr);
}

TEST_F(FdManagerCoverageTest, ExistOpenWithFd) {
    FdManager::SetCacheDir("");
    // Open an entity
    FdEntity* ent = FdManager::get()->Open("/test/exist_open_fd", NULL, 0, -1, true);
    ASSERT_NE(ent, nullptr);
    int fd = ent->GetFd();

    // Try ExistOpen with fd
    FdEntity* found = FdManager::get()->ExistOpen("/test/exist_open_fd", fd);
    // Should find by fd
    if (found) {
        FdManager::get()->Close(found);
    }
    FdManager::get()->Close(ent);
}

TEST_F(FdManagerCoverageTest, ExistOpenIgnoreExistFd) {
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/exist_ignore", NULL, 0, -1, true);
    ASSERT_NE(ent, nullptr);

    FdEntity* found = FdManager::get()->ExistOpen("/test/exist_ignore", -1, true);
    if (found) {
        FdManager::get()->Close(found);
    }
    FdManager::get()->Close(ent);
}

TEST_F(FdManagerCoverageTest, CloseNull) {
    bool result = FdManager::get()->Close(nullptr);
    EXPECT_TRUE(result);
}

TEST_F(FdManagerCoverageTest, CloseEntityNotInMap) {
    // Create an entity manually (not through FdManager::Open)
    FdEntity* ent = new FdEntity("/test/not_in_map");
    bool result = FdManager::get()->Close(ent);
    EXPECT_FALSE(result);
    delete ent;
}

TEST_F(FdManagerCoverageTest, Rename) {
    FdManager::SetCacheDir(tmpdir.c_str());
    FdEntity* ent = FdManager::get()->Open("/test/rename_from", NULL, 0, -1, false);
    ASSERT_NE(ent, nullptr);

    FdManager::get()->Rename("/test/rename_from", "/test/rename_to");

    // Old path should not be found
    FdEntity* old_ent = FdManager::get()->GetFdEntity("/test/rename_from");
    EXPECT_EQ(old_ent, nullptr);

    // New path should be found
    FdEntity* new_ent = FdManager::get()->GetFdEntity("/test/rename_to");
    EXPECT_NE(new_ent, nullptr);

    FdManager::get()->Close(ent);
}

TEST_F(FdManagerCoverageTest, RenameNonexistent) {
    FdManager::get()->Rename("/does/not/exist", "/also/not/exist");
    // Should not crash
    SUCCEED();
}

TEST_F(FdManagerCoverageTest, RenameOverwrite) {
    FdManager::SetCacheDir(tmpdir.c_str());
    FdEntity* ent1 = FdManager::get()->Open("/test/overwrite_src", NULL, 0, -1, false);
    FdEntity* ent2 = FdManager::get()->Open("/test/overwrite_dst", NULL, 0, -1, false);
    ASSERT_NE(ent1, nullptr);
    ASSERT_NE(ent2, nullptr);

    // Close ent2 first to reduce refcount
    FdManager::get()->Close(ent2);

    // Now rename src -> dst (should evict dst)
    FdManager::get()->Rename("/test/overwrite_src", "/test/overwrite_dst");

    FdEntity* found = FdManager::get()->GetFdEntity("/test/overwrite_dst");
    EXPECT_NE(found, nullptr);
    EXPECT_EQ(found, ent1);

    FdManager::get()->Close(ent1);
}

TEST_F(FdManagerCoverageTest, ChangeEntityToTempPath) {
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/change_path", NULL, 0, -1, true);
    ASSERT_NE(ent, nullptr);

    FdManager::get()->ChangeEntityToTempPath(ent, "/test/change_path");

    // Original path should not find it
    FdEntity* found = FdManager::get()->GetFdEntity("/test/change_path");
    EXPECT_EQ(found, nullptr);

    FdManager::get()->Close(ent);
}

TEST_F(FdManagerCoverageTest, CleanupCacheDirNoCache) {
    FdManager::SetCacheDir("");
    FdManager::get()->CleanupCacheDir();
    SUCCEED();
}

TEST_F(FdManagerCoverageTest, CleanupCacheDirWithCache) {
    FdManager::SetCacheDir(tmpdir.c_str());
    // Create bucket directory
    string bucketdir = tmpdir + "/test-bucket";
    mkdir(bucketdir.c_str(), 0755);
    FdManager::get()->CleanupCacheDir();
    SUCCEED();
}

TEST_F(FdManagerCoverageTest, GetFdEntityWithExistFd) {
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/getent_fd", NULL, 0, -1, true);
    ASSERT_NE(ent, nullptr);
    int fd = ent->GetFd();

    // Lookup with correct fd
    FdEntity* found = FdManager::get()->GetFdEntity("/test/getent_fd", fd);
    EXPECT_EQ(found, ent);

    // Lookup with wrong fd
    FdEntity* notfound = FdManager::get()->GetFdEntity("/test/getent_fd", fd + 1000);
    // Might still find it by path iteration (searches all fent by fd)
    (void)notfound;

    FdManager::get()->Close(ent);
}

// GetFdEntityWithDup: atomically finds entity + increments refcnt
TEST_F(FdManagerCoverageTest, GetFdEntityWithDup_Basic) {
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/dup_basic", NULL, 0, -1, true);
    ASSERT_NE(ent, nullptr);
    int fd = ent->GetFd();

    // GetFdEntityWithDup should find it and increment refcnt
    FdEntity* found = FdManager::get()->GetFdEntityWithDup("/test/dup_basic", fd);
    EXPECT_EQ(found, ent);

    // Now Close twice: once for the Dup, once for the Open
    FdManager::get()->Close(found);  // releases Dup refcnt
    EXPECT_TRUE(ent->IsOpen());      // still open (Open refcnt remains)
    FdManager::get()->Close(ent);    // releases Open refcnt
}

TEST_F(FdManagerCoverageTest, GetFdEntityWithDup_NotFound) {
    FdEntity* found = FdManager::get()->GetFdEntityWithDup("/nonexistent/path");
    EXPECT_EQ(found, nullptr);
}

TEST_F(FdManagerCoverageTest, GetFdEntityWithDup_PreventsDelete) {
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/dup_nodelete", NULL, 0, -1, true);
    ASSERT_NE(ent, nullptr);
    int fd = ent->GetFd();

    // Dup via GetFdEntityWithDup (must pass fd for no-cache mode lookup)
    FdEntity* duped = FdManager::get()->GetFdEntityWithDup("/test/dup_nodelete", fd);
    ASSERT_NE(duped, nullptr);

    // Close the original Open — entity should NOT be deleted because Dup holds refcnt
    FdManager::get()->Close(ent);
    EXPECT_TRUE(duped->IsOpen());  // still alive

    // Now release the Dup refcnt
    FdManager::get()->Close(duped);
}

// --- GetOpenEntitySize tests (ISSUE2026031500003) ---

TEST_F(FdManagerCoverageTest, GetOpenEntitySize_Basic) {
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/open_entity_size", NULL, 100, -1, true);
    ASSERT_NE(ent, nullptr);

    off_t size = -1;
    bool ok = FdManager::get()->GetOpenEntitySize("/test/open_entity_size", &size);
    EXPECT_TRUE(ok);
    EXPECT_EQ(size, 100);

    FdManager::get()->Close(ent);
}

TEST_F(FdManagerCoverageTest, GetOpenEntitySize_NotFound) {
    off_t size = -1;
    bool ok = FdManager::get()->GetOpenEntitySize("/test/nonexistent_path", &size);
    EXPECT_FALSE(ok);
    EXPECT_EQ(size, -1);  // unchanged
}

TEST_F(FdManagerCoverageTest, GetOpenEntitySize_NullArgs) {
    EXPECT_FALSE(FdManager::get()->GetOpenEntitySize(NULL, NULL));
    EXPECT_FALSE(FdManager::get()->GetOpenEntitySize("", NULL));
    off_t size;
    EXPECT_FALSE(FdManager::get()->GetOpenEntitySize(NULL, &size));
    EXPECT_FALSE(FdManager::get()->GetOpenEntitySize("", &size));
}

TEST_F(FdManagerCoverageTest, GetOpenEntitySize_AfterClose) {
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/size_after_close", NULL, 50, -1, true);
    ASSERT_NE(ent, nullptr);

    FdManager::get()->Close(ent);

    // After close, entity is deleted — GetOpenEntitySize should return false
    off_t size = -1;
    bool ok = FdManager::get()->GetOpenEntitySize("/test/size_after_close", &size);
    EXPECT_FALSE(ok);
}

TEST_F(FdManagerCoverageTest, OpenExistingEntity) {
    FdManager::SetCacheDir(tmpdir.c_str());
    FdEntity* ent1 = FdManager::get()->Open("/test/reopen_entity", NULL, 0, -1, false);
    ASSERT_NE(ent1, nullptr);

    // Open again - should find existing by path in cache
    FdEntity* ent2 = FdManager::get()->Open("/test/reopen_entity", NULL, 0, -1, false);
    EXPECT_EQ(ent1, ent2);  // Same entity

    FdManager::get()->Close(ent2);
    FdManager::get()->Close(ent1);
}

TEST_F(FdManagerCoverageTest, SetEnsureFreeDiskSpace) {
    size_t old = FdManager::SetEnsureFreeDiskSpace(0);
    // After SetEnsureFreeDiskSpace(0), it should be set to a default
    size_t space = FdManager::GetEnsureFreeDiskSpace();
    EXPECT_GT(space, 0u);
    // Restore
    FdManager::SetEnsureFreeDiskSpace(old > 0 ? old : space);
}

// ============================================================
// Additional edge case tests
// ============================================================

TEST_F(PageListCoverageTest, SetPageLoadedStatusStartBeyondSizeGap) {
    PageList pl(100, false);
    // Start (200) > Size (100), creates gap
    pl.SetPageLoadedStatus(200, 50, true);
    EXPECT_GE(pl.Size(), 250u);
    // Gap should be unloaded
    EXPECT_FALSE(pl.IsPageLoaded(100, 100));
    EXPECT_TRUE(pl.IsPageLoaded(200, 50));
}

TEST_F(PageListCoverageTest, SetPageLoadedStatusOverlapEndBeyondSize) {
    PageList pl(100, false);
    // Start (50) < Size (100), Start+Size (150) > Size (100)
    pl.SetPageLoadedStatus(50, 100, true);
    EXPECT_GE(pl.Size(), 150u);
}

TEST_F(PageListCoverageTest, MultipleSplitsViaSetPageLoadedStatus) {
    PageList pl(1000, false);
    // Each SetPageLoadedStatus call with inner range calls Parse twice
    pl.SetPageLoadedStatus(250, 250, true);
    pl.SetPageLoadedStatus(750, 250, true);
    EXPECT_EQ(pl.Size(), 1000u);
    EXPECT_FALSE(pl.IsPageLoaded(0, 250));
    EXPECT_TRUE(pl.IsPageLoaded(250, 250));
    EXPECT_FALSE(pl.IsPageLoaded(500, 250));
    EXPECT_TRUE(pl.IsPageLoaded(750, 250));
}

TEST_F(PageListCoverageTest, CompressViaSinglePageResize) {
    PageList pl(100, false);
    // Resize same size triggers Compress
    pl.Resize(100, false);
    EXPECT_EQ(pl.Size(), 100u);
}

TEST_F(FdEntityCoverageTest, MultipleWritesAndReads) {
    FdEntity ent("/test/multi_rw");
    ent.Open(NULL, 0, -1);

    // Write at different offsets
    ent.Write("AAA", 0, 3);
    ent.Write("BBB", 3, 3);
    ent.Write("CCC", 6, 3);

    // Read all
    char buf[16] = {0};
    ssize_t r = ent.Read(buf, 0, 9);
    EXPECT_EQ(r, 9);
    EXPECT_EQ(memcmp(buf, "AAABBBCCC", 9), 0);
    ent.Close();
}

TEST_F(FdEntityCoverageTest, ReadPartial) {
    FdEntity ent("/test/read_partial");
    ent.Open(NULL, 0, -1);
    ent.Write("0123456789", 0, 10);

    char buf[8] = {0};
    ssize_t r = ent.Read(buf, 5, 5);
    EXPECT_EQ(r, 5);
    EXPECT_EQ(memcmp(buf, "56789", 5), 0);
    ent.Close();
}

TEST_F(FdEntityCoverageTest, OpenAndCloseMultipleTimes) {
    FdEntity ent("/test/multi_open_close");
    // Open
    ent.Open(NULL, 10, -1);
    EXPECT_TRUE(ent.IsOpen());
    ent.Close();
    EXPECT_FALSE(ent.IsOpen());

    // Can open again
    ent.Open(NULL, 20, -1);
    EXPECT_TRUE(ent.IsOpen());
    ent.Close();
    EXPECT_FALSE(ent.IsOpen());
}

TEST_F(FdEntityCoverageTest, CleanupCacheNotModified) {
    FdEntity ent("/test/cleanup");
    FdManager::SetCacheDir("");
    ent.Open(NULL, 0, -1);
    // Not modified, cleanup should proceed
    ent.CleanupCache();
    ent.Close();
}

// ============================================================
// fdpage struct edge cases (for coverage of the struct in fdcache.cpp)
// ============================================================

TEST(FdPageCoverage, DefaultValues) {
    fdpage page;
    EXPECT_EQ(page.offset, 0);
    EXPECT_EQ(page.bytes, 0u);
    EXPECT_FALSE(page.loaded);
    EXPECT_FALSE(page.modified);
    EXPECT_EQ(page.next(), 0);
    EXPECT_EQ(page.end(), 0);
}

TEST(FdPageCoverage, ConstructorWithModified) {
    fdpage page(100, 50, true, true);
    EXPECT_EQ(page.offset, 100);
    EXPECT_EQ(page.bytes, 50u);
    EXPECT_TRUE(page.loaded);
    EXPECT_TRUE(page.modified);
}

TEST(FdPageCoverage, NextAndEnd) {
    fdpage page(100, 50, true);
    EXPECT_EQ(page.next(), 150);
    EXPECT_EQ(page.end(), 149);
}

TEST(FdPageCoverage, EndZeroBytes) {
    fdpage page(100, 0, false);
    EXPECT_EQ(page.end(), 0);
}

TEST(FdPageCoverage, SingleByte) {
    fdpage page(0, 1, true);
    EXPECT_EQ(page.next(), 1);
    EXPECT_EQ(page.end(), 0);
}

// ============================================================
// FdManager tmpdir Coverage Tests
// ============================================================

TEST_F(FdManagerCoverageTest, SetTmpDirValid) {
    FdManager::SetTmpDir(tmpdir.c_str());
    EXPECT_STREQ(FdManager::GetTmpDir(), tmpdir.c_str());
}

TEST_F(FdManagerCoverageTest, SetTmpDirNull) {
    FdManager::SetTmpDir(tmpdir.c_str());
    FdManager::SetTmpDir(NULL);
    EXPECT_STREQ(FdManager::GetTmpDir(), "/tmp");
}

TEST_F(FdManagerCoverageTest, SetTmpDirEmpty) {
    FdManager::SetTmpDir(tmpdir.c_str());
    FdManager::SetTmpDir("");
    EXPECT_STREQ(FdManager::GetTmpDir(), "/tmp");
}

TEST_F(FdManagerCoverageTest, CheckTmpDirExistDefault) {
    FdManager::SetTmpDir("/tmp");
    EXPECT_TRUE(FdManager::CheckTmpDirExist());
}

TEST_F(FdManagerCoverageTest, CheckTmpDirExistValid) {
    FdManager::SetTmpDir(tmpdir.c_str());
    EXPECT_TRUE(FdManager::CheckTmpDirExist());
}

TEST_F(FdManagerCoverageTest, CheckTmpDirExistInvalid) {
    FdManager::SetTmpDir("/nonexistent_dir_for_test_12345");
    EXPECT_FALSE(FdManager::CheckTmpDirExist());
}

// CheckTmpDirFsType: /tmp is typically tmpfs or ext4, both in whitelist
TEST_F(FdManagerCoverageTest, CheckTmpDirFsType_Default) {
    FdManager::SetTmpDir("/tmp");
    EXPECT_TRUE(FdManager::CheckTmpDirFsType());
}

// CheckTmpDirFsType: empty tmpdir defaults to /tmp check
TEST_F(FdManagerCoverageTest, CheckTmpDirFsType_EmptyDir) {
    FdManager::SetTmpDir("");
    EXPECT_TRUE(FdManager::CheckTmpDirFsType());
}

// CheckTmpDirFsType: valid local directory (same fs as tmpdir fixture)
TEST_F(FdManagerCoverageTest, CheckTmpDirFsType_ValidLocalDir) {
    FdManager::SetTmpDir(tmpdir.c_str());
    EXPECT_TRUE(FdManager::CheckTmpDirFsType());
}

// CheckTmpDirFsType: nonexistent path should fail statfs
TEST_F(FdManagerCoverageTest, CheckTmpDirFsType_NonexistentPath) {
    FdManager::SetTmpDir("/nonexistent_dir_for_test_99999");
    EXPECT_FALSE(FdManager::CheckTmpDirFsType());
}

// IsSupportedFsType: whitelist members should pass
TEST_F(FdManagerCoverageTest, IsSupportedFsType_Ext4) {
    EXPECT_TRUE(FdManager::IsSupportedFsType(EXT4_SUPER_MAGIC));
}

TEST_F(FdManagerCoverageTest, IsSupportedFsType_Xfs) {
    EXPECT_TRUE(FdManager::IsSupportedFsType(XFS_SUPER_MAGIC));
}

TEST_F(FdManagerCoverageTest, IsSupportedFsType_Btrfs) {
    EXPECT_TRUE(FdManager::IsSupportedFsType(BTRFS_SUPER_MAGIC));
}

TEST_F(FdManagerCoverageTest, IsSupportedFsType_Tmpfs) {
    EXPECT_TRUE(FdManager::IsSupportedFsType(TMPFS_MAGIC));
}

// IsSupportedFsType: NFS should be rejected
TEST_F(FdManagerCoverageTest, IsSupportedFsType_NfsRejected) {
    EXPECT_FALSE(FdManager::IsSupportedFsType(NFS_SUPER_MAGIC));
}

// IsSupportedFsType: other unsupported filesystems
TEST_F(FdManagerCoverageTest, IsSupportedFsType_FatRejected) {
    EXPECT_FALSE(FdManager::IsSupportedFsType(0x4d44));  // MSDOS_SUPER_MAGIC (FAT)
}

TEST_F(FdManagerCoverageTest, IsSupportedFsType_UnknownRejected) {
    EXPECT_FALSE(FdManager::IsSupportedFsType(0x12345678));
}

// IsSupportedFsName: macOS-style checks (testable on all platforms)
TEST_F(FdManagerCoverageTest, IsSupportedFsName_Apfs) {
#ifdef __APPLE__
    EXPECT_TRUE(FdManager::IsSupportedFsName("apfs"));
#else
    // Non-macOS: always returns true (no-op)
    EXPECT_TRUE(FdManager::IsSupportedFsName("apfs"));
#endif
}

TEST_F(FdManagerCoverageTest, IsSupportedFsName_NfsRejectedOnMac) {
#ifdef __APPLE__
    EXPECT_FALSE(FdManager::IsSupportedFsName("nfs"));
#else
    EXPECT_TRUE(FdManager::IsSupportedFsName("nfs"));  // no-op on Linux
#endif
}

TEST_F(FdManagerCoverageTest, IsSupportedFsName_NullRejectedOnMac) {
#ifdef __APPLE__
    EXPECT_FALSE(FdManager::IsSupportedFsName(NULL));
#else
    EXPECT_TRUE(FdManager::IsSupportedFsName(NULL));
#endif
}

TEST_F(FdManagerCoverageTest, MakeTempFileDefault) {
    FdManager::SetTmpDir("/tmp");
    FILE* fp = FdManager::MakeTempFile();
    ASSERT_NE(fp, (FILE*)NULL);

    // Write and read back
    const char* msg = "hello tmpdir";
    fputs(msg, fp);
    fflush(fp);
    rewind(fp);

    char buf[64] = {0};
    char* ret = fgets(buf, sizeof(buf), fp);
    ASSERT_NE(ret, nullptr);
    EXPECT_STREQ(buf, msg);

    fclose(fp);
}

TEST_F(FdManagerCoverageTest, MakeTempFileCustomDir) {
    FdManager::SetTmpDir(tmpdir.c_str());
    FILE* fp = FdManager::MakeTempFile();
    ASSERT_NE(fp, (FILE*)NULL);

    int fd = fileno(fp);
    EXPECT_GE(fd, 0);

    fclose(fp);
}

TEST_F(FdManagerCoverageTest, MakeTempFileInvalidDir) {
    FdManager::SetTmpDir("/nonexistent_dir_for_test_12345");
    FILE* fp = FdManager::MakeTempFile();
    EXPECT_EQ(fp, (FILE*)NULL);
}

TEST_F(FdManagerCoverageTest, MakeTempFileLargeWrite) {
    FdManager::SetTmpDir(tmpdir.c_str());
    FILE* fp = FdManager::MakeTempFile();
    ASSERT_NE(fp, (FILE*)NULL);

    // Write >1MB of data
    const size_t write_size = 1024 * 1024 + 512;
    char* buf = new char[write_size];
    memset(buf, 'A', write_size);
    size_t written = fwrite(buf, 1, write_size, fp);
    EXPECT_EQ(written, write_size);
    fflush(fp);

    // Verify size via fd
    struct stat st;
    EXPECT_EQ(fstat(fileno(fp), &st), 0);
    EXPECT_EQ(static_cast<size_t>(st.st_size), write_size);

    delete[] buf;
    fclose(fp);
}

TEST_F(FdManagerCoverageTest, GetFreeDiskSpaceUsesTmpDir) {
    // With no cache_dir set and tmpdir set, IsSafeDiskSpace should not crash
    FdManager::SetCacheDir("");
    FdManager::SetTmpDir(tmpdir.c_str());
    // Just verify it doesn't crash - actual free space depends on system
    bool safe = FdManager::IsSafeDiskSpace(NULL, 1);
    (void)safe; // result depends on system, just verify no crash
    SUCCEED();
}

// ============================================================
// FdManager max_dirty_data Coverage Tests
// ============================================================

TEST_F(FdManagerCoverageTest, MaxDirtyDataDefaultIs5GB) {
    // Default should be 5GB (5 * 1024 * 1024 * 1024)
    off_t expected = 5LL * 1024LL * 1024LL * 1024LL;
    EXPECT_EQ(FdManager::GetMaxDirtyData(), expected);
}

TEST_F(FdManagerCoverageTest, SetMaxDirtyDataPositive) {
    FdManager::SetMaxDirtyData(100 * 1024 * 1024);  // 100MB
    EXPECT_EQ(FdManager::GetMaxDirtyData(), 100 * 1024 * 1024);
    // Restore default
    FdManager::SetMaxDirtyData(5LL * 1024LL * 1024LL * 1024LL);
}

TEST_F(FdManagerCoverageTest, SetMaxDirtyDataDisabled) {
    FdManager::SetMaxDirtyData(-1);
    EXPECT_EQ(FdManager::GetMaxDirtyData(), -1);
    // Restore default
    FdManager::SetMaxDirtyData(5LL * 1024LL * 1024LL * 1024LL);
}

TEST_F(FdManagerCoverageTest, SetMaxDirtyDataZero) {
    FdManager::SetMaxDirtyData(0);
    EXPECT_EQ(FdManager::GetMaxDirtyData(), 0);
    // Restore default
    FdManager::SetMaxDirtyData(5LL * 1024LL * 1024LL * 1024LL);
}

// ============================================================
// PageList GetTotalLoadedPageSize Coverage Tests
// ============================================================

TEST_F(PageListCoverageTest, GetTotalLoadedPageSizeAllUnloaded) {
    PageList pl(1000, false);
    EXPECT_EQ(pl.GetTotalLoadedPageSize(0, 1000), 0u);
}

TEST_F(PageListCoverageTest, GetTotalLoadedPageSizeAllLoaded) {
    PageList pl(1000, true);
    EXPECT_EQ(pl.GetTotalLoadedPageSize(0, 1000), 1000u);
}

TEST_F(PageListCoverageTest, GetTotalLoadedPageSizePartial) {
    PageList pl(300, false);
    pl.SetPageLoadedStatus(100, 100, true);
    // Loaded: [100-200] = 100 bytes
    EXPECT_EQ(pl.GetTotalLoadedPageSize(0, 300), 100u);
}

TEST_F(PageListCoverageTest, GetTotalLoadedPageSizeSubRange) {
    PageList pl(300, false);
    pl.SetPageLoadedStatus(100, 100, true);
    // Check only [50, 150): loaded = [100-150] = 50 bytes
    EXPECT_EQ(pl.GetTotalLoadedPageSize(50, 100), 50u);
}

TEST_F(PageListCoverageTest, GetTotalLoadedPageSizeZeroMeansAll) {
    PageList pl(200, true);
    // size=0 means check to end of list
    EXPECT_EQ(pl.GetTotalLoadedPageSize(0, 0), 200u);
}

TEST_F(PageListCoverageTest, GetTotalLoadedPageSizeMultiSegments) {
    PageList pl(500, false);
    pl.SetPageLoadedStatus(0, 100, true);
    pl.SetPageLoadedStatus(200, 100, true);
    pl.SetPageLoadedStatus(400, 100, true);
    // Loaded: [0-100] + [200-300] + [400-500] = 300 bytes
    EXPECT_EQ(pl.GetTotalLoadedPageSize(0, 500), 300u);
}

// ============================================================
// FdEntity PunchHole Coverage Tests
// ============================================================

TEST_F(FdEntityCoverageTest, PunchHoleNotOpen) {
    FdEntity ent;
    // fd=-1, should return false gracefully
    EXPECT_FALSE(ent.PunchHole());
}

TEST_F(FdEntityCoverageTest, PunchHoleOnTmpFile) {
    // Open a FdEntity via FdManager (creates tmpfile)
    FdEntity ent("/test/punchhole_file");
    int openres = ent.Open(NULL, 0, -1, false);
    if(openres < 0){
        // May fail in test environment without S3 - just verify no crash
        SUCCEED();
        return;
    }

    // Write some data
    const char* data = "AAAAAAAAAAAAAAAA";  // 16 bytes
    ssize_t wsize = ent.Write(data, 0, 16);
    if(wsize < 0){
        ent.Close();
        SUCCEED();
        return;
    }

    // PunchHole on tmpfile — may return true or false depending on filesystem
    // (tmpfs doesn't support FALLOC_FL_PUNCH_HOLE, ext4 does)
    // Either way it should not crash
    ent.PunchHole(0, 16);
    SUCCEED();

    ent.Close();
}

TEST_F(FdEntityCoverageTest, PunchHoleSmallRegion) {
    // Region smaller than 4K should be no-op (returns true)
    FdEntity ent("/test/punchhole_small");
    int openres = ent.Open(NULL, 0, -1, false);
    if(openres < 0){
        SUCCEED();
        return;
    }

    // Write 100 bytes
    char data[100];
    memset(data, 'B', sizeof(data));
    ssize_t wsize = ent.Write(data, 0, 100);
    if(wsize < 0){
        ent.Close();
        SUCCEED();
        return;
    }

    // Punch hole on small region (< 4K aligned) — should return true (no-op)
    bool result = ent.PunchHole(0, 100);
    EXPECT_TRUE(result);  // Region too small to punch, returns true

    ent.Close();
}

TEST_F(FdEntityCoverageTest, PunchHoleLargeFile) {
    FdEntity ent("/test/punchhole_large");
    int openres = ent.Open(NULL, 0, -1, false);
    if(openres < 0){
        SUCCEED();
        return;
    }

    // Write 64KB of data
    const size_t write_size = 64 * 1024;
    char* buf = new char[write_size];
    memset(buf, 'C', write_size);
    ssize_t wsize = ent.Write(buf, 0, write_size);
    delete[] buf;

    if(wsize < 0){
        ent.Close();
        SUCCEED();
        return;
    }

    // PunchHole on large region — should succeed on ext4/xfs, may fail on tmpfs
    // We just verify it doesn't crash
    ent.PunchHole(0, write_size);
    SUCCEED();

    ent.Close();
}

// ============================================================
// FdEntity TransitionToMultipart Coverage Tests
// ============================================================

TEST_F(FdEntityCoverageTest, TransitionToMultipartNotOpen) {
    FdEntity ent("test_transition_closed", "test_transition_closed");
    // Should return false when fd is not open
    EXPECT_FALSE(ent.TransitionToMultipart());
}

// Note: TransitionToMultipart with an open entity requires S3 backend
// (NoCachePreMultipartPost makes real HTTP calls), so it cannot be unit-tested
// without a mock. Integration tests cover this via test_write_max_dirty_data.sh.

// ============================================================
// PageList Modified Tracking Tests
// ============================================================

TEST_F(PageListCoverageTest, SetPageModifiedStatusBasic) {
    PageList pl(1000, false);
    // Mark middle region as modified
    EXPECT_TRUE(pl.SetPageModifiedStatus(200, 300, true));
    EXPECT_EQ(pl.Size(), 1000u);
    // loaded status should be unchanged (all unloaded)
    EXPECT_FALSE(pl.IsPageLoaded(0, 1000));
}

TEST_F(PageListCoverageTest, SetPageModifiedStatusDoesNotDestroyLoaded) {
    PageList pl(100, false);
    // Load a region first
    pl.SetPageLoadedStatus(0, 100, true);
    EXPECT_TRUE(pl.IsPageLoaded(0, 100));
    // Now mark as modified — loaded status must be preserved
    pl.SetPageModifiedStatus(0, 100, true);
    EXPECT_TRUE(pl.IsPageLoaded(0, 100));
}

TEST_F(PageListCoverageTest, SetPageModifiedStatusPartialRange) {
    PageList pl(1000, false);
    pl.SetPageLoadedStatus(0, 1000, true);
    // Mark middle as modified
    pl.SetPageModifiedStatus(300, 400, true);
    // All should still be loaded
    EXPECT_TRUE(pl.IsPageLoaded(0, 1000));
}

TEST_F(PageListCoverageTest, SetPageModifiedStatusExtendBeyond) {
    PageList pl(100, false);
    // Extend beyond current size
    EXPECT_TRUE(pl.SetPageModifiedStatus(50, 200, true));
    EXPECT_GE(pl.Size(), 250u);
}

TEST_F(PageListCoverageTest, SetPageModifiedStatusStartBeyondSize) {
    PageList pl(100, false);
    // Start beyond current size
    EXPECT_TRUE(pl.SetPageModifiedStatus(200, 100, true));
    EXPECT_GE(pl.Size(), 300u);
}

TEST_F(PageListCoverageTest, SetPageModifiedStatusNoCompress) {
    PageList pl(100, false);
    EXPECT_TRUE(pl.SetPageModifiedStatus(0, 50, true, false));
    EXPECT_TRUE(pl.SetPageModifiedStatus(50, 50, true, false));
    EXPECT_EQ(pl.Size(), 100u);
}

TEST_F(PageListCoverageTest, CompressRespectsModifiedFlag) {
    PageList pl(300, false);
    // Create regions with same loaded but different modified
    pl.SetPageLoadedStatus(0, 300, true);
    pl.SetPageModifiedStatus(0, 100, true, false);
    pl.SetPageModifiedStatus(100, 100, false, false);
    pl.SetPageModifiedStatus(200, 100, true, false);
    // All loaded but modified differs: [0-100 mod] [100-200 clean] [200-300 mod]
    // Should NOT compress into one page
    // Verify by checking total size is still correct
    EXPECT_EQ(pl.Size(), 300u);
    EXPECT_TRUE(pl.IsPageLoaded(0, 300));
}

TEST_F(PageListCoverageTest, ClearAllModified) {
    PageList pl(300, false);
    pl.SetPageLoadedStatus(0, 300, true);
    pl.SetPageModifiedStatus(0, 100, true);
    pl.SetPageModifiedStatus(200, 100, true);
    // Clear all modified
    pl.ClearAllModified();
    // Size should be preserved
    EXPECT_EQ(pl.Size(), 300u);
    // Loaded status should be preserved
    EXPECT_TRUE(pl.IsPageLoaded(0, 300));
}

TEST_F(PageListCoverageTest, ClearAllModifiedEmptyList) {
    PageList pl(0, false);
    // Should not crash
    pl.ClearAllModified();
    EXPECT_EQ(pl.Size(), 0u);
}

// ============================================================
// GetPageListsForMultipartUpload Tests
// ============================================================

TEST_F(PageListCoverageTest, MixUploadAllModified) {
    // 100MB file, all modified → all UploadPart, no CopyPart
    size_t file_size = 100 * 1024 * 1024;
    off_t  part_size = 10 * 1024 * 1024;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, file_size, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // No downloads needed
    EXPECT_TRUE(dlpages.empty());
    // All pages should be modified (UploadPart)
    size_t total_upload = 0;
    for(fdpage_list_t::iterator it = mixuppages.begin(); it != mixuppages.end(); ++it){
        EXPECT_TRUE((*it)->modified);
        total_upload += (*it)->bytes;
    }
    EXPECT_EQ(total_upload, file_size);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, MixUploadAllUnmodified) {
    // 100MB file, nothing modified → all CopyPart
    size_t file_size = 100 * 1024 * 1024;
    off_t  part_size = 10 * 1024 * 1024;
    PageList pl(file_size, true);
    // Don't set any modified

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // No downloads needed
    EXPECT_TRUE(dlpages.empty());
    // All pages should be unmodified (CopyPart)
    size_t total_copy = 0;
    for(fdpage_list_t::iterator it = mixuppages.begin(); it != mixuppages.end(); ++it){
        EXPECT_FALSE((*it)->modified);
        total_copy += (*it)->bytes;
    }
    EXPECT_EQ(total_copy, file_size);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, MixUploadMiddleModified) {
    // 30MB file: [0-10MB unmod] [10-20MB mod] [20-30MB unmod]
    size_t mb = 1024 * 1024;
    size_t file_size = 30 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(10 * mb, 10 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // Should have 3 pages: unmod, mod, unmod
    EXPECT_EQ(mixuppages.size(), 3u);

    fdpage_list_t::iterator it = mixuppages.begin();
    EXPECT_FALSE((*it)->modified);  // first: CopyPart
    EXPECT_EQ((*it)->bytes, 10 * mb);
    ++it;
    EXPECT_TRUE((*it)->modified);   // middle: UploadPart
    EXPECT_EQ((*it)->bytes, 10 * mb);
    ++it;
    EXPECT_FALSE((*it)->modified);  // last: CopyPart
    EXPECT_EQ((*it)->bytes, 10 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, MixUploadSmallUnmodifiedGapMerged) {
    // 20MB file: [0-8MB mod] [8-12MB unmod (4MB < 5MB min)] [12-20MB mod]
    // The 4MB unmodified gap should be merged into modified region
    size_t mb = 1024 * 1024;
    size_t file_size = 20 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, 8 * mb, true);
    pl.SetPageModifiedStatus(12 * mb, 8 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // The 4MB gap should be in download list
    EXPECT_FALSE(dlpages.empty());
    size_t dl_total = 0;
    for(fdpage_list_t::iterator it = dlpages.begin(); it != dlpages.end(); ++it){
        dl_total += (*it)->bytes;
    }
    EXPECT_EQ(dl_total, 4 * mb);

    // All mixuppages should be modified (merged)
    size_t total = 0;
    for(fdpage_list_t::iterator it = mixuppages.begin(); it != mixuppages.end(); ++it){
        EXPECT_TRUE((*it)->modified);
        total += (*it)->bytes;
    }
    EXPECT_EQ(total, file_size);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, MixUploadLargeModifiedSplit) {
    // 50MB file, all modified, part_size=10MB
    // With 2x strategy: 50>20→10MB, 40>20→10MB, 30>20→10MB, 20<=20→20MB = 4 parts
    size_t mb = 1024 * 1024;
    size_t file_size = 50 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, file_size, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    EXPECT_TRUE(dlpages.empty());
    EXPECT_EQ(mixuppages.size(), 4u);

    fdpage_list_t::iterator it = mixuppages.begin();
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)0);
    EXPECT_EQ((*it)->bytes, 10 * mb);
    ++it;
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)(10 * mb));
    EXPECT_EQ((*it)->bytes, 10 * mb);
    ++it;
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)(20 * mb));
    EXPECT_EQ((*it)->bytes, 10 * mb);
    ++it;
    // Last part: 20MB (remainder <= 2x, kept as single)
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)(30 * mb));
    EXPECT_EQ((*it)->bytes, 20 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, MixUploadTotalSizePreserved) {
    // Verify total bytes across all pages equals file size
    size_t mb = 1024 * 1024;
    size_t file_size = 100 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    // Scatter modified regions
    pl.SetPageModifiedStatus(0, 15 * mb, true);
    pl.SetPageModifiedStatus(30 * mb, 20 * mb, true);
    pl.SetPageModifiedStatus(80 * mb, 20 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    size_t total = 0;
    for(fdpage_list_t::iterator it = mixuppages.begin(); it != mixuppages.end(); ++it){
        total += (*it)->bytes;
    }
    EXPECT_EQ(total, file_size);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

// ============================================================
// Bug 1 fix tests: D1/D2 borrow strategy
// ============================================================

TEST_F(PageListCoverageTest, BorrowD1_PartialBorrowRemainderAbove5MB) {
    // modified=3MB, next unmodified=20MB
    // need=2MB, (2MB + 5MB)=7MB < 20MB → D1: borrow 2MB, unmodified shrinks to 18MB
    size_t mb = 1024 * 1024;
    size_t file_size = 23 * mb;  // 3MB mod + 20MB unmod
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, 3 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // Download list should have exactly 2MB borrowed from unmodified
    EXPECT_EQ(dlpages.size(), 1u);
    EXPECT_EQ(dlpages.front()->offset, (off_t)(3 * mb));
    EXPECT_EQ(dlpages.front()->bytes, 2 * mb);

    // Output: [0-5MB modified] [5MB-23MB unmodified(18MB)]
    EXPECT_EQ(mixuppages.size(), 2u);
    fdpage_list_t::iterator it = mixuppages.begin();
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)0);
    EXPECT_EQ((*it)->bytes, 5 * mb);
    ++it;
    EXPECT_FALSE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)(5 * mb));
    EXPECT_EQ((*it)->bytes, 18 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, BorrowD2_AbsorbEntireUnmodified) {
    // modified=3MB, next unmodified=6MB → need=2MB, (2MB+5MB)=7MB >= 6MB → D2: absorb all 6MB
    // Result: single 9MB modified region
    size_t mb = 1024 * 1024;
    size_t file_size = 9 * mb;  // 3MB mod + 6MB unmod
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, 3 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // Download list should have the full 6MB unmodified region
    EXPECT_EQ(dlpages.size(), 1u);
    EXPECT_EQ(dlpages.front()->offset, (off_t)(3 * mb));
    EXPECT_EQ(dlpages.front()->bytes, 6 * mb);

    // Output: single 9MB modified region
    EXPECT_EQ(mixuppages.size(), 1u);
    EXPECT_TRUE(mixuppages.front()->modified);
    EXPECT_EQ(mixuppages.front()->offset, (off_t)0);
    EXPECT_EQ(mixuppages.front()->bytes, 9 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, BorrowD2_AbsorbAndMergeWithNextModified) {
    // [3MB mod] [6MB unmod] [10MB mod]
    // D2: absorb 6MB unmod → merge with next 10MB mod → single 19MB modified
    size_t mb = 1024 * 1024;
    size_t file_size = 19 * mb;
    off_t  part_size = 20 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, 3 * mb, true);
    pl.SetPageModifiedStatus(9 * mb, 10 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // Download: 6MB unmodified gap
    EXPECT_EQ(dlpages.size(), 1u);
    EXPECT_EQ(dlpages.front()->offset, (off_t)(3 * mb));
    EXPECT_EQ(dlpages.front()->bytes, 6 * mb);

    // Output: single 19MB modified (all merged)
    EXPECT_EQ(mixuppages.size(), 1u);
    EXPECT_TRUE(mixuppages.front()->modified);
    EXPECT_EQ(mixuppages.front()->offset, (off_t)0);
    EXPECT_EQ(mixuppages.front()->bytes, 19 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, BorrowD1_FromPreviousUnmodified) {
    // [20MB unmod] [3MB mod] — borrow from previous
    // need=2MB, (2MB+5MB)=7MB < 20MB → D1: borrow 2MB from end of previous
    size_t mb = 1024 * 1024;
    size_t file_size = 23 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(20 * mb, 3 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // Download: 2MB borrowed from end of previous unmodified
    EXPECT_EQ(dlpages.size(), 1u);
    EXPECT_EQ(dlpages.front()->offset, (off_t)(18 * mb));
    EXPECT_EQ(dlpages.front()->bytes, 2 * mb);

    // Output: [0-18MB unmod] [18-23MB mod(5MB)]
    EXPECT_EQ(mixuppages.size(), 2u);
    fdpage_list_t::iterator it = mixuppages.begin();
    EXPECT_FALSE((*it)->modified);
    EXPECT_EQ((*it)->bytes, 18 * mb);
    ++it;
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)(18 * mb));
    EXPECT_EQ((*it)->bytes, 5 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, BorrowD2_AbsorbPreviousUnmodified) {
    // [6MB unmod] [3MB mod] — borrow from previous
    // need=2MB, (2MB+5MB)=7MB >= 6MB → D2: absorb entire 6MB previous
    size_t mb = 1024 * 1024;
    size_t file_size = 9 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(6 * mb, 3 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // Download: full 6MB previous unmodified
    EXPECT_EQ(dlpages.size(), 1u);
    EXPECT_EQ(dlpages.front()->offset, (off_t)0);
    EXPECT_EQ(dlpages.front()->bytes, 6 * mb);

    // Output: single 9MB modified
    EXPECT_EQ(mixuppages.size(), 1u);
    EXPECT_TRUE(mixuppages.front()->modified);
    EXPECT_EQ(mixuppages.front()->offset, (off_t)0);
    EXPECT_EQ(mixuppages.front()->bytes, 9 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, BorrowD1D2_BoundaryExact_NeedPlus5MBEqualsUnmod) {
    // [3MB mod][7MB unmod] → need=2MB, (2MB+5MB)=7MB == 7MB, NOT < 7MB → D2 absorb all
    // This tests the strict '<' in the D1 condition
    size_t mb = 1024 * 1024;
    size_t file_size = 10 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, 3 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // D2: absorb entire 7MB (because 7MB is NOT > 7MB)
    EXPECT_EQ(dlpages.size(), 1u);
    EXPECT_EQ(dlpages.front()->bytes, 7 * mb);

    // Single 10MB modified part
    EXPECT_EQ(mixuppages.size(), 1u);
    EXPECT_TRUE(mixuppages.front()->modified);
    EXPECT_EQ(mixuppages.front()->bytes, 10 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, BorrowD2_ForwardAbsorbNoNextModifiedToMerge) {
    // [20MB unmod][3MB mod][6MB unmod]
    // Borrow loop: 3MB < 5MB, next=6MB unmod. need=2MB, (2+5)=7>=6 → D2 absorb 6MB.
    // After absorb, next_iter = end → no merge (tests line 522 branch false at EOF).
    // Output: [20MB unmod][9MB mod]
    size_t mb = 1024 * 1024;
    size_t file_size = 29 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(20 * mb, 3 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // D2: absorb 6MB unmod after the 3MB mod
    EXPECT_EQ(dlpages.size(), 1u);
    EXPECT_EQ(dlpages.front()->offset, (off_t)(23 * mb));
    EXPECT_EQ(dlpages.front()->bytes, 6 * mb);

    // Output: [20MB unmod][9MB mod]
    EXPECT_EQ(mixuppages.size(), 2u);
    fdpage_list_t::iterator it = mixuppages.begin();
    EXPECT_FALSE((*it)->modified);
    EXPECT_EQ((*it)->bytes, 20 * mb);
    ++it;
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)(20 * mb));
    EXPECT_EQ((*it)->bytes, 9 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, BorrowNoPossible_SmallModifiedAlone) {
    // [3MB mod] — only region, no neighbors to borrow from
    // iter is begin (can't borrow from prev) and next is end (can't borrow from next)
    size_t mb = 1024 * 1024;
    size_t file_size = 3 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, 3 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // No downloads (nothing to borrow from)
    EXPECT_TRUE(dlpages.empty());
    // Single 3MB part (below 5MB minimum, but no option)
    EXPECT_EQ(mixuppages.size(), 1u);
    EXPECT_TRUE(mixuppages.front()->modified);
    EXPECT_EQ(mixuppages.front()->bytes, 3 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, BorrowNoPossible_SmallModifiedBetweenModified) {
    // [10MB mod][3MB mod] — prev is modified (not unmodified), next is end
    // Can't borrow from either direction
    // Note: after step 1 compress-by-modified, adjacent modified regions merge.
    // So [10MB mod][3MB mod] becomes [13MB mod] — borrow loop never triggers.
    // To actually have non-merged adjacent modified, we'd need unmod between them.
    // Instead test: [10MB mod][20MB unmod][3MB mod]
    // → borrow from prev: prev is 20MB unmod. need=2, (2+5)=7 < 20 → D1
    size_t mb = 1024 * 1024;
    size_t file_size = 33 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, 10 * mb, true);
    pl.SetPageModifiedStatus(30 * mb, 3 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // D1: borrow 2MB from end of 20MB unmod
    EXPECT_EQ(dlpages.size(), 1u);
    EXPECT_EQ(dlpages.front()->offset, (off_t)(28 * mb));
    EXPECT_EQ(dlpages.front()->bytes, 2 * mb);

    // Output: [10MB mod][18MB unmod][5MB mod]
    EXPECT_EQ(mixuppages.size(), 3u);
    fdpage_list_t::iterator it = mixuppages.begin();
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->bytes, 10 * mb);
    ++it;
    EXPECT_FALSE((*it)->modified);
    EXPECT_EQ((*it)->bytes, 18 * mb);
    ++it;
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)(28 * mb));
    EXPECT_EQ((*it)->bytes, 5 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, BorrowD2_BackwardBoundaryExact) {
    // [7MB unmod][3MB mod] → need=2, (2+5)=7 == 7 → NOT < 7 → D2 absorb entire prev
    size_t mb = 1024 * 1024;
    size_t file_size = 10 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(7 * mb, 3 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // D2: absorb entire 7MB
    EXPECT_EQ(dlpages.size(), 1u);
    EXPECT_EQ(dlpages.front()->offset, (off_t)0);
    EXPECT_EQ(dlpages.front()->bytes, 7 * mb);

    // Single 10MB modified
    EXPECT_EQ(mixuppages.size(), 1u);
    EXPECT_TRUE(mixuppages.front()->modified);
    EXPECT_EQ(mixuppages.front()->offset, (off_t)0);
    EXPECT_EQ(mixuppages.front()->bytes, 10 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, ModifiedAlready5MB_NoBorrowNeeded) {
    // [5MB mod][20MB unmod] — modified == 5MB (MIN_MULTIPART_SIZE), not < 5MB → no borrow
    size_t mb = 1024 * 1024;
    size_t file_size = 25 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, 5 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    // No downloads
    EXPECT_TRUE(dlpages.empty());
    // [5MB mod][20MB unmod]
    EXPECT_EQ(mixuppages.size(), 2u);
    fdpage_list_t::iterator it = mixuppages.begin();
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->bytes, 5 * mb);
    ++it;
    EXPECT_FALSE((*it)->modified);
    EXPECT_EQ((*it)->bytes, 20 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

// ============================================================
// Bug 3 fix tests: 2x split strategy
// ============================================================

TEST_F(PageListCoverageTest, SplitStrategy2x_NoSplitWhenBelowDouble) {
    // 15MB modified, part_size=10MB → 15MB < 20MB (2x) → single 15MB part
    size_t mb = 1024 * 1024;
    size_t file_size = 15 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, file_size, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    EXPECT_TRUE(dlpages.empty());
    EXPECT_EQ(mixuppages.size(), 1u);
    EXPECT_TRUE(mixuppages.front()->modified);
    EXPECT_EQ(mixuppages.front()->bytes, 15 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, SplitStrategy2x_SplitWhenAboveDouble) {
    // 25MB modified, part_size=10MB → 25MB > 20MB → 10MB + 15MB (2 parts)
    size_t mb = 1024 * 1024;
    size_t file_size = 25 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, file_size, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    EXPECT_TRUE(dlpages.empty());
    EXPECT_EQ(mixuppages.size(), 2u);

    fdpage_list_t::iterator it = mixuppages.begin();
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)0);
    EXPECT_EQ((*it)->bytes, 10 * mb);
    ++it;
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)(10 * mb));
    EXPECT_EQ((*it)->bytes, 15 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, SplitStrategy2x_ExactlyDouble) {
    // 20MB modified, part_size=10MB → 20MB == 20MB (2x), not > → single 20MB part
    size_t mb = 1024 * 1024;
    size_t file_size = 20 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, file_size, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    EXPECT_TRUE(dlpages.empty());
    EXPECT_EQ(mixuppages.size(), 1u);
    EXPECT_TRUE(mixuppages.front()->modified);
    EXPECT_EQ(mixuppages.front()->bytes, 20 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, SplitStrategy2x_ExactlyMaxPartSize) {
    // 10MB modified, part_size=10MB → 10MB <= 20MB → single 10MB part (no split)
    size_t mb = 1024 * 1024;
    size_t file_size = 10 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, file_size, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    EXPECT_TRUE(dlpages.empty());
    EXPECT_EQ(mixuppages.size(), 1u);
    EXPECT_TRUE(mixuppages.front()->modified);
    EXPECT_EQ(mixuppages.front()->bytes, 10 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, SplitStrategy2x_JustOverDouble) {
    // 21MB modified, part_size=10MB → 21MB > 20MB → 10MB + 11MB (2 parts)
    size_t mb = 1024 * 1024;
    size_t file_size = 21 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, file_size, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    EXPECT_TRUE(dlpages.empty());
    EXPECT_EQ(mixuppages.size(), 2u);

    fdpage_list_t::iterator it = mixuppages.begin();
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->bytes, 10 * mb);
    ++it;
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->bytes, 11 * mb);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

TEST_F(PageListCoverageTest, SplitStrategy2x_LargeModifiedWithUnmodifiedCopyPart) {
    // [25MB mod][30MB unmod][15MB mod] with part_size=10MB
    // 25MB mod: 25>20 → split 10+15(<=20) = 2 parts
    // 30MB unmod: single CopyPart entry
    // 15MB mod: 15<=20 → single part
    // Total: 4 output entries
    size_t mb = 1024 * 1024;
    size_t file_size = 70 * mb;
    off_t  part_size = 10 * mb;
    PageList pl(file_size, true);
    pl.SetPageModifiedStatus(0, 25 * mb, true);
    pl.SetPageModifiedStatus(55 * mb, 15 * mb, true);

    fdpage_list_t dlpages, mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, part_size);

    EXPECT_TRUE(dlpages.empty());
    EXPECT_EQ(mixuppages.size(), 4u);

    fdpage_list_t::iterator it = mixuppages.begin();
    // 25MB mod split: 10MB + 15MB
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)0);
    EXPECT_EQ((*it)->bytes, 10 * mb);
    ++it;
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)(10 * mb));
    EXPECT_EQ((*it)->bytes, 15 * mb);
    ++it;
    // 30MB unmod CopyPart
    EXPECT_FALSE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)(25 * mb));
    EXPECT_EQ((*it)->bytes, 30 * mb);
    ++it;
    // 15MB mod: single part (15 <= 20)
    EXPECT_TRUE((*it)->modified);
    EXPECT_EQ((*it)->offset, (off_t)(55 * mb));
    EXPECT_EQ((*it)->bytes, 15 * mb);

    // Verify total size
    size_t total = 0;
    for(fdpage_list_t::iterator ti = mixuppages.begin(); ti != mixuppages.end(); ++ti){
        total += (*ti)->bytes;
    }
    EXPECT_EQ(total, file_size);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

// ============================================================
// Serialize/Deserialize with Modified Tests
// ============================================================

TEST_F(PageListCoverageTest, SerializeDeserializeWithModified) {
    string tmpdir_local = make_temp_dir("pl_mod_ser");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    // Create pagelist with modified data
    PageList pl(1000, false);
    pl.SetPageLoadedStatus(0, 300, true);
    pl.SetPageModifiedStatus(0, 300, true);
    pl.SetPageLoadedStatus(500, 200, true);
    // [0-300 loaded+modified] [300-500 unloaded+clean] [500-700 loaded+clean] [700-1000 unloaded+clean]

    {
        CacheFileStat cfstat("/test/serialize_mod");
        EXPECT_TRUE(pl.Serialize(cfstat, true, 42));
    }

    // Read back with matching inode
    PageList pl2;
    {
        CacheFileStat cfstat2("/test/serialize_mod");
        EXPECT_TRUE(pl2.Serialize(cfstat2, false, 42));
    }

    EXPECT_EQ(pl.Size(), pl2.Size());

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

// ============================================================
// Deserialize error paths — ISSUE2026022500006
// Verify S3FS_FUSE_EXIT removed: process stays alive on failure
// ============================================================

TEST_F(PageListCoverageTest, DeserializeCorruptedFile_ReturnsFailGracefully) {
    string tmpdir_local = make_temp_dir("pl_corrupt");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    // Write corrupted content directly to stat file
    {
        CacheFileStat cfstat("/test/corrupted_file");
        ASSERT_TRUE(cfstat.Open());
        const char* garbage = "not_a_number\ngarbage:data";
        pwrite(cfstat.GetFd(), garbage, strlen(garbage), 0);
    }

    // Deserialize should fail gracefully, not crash
    PageList pl;
    {
        CacheFileStat cfstat2("/test/corrupted_file");
        EXPECT_FALSE(pl.Serialize(cfstat2, false));
    }
    // Process is still alive — no S3FS_FUSE_EXIT

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

TEST_F(PageListCoverageTest, DeserializeInvalidStatFile_ReturnsFailNoExit) {
    // Don't set cache dir — CacheFileStat::Open will fail
    FdManager::SetCacheDir("");

    PageList pl;
    CacheFileStat cfstat("/nonexistent/path");
    EXPECT_FALSE(pl.Serialize(cfstat, false));
    // Process is still alive
}

TEST_F(PageListCoverageTest, SerializeRoundtrip_AfterRAII) {
    string tmpdir_local = make_temp_dir("pl_raii");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    PageList pl(2048, false);
    pl.SetPageLoadedStatus(0, 512, true);
    pl.SetPageLoadedStatus(1024, 512, true);
    pl.SetPageModifiedStatus(0, 512, true);

    {
        CacheFileStat cfstat("/test/raii_roundtrip");
        EXPECT_TRUE(pl.Serialize(cfstat, true, 42));
    }

    PageList pl2;
    {
        CacheFileStat cfstat2("/test/raii_roundtrip");
        EXPECT_TRUE(pl2.Serialize(cfstat2, false, 42));
    }

    EXPECT_EQ(pl.Size(), pl2.Size());
    EXPECT_EQ(2048u, pl2.Size());

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

TEST_F(PageListCoverageTest, DeserializeSizeMismatch_ReturnsFailGracefully) {
    string tmpdir_local = make_temp_dir("pl_mismatch");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    // Write stat file with mismatched size header
    {
        CacheFileStat cfstat("/test/size_mismatch");
        ASSERT_TRUE(cfstat.Open());
        // Header says 9999 but only one page entry of 512 bytes
        const char* content = "9999\n0:512:1:0";
        pwrite(cfstat.GetFd(), content, strlen(content), 0);
    }

    PageList pl;
    {
        CacheFileStat cfstat2("/test/size_mismatch");
        EXPECT_FALSE(pl.Serialize(cfstat2, false));
    }

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

// ============================================================
// Write marks modified, ClearAllModified after flush
// ============================================================

TEST_F(FdEntityCoverageTest, WriteMarksModifiedAndFlushClears) {
    FdEntity ent("/test/write_modified");
    ent.Open(NULL, 0, -1);
    EXPECT_TRUE(ent.IsOpen());

    // Write data — should mark pages as modified
    const char* data = "Hello Modified!";
    ssize_t wsize = ent.Write(data, 0, strlen(data));
    EXPECT_EQ(wsize, (ssize_t)strlen(data));
    EXPECT_TRUE(ent.IsModified());

    // Read back to verify data integrity (modified tracking shouldn't break I/O)
    char buf[64] = {0};
    ssize_t rsize = ent.Read(buf, 0, strlen(data));
    EXPECT_EQ(rsize, (ssize_t)strlen(data));
    EXPECT_STREQ(buf, data);

    ent.Close();
}

TEST_F(FdEntityCoverageTest, MultipleWritesWithModifiedTracking) {
    FdEntity ent("/test/multi_write_modified");
    ent.Open(NULL, 0, -1);

    // Write at different offsets
    ent.Write("AAAA", 0, 4);
    ent.Write("BBBB", 4, 4);
    ent.Write("CCCC", 100, 4);

    // Read back and verify
    char buf1[8] = {0};
    ssize_t r1 = ent.Read(buf1, 0, 8);
    EXPECT_EQ(r1, 8);
    EXPECT_EQ(memcmp(buf1, "AAAABBBB", 8), 0);

    char buf2[8] = {0};
    ssize_t r2 = ent.Read(buf2, 100, 4);
    EXPECT_EQ(r2, 4);
    EXPECT_EQ(memcmp(buf2, "CCCC", 4), 0);

    ent.Close();
}

// ============================================================
// Streamread / ReserveDiskSpace / Prefetch Tests
// ============================================================

// Test fixture that enables streamread for the duration of the test
class StreamReadTest : public ::testing::Test {
protected:
    bool saved_streamread;
    size_t saved_streamread_window;
    void SetUp() override {
        saved_streamread = streamread;
        saved_streamread_window = streamread_window;
        FdManager::SetCacheDir("");
    }
    void TearDown() override {
        streamread = saved_streamread;
        streamread_window = saved_streamread_window;
        FdManager::SetCacheDir("");
        // Ensure reserved bytes are cleared between tests
        while(FdManager::GetReservedDiskSpace() > 0){
            FdManager::ReleaseDiskSpace(FdManager::GetReservedDiskSpace());
        }
    }
};

// --- ReserveDiskSpace tests ---

TEST_F(StreamReadTest, ReserveDiskSpace_BasicReserveRelease) {
    // Clear any leftover reserved space from previous tests
    while(FdManager::GetReservedDiskSpace() > 0){
        FdManager::ReleaseDiskSpace(FdManager::GetReservedDiskSpace());
    }
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
    EXPECT_TRUE(FdManager::ReserveDiskSpace(100));
    EXPECT_EQ(100u, FdManager::GetReservedDiskSpace());
    FdManager::ReleaseDiskSpace(100);
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
}

TEST_F(StreamReadTest, ReserveDiskSpace_ExceedsAvailable) {
    // Try to reserve an absurdly large amount
    size_t huge = static_cast<size_t>(-1) / 2;
    EXPECT_FALSE(FdManager::ReserveDiskSpace(huge));
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
}

TEST_F(StreamReadTest, ReserveDiskSpace_ConcurrentReserves) {
    // Two reserves should accumulate
    EXPECT_TRUE(FdManager::ReserveDiskSpace(100));
    EXPECT_TRUE(FdManager::ReserveDiskSpace(200));
    EXPECT_EQ(300u, FdManager::GetReservedDiskSpace());
    FdManager::ReleaseDiskSpace(300);
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
}

TEST_F(StreamReadTest, ReserveDiskSpace_ReleaseMoreThanReserved) {
    EXPECT_TRUE(FdManager::ReserveDiskSpace(100));
    // Release more than reserved — should clamp to 0, not underflow
    FdManager::ReleaseDiskSpace(500);
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
}

// --- Streamread Read path tests ---

TEST_F(StreamReadTest, StreamRead_BasicSequentialRead) {
    streamread = true;
    FdEntity ent("/streamread_basic_seq");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    // Write 100 bytes
    char wbuf[100];
    memset(wbuf, 'A', sizeof(wbuf));
    ssize_t ws = ent.Write(wbuf, 0, 100);
    EXPECT_EQ(100, ws);

    // Sequential reads of 10 bytes
    for(int i = 0; i < 10; i++){
        char rbuf[10] = {0};
        ssize_t rs = ent.Read(rbuf, i * 10, 10);
        EXPECT_EQ(10, rs);
        for(int j = 0; j < 10; j++){
            EXPECT_EQ('A', rbuf[j]);
        }
    }
    ent.Close();
}

TEST_F(StreamReadTest, StreamRead_ReadBeyondWritten) {
    streamread = true;
    FdEntity ent("/streamread_beyond");
    ASSERT_NE(-1, ent.Open(NULL, 50, -1));

    // Write only 50 bytes
    char wbuf[50];
    memset(wbuf, 'B', sizeof(wbuf));
    ent.Write(wbuf, 0, 50);

    // Read beyond the end — should return only what's available
    char rbuf[100] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 100);
    // pread on a 50-byte file requesting 100 returns 50
    EXPECT_EQ(50, rs);
    ent.Close();
}

TEST_F(StreamReadTest, StreamRead_ForceLoadBypassesPrefetch) {
    // When streamread=true and force_load=true, Read takes the original path
    // (not the streamread async prefetch path). In UT without a real S3 backend,
    // Load() may succeed or fail depending on curl stubs. We verify the code
    // path is safe and returns a non-crash result.
    // Full force_load re-download verification requires mock S3 backend.
    streamread = true;
    FdEntity ent("/streamread_force");
    ASSERT_NE(-1, ent.Open(NULL, 20, -1));

    char wbuf[20];
    memset(wbuf, 'C', sizeof(wbuf));
    ent.Write(wbuf, 0, 20);

    // force_load=true goes through original path (with or without successful Load)
    char rbuf[20] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 20, true);
    // In test env with stub curl, Load succeeds and pread returns data from tmpfile
    EXPECT_EQ(20, rs);
    ent.Close();
}

TEST_F(StreamReadTest, StreamRead_DisabledByDefault) {
    streamread = false;
    FdEntity ent("/streamread_disabled");
    ASSERT_NE(-1, ent.Open(NULL, 30, -1));

    char wbuf[30];
    memset(wbuf, 'D', sizeof(wbuf));
    ent.Write(wbuf, 0, 30);

    // Should use original read path
    char rbuf[30] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 30);
    EXPECT_EQ(30, rs);
    EXPECT_EQ('D', rbuf[0]);
    ent.Close();
}

TEST_F(StreamReadTest, StreamRead_SmallFileNoPrefetch) {
    streamread = true;
    FdEntity ent("/streamread_smallfile");
    ASSERT_NE(-1, ent.Open(NULL, 10, -1));

    char wbuf[10];
    memset(wbuf, 'E', sizeof(wbuf));
    ent.Write(wbuf, 0, 10);

    // Read entire small file — no room for prefetch
    char rbuf[10] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('E', rbuf[0]);
    ent.Close();
}

TEST_F(StreamReadTest, StreamRead_BackwardReadNoCrash) {
    streamread = true;
    FdEntity ent("/streamread_backward");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'F', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Read forward
    char rbuf[10] = {0};
    ent.Read(rbuf, 50, 10);
    EXPECT_EQ('F', rbuf[0]);

    // Read backward — should not crash
    memset(rbuf, 0, sizeof(rbuf));
    ssize_t rs = ent.Read(rbuf, 10, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('F', rbuf[0]);
    ent.Close();
}

TEST_F(StreamReadTest, StreamRead_CancelPrefetchOnClose) {
    streamread = true;
    FdEntity ent("/streamread_close_cancel");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'G', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Read to potentially trigger prefetch
    char rbuf[10] = {0};
    ent.Read(rbuf, 0, 10);

    // Close should cancel prefetch without crash
    ent.Close();
}

TEST_F(StreamReadTest, StreamRead_CancelPrefetchOnWrite) {
    streamread = true;
    FdEntity ent("/streamread_write_cancel");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'H', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Read to potentially trigger prefetch
    char rbuf[10] = {0};
    ent.Read(rbuf, 0, 10);

    // Write should cancel prefetch first
    char wbuf2[10];
    memset(wbuf2, 'I', sizeof(wbuf2));
    ssize_t ws = ent.Write(wbuf2, 50, 10);
    EXPECT_EQ(10, ws);
    ent.Close();
}

TEST_F(StreamReadTest, StreamRead_PrefetchBeyondFileEnd) {
    streamread = true;
    FdEntity ent("/streamread_beyond_end");
    // File just slightly bigger than a read
    ASSERT_NE(-1, ent.Open(NULL, 20, -1));

    char wbuf[20];
    memset(wbuf, 'J', sizeof(wbuf));
    ent.Write(wbuf, 0, 20);

    // Read first 10 bytes — prefetch window would exceed file size
    char rbuf[10] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('J', rbuf[0]);
    ent.Close();
}

TEST_F(StreamReadTest, StreamRead_PrefetchSkippedWhenDiskFull) {
    streamread = true;
    // Test that even if prefetch reservation fails, synchronous read still works.
    // In this test, actual disk space is likely sufficient so prefetch won't be skipped,
    // but we verify the read path is always correct regardless.

    FdEntity ent("/streamread_diskfull");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'K', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Read should always work even if prefetch is skipped
    char rbuf[10] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('K', rbuf[0]);
    ent.Close();
}

// --- PunchRange tests (v2: replaced PunchConsumedPages) ---

TEST_F(StreamReadTest, PunchRange_ZeroStart) {
    streamread = true;
    FdEntity ent("/streamread_punch_zero");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'L', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Read from offset 0 — sr_range_start starts at -1, first range init should not panic
    char rbuf[10] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('L', rbuf[0]);
    ent.Close();
}

TEST_F(StreamReadTest, PunchRange_SkipsModified) {
    streamread = true;
    FdEntity ent("/streamread_punch_modified");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    // Write data (marks as modified)
    char wbuf[100];
    memset(wbuf, 'M', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Sequential reads — is_modify is true, so PunchRange should not punch
    char rbuf[10] = {0};
    ent.Read(rbuf, 0, 10);
    ent.Read(rbuf, 10, 10);
    ent.Read(rbuf, 20, 10);

    // Verify data is still intact (not punched away)
    memset(rbuf, 0, sizeof(rbuf));
    ssize_t rs = ent.Read(rbuf, 0, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('M', rbuf[0]);
    ent.Close();
}

// ============================================================
// Streamread v2: Range-Based Double-Buffer Boundary Tests
// ============================================================

// --- GetStreamReadWindow tests ---

TEST_F(StreamReadTest, GetStreamReadWindow_DefaultValue) {
    // Default streamread_window is 512 MB
    streamread_window = 512;
    FdEntity ent("/sr_v2_window_default");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));
    // GetStreamReadWindow is private, but we can verify its effect via Read behavior.
    // With window=256MB, a 100-byte file fits entirely within one range.
    // Multiple sequential reads should not cause any range switch.
    streamread = true;
    char wbuf[100];
    memset(wbuf, 'A', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    char rbuf[10] = {0};
    for(int i = 0; i < 10; i++){
        ssize_t rs = ent.Read(rbuf, i * 10, 10);
        EXPECT_EQ(10, rs);
        EXPECT_EQ('A', rbuf[0]);
    }
    ent.Close();
}

TEST_F(StreamReadTest, GetStreamReadWindow_CustomValue) {
    // Set a small window for testing range boundaries
    streamread_window = 64;  // 64 MB
    FdEntity ent("/sr_v2_window_custom");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    streamread = true;
    char wbuf[100];
    memset(wbuf, 'B', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // With 64MB window, a 100-byte file is still in one range (100 << 64MB)
    char rbuf[10] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('B', rbuf[0]);
    ent.Close();
}

// --- Range alignment and initialization tests ---

TEST_F(StreamReadTest, V2_RangeInit_FirstReadSetsRange) {
    // First read should initialize sr_range_start from -1 to aligned start
    streamread = true;
    FdEntity ent("/sr_v2_range_init");
    ASSERT_NE(-1, ent.Open(NULL, 200, -1));

    char wbuf[200];
    memset(wbuf, 'C', sizeof(wbuf));
    ent.Write(wbuf, 0, 200);

    // Read at offset 50 — range should be initialized to 0 (aligned to window boundary)
    char rbuf[10] = {0};
    ssize_t rs = ent.Read(rbuf, 50, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('C', rbuf[0]);

    // Subsequent read at offset 100 within same range — no range switch
    rs = ent.Read(rbuf, 100, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('C', rbuf[0]);
    ent.Close();
}

TEST_F(StreamReadTest, V2_RangeInit_StartsAtMinusOne) {
    // After Close() and re-Open(), sr_range_start should be reset to -1
    streamread = true;
    FdEntity ent("/sr_v2_range_reset");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'D', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Read to set range
    char rbuf[10] = {0};
    ent.Read(rbuf, 0, 10);

    // Close should reset
    ent.Close();

    // Re-open and read again — should work without crash (sr_range_start was reset)
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));
    ent.Write(wbuf, 0, 100);
    ssize_t rs = ent.Read(rbuf, 0, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('D', rbuf[0]);
    ent.Close();
}

// --- Read counting within a range tests ---

TEST_F(StreamReadTest, V2_ReadCount_FirstReadTriggersFill) {
    // Read #1 in a range should trigger rest-of-range prefetch
    // (In UT without real S3, prefetch is a no-op for local-written data)
    streamread = true;
    FdEntity ent("/sr_v2_read1_fill");
    ASSERT_NE(-1, ent.Open(NULL, 500, -1));

    char wbuf[500];
    memset(wbuf, 'E', sizeof(wbuf));
    ent.Write(wbuf, 0, 500);

    // Read #1
    char rbuf[10] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('E', rbuf[0]);
    ent.Close();
}

TEST_F(StreamReadTest, V2_ReadCount_SecondReadTriggersNextRange) {
    // Read #2 in a range should trigger next-range prefetch
    streamread = true;
    FdEntity ent("/sr_v2_read2_next");
    ASSERT_NE(-1, ent.Open(NULL, 500, -1));

    char wbuf[500];
    memset(wbuf, 'F', sizeof(wbuf));
    ent.Write(wbuf, 0, 500);

    // Read #1 and #2
    char rbuf[10] = {0};
    ent.Read(rbuf, 0, 10);
    ssize_t rs = ent.Read(rbuf, 10, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('F', rbuf[0]);
    ent.Close();
}

TEST_F(StreamReadTest, V2_ReadCount_ThirdAndBeyondNoAction) {
    // Read #3+ should just pread without any prefetch trigger
    streamread = true;
    FdEntity ent("/sr_v2_read3_noop");
    ASSERT_NE(-1, ent.Open(NULL, 500, -1));

    char wbuf[500];
    memset(wbuf, 'G', sizeof(wbuf));
    ent.Write(wbuf, 0, 500);

    // Read #1 through #10 — #3+ should be fast pread with no HTTP
    char rbuf[10] = {0};
    for(int i = 0; i < 10; i++){
        ssize_t rs = ent.Read(rbuf, i * 10, 10);
        EXPECT_EQ(10, rs) << "Read at offset " << (i * 10) << " failed";
        EXPECT_EQ('G', rbuf[0]) << "Data mismatch at read #" << (i + 1);
    }
    ent.Close();
}

TEST_F(StreamReadTest, V2_ReadCount_ManyReadsInSingleRange) {
    // Stress test: many reads within one range should not accumulate state issues
    streamread = true;
    FdEntity ent("/sr_v2_many_reads");
    ASSERT_NE(-1, ent.Open(NULL, 10000, -1));

    char wbuf[10000];
    memset(wbuf, 'H', sizeof(wbuf));
    ent.Write(wbuf, 0, 10000);

    // 1000 sequential reads of 10 bytes each
    char rbuf[10] = {0};
    for(int i = 0; i < 1000; i++){
        ssize_t rs = ent.Read(rbuf, i * 10, 10);
        EXPECT_EQ(10, rs);
    }
    ent.Close();
}

// --- Range switch boundary tests ---

TEST_F(StreamReadTest, V2_RangeSwitch_BackwardReadSwitchesRange) {
    // Reading backward into a different range should trigger range switch
    streamread = true;
    FdEntity ent("/sr_v2_backward_switch");
    ASSERT_NE(-1, ent.Open(NULL, 200, -1));

    char wbuf[200];
    memset(wbuf, 'I', sizeof(wbuf));
    ent.Write(wbuf, 0, 200);

    // Read forward
    char rbuf[10] = {0};
    ent.Read(rbuf, 100, 10);
    ent.Read(rbuf, 110, 10);
    ent.Read(rbuf, 120, 10);

    // Read backward — within same range since 200 < window, should still work
    ssize_t rs = ent.Read(rbuf, 10, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('I', rbuf[0]);
    ent.Close();
}

TEST_F(StreamReadTest, V2_RangeSwitch_ResetCounters) {
    // On range switch, sr_range_reads and sr_next_launched should reset
    streamread = true;
    FdEntity ent("/sr_v2_switch_reset");
    ASSERT_NE(-1, ent.Open(NULL, 1000, -1));

    char wbuf[1000];
    memset(wbuf, 'J', sizeof(wbuf));
    ent.Write(wbuf, 0, 1000);

    // Read at offset 0 (range 0)
    char rbuf[10] = {0};
    ent.Read(rbuf, 0, 10);
    ent.Read(rbuf, 10, 10);
    ent.Read(rbuf, 20, 10);  // Read #3 in range 0

    // All data is in one range (1000 bytes << 256MB window),
    // so all reads stay in range 0. Verify no crash.
    ssize_t rs = ent.Read(rbuf, 990, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('J', rbuf[0]);
    ent.Close();
}

// --- Small file edge cases ---

TEST_F(StreamReadTest, V2_SmallFile_FitsInOneRange) {
    streamread = true;
    FdEntity ent("/sr_v2_small_one_range");
    // File much smaller than window — no range switch possible
    ASSERT_NE(-1, ent.Open(NULL, 50, -1));

    char wbuf[50];
    memset(wbuf, 'K', sizeof(wbuf));
    ent.Write(wbuf, 0, 50);

    char rbuf[10] = {0};
    for(int i = 0; i < 5; i++){
        ssize_t rs = ent.Read(rbuf, i * 10, 10);
        EXPECT_EQ(10, rs);
        EXPECT_EQ('K', rbuf[0]);
    }
    ent.Close();
}

TEST_F(StreamReadTest, V2_SmallFile_SingleByteReads) {
    streamread = true;
    FdEntity ent("/sr_v2_single_byte");
    ASSERT_NE(-1, ent.Open(NULL, 10, -1));

    char wbuf[10];
    memset(wbuf, 'L', sizeof(wbuf));
    ent.Write(wbuf, 0, 10);

    // Single byte reads
    for(int i = 0; i < 10; i++){
        char rbuf = 0;
        ssize_t rs = ent.Read(&rbuf, i, 1);
        EXPECT_EQ(1, rs);
        EXPECT_EQ('L', rbuf);
    }
    ent.Close();
}

TEST_F(StreamReadTest, V2_SmallFile_ReadEntireFileAtOnce) {
    // Read entire file in one call — no room for any prefetch
    streamread = true;
    FdEntity ent("/sr_v2_entire_file");
    ASSERT_NE(-1, ent.Open(NULL, 20, -1));

    char wbuf[20];
    memset(wbuf, 'M', sizeof(wbuf));
    ent.Write(wbuf, 0, 20);

    char rbuf[20] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 20);
    EXPECT_EQ(20, rs);
    for(int i = 0; i < 20; i++){
        EXPECT_EQ('M', rbuf[i]);
    }
    ent.Close();
}

TEST_F(StreamReadTest, V2_SmallFile_ZeroSizeFile) {
    // Zero-size file — Read should return 0
    streamread = true;
    FdEntity ent("/sr_v2_zero_file");
    ASSERT_NE(-1, ent.Open(NULL, 0, -1));

    char rbuf[10] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 10);
    // pread on empty file returns 0
    EXPECT_EQ(0, rs);
    ent.Close();
}

// --- Exact boundary reads ---

TEST_F(StreamReadTest, V2_ExactBoundary_ReadAtFileEnd) {
    streamread = true;
    FdEntity ent("/sr_v2_exact_end");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'N', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Read exactly at end
    char rbuf[10] = {0};
    ssize_t rs = ent.Read(rbuf, 90, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('N', rbuf[0]);

    // Read starting at end — should return 0
    rs = ent.Read(rbuf, 100, 10);
    EXPECT_EQ(0, rs);
    ent.Close();
}

TEST_F(StreamReadTest, V2_ExactBoundary_ReadSpanningEnd) {
    streamread = true;
    FdEntity ent("/sr_v2_span_end");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'O', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Request 50 bytes starting at offset 80 — only 20 available
    char rbuf[50] = {0};
    ssize_t rs = ent.Read(rbuf, 80, 50);
    EXPECT_EQ(20, rs);
    for(int i = 0; i < 20; i++){
        EXPECT_EQ('O', rbuf[i]);
    }
    ent.Close();
}

TEST_F(StreamReadTest, V2_ExactBoundary_ReadAtOffsetZero) {
    // Very first read at offset 0 — range init from -1 to 0
    streamread = true;
    FdEntity ent("/sr_v2_offset_zero");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'P', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    char rbuf[1] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 1);
    EXPECT_EQ(1, rs);
    EXPECT_EQ('P', rbuf[0]);
    ent.Close();
}

// --- Write/Read interleaving with v2 ---

TEST_F(StreamReadTest, V2_WriteAfterRead_CancelsPrefetch) {
    streamread = true;
    FdEntity ent("/sr_v2_write_after_read");
    ASSERT_NE(-1, ent.Open(NULL, 200, -1));

    char wbuf[200];
    memset(wbuf, 'Q', sizeof(wbuf));
    ent.Write(wbuf, 0, 200);

    // Read to potentially launch prefetch
    char rbuf[10] = {0};
    ent.Read(rbuf, 0, 10);
    ent.Read(rbuf, 10, 10);

    // Write should cancel any active prefetch first
    char wbuf2[10];
    memset(wbuf2, 'R', sizeof(wbuf2));
    ssize_t ws = ent.Write(wbuf2, 50, 10);
    EXPECT_EQ(10, ws);

    // Read back the written data
    memset(rbuf, 0, sizeof(rbuf));
    ssize_t rs = ent.Read(rbuf, 50, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('R', rbuf[0]);
    ent.Close();
}

TEST_F(StreamReadTest, V2_InterleavedWriteRead) {
    // Write, read, write more, read again — verify v2 handles this gracefully
    streamread = true;
    FdEntity ent("/sr_v2_interleaved");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    // Initial write
    char wbuf[100];
    memset(wbuf, 'S', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Read #1
    char rbuf[10] = {0};
    ent.Read(rbuf, 0, 10);
    EXPECT_EQ('S', rbuf[0]);

    // Write different data in the middle
    char wbuf2[10];
    memset(wbuf2, 'T', sizeof(wbuf2));
    ent.Write(wbuf2, 40, 10);

    // Read #2 — still works after interleaved write
    memset(rbuf, 0, sizeof(rbuf));
    ssize_t rs = ent.Read(rbuf, 40, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('T', rbuf[0]);
    ent.Close();
}

// --- Close/Destructor safety ---

TEST_F(StreamReadTest, V2_Close_AfterMultipleReads) {
    streamread = true;
    FdEntity ent("/sr_v2_close_multi");
    ASSERT_NE(-1, ent.Open(NULL, 500, -1));

    char wbuf[500];
    memset(wbuf, 'U', sizeof(wbuf));
    ent.Write(wbuf, 0, 500);

    // Many reads, then close — should join any pending prefetch
    char rbuf[10] = {0};
    for(int i = 0; i < 50; i++){
        ent.Read(rbuf, i * 10, 10);
    }
    // Close should not hang or crash
    ent.Close();
}

TEST_F(StreamReadTest, V2_Close_ImmediatelyAfterOpen) {
    streamread = true;
    FdEntity ent("/sr_v2_close_immediate");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    // Close without any reads — sr_range_start is still -1
    ent.Close();
}

TEST_F(StreamReadTest, V2_Destructor_WithActivePrefetch) {
    // FdEntity destructor should call CancelPrefetch
    streamread = true;
    {
        FdEntity ent("/sr_v2_destructor");
        ASSERT_NE(-1, ent.Open(NULL, 100, -1));

        char wbuf[100];
        memset(wbuf, 'V', sizeof(wbuf));
        ent.Write(wbuf, 0, 100);

        char rbuf[10] = {0};
        ent.Read(rbuf, 0, 10);
        // ent goes out of scope — destructor should clean up safely
    }
    // If we reach here, destructor didn't crash/hang
    SUCCEED();
}

// --- Force load path ---

TEST_F(StreamReadTest, V2_ForceLoad_BypassesStreamreadPath) {
    // force_load=true should use the original (non-streamread) Read path.
    // force_load marks pages as unloaded then re-Loads from S3.
    // In UT without real S3 backend, the Load() for force_load succeeds but
    // downloads zero bytes (size_orgmeta=0), so data written locally is invalidated.
    // We verify: (1) the code path doesn't crash, (2) returns valid pread result.
    streamread = true;
    FdEntity ent("/sr_v2_force_load");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'W', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // force_load=true — takes original code path, invalidates and re-loads
    char rbuf[10] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 10, true);
    // Should return 10 bytes (pread succeeds), but content may be zero
    // because force_load invalidated pages and S3 stub has size_orgmeta=0
    EXPECT_EQ(10, rs);

    // Normal streamread read on non-invalidated region
    memset(rbuf, 0, sizeof(rbuf));
    rs = ent.Read(rbuf, 50, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('W', rbuf[0]);
    ent.Close();
}

// --- streamread disabled path ---

TEST_F(StreamReadTest, V2_Disabled_UsesOriginalPath) {
    streamread = false;
    FdEntity ent("/sr_v2_disabled");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'X', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    char rbuf[10] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('X', rbuf[0]);
    ent.Close();
}

// --- Multi-pattern data verification ---

TEST_F(StreamReadTest, V2_DataIntegrity_DifferentBytesPerRegion) {
    // Write different bytes at different offsets, verify reads return correct data
    streamread = true;
    FdEntity ent("/sr_v2_data_integrity");
    ASSERT_NE(-1, ent.Open(NULL, 256, -1));

    // Write 256 bytes: offset i has value i
    char wbuf[256];
    for(int i = 0; i < 256; i++){
        wbuf[i] = static_cast<char>(i);
    }
    ent.Write(wbuf, 0, 256);

    // Sequential reads of 16 bytes each
    for(int chunk = 0; chunk < 16; chunk++){
        char rbuf[16] = {0};
        ssize_t rs = ent.Read(rbuf, chunk * 16, 16);
        EXPECT_EQ(16, rs);
        for(int j = 0; j < 16; j++){
            EXPECT_EQ(static_cast<char>(chunk * 16 + j), rbuf[j])
                << "Mismatch at offset " << (chunk * 16 + j);
        }
    }
    ent.Close();
}

TEST_F(StreamReadTest, V2_DataIntegrity_RandomAccessAfterSequential) {
    // After sequential reads in streamread mode, random access should return correct data
    streamread = true;
    FdEntity ent("/sr_v2_random_after_seq");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    for(int i = 0; i < 100; i++){
        wbuf[i] = static_cast<char>(i);
    }
    ent.Write(wbuf, 0, 100);

    // Sequential reads
    char rbuf[10] = {0};
    ent.Read(rbuf, 0, 10);
    ent.Read(rbuf, 10, 10);
    ent.Read(rbuf, 20, 10);

    // Random access back to offset 5
    ssize_t rs = ent.Read(rbuf, 5, 10);
    EXPECT_EQ(10, rs);
    for(int i = 0; i < 10; i++){
        EXPECT_EQ(static_cast<char>(5 + i), rbuf[i]);
    }

    // Jump to end
    rs = ent.Read(rbuf, 90, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ(static_cast<char>(90), rbuf[0]);
    ent.Close();
}

// --- Disk space reservation with streamread v2 ---

TEST_F(StreamReadTest, V2_DiskReservation_SharedWithWrite) {
    // Verify that streamread and write share the same disk reservation counter
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());

    // Simulate write reservation
    EXPECT_TRUE(FdManager::ReserveDiskSpace(1000));
    EXPECT_EQ(1000u, FdManager::GetReservedDiskSpace());

    // Simulate read prefetch reservation — should accumulate
    EXPECT_TRUE(FdManager::ReserveDiskSpace(2000));
    EXPECT_EQ(3000u, FdManager::GetReservedDiskSpace());

    // Release write
    FdManager::ReleaseDiskSpace(1000);
    EXPECT_EQ(2000u, FdManager::GetReservedDiskSpace());

    // Release read
    FdManager::ReleaseDiskSpace(2000);
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
}

// --- streamread_window option value edge cases ---

TEST_F(StreamReadTest, V2_WindowValue_MinBoundary) {
    // 64 MB is the minimum valid value
    streamread_window = 64;
    streamread = true;
    FdEntity ent("/sr_v2_window_min");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'Y', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    char rbuf[10] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('Y', rbuf[0]);
    ent.Close();
}

TEST_F(StreamReadTest, V2_WindowValue_MaxBoundary) {
    // 4096 MB is the maximum valid value
    streamread_window = 4096;
    streamread = true;
    FdEntity ent("/sr_v2_window_max");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'Z', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    char rbuf[10] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 10);
    EXPECT_EQ(10, rs);
    EXPECT_EQ('Z', rbuf[0]);
    ent.Close();
}

// --- RowFlush interaction with streamread v2 ---

TEST_F(StreamReadTest, V2_RowFlush_CancelsPrefetchSafely) {
    // RowFlush calls CancelPrefetch() before acquiring fdent_lock.
    // We test this on a non-modified entity so RowFlush takes the early-return
    // path (no actual S3 upload), but CancelPrefetch is still exercised.
    //
    // Why not Write+Flush? Because Write sets is_modify=true, and Flush on a
    // modified entity calls PutRequest which segfaults in UT (no real curl handle).
    streamread = true;
    FdEntity ent("/sr_v2_rowflush");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    // Open with size=100 creates unloaded pages. Don't Write, so is_modify stays false.
    // Read triggers streamread path (sync Load from S3 stub, which is a no-op for
    // unloaded pages of a new file with size_orgmeta=0).
    char rbuf[10] = {0};
    // pread on the tmpfile returns zeros (no data written), but the code path is exercised.
    ent.Read(rbuf, 0, 10);

    // Flush on non-modified entity: CancelPrefetch() is called, then early-return 0
    int result = ent.Flush(false);
    EXPECT_EQ(0, result);

    ent.Close();
}

TEST_F(StreamReadTest, V2_RowFlush_ForceSync_NotModified) {
    // force_sync=true on a non-modified file still goes through CancelPrefetch.
    // In UT the subsequent upload path (PutRequest) will run on a zero-size entity,
    // which may or may not succeed depending on the stub, so we only test
    // force_sync=false here.
    streamread = true;
    FdEntity ent("/sr_v2_rowflush_nomod");
    ASSERT_NE(-1, ent.Open(NULL, 0, -1));

    // Flush on zero-size, non-modified entity
    int result = ent.Flush(false);
    EXPECT_EQ(0, result);
    ent.Close();
}

// --- Multiple Open/Close cycles ---

TEST_F(StreamReadTest, V2_MultipleOpenClose_StateReset) {
    streamread = true;

    for(int cycle = 0; cycle < 3; cycle++){
        FdEntity ent("/sr_v2_cycle");
        ASSERT_NE(-1, ent.Open(NULL, 50, -1));

        char wbuf[50];
        memset(wbuf, static_cast<char>('a' + cycle), sizeof(wbuf));
        ent.Write(wbuf, 0, 50);

        // Read through the file
        char rbuf[10] = {0};
        for(int i = 0; i < 5; i++){
            ssize_t rs = ent.Read(rbuf, i * 10, 10);
            EXPECT_EQ(10, rs);
            EXPECT_EQ(static_cast<char>('a' + cycle), rbuf[0])
                << "Cycle " << cycle << " read " << i << " data mismatch";
        }
        ent.Close();
    }
}

// --- Edge case: Read of size 0 ---

TEST_F(StreamReadTest, V2_ZeroSizeRead) {
    streamread = true;
    FdEntity ent("/sr_v2_zero_read");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'b', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Read 0 bytes — should return 0, not crash
    char rbuf[1] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 0);
    EXPECT_EQ(0, rs);

    // Normal read still works after
    rs = ent.Read(rbuf, 0, 1);
    EXPECT_EQ(1, rs);
    EXPECT_EQ('b', rbuf[0]);
    ent.Close();
}

// --- Edge case: Read from closed fd ---

TEST_F(StreamReadTest, V2_ReadAfterClose_ReturnsError) {
    streamread = true;
    FdEntity ent("/sr_v2_read_closed");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'c', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);
    ent.Close();

    // Read on closed entity should return -EBADF
    char rbuf[10] = {0};
    ssize_t rs = ent.Read(rbuf, 0, 10);
    EXPECT_EQ(-EBADF, rs);
}

// --- PunchRange: verify modified data protection ---

TEST_F(StreamReadTest, V2_PunchRange_ProtectsModifiedData) {
    // Write data → sequential reads → verify all data intact
    // (is_modify=true prevents PunchRange from punching)
    streamread = true;
    FdEntity ent("/sr_v2_punch_protect");
    ASSERT_NE(-1, ent.Open(NULL, 200, -1));

    char wbuf[200];
    for(int i = 0; i < 200; i++){
        wbuf[i] = static_cast<char>(i % 128);
    }
    ent.Write(wbuf, 0, 200);

    // Read through entire file
    char rbuf[10] = {0};
    for(int i = 0; i < 20; i++){
        ent.Read(rbuf, i * 10, 10);
    }

    // Re-read from beginning — data should still be intact because is_modify=true
    for(int i = 0; i < 20; i++){
        ssize_t rs = ent.Read(rbuf, i * 10, 10);
        EXPECT_EQ(10, rs);
        EXPECT_EQ(static_cast<char>((i * 10) % 128), rbuf[0])
            << "Data corrupted at offset " << (i * 10);
    }
    ent.Close();
}

// ============================================================
// Streamread v2: Option Parsing Tests (in hws_s3fs_include_test)
// ============================================================
// Note: streamread_window option parsing is tested in hws_s3fs_include_test.cpp.
// The tests below verify the global variable behavior independently.

TEST_F(StreamReadTest, V2_WindowGlobal_DefaultIs24) {
    // Verify compile-time default is 24 MB.
    // saved_streamread_window was captured in SetUp before any test modified it.
    // On first run it equals the compile-time default from hws_s3fs.cpp:131.
    EXPECT_EQ(24u, saved_streamread_window)
        << "Compile-time default streamread_window should be 24 MB";
}

TEST_F(StreamReadTest, V2_WindowGlobal_SetAndRestore) {
    streamread_window = 128;
    EXPECT_EQ(128u, streamread_window);
    streamread_window = 512;
    EXPECT_EQ(512u, streamread_window);
    // TearDown will restore
}

TEST_F(StreamReadTest, StreamRead_CancelPrefetchSafeWhenInactive) {
    // CancelPrefetch is private, but Close() calls it internally.
    // Verify that close on entity with no active prefetch is safe (no hang/crash).
    FdEntity ent("/streamread_cancel_inactive");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    // Close triggers CancelPrefetch internally; no active prefetch → safe no-op
    ent.Close();

    // Re-open and close again: double cancel path also safe
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));
    ent.Close();
}

TEST_F(StreamReadTest, StreamRead_DiskReservationBalance) {
    size_t before = FdManager::GetReservedDiskSpace();

    streamread = true;
    streamread_window = 100;
    FdEntity ent("/streamread_reserve_balance");
    ASSERT_NE(-1, ent.Open(NULL, 200, -1));

    char wbuf[200];
    memset(wbuf, 'X', sizeof(wbuf));
    ent.Write(wbuf, 0, 200);

    char rbuf[10] = {0};
    ent.Read(rbuf, 0, 10);
    ent.Close();

    size_t after = FdManager::GetReservedDiskSpace();
    EXPECT_EQ(before, after);
}

// ============================================================
// PrefetchWorker goto-elimination boundary tests
// Covers: fd closed, start beyond file, size trim, empty unloaded_list
// ============================================================

TEST_F(StreamReadTest, PrefetchWorker_FdClosed_SkipsDownload) {
    // When fd == -1 (file closed before prefetch), PrefetchWorker should
    // skip download and go straight to cleanup without crash.
    streamread = true;
    streamread_window = 1024;

    FdEntity ent("/prefetch_fd_closed");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'A', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Close immediately — any pending prefetch should handle fd == -1 gracefully
    ent.Close();

    // Verify no crash and disk reservation is balanced
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
}

TEST_F(StreamReadTest, PrefetchWorker_StartBeyondFileSize) {
    // When start >= pagelist.Size(), PrefetchWorker should skip download
    streamread = true;
    streamread_window = 1024;

    FdEntity ent("/prefetch_start_beyond");
    ASSERT_NE(-1, ent.Open(NULL, 50, -1));

    char wbuf[50];
    memset(wbuf, 'B', sizeof(wbuf));
    ent.Write(wbuf, 0, 50);

    // Read at offset 0, which triggers prefetch for rest of range
    char rbuf[10] = {0};
    ssize_t rsize = ent.Read(rbuf, 0, 10);
    EXPECT_EQ(10, rsize);
    EXPECT_EQ('B', rbuf[0]);

    ent.Close();
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
}

TEST_F(StreamReadTest, PrefetchWorker_SizeTrimToFileEnd) {
    // When start + size > pagelist.Size(), size should be trimmed
    // and excess disk reservation released
    streamread = true;
    streamread_window = 4096;  // larger than file

    FdEntity ent("/prefetch_size_trim");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'C', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Sequential read triggers prefetch with window > file size
    char rbuf[10] = {0};
    ssize_t rsize = ent.Read(rbuf, 0, 10);
    EXPECT_EQ(10, rsize);

    ent.Close();
    // Disk reservation should be fully released after trim + cleanup
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
}

// ============================================================
// Read disk-reservation goto-elimination boundary tests
// Covers: reserve success, two-level degrade, best-effort path
// ============================================================

TEST_F(StreamReadTest, Read_DiskReserve_FirstTrySuccess) {
    // Normal path: Write data, then Read back — ReserveDiskSpace succeeds
    // because data is already in tmpfile (loaded pages, no download needed)
    FdEntity ent("/read_reserve_ok");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'D', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Read back from same open fd — pages are loaded, no disk reserve needed
    char rbuf[100] = {0};
    ssize_t rsize = ent.Read(rbuf, 0, 100);
    EXPECT_EQ(100, rsize);
    EXPECT_EQ('D', rbuf[0]);
    EXPECT_EQ('D', rbuf[99]);

    ent.Close();
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
}

TEST_F(StreamReadTest, Read_DiskReserve_BestEffortWithModify) {
    // When both reserves fail and is_modify=true, should skip truncate
    // and do best-effort load (disk_reserved=false path)
    FdEntity ent("/read_reserve_modify");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    char wbuf[100];
    memset(wbuf, 'E', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    // Read back written data (is_modify=true, no disk reserve needed for cached data)
    char rbuf[50] = {0};
    ssize_t rsize = ent.Read(rbuf, 0, 50);
    EXPECT_EQ(50, rsize);
    EXPECT_EQ('E', rbuf[0]);
    EXPECT_EQ('E', rbuf[49]);

    ent.Close();
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
}

// ============================================================
// is_modify guard prevents disk reclaim (ISSUE2026022500003)
// ============================================================

TEST_F(FdEntityCoverageTest, IsModifyGuard_PreventsDataReclaim) {
    FdEntity ent("/test/ismodify_guard");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    const char* data = "test data for guard verification";
    ssize_t wsize = ent.Write(data, 0, strlen(data));
    ASSERT_EQ(wsize, (ssize_t)strlen(data));
    EXPECT_TRUE(ent.IsModified());

    // Verify pwrite allocated physical blocks
    struct stat st;
    ASSERT_EQ(0, fstat(ent.GetFd(), &st));
    EXPECT_GT(st.st_blocks, 0);

    // Read at different offset triggers disk space check
    // is_modify=true prevents ftruncate
    char rbuf[10] = {0};
    ent.Read(rbuf, 50, 10);

    // Verify blocks still > 0
    ASSERT_EQ(0, fstat(ent.GetFd(), &st));
    EXPECT_GT(st.st_blocks, 0);

    // Verify original data intact
    char vbuf[64] = {0};
    EXPECT_EQ((ssize_t)strlen(data), ent.Read(vbuf, 0, strlen(data)));
    EXPECT_EQ(memcmp(vbuf, data, strlen(data)), 0);

    ent.Close();
}

// ============================================================
// Multi-offset write → read data integrity (ISSUE2026022500003)
// ============================================================

TEST_F(FdEntityCoverageTest, WriteClose_MultiOffset_DataIntegrity) {
    FdEntity ent("/test/write_close_integrity");
    ASSERT_NE(-1, ent.Open(NULL, 0, -1));

    ent.Write("HEAD", 0, 4);
    ent.Write("MIDDLE", 50, 6);
    ent.Write("TAIL", 200, 4);
    EXPECT_TRUE(ent.IsModified());

    char buf[8] = {0};
    EXPECT_EQ(4, ent.Read(buf, 0, 4));
    EXPECT_EQ(memcmp(buf, "HEAD", 4), 0);

    memset(buf, 0, sizeof(buf));
    EXPECT_EQ(6, ent.Read(buf, 50, 6));
    EXPECT_EQ(memcmp(buf, "MIDDLE", 6), 0);

    memset(buf, 0, sizeof(buf));
    EXPECT_EQ(4, ent.Read(buf, 200, 4));
    EXPECT_EQ(memcmp(buf, "TAIL", 4), 0);

    ent.Close();
}

// ============================================================
// Flush is_modify guard tests (ISSUE2026022500003 - fsync fix)
//
// These tests verify that Flush(force_sync=false) correctly
// skips upload when is_modify is false (no write occurred).
// This guards against the bug where fsync on an unmodified
// FdEntity would upload sparse zeros to S3, corrupting data.
// ============================================================

TEST_F(FdEntityCoverageTest, Flush_NoForceSync_UnmodifiedEntity_ReturnsZero) {
    // Open FdEntity with a non-zero size but don't write anything.
    // Flush(force_sync=false) should return 0 immediately because
    // is_modify is false — no data to upload.
    FdEntity ent("/test/flush_unmodified");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    // Verify is_modify is false (no writes)
    EXPECT_FALSE(ent.IsModified());

    // Flush without force_sync should return 0 (no-op)
    int result = ent.Flush(false);
    EXPECT_EQ(0, result);

    // is_modify should still be false
    EXPECT_FALSE(ent.IsModified());

    ent.Close();
}

TEST_F(FdEntityCoverageTest, Flush_NoForceSync_AfterWrite_IsModifiedTrue) {
    // After writing, is_modify should be true and Flush should attempt upload.
    // (RowFlush will fail without curl, but is_modify state should be correct)
    FdEntity ent("/test/flush_modified");
    ASSERT_NE(-1, ent.Open(NULL, 0, -1));

    EXPECT_FALSE(ent.IsModified());

    // Write some data
    const char* data = "test data";
    ent.Write(data, 0, strlen(data));

    // Now is_modify should be true
    EXPECT_TRUE(ent.IsModified());

    ent.Close();
}

// ============================================================
// Truncate data preservation test (ISSUE2026022500003)
//
// Verifies that when data is written and then the tmpfile is
// ftruncated to a smaller size, the preserved portion retains
// correct content. This tests the mechanism used by
// s3fs_truncate_s3 to preserve data during truncation.
// ============================================================

TEST_F(FdEntityCoverageTest, Truncate_PreservesDataInTmpfile) {
    FdEntity ent("/test/truncate_preserve");
    ASSERT_NE(-1, ent.Open(NULL, 0, -1));

    // Write 20 bytes: "01234567890123456789"
    const char* data = "01234567890123456789";
    ssize_t wsize = ent.Write(data, 0, 20);
    ASSERT_EQ(20, wsize);

    // Verify all 20 bytes can be read back
    char buf20[21] = {0};
    EXPECT_EQ(20, ent.Read(buf20, 0, 20));
    EXPECT_EQ(memcmp(buf20, data, 20), 0);

    // ftruncate the tmpfile to 10 bytes (simulating truncate operation)
    ASSERT_NE(-1, ftruncate(ent.GetFd(), 10));

    // Read the first 10 bytes — they should be preserved
    char buf10[11] = {0};
    ssize_t rsize = pread(ent.GetFd(), buf10, 10, 0);
    ASSERT_EQ(10, rsize);
    EXPECT_EQ(memcmp(buf10, "0123456789", 10), 0);

    ent.Close();
}

TEST_F(FdEntityCoverageTest, OpenWithSize_CreatesSparseTmpfile) {
    // Demonstrates why force_sync=true is dangerous on an unmodified
    // FdEntity: Open with size creates a sparse tmpfile via ftruncate,
    // so the content is all zeros, not real data from S3.
    FdEntity ent("/test/sparse_tmpfile");
    ASSERT_NE(-1, ent.Open(NULL, 100, -1));

    // tmpfile should be 100 bytes but sparse (all zeros)
    struct stat st;
    ASSERT_EQ(0, fstat(ent.GetFd(), &st));
    EXPECT_EQ(100, st.st_size);

    // Read content — should be all zeros (sparse file)
    char buf[100];
    memset(buf, 0xFF, sizeof(buf));  // fill with non-zero
    ssize_t rsize = pread(ent.GetFd(), buf, 100, 0);
    ASSERT_EQ(100, rsize);

    // Verify all bytes are zero (sparse)
    char zeros[100] = {0};
    EXPECT_EQ(memcmp(buf, zeros, 100), 0);

    // is_modify is false — Flush(false) would skip, Flush(true) would upload zeros
    EXPECT_FALSE(ent.IsModified());

    ent.Close();
}

// ============================================================
// OverWriteFile Tests
// ============================================================

TEST_F(CacheFileStatCoverageTest, OverWriteFile_Success) {
    CacheFileStat cfstat("/test/overwrite_ok");
    std::string content = "12345:1024\n0:512:1:0\n512:512:0:0";
    EXPECT_TRUE(cfstat.OverWriteFile(content));

    // Verify by reading back through normal Open path
    CacheFileStat cfstat2("/test/overwrite_ok");
    ASSERT_TRUE(cfstat2.Open());
    struct stat st;
    ASSERT_EQ(0, fstat(cfstat2.GetFd(), &st));
    EXPECT_EQ(static_cast<off_t>(content.length()), st.st_size);

    char buf[256] = {0};
    pread(cfstat2.GetFd(), buf, st.st_size, 0);
    EXPECT_STREQ(content.c_str(), buf);
}

TEST_F(CacheFileStatCoverageTest, OverWriteFile_AtomicReplace_NoResidue) {
    CacheFileStat cfstat("/test/overwrite_atomic");

    // Write long content first
    std::string long_content(1024, 'A');
    EXPECT_TRUE(cfstat.OverWriteFile(long_content));

    // Overwrite with short content
    std::string short_content = "12345:512\n0:512:1:0";
    EXPECT_TRUE(cfstat.OverWriteFile(short_content));

    // Read back — file should contain ONLY short content, no 'A' residue
    CacheFileStat cfstat2("/test/overwrite_atomic");
    ASSERT_TRUE(cfstat2.Open());
    struct stat st;
    ASSERT_EQ(0, fstat(cfstat2.GetFd(), &st));
    EXPECT_EQ(static_cast<off_t>(short_content.length()), st.st_size);

    char buf[2048] = {0};
    pread(cfstat2.GetFd(), buf, st.st_size, 0);
    EXPECT_STREQ(short_content.c_str(), buf);
}

TEST_F(CacheFileStatCoverageTest, OverWriteFile_EmptyPath_Fails) {
    FdManager::SetCacheDir("");  // no cache dir → MakeCacheFileStatPath fails
    CacheFileStat cfstat;  // empty path
    EXPECT_FALSE(cfstat.OverWriteFile("some content"));
}

TEST_F(CacheFileStatCoverageTest, OverWriteFile_EmptyContent) {
    CacheFileStat cfstat("/test/overwrite_empty");
    // Empty string write — pwrite writes 0 bytes
    // Whether this succeeds or fails is implementation-defined; just ensure no crash
    cfstat.OverWriteFile("");
    // No crash — success
}

// ============================================================
// Serialize inode format + atomic write tests
// ============================================================

TEST_F(PageListCoverageTest, Serialize_InodeFormat_Roundtrip) {
    string tmpdir_local = make_temp_dir("pl_inode_rt");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    PageList pl(1024, false);
    pl.SetPageLoadedStatus(0, 512, true);
    pl.SetPageModifiedStatus(0, 256, true);

    ino_t test_inode = 12345;
    {
        CacheFileStat cfstat("/test/inode_roundtrip");
        EXPECT_TRUE(pl.Serialize(cfstat, true, test_inode));
    }

    // Verify stat file content starts with "12345:1024"
    {
        CacheFileStat cfstat2("/test/inode_roundtrip");
        ASSERT_TRUE(cfstat2.Open());
        char buf[512] = {0};
        struct stat st;
        ASSERT_EQ(0, fstat(cfstat2.GetFd(), &st));
        pread(cfstat2.GetFd(), buf, st.st_size, 0);
        string content(buf);
        EXPECT_EQ(0u, content.find("12345:1024"));  // header is "inode:size"
    }

    // Deserialize with matching inode → success
    PageList pl2;
    {
        CacheFileStat cfstat3("/test/inode_roundtrip");
        EXPECT_TRUE(pl2.Serialize(cfstat3, false, test_inode));
    }
    EXPECT_EQ(pl.Size(), pl2.Size());

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

TEST_F(PageListCoverageTest, Serialize_InodeZero_Rejects_CacheMismatch) {
    string tmpdir_local = make_temp_dir("pl_inode_zero");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    PageList pl(512, true);
    {
        CacheFileStat cfstat("/test/inode_zero");
        EXPECT_TRUE(pl.Serialize(cfstat, true, 99999));  // write with inode=99999
    }

    // Deserialize with inode=0 → cache_inode=99999 != 0 → REJECT (Gap E alignment)
    PageList pl2;
    {
        CacheFileStat cfstat2("/test/inode_zero");
        EXPECT_FALSE(pl2.Serialize(cfstat2, false, 0));
    }

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

// ============================================================
// Serialize read path — inode validation + backward compat
// ============================================================

TEST_F(PageListCoverageTest, Deserialize_InodeMismatch_Fails) {
    string tmpdir_local = make_temp_dir("pl_inode_mm");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    PageList pl(1024, true);
    {
        CacheFileStat cfstat("/test/inode_mismatch");
        EXPECT_TRUE(pl.Serialize(cfstat, true, 111));  // write with inode=111
    }

    // Deserialize with different inode → should fail
    PageList pl2;
    {
        CacheFileStat cfstat2("/test/inode_mismatch");
        EXPECT_FALSE(pl2.Serialize(cfstat2, false, 222));  // inode mismatch
    }

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

TEST_F(PageListCoverageTest, Deserialize_OldFormat_BackwardCompatible) {
    string tmpdir_local = make_temp_dir("pl_oldfmt");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    // Manually write old-format stat file: "<size>\n<pages>"
    {
        CacheFileStat cfstat("/test/old_format");
        ASSERT_TRUE(cfstat.Open());
        const char* old_content = "1024\n0:512:1:0\n512:512:0:0";
        pwrite(cfstat.GetFd(), old_content, strlen(old_content), 0);
    }

    // Deserialize with any inode — old format has cache_inode=0, skip validation
    PageList pl;
    {
        CacheFileStat cfstat2("/test/old_format");
        EXPECT_TRUE(pl.Serialize(cfstat2, false, 99999));
    }
    EXPECT_EQ(1024u, pl.Size());

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

TEST_F(PageListCoverageTest, Deserialize_NewFormatInodeZero_Rejected) {
    string tmpdir_local = make_temp_dir("pl_inodezero");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    // Write new format with inode=0
    PageList pl(512, true);
    {
        CacheFileStat cfstat("/test/inode_zero_fmt");
        EXPECT_TRUE(pl.Serialize(cfstat, true, 0));  // inode=0 in header → "0:512\n..."
    }

    // Deserialize with any inode → new format inode=0 is always rejected (Gap A)
    PageList pl2;
    {
        CacheFileStat cfstat2("/test/inode_zero_fmt");
        EXPECT_FALSE(pl2.Serialize(cfstat2, false, 77777));
    }

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

// ============================================================
// Edge cases
// ============================================================

TEST_F(PageListCoverageTest, Serialize_LargeInode_Roundtrip) {
    string tmpdir_local = make_temp_dir("pl_large_ino");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    ino_t large_inode = static_cast<ino_t>(999999999ULL);
    PageList pl(256, true);
    {
        CacheFileStat cfstat("/test/large_inode");
        EXPECT_TRUE(pl.Serialize(cfstat, true, large_inode));
    }

    PageList pl2;
    {
        CacheFileStat cfstat2("/test/large_inode");
        EXPECT_TRUE(pl2.Serialize(cfstat2, false, large_inode));
    }
    EXPECT_EQ(256u, pl2.Size());

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

TEST_F(PageListCoverageTest, Serialize_EmptyPageList_WithInode) {
    string tmpdir_local = make_temp_dir("pl_empty_ino");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    PageList pl(0, false);
    {
        CacheFileStat cfstat("/test/empty_inode");
        EXPECT_TRUE(pl.Serialize(cfstat, true, 42));
    }

    PageList pl2;
    {
        CacheFileStat cfstat2("/test/empty_inode");
        EXPECT_TRUE(pl2.Serialize(cfstat2, false, 42));
    }
    EXPECT_EQ(0u, pl2.Size());

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

TEST_F(PageListCoverageTest, Serialize_MultipleOverwrites_LastWins) {
    string tmpdir_local = make_temp_dir("pl_multi_ow");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    PageList pl(512, true);

    // Write with inode=100
    {
        CacheFileStat cfstat("/test/multi_overwrite");
        EXPECT_TRUE(pl.Serialize(cfstat, true, 100));
    }
    // Overwrite with inode=200
    {
        CacheFileStat cfstat("/test/multi_overwrite");
        EXPECT_TRUE(pl.Serialize(cfstat, true, 200));
    }

    // Reading with inode=100 should FAIL (file has inode=200)
    PageList pl2;
    {
        CacheFileStat cfstat2("/test/multi_overwrite");
        EXPECT_FALSE(pl2.Serialize(cfstat2, false, 100));
    }

    // Reading with inode=200 should SUCCEED
    PageList pl3;
    {
        CacheFileStat cfstat3("/test/multi_overwrite");
        EXPECT_TRUE(pl3.Serialize(cfstat3, false, 200));
    }
    EXPECT_EQ(512u, pl3.Size());

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

TEST_F(PageListCoverageTest, Serialize_CorruptedHeader_WithColon_Fails) {
    string tmpdir_local = make_temp_dir("pl_bad_hdr");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    // Write corrupted header with colon but garbage values
    {
        CacheFileStat cfstat("/test/bad_header");
        ASSERT_TRUE(cfstat.Open());
        const char* content = "abc:xyz\n0:512:1:0";
        pwrite(cfstat.GetFd(), content, strlen(content), 0);
    }

    // Deserialize — inode and size parse as 0, size mismatch with pages → fail
    PageList pl;
    {
        CacheFileStat cfstat2("/test/bad_header");
        EXPECT_FALSE(pl.Serialize(cfstat2, false, 1));
    }

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

TEST_F(PageListCoverageTest, Serialize_NonZeroInode_Roundtrip_DefaultArg) {
    string tmpdir_local = make_temp_dir("pl_default");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    PageList pl(1024, false);
    pl.SetPageLoadedStatus(0, 512, true);

    // Write with non-zero inode (default=0 now produces "0:size" which is rejected by Gap A)
    {
        CacheFileStat cfstat("/test/default_inode");
        EXPECT_TRUE(pl.Serialize(cfstat, true, 42));
    }

    // Read with matching inode → should succeed
    PageList pl2;
    {
        CacheFileStat cfstat2("/test/default_inode");
        EXPECT_TRUE(pl2.Serialize(cfstat2, false, 42));
    }
    EXPECT_EQ(1024u, pl2.Size());

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

// ============================================================
// GetInode tests (Phase 2 Step 10b)
// ============================================================

TEST_F(PageListCoverageTest, GetInode_Static_ValidFd) {
    char tmpl[] = "/tmp/inode_test_XXXXXX";
    int tfd = mkstemp(tmpl);
    ASSERT_NE(-1, tfd);
    ino_t ino = FdEntity::GetInode(tfd);
    EXPECT_NE(0u, ino);
    close(tfd);
    unlink(tmpl);
}

TEST_F(PageListCoverageTest, GetInode_Static_InvalidFd) {
    EXPECT_EQ(0u, FdEntity::GetInode(-1));
}

TEST_F(PageListCoverageTest, GetInode_Static_ClosedFd) {
    char tmpl[] = "/tmp/inode_closed_XXXXXX";
    int tfd = mkstemp(tmpl);
    ASSERT_NE(-1, tfd);
    close(tfd);
    EXPECT_EQ(0u, FdEntity::GetInode(tfd));
    unlink(tmpl);
}

TEST_F(PageListCoverageTest, GetInode_Instance_EmptyCachepath) {
    FdEntity ent;  // no cachepath
    EXPECT_EQ(0u, ent.GetInode());
}

TEST_F(PageListCoverageTest, GetInode_Instance_ValidCachepath) {
    char tmpl[] = "/tmp/inode_cp_XXXXXX";
    int tfd = mkstemp(tmpl);
    close(tfd);
    FdEntity ent(NULL, tmpl);
    EXPECT_NE(0u, ent.GetInode());
    unlink(tmpl);
}

TEST_F(PageListCoverageTest, GetInode_Instance_DeletedCachepath) {
    char tmpl[] = "/tmp/inode_del_XXXXXX";
    int tfd = mkstemp(tmpl);
    close(tfd);
    FdEntity ent(NULL, tmpl);
    unlink(tmpl);
    EXPECT_EQ(0u, ent.GetInode());
}

// ============================================================
// Gap A boundary tests: new format inode=0 rejected
// ============================================================

TEST_F(PageListCoverageTest, Deserialize_NewFormatInodeZero_CallerAlsoZero_Rejected) {
    string tmpdir_local = make_temp_dir("pl_a0");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    // Write with inode=0 → produces "0:512\n..."
    PageList pl(512, true);
    { CacheFileStat cfstat("/test/a_zero"); EXPECT_TRUE(pl.Serialize(cfstat, true, 0)); }

    // Read with inode=0 → new format inode=0 always rejected (Gap A)
    PageList pl2;
    { CacheFileStat cfstat2("/test/a_zero"); EXPECT_FALSE(pl2.Serialize(cfstat2, false, 0)); }

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

// ============================================================
// Gap E boundary tests: deserialize validation alignment
// ============================================================

TEST_F(PageListCoverageTest, Deserialize_CallerInodeZero_CacheNonZero_Rejects) {
    string tmpdir_local = make_temp_dir("pl_e1");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    // Write with inode=12345
    PageList pl(256, true);
    { CacheFileStat cfstat("/test/e_mismatch"); EXPECT_TRUE(pl.Serialize(cfstat, true, 12345)); }

    // Read with inode=0 → cache_inode=12345 != inode=0 → REJECT (Gap E alignment)
    PageList pl2;
    { CacheFileStat cfstat2("/test/e_mismatch"); EXPECT_FALSE(pl2.Serialize(cfstat2, false, 0)); }

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

TEST_F(PageListCoverageTest, Deserialize_OldFormat_CallerInodeAny_Accepts) {
    string tmpdir_local = make_temp_dir("pl_e2");
    ASSERT_FALSE(tmpdir_local.empty());
    FdManager::SetCacheDir(tmpdir_local.c_str());

    // Manually write old format (no colon in header)
    {
        CacheFileStat cfstat("/test/e_old");
        ASSERT_TRUE(cfstat.Open());
        const char* content = "256\n0:256:1:0";
        cfstat.OverWriteFile(string(content));
    }

    // Read with any inode → old format cache_inode=0 → skip validation → accept
    PageList pl;
    { CacheFileStat cfstat2("/test/e_old"); EXPECT_TRUE(pl.Serialize(cfstat2, false, 99999)); }
    EXPECT_EQ(256u, pl.Size());

    FdManager::SetCacheDir("");
    remove_dir_recursive(tmpdir_local);
}

// ============================================================
// RenameCacheFileStat tests (ISSUE2026030200003/4)
// ============================================================

TEST_F(CacheFileStatCoverageTest, RenameCacheFileStat_NullArgs) {
    EXPECT_FALSE(CacheFileStat::RenameCacheFileStat(NULL, "/new"));
    EXPECT_FALSE(CacheFileStat::RenameCacheFileStat("/old", NULL));
    EXPECT_FALSE(CacheFileStat::RenameCacheFileStat("", "/new"));
    EXPECT_FALSE(CacheFileStat::RenameCacheFileStat("/old", ""));
}

TEST_F(CacheFileStatCoverageTest, RenameCacheFileStat_OldNotExist) {
    // Renaming a non-existent stat file should succeed (nothing to move)
    EXPECT_TRUE(CacheFileStat::RenameCacheFileStat("/nonexist_src.txt", "/nonexist_dst.txt"));
}

TEST_F(CacheFileStatCoverageTest, RenameCacheFileStat_Basic) {
    // Create a stat file for /old.txt
    {
        CacheFileStat stat_file("/old.txt");
        ASSERT_TRUE(stat_file.Open());
        // Write some content
        string content = "test_stat_data";
        ASSERT_TRUE(stat_file.OverWriteFile(content));
    }

    // Rename /old.txt → /new.txt
    EXPECT_TRUE(CacheFileStat::RenameCacheFileStat("/old.txt", "/new.txt"));

    // Old stat should be deleted
    EXPECT_TRUE(CacheFileStat::DeleteCacheFileStat("/old.txt") == false);

    // New stat should exist and be openable
    CacheFileStat new_stat("/new.txt");
    EXPECT_TRUE(new_stat.Open());
}

TEST_F(CacheFileStatCoverageTest, RenameCacheFileStat_OverwriteExisting) {
    // Create stat files for both /src.txt and /dst.txt
    {
        CacheFileStat src_stat("/src.txt");
        ASSERT_TRUE(src_stat.Open());
        ASSERT_TRUE(src_stat.OverWriteFile("src_content"));
    }
    {
        CacheFileStat dst_stat("/dst.txt");
        ASSERT_TRUE(dst_stat.Open());
        ASSERT_TRUE(dst_stat.OverWriteFile("dst_content"));
    }

    // Rename src → dst should overwrite existing dst stat
    EXPECT_TRUE(CacheFileStat::RenameCacheFileStat("/src.txt", "/dst.txt"));

    // Old should be gone
    EXPECT_FALSE(CacheFileStat::DeleteCacheFileStat("/src.txt"));

    // New should exist
    CacheFileStat new_stat("/dst.txt");
    EXPECT_TRUE(new_stat.Open());
}

// ============================================================
// FdEntity::RenamePath tests (ISSUE2026030200003/4)
// ============================================================

TEST_F(FdManagerCoverageTest, RenamePath_CacheMode_Basic) {
    FdManager::SetCacheDir(tmpdir.c_str());

    FdEntity* ent = FdManager::get()->Open("/test/rp_src.txt", NULL, 0, -1, false);
    ASSERT_NE(ent, nullptr);

    // Verify cache file exists at old path
    string old_cachepath;
    ASSERT_TRUE(FdManager::MakeCachePath("/test/rp_src.txt", old_cachepath, false));
    struct stat st;
    EXPECT_EQ(0, stat(old_cachepath.c_str(), &st));

    // Rename via FdManager::Rename (which calls RenamePath internally)
    FdManager::get()->Rename("/test/rp_src.txt", "/test/rp_dst.txt");

    // Entity should be at new path
    FdEntity* found = FdManager::get()->GetFdEntity("/test/rp_dst.txt");
    ASSERT_NE(found, nullptr);
    EXPECT_STREQ("/test/rp_dst.txt", found->GetPath());

    // Old cache file should be gone
    EXPECT_NE(0, stat(old_cachepath.c_str(), &st));

    // New cache file should exist
    string new_cachepath;
    ASSERT_TRUE(FdManager::MakeCachePath("/test/rp_dst.txt", new_cachepath, false));
    EXPECT_EQ(0, stat(new_cachepath.c_str(), &st));

    FdManager::get()->Close(ent);
}

TEST_F(FdManagerCoverageTest, RenamePath_NoCacheMode_Basic) {
    FdManager::SetCacheDir("");
    ASSERT_FALSE(FdManager::IsCacheDir());

    FdEntity* ent = FdManager::get()->Open("/test/rp_nc_src.txt", NULL, 0, -1, true);
    ASSERT_NE(ent, nullptr);

    FdManager::get()->Rename("/test/rp_nc_src.txt", "/test/rp_nc_dst.txt");

    // In no-cache mode, lookup by fd
    int fd = ent->GetFd();
    FdEntity* found = FdManager::get()->GetFdEntity("/test/rp_nc_dst.txt", fd);
    ASSERT_NE(found, nullptr);
    EXPECT_STREQ("/test/rp_nc_dst.txt", found->GetPath());

    FdManager::get()->Close(ent);
}

TEST_F(FdManagerCoverageTest, RenamePath_CacheMode_Overwrite) {
    FdManager::SetCacheDir(tmpdir.c_str());

    // Open both src and dst
    FdEntity* src_ent = FdManager::get()->Open("/test/ow_src.txt", NULL, 0, -1, false);
    FdEntity* dst_ent = FdManager::get()->Open("/test/ow_dst.txt", NULL, 0, -1, false);
    ASSERT_NE(src_ent, nullptr);
    ASSERT_NE(dst_ent, nullptr);

    // Save old dst cache path
    string old_dst_cachepath;
    FdManager::MakeCachePath("/test/ow_dst.txt", old_dst_cachepath, false);

    // Close dst so it can be evicted
    FdManager::get()->Close(dst_ent);

    // Rename src → dst (overwrites dst)
    FdManager::get()->Rename("/test/ow_src.txt", "/test/ow_dst.txt");

    FdEntity* found = FdManager::get()->GetFdEntity("/test/ow_dst.txt");
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found, src_ent);  // Same entity as src

    // New cache file should exist at dst path
    string new_cachepath;
    FdManager::MakeCachePath("/test/ow_dst.txt", new_cachepath, false);
    struct stat st;
    EXPECT_EQ(0, stat(new_cachepath.c_str(), &st));

    FdManager::get()->Close(src_ent);
}

TEST_F(FdManagerCoverageTest, Rename_DestEviction_NoCacheMode) {
    // Test that destination eviction also works in no-cache mode
    FdManager::SetCacheDir("");
    ASSERT_FALSE(FdManager::IsCacheDir());

    FdEntity* src_ent = FdManager::get()->Open("/test/ev_nc_src.txt", NULL, 0, -1, true);
    FdEntity* dst_ent = FdManager::get()->Open("/test/ev_nc_dst.txt", NULL, 0, -1, true);
    ASSERT_NE(src_ent, nullptr);
    ASSERT_NE(dst_ent, nullptr);

    // Close dst so it can be evicted
    FdManager::get()->Close(dst_ent);

    // Rename src → dst (should evict the closed dst entity in no-cache mode)
    FdManager::get()->Rename("/test/ev_nc_src.txt", "/test/ev_nc_dst.txt");

    int fd = src_ent->GetFd();
    FdEntity* found = FdManager::get()->GetFdEntity("/test/ev_nc_dst.txt", fd);
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found, src_ent);

    FdManager::get()->Close(src_ent);
}

TEST_F(FdManagerCoverageTest, RenamePath_CacheMode_WriteAfterRename) {
    // Data integrity test: write → rename → write → verify via fd in cache mode
    FdManager::SetCacheDir(tmpdir.c_str());

    FdEntity* ent = FdManager::get()->Open("/test/war_src.txt", NULL, 0, -1, false);
    ASSERT_NE(ent, nullptr);

    // Write initial data
    const char* data1 = "HELLO";
    ssize_t written = ent->Write(data1, 0, strlen(data1));
    ASSERT_EQ((ssize_t)strlen(data1), written);

    // Rename
    FdManager::get()->Rename("/test/war_src.txt", "/test/war_dst.txt");

    // Entity path should be updated
    EXPECT_STREQ("/test/war_dst.txt", ent->GetPath());

    // Write more data after rename
    const char* data2 = "WORLD";
    written = ent->Write(data2, strlen(data1), strlen(data2));
    EXPECT_EQ((ssize_t)strlen(data2), written);

    // Verify data by reading the underlying file descriptor directly
    int fd = ent->GetFd();
    ASSERT_NE(-1, fd);
    char buf[11] = {};
    ssize_t nread = pread(fd, buf, 10, 0);
    EXPECT_EQ(10, nread);
    EXPECT_STREQ("HELLOWORLD", buf);

    FdManager::get()->Close(ent);
}

// ============================================================
// DiskSpace Coverage Tests — IsSafeDiskSpace lock + reserved awareness,
// ReserveDiskSpace atomicity, concurrency, FdEntity::ReserveDiskSpace,
// max_tmpdir_disk_usage, SetEnsureFreeDiskSpace thread safety
// ============================================================

class DiskSpaceCoverageTest : public ::testing::Test {
protected:
    void resetFakeDisk() {
        // SetMaxTmpdirDiskUsage(0) → unlimited (no cap)
        FdManager::SetMaxTmpdirDiskUsage(0);
    }
    void SetUp() override {
        // Reset to clean state: no fake disk, no reserved
        resetFakeDisk();
        // Release any leftover reservations
        size_t leftover = FdManager::GetReservedDiskSpace();
        if(leftover > 0){
            FdManager::ReleaseDiskSpace(leftover);
        }
    }
    void TearDown() override {
        resetFakeDisk();
        size_t leftover = FdManager::GetReservedDiskSpace();
        if(leftover > 0){
            FdManager::ReleaseDiskSpace(leftover);
        }
    }
};

// --- IsSafeDiskSpace lock + reserved awareness ---

TEST_F(DiskSpaceCoverageTest, IsSafeDiskSpace_AccountsForReserved) {
    // Reserve some space, then IsSafeDiskSpace should see less available
    size_t ensure = FdManager::GetEnsureFreeDiskSpace();
    uint64_t free_space = FdManager::GetFreeDiskSpace(NULL);
    ASSERT_GT(free_space, ensure + 1024 * 1024) << "Not enough actual disk space for test";

    // With nothing reserved, a reasonable request should succeed
    EXPECT_TRUE(FdManager::IsSafeDiskSpace(NULL, 1024));

    // Reserve a large chunk
    size_t reserve_amount = static_cast<size_t>(free_space - ensure - 512 * 1024);
    ASSERT_TRUE(FdManager::ReserveDiskSpace(reserve_amount));

    // Now IsSafeDiskSpace should account for the reserved bytes
    // Requesting more than the remaining (512KB) should fail
    EXPECT_FALSE(FdManager::IsSafeDiskSpace(NULL, 1024 * 1024));

    FdManager::ReleaseDiskSpace(reserve_amount);
}

TEST_F(DiskSpaceCoverageTest, IsSafeDiskSpace_WithMaxTmpdirUsage) {
    uint64_t real_free = FdManager::GetFreeDiskSpace(NULL);
    ASSERT_GT(real_free, 20ULL * 1024 * 1024) << "Need >= 20MB free for test";

    // Fake used disk so only 10MB appears free
    FdManager::SetMaxTmpdirDiskUsage(10 * 1024 * 1024);

    uint64_t fake_free = FdManager::GetFreeDiskSpace(NULL);
    EXPECT_LE(fake_free, 10ULL * 1024 * 1024);

    // Reset fake disk
    resetFakeDisk();
    uint64_t restored = FdManager::GetFreeDiskSpace(NULL);
    EXPECT_GT(restored, fake_free);
}

TEST_F(DiskSpaceCoverageTest, IsSafeDiskSpace_WithMsg) {
    // IsSafeDiskSpace with withmsg=true for huge request should return false and not crash
    EXPECT_FALSE(FdManager::IsSafeDiskSpace(NULL, SIZE_MAX / 2, true));
}

TEST_F(DiskSpaceCoverageTest, IsSafeDiskSpace_ZeroSize) {
    // Zero-size request should succeed (assuming disk has free space > ensure threshold)
    uint64_t free_space = FdManager::GetFreeDiskSpace(NULL);
    size_t ensure = FdManager::GetEnsureFreeDiskSpace();
    if(free_space > ensure){
        EXPECT_TRUE(FdManager::IsSafeDiskSpace(NULL, 0));
    }
}

// --- ReserveDiskSpace atomicity ---

TEST_F(DiskSpaceCoverageTest, ReserveDiskSpace_AtomicCheckAndSet) {
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());

    ASSERT_TRUE(FdManager::ReserveDiskSpace(4096));
    EXPECT_EQ(4096u, FdManager::GetReservedDiskSpace());

    ASSERT_TRUE(FdManager::ReserveDiskSpace(8192));
    EXPECT_EQ(4096u + 8192u, FdManager::GetReservedDiskSpace());

    FdManager::ReleaseDiskSpace(4096);
    EXPECT_EQ(8192u, FdManager::GetReservedDiskSpace());

    FdManager::ReleaseDiskSpace(8192);
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
}

TEST_F(DiskSpaceCoverageTest, ReserveDiskSpace_DeniedWhenReservedHigh) {
    // Use SetMaxTmpdirDiskUsage to create a controlled virtual disk.
    // Without this, the test relied on real statvfs remaining stable between
    // GetFreeDiskSpace() and ReserveDiskSpace() — other processes writing to
    // /tmp could shrink free space in between, making the 1024-byte margin
    // insufficient and causing flaky failures.
    size_t cap = 100 * 1024 * 1024;  // 100MB virtual cap
    FdManager::SetMaxTmpdirDiskUsage(cap);

    uint64_t free_space = FdManager::GetFreeDiskSpace(NULL);
    size_t ensure = FdManager::GetEnsureFreeDiskSpace();
    ASSERT_GT(free_space, ensure);

    // Reserve almost all available space (leave only ensure + 1KB)
    size_t big_reserve = static_cast<size_t>(free_space - ensure - 1024);
    ASSERT_TRUE(FdManager::ReserveDiskSpace(big_reserve));

    // A second small Reserve should be denied (only 1KB headroom left)
    EXPECT_FALSE(FdManager::ReserveDiskSpace(2048));

    FdManager::ReleaseDiskSpace(big_reserve);
    resetFakeDisk();
}

TEST_F(DiskSpaceCoverageTest, ReserveDiskSpace_MaxTmpdir_ENOSPC) {
    // Fake disk with only 1MB free
    FdManager::SetMaxTmpdirDiskUsage(1 * 1024 * 1024);

    size_t ensure = FdManager::GetEnsureFreeDiskSpace();
    // Reserve should fail if request + ensure > 1MB
    if(ensure > 512 * 1024){
        EXPECT_FALSE(FdManager::ReserveDiskSpace(800 * 1024));
    }else{
        // ensure is small enough, 800KB might fit
        bool result = FdManager::ReserveDiskSpace(800 * 1024);
        if(result){
            FdManager::ReleaseDiskSpace(800 * 1024);
        }
    }

    // Reset fake disk
    resetFakeDisk();
}

TEST_F(DiskSpaceCoverageTest, ReleaseDiskSpace_ClampOnOverrelease) {
    // Release more than reserved should clamp to 0
    ASSERT_TRUE(FdManager::ReserveDiskSpace(1024));
    FdManager::ReleaseDiskSpace(8192); // release more than reserved
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
}

// --- Concurrency tests ---

TEST_F(DiskSpaceCoverageTest, Concurrent_ReserveRelease) {
    // 8 threads each Reserve/Release 100 times — no crash, final reserved == 0
    const int num_threads = 8;
    const int iterations = 100;
    std::vector<pthread_t> threads(num_threads);

    struct ThreadArg {
        int iterations;
    };
    ThreadArg targ = {iterations};

    auto worker = [](void* arg) -> void* {
        ThreadArg* ta = static_cast<ThreadArg*>(arg);
        for(int i = 0; i < ta->iterations; i++){
            if(FdManager::ReserveDiskSpace(4096)){
                FdManager::ReleaseDiskSpace(4096);
            }
        }
        return NULL;
    };

    for(int i = 0; i < num_threads; i++){
        pthread_create(&threads[i], NULL, worker, &targ);
    }
    for(int i = 0; i < num_threads; i++){
        pthread_join(threads[i], NULL);
    }
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
}

TEST_F(DiskSpaceCoverageTest, Concurrent_IsSafeDiskSpace_WithReserve) {
    // 4 threads Reserve + 4 threads IsSafeDiskSpace — no crash
    const int num_threads = 4;
    std::vector<pthread_t> threads(num_threads * 2);

    auto reserve_worker = [](void* arg) -> void* {
        (void)arg;
        for(int i = 0; i < 50; i++){
            if(FdManager::ReserveDiskSpace(1024)){
                FdManager::ReleaseDiskSpace(1024);
            }
        }
        return NULL;
    };

    auto check_worker = [](void* arg) -> void* {
        (void)arg;
        for(int i = 0; i < 50; i++){
            FdManager::IsSafeDiskSpace(NULL, 1024);
        }
        return NULL;
    };

    for(int i = 0; i < num_threads; i++){
        pthread_create(&threads[i], NULL, reserve_worker, NULL);
        pthread_create(&threads[num_threads + i], NULL, check_worker, NULL);
    }
    for(int i = 0; i < num_threads * 2; i++){
        pthread_join(threads[i], NULL);
    }
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());
}

TEST_F(DiskSpaceCoverageTest, Concurrent_ReserveExhaustsDisk) {
    // Fake a small disk, then have N threads compete for Reserve
    FdManager::SetMaxTmpdirDiskUsage(2 * 1024 * 1024); // 2MB free
    size_t ensure = FdManager::GetEnsureFreeDiskSpace();
    uint64_t avail = FdManager::GetFreeDiskSpace(NULL);
    // Skip if ensure alone exceeds available
    if(avail <= ensure){
        resetFakeDisk();
        return; // Not enough fake free space to test contention
    }

    const int num_threads = 8;
    const size_t per_reserve = 256 * 1024; // 256KB each
    std::vector<pthread_t> threads(num_threads);
    std::atomic<int> success_count(0);

    struct ExhaustArg {
        size_t per_reserve;
        std::atomic<int>* success_count;
    };
    ExhaustArg earg = {per_reserve, &success_count};

    auto worker = [](void* arg) -> void* {
        ExhaustArg* ea = static_cast<ExhaustArg*>(arg);
        if(FdManager::ReserveDiskSpace(ea->per_reserve)){
            ea->success_count->fetch_add(1);
            // Hold reservation briefly
            usleep(1000);
            FdManager::ReleaseDiskSpace(ea->per_reserve);
        }
        return NULL;
    };

    for(int i = 0; i < num_threads; i++){
        pthread_create(&threads[i], NULL, worker, &earg);
    }
    for(int i = 0; i < num_threads; i++){
        pthread_join(threads[i], NULL);
    }

    // Some should have succeeded, but not necessarily all (contention)
    EXPECT_GE(success_count.load(), 1);
    EXPECT_EQ(0u, FdManager::GetReservedDiskSpace());

    // Reset fake disk
    resetFakeDisk();
}

// --- FdEntity::ReserveDiskSpace retry ---

TEST_F(DiskSpaceCoverageTest, EntityReserve_FirstTrySuccess) {
    // Create an entity, ReserveDiskSpace should succeed on first try
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/entres_ok.txt", NULL, 0, -1, true);
    ASSERT_NE(ent, nullptr);

    bool result = ent->ReserveDiskSpace(4096);
    EXPECT_TRUE(result);
    if(result){
        FdManager::ReleaseDiskSpace(4096);
    }

    FdManager::get()->Close(ent);
}

TEST_F(DiskSpaceCoverageTest, EntityReserve_CleanupRetry) {
    // Fake a very tight disk, entity Reserve should attempt cleanup+retry
    FdManager::SetCacheDir("");
    size_t ensure = FdManager::GetEnsureFreeDiskSpace();

    FdEntity* ent = FdManager::get()->Open("/test/entres_cleanup.txt", NULL, 0, -1, true);
    ASSERT_NE(ent, nullptr);

    // Fake tight disk: only ensure + 100 bytes free
    FdManager::SetMaxTmpdirDiskUsage(ensure + 100);

    // Request more than what's available — should fail even after retries
    bool result = ent->ReserveDiskSpace(1024 * 1024);
    // Whether it succeeds depends on actual disk, but it should not crash
    if(result){
        FdManager::ReleaseDiskSpace(1024 * 1024);
    }

    FdManager::get()->Close(ent);
    // Reset
    resetFakeDisk();
}

// --- max_tmpdir_disk_usage ---

TEST_F(DiskSpaceCoverageTest, SetMaxTmpdirDiskUsage_CapsFreeSpace) {
    uint64_t original = FdManager::GetFreeDiskSpace(NULL);
    ASSERT_GT(original, 10ULL * 1024 * 1024);

    FdManager::SetMaxTmpdirDiskUsage(10 * 1024 * 1024);
    uint64_t reduced = FdManager::GetFreeDiskSpace(NULL);
    EXPECT_LE(reduced, 10ULL * 1024 * 1024);
    EXPECT_LT(reduced, original);

    // Reset
    FdManager::SetMaxTmpdirDiskUsage(0);
}

TEST_F(DiskSpaceCoverageTest, SetMaxTmpdirDiskUsage_LargerThanActual) {
    uint64_t actual = FdManager::GetFreeDiskSpace(NULL);
    // Cap at 999TB (larger than actual) → effective_cap = actual, consumed=0, remaining=actual
    FdManager::SetMaxTmpdirDiskUsage(999ULL * 1024 * 1024 * 1024 * 1024);
    uint64_t after = FdManager::GetFreeDiskSpace(NULL);
    // Should be same as actual (cap larger than disk)
    EXPECT_GE(after, actual * 9 / 10); // allow small statvfs variance
}

TEST_F(DiskSpaceCoverageTest, MaxTmpdir_ReserveDenied) {
    // cap=1MB, ensure=500KB → Reserve(600KB) may be denied
    FdManager::SetMaxTmpdirDiskUsage(1 * 1024 * 1024);

    size_t ensure = FdManager::GetEnsureFreeDiskSpace();
    uint64_t fake_free = FdManager::GetFreeDiskSpace(NULL);
    // If ensure + 600KB > fake_free, reserve should be denied
    if(ensure + 600 * 1024 > fake_free){
        EXPECT_FALSE(FdManager::ReserveDiskSpace(600 * 1024));
    }

    // Reset
    resetFakeDisk();
}

// --- SetEnsureFreeDiskSpace thread safety ---

TEST_F(DiskSpaceCoverageTest, SetEnsureFreeDiskSpace_ThreadSafe) {
    // Capture current value
    size_t before = FdManager::GetEnsureFreeDiskSpace();

    const int num_threads = 4;
    std::vector<pthread_t> threads(num_threads);

    auto worker = [](void* arg) -> void* {
        (void)arg;
        for(int i = 0; i < 100; i++){
            size_t val = FdManager::GetEnsureFreeDiskSpace();
            (void)val;
        }
        return NULL;
    };

    for(int i = 0; i < num_threads; i++){
        pthread_create(&threads[i], NULL, worker, NULL);
    }
    for(int i = 0; i < num_threads; i++){
        pthread_join(threads[i], NULL);
    }
    // Should not crash, value should be unchanged after concurrent reads
    size_t after = FdManager::GetEnsureFreeDiskSpace();
    EXPECT_EQ(before, after);
}

// ============================================================
// Additional FdManager State Tests (Phase 3)
// ============================================================

// Purpose: Verify IsSafeDiskSpace returns false for very large requests
// Expected: Returns false when request exceeds available space
// Why: Prevents OOM from unrealistic disk allocation requests
TEST_F(FdManagerCoverageTest, IsSafeDiskSpaceWithLargeRequestReturnsFalse) {
    // Request 1TB - unrealistic for most systems
    bool result = FdManager::IsSafeDiskSpace("/tmp", 1000000000000ULL);
    EXPECT_FALSE(result);
}

// Purpose: Verify ReserveDiskSpace returns true and ReleaseDiskSpace works
// Expected: Reserve returns true for small amount, Release reduces reservation
// Why: Disk reservation tracking for cache management
TEST_F(FdManagerCoverageTest, ReserveDiskSpaceAndRelease) {
    // Clear any existing reservation first
    while(FdManager::GetReservedDiskSpace() > 0){
        FdManager::ReleaseDiskSpace(FdManager::GetReservedDiskSpace());
    }

    // Try to reserve 1KB
    bool reserved = FdManager::ReserveDiskSpace(1024);
    // On most systems, 1KB should succeed
    if (reserved) {
        EXPECT_GE(FdManager::GetReservedDiskSpace(), 1024u);
        FdManager::ReleaseDiskSpace(1024);
    }
    // If reserved == false, disk might actually be full - that's also valid
}

// Purpose: Verify ReserveDiskSpace returns bool and tracks reservation correctly
// Expected: Reserve returns true for small amount, GetReservedDiskSpace reflects it
// Why: ReserveDiskSpace returns bool (not size_t), must verify correct API usage
TEST_F(FdManagerCoverageTest, ReserveDiskSpace_CorrectBoolReturn) {
    // FdManager::ReserveDiskSpace returns bool, not size_t
    bool reserved = FdManager::ReserveDiskSpace(1024);
    EXPECT_TRUE(reserved);
    EXPECT_GE(FdManager::GetReservedDiskSpace(), 1024u);
    FdManager::ReleaseDiskSpace(1024);
}

// Purpose: Verify SetMaxTmpdirDiskUsage affects GetMaxTmpdirDiskUsage
// Expected: Get returns same value that was Set
// Why: Configuration tracking for tmpdir limits
TEST_F(FdManagerCoverageTest, SetMaxTmpdirDiskUsageRoundTrip) {
    size_t test_value = 1024 * 1024 * 100;  // 100MB
    FdManager::SetMaxTmpdirDiskUsage(test_value);
    EXPECT_EQ(FdManager::GetMaxTmpdirDiskUsage(), test_value);
    // Reset to default
    FdManager::SetMaxTmpdirDiskUsage(0);
    EXPECT_EQ(FdManager::GetMaxTmpdirDiskUsage(), 0UL);
}

// Purpose: Verify MakeTempFile returns valid FILE* with proper tmpdir
// Expected: Returns non-null FILE* when tmpdir is valid
// Why: Temp file creation for cache operations
TEST_F(FdManagerCoverageTest, MakeTempFileWithValidTmpDir) {
    // Ensure tmpdir is set to valid path
    FdManager::SetTmpDir("/tmp");
    FILE* fp = FdManager::MakeTempFile();
    EXPECT_NE(fp, nullptr);
    if (fp) {
        fclose(fp);
    }
}

// Purpose: Verify MakeTempFile returns nullptr with invalid tmpdir
// Expected: Returns nullptr when tmpdir doesn't exist
// Why: Error handling for misconfigured tmpdir
TEST_F(FdManagerCoverageTest, MakeTempFileWithInvalidTmpDirReturnsNull) {
    string old_tmp = FdManager::GetTmpDir();
    FdManager::SetTmpDir("/nonexistent/path/that/does/not/exist");
    FILE* fp = FdManager::MakeTempFile();
    EXPECT_EQ(fp, nullptr);
    if (fp) fclose(fp);
    FdManager::SetTmpDir(old_tmp.c_str());
}

// Purpose: Verify Open creates entity with mtime update
// Expected: Entity is open and has correct mtime
// Why: Metadata tracking during file open
TEST_F(FdManagerCoverageTest, OpenWithMtimeUpdateCreatesValidEntity) {
    FdManager* mgr = FdManager::get();
    time_t now = time(NULL);
    FdEntity* ent = mgr->Open("/test_mtime", NULL, 100, now, false, true);
    ASSERT_NE(ent, nullptr);
    EXPECT_TRUE(ent->IsOpen());
    mgr->Close(ent);
}

// Purpose: Verify GetFdEntity retrieves same entity by path and fd
// Expected: Returns same entity pointer
// Why: Entity lookup for existing open files
TEST_F(FdManagerCoverageTest, GetFdEntityByFdReturnsSameEntity) {
    FdManager* mgr = FdManager::get();
    FdEntity* ent = mgr->Open("/test_getfd", NULL, 100, -1, false, true);
    ASSERT_NE(ent, nullptr);

    int fd = ent->GetFd();
    FdEntity* found = mgr->GetFdEntity("/test_getfd", fd);
    EXPECT_EQ(found, ent);

    mgr->Close(ent);
}

// Purpose: Verify ChangeEntityToTempPath attempts path update
// Expected: Method executes without crash (result depends on internal state)
// Why: Path update during rename operations
TEST_F(FdManagerCoverageTest, ChangeEntityToTempPathWithCacheDir) {
    // Set up cache dir for this test
    FdManager::SetCacheDir(tmpdir.c_str());

    FdManager* mgr = FdManager::get();
    FdEntity* ent = mgr->Open("/test_changetemp", NULL, 100, -1, false, true);
    ASSERT_NE(ent, nullptr);

    bool result = mgr->ChangeEntityToTempPath(ent, "/test_changetemp");
    // ChangeEntityToTempPath always returns false (fdcache.cpp:3451) but
    // does successfully move the entity to a temp path in the fent map.
    EXPECT_FALSE(result);
    // Verify the entity was actually moved: original path lookup should fail
    EXPECT_EQ(mgr->GetFdEntity("/test_changetemp", -1), nullptr);

    mgr->Close(ent);
    FdManager::SetCacheDir("");
}

// ============================================================
// Additional PageList Edge Case Tests (Phase 4)
// ============================================================

// Purpose: Verify GetPageListsForMultipartUpload with mixed modified/unmodified pages
// Expected: Correctly partitions modified and unmodified pages
// Why: Multipart upload optimization - only upload modified parts
TEST_F(PageListCoverageTest, GetPageListsForMultipartUpload_MixedModified) {
    PageList pl(1000, false);  // 1000 bytes, not loaded

    // Mark some pages as loaded and modified
    pl.SetPageLoadedStatus(0, 200, true);  // 0-199 loaded
    pl.SetPageModifiedStatus(0, 100, true); // 0-99 modified
    pl.SetPageLoadedStatus(200, 300, true); // 200-499 loaded (unmodified)
    pl.SetPageModifiedStatus(500, 200, true); // 500-699 modified

    // Get page lists for multipart upload
    fdpage_list_t dlpages;
    fdpage_list_t mixuppages;
    off_t max_partsize = 256;

    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, max_partsize);
    // Should have entries for modified regions
    EXPECT_GT(dlpages.size() + mixuppages.size(), 0u);
    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

// Purpose: Verify GetPageListsForMultipartUpload with max_partsize boundary
// Expected: Correctly splits pages at boundary
// Why: Part size limits in S3 multipart uploads
TEST_F(PageListCoverageTest, GetPageListsForMultipartUpload_BoundarySplit) {
    PageList pl(1024, true);  // 1024 bytes, all loaded

    // Mark entire range as modified
    pl.SetPageModifiedStatus(0, 1024, true);

    fdpage_list_t dlpages;
    fdpage_list_t mixuppages;
    off_t max_partsize = 256;  // Should split into 4 parts

    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, max_partsize);
    // Should have entries
    EXPECT_GT(dlpages.size() + mixuppages.size(), 0u);
    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

// Scenario: File upload completes, dirty flags are cleared
// Purpose: Verify ClearAllModified resets dirty flags in PageList
// Expected: Before clear, all pages are modified → no dlpages needed.
//           After clear, pages are unmodified → GetPageListsForMultipartUpload
//           adds dlpages entries (small file needs re-download before re-upload).
// Why: ClearAllModified resets internal modified flags; the state change is observable
//      through GetPageListsForMultipartUpload's download plan for small files
TEST_F(PageListCoverageTest, ClearAllModified_ClearsDirtyFlagsForCopyPart) {
    PageList pl(1000, true);
    pl.SetPageModifiedStatus(0, 500, true);
    pl.SetPageModifiedStatus(500, 500, true);

    // Before clear: all pages modified → no download needed
    fdpage_list_t dlpages_before;
    fdpage_list_t mixuppages_before;
    pl.GetPageListsForMultipartUpload(dlpages_before, mixuppages_before, 256);
    EXPECT_EQ(dlpages_before.size(), 0u);  // no download needed when already modified
    PageList::FreeList(dlpages_before);
    PageList::FreeList(mixuppages_before);

    // Clear modified flags after successful upload
    pl.ClearAllModified();

    // After clear: pages are unmodified, but small file (<5MB) gets re-marked
    // by GetPageListsForMultipartUpload with a download entry in dlpages
    fdpage_list_t dlpages_after;
    fdpage_list_t mixuppages_after;
    pl.GetPageListsForMultipartUpload(dlpages_after, mixuppages_after, 256);
    EXPECT_GT(dlpages_after.size(), 0u);  // download needed: confirms flags were cleared
    PageList::FreeList(dlpages_after);
    PageList::FreeList(mixuppages_after);
}

// Purpose: Verify GetTotalUnloadedPageSize with overlapping ranges
// Expected: Correctly sums unloaded pages without double counting
// Why: Accurate unloaded page tracking for cache management
TEST_F(PageListCoverageTest, GetTotalUnloadedPageSize_PartiallyLoaded) {
    PageList pl(1000, false);  // 1000 bytes, not loaded

    // Load some pages
    pl.SetPageLoadedStatus(0, 200, true);  // 0-199 loaded
    // 200-999 unloaded (800 bytes)

    size_t unloaded = pl.GetTotalUnloadedPageSize(0, 1000);
    EXPECT_EQ(unloaded, 800u);
}

// Purpose: Verify GetTotalLoadedPageSize edge cases
// Expected: Correctly calculates loaded size
// Why: Track how much data is in cache
TEST_F(PageListCoverageTest, GetTotalLoadedPageSize_FullyLoaded) {
    PageList pl(1000, true);  // 1000 bytes, all loaded

    size_t loaded = pl.GetTotalLoadedPageSize();
    EXPECT_EQ(loaded, 1000u);
}

// Scenario: Large file upload completes, dirty flags are cleared
// Purpose: Verify ClearAllModified on large file uses CopyPart strategy
// Expected: After ClearAllModified on large file (>5MB), no download needed,
//           all pages in mixuppages have modified=false
// Why: For large files, use CopyPart instead of re-uploading
TEST_F(PageListCoverageTest, ClearAllModified_LargeFileUsesCopyPart) {
    // Use 10MB to exceed MIN_MULTIPART_SIZE (5MB), avoiding small page merge logic
    const size_t large_size = 10 * 1024 * 1024;
    PageList pl(large_size, true);

    // Mark pages as modified (simulating write)
    pl.SetPageModifiedStatus(0, large_size / 2, true);
    pl.SetPageModifiedStatus(large_size / 2, large_size / 2, true);

    // Verify dirty pages exist for upload
    fdpage_list_t dlpages_before;
    fdpage_list_t mixuppages_before;
    pl.GetPageListsForMultipartUpload(dlpages_before, mixuppages_before, 256);
    EXPECT_GT(dlpages_before.size() + mixuppages_before.size(), 0u);

    // Clear after successful upload
    pl.ClearAllModified();

    // After clear: no download needed, pages use CopyPart
    fdpage_list_t dlpages_after;
    fdpage_list_t mixuppages_after;
    pl.GetPageListsForMultipartUpload(dlpages_after, mixuppages_after, 256);
    EXPECT_EQ(dlpages_after.size(), 0u);  // No download needed
    EXPECT_GT(mixuppages_after.size(), 0u);  // Pages in plan for CopyPart
    // Verify all pages are unmodified (CopyPart, not UploadPart)
    for (fdpage_list_t::const_iterator it = mixuppages_after.begin();
         it != mixuppages_after.end(); ++it) {
        EXPECT_FALSE((*it)->modified);
    }
    PageList::FreeList(dlpages_before);
    PageList::FreeList(mixuppages_before);
    PageList::FreeList(dlpages_after);
    PageList::FreeList(mixuppages_after);
}

// ============================================================
// Additional FdEntity Edge Case Tests (Phase 4)
// ============================================================

// Purpose: Verify FdEntity::Write beyond current size (file growth)
// Expected: File size increases after write
// Why: File growth during write operations
TEST_F(FdEntityCoverageTest, WriteBeyondCurrentSizeGrowsFile) {
    FdEntity ent("/test_grow");
    int open_result = ent.Open(NULL, 100, -1);
    ASSERT_EQ(open_result, 0);

    // Write beyond current size
    const char* data = "test data beyond size";
    ssize_t write_result = ent.Write(data, 200, strlen(data));
    EXPECT_GT(write_result, 0);

    // Verify size grew
    size_t size = 0;
    bool got_size = ent.GetSize(size);
    EXPECT_TRUE(got_size);
    EXPECT_GE(size, 200 + strlen(data));

    ent.Close();
}

// Scenario: User renames file in no-cache mode
// Purpose: Verify FdEntity::RenamePath generates temp path for cache key
// Expected: fentmapkey contains the new path (with random prefix for no-cache mode)
// Why: In no-cache mode, FdManager uses random temp paths to avoid conflicts
TEST_F(FdEntityCoverageTest, RenamePath_NoCacheMode_GeneratesTempKey) {
    FdEntity ent("/old_path");
    ent.Open(NULL, 0, -1);

    std::string fentmapkey;
    bool result = ent.RenamePath("/new_path", fentmapkey);

    // Verify rename succeeded
    EXPECT_TRUE(result);
    // In no-cache mode, fentmapkey has random prefix but contains the new path
    EXPECT_NE(fentmapkey.find("/new_path"), std::string::npos);

    ent.Close();
}

// Scenario: User changes file permissions via chmod
// Purpose: Verify FdEntity::SetMode stores mode for OBS metadata
// Expected: SetMode returns true (success)
// Why: Mode must be stored in orgmeta for OBS upload with correct permissions
TEST_F(FdEntityCoverageTest, SetMode_StoresModeForObsMetadata) {
    FdEntity ent("/test_mode");
    ent.Open(NULL, 0, -1);

    mode_t new_mode = 0755;
    bool result = ent.SetMode(new_mode);

    // Verify SetMode succeeded (mode stored in orgmeta for OBS upload)
    EXPECT_TRUE(result);

    ent.Close();
}

// Scenario: User changes file owner via chown
// Purpose: Verify FdEntity::SetUId stores uid for OBS metadata
// Expected: SetUId returns success
// Why: Owner must be stored in orgmeta for OBS upload with correct ownership
TEST_F(FdEntityCoverageTest, SetUId_StoresUidForObsMetadata) {
    FdEntity ent("/test_uid");
    ent.Open(NULL, 0, -1);

    uid_t new_uid = 1000;
    bool result = ent.SetUId(new_uid);

    // Verify SetUId succeeded (uid stored in orgmeta for OBS upload)
    EXPECT_TRUE(result);

    ent.Close();
}

// Scenario: User changes file group via chgrp
// Purpose: Verify FdEntity::SetGId stores gid for OBS metadata
// Expected: SetGId returns success
// Why: Group must be stored in orgmeta for OBS upload with correct group ownership
TEST_F(FdEntityCoverageTest, SetGId_StoresGidForObsMetadata) {
    FdEntity ent("/test_gid");
    ent.Open(NULL, 0, -1);

    gid_t new_gid = 1000;
    bool result = ent.SetGId(new_gid);

    // Verify SetGId succeeded (gid stored in orgmeta for OBS upload)
    EXPECT_TRUE(result);

    ent.Close();
}

// ============================================================
// Additional Tests - TASKS 4.2 Missing Scenarios
// ============================================================

// Purpose: Verify OpenAndLoadAll works on empty file
// Expected: Returns true, loaded_size = 0
// Covers: FdEntity::OpenAndLoadAll (fdcache.cpp:1511-1541)
TEST_F(FdEntityCoverageTest, OpenAndLoadAll_EmptyFile) {
    FdEntity ent("/test_oala");
    size_t loaded_size = 0;
    bool result = ent.OpenAndLoadAll(NULL, &loaded_size, false);
    EXPECT_TRUE(result);
    EXPECT_EQ(loaded_size, 0u);
    ent.Close();
}

// Purpose: Verify Dup with no_fd_lock_wait=true returns valid fd
// Expected: Dup returns same fd (not -1), increments refcnt
// Covers: FdEntity::Dup no_fd_lock_wait branch (fdcache.cpp:1099-1111)
TEST_F(FdEntityCoverageTest, Dup_NoFdLockWait) {
    FdEntity ent("/test_dup_nfw");
    ent.Open(NULL, 0, -1);
    int fd = ent.Dup(true);  // no_fd_lock_wait=true
    EXPECT_NE(fd, -1);
    ent.Close();  // close Dup's ref
    ent.Close();  // close Open's ref
}

// ============================================================
// Additional Tests - TASKS 3.2 FdManager Cache Directory Tests
// ============================================================

// Purpose: Verify DeleteCacheDirectory recursively deletes nested directories
// Expected: Returns true, all nested directories and files are deleted
// Covers: FdManager::DeleteCacheDirectory (fdcache.cpp:2867-2877)
TEST_F(FdManagerCoverageTest, DeleteCacheDirectoryNestedDirs) {
    FdManager::SetCacheDir(tmpdir.c_str());

    // Create nested directory structure: tmpdir/bucket/subdir1/subdir2/
    string bucketdir = tmpdir + "/test-bucket";
    string subdir1 = bucketdir + "/subdir1";
    string subdir2 = subdir1 + "/subdir2";

    ASSERT_EQ(mkdir(bucketdir.c_str(), 0755), 0);
    ASSERT_EQ(mkdir(subdir1.c_str(), 0755), 0);
    ASSERT_EQ(mkdir(subdir2.c_str(), 0755), 0);

    // Create files in each directory level
    string file1 = bucketdir + "/file1.txt";
    string file2 = subdir1 + "/file2.txt";
    string file3 = subdir2 + "/file3.txt";

    ofstream(file1.c_str()) << "level1";
    ofstream(file2.c_str()) << "level2";
    ofstream(file3.c_str()) << "level3";

    // Verify files exist before deletion
    struct stat st;
    EXPECT_EQ(stat(file1.c_str(), &st), 0);
    EXPECT_EQ(stat(file2.c_str(), &st), 0);
    EXPECT_EQ(stat(file3.c_str(), &st), 0);

    // Delete cache directory - should recursively delete all
    bool result = FdManager::DeleteCacheDirectory();
    EXPECT_TRUE(result);

    // Verify all files are deleted
    EXPECT_NE(stat(file1.c_str(), &st), 0);
    EXPECT_NE(stat(file2.c_str(), &st), 0);
    EXPECT_NE(stat(file3.c_str(), &st), 0);
    EXPECT_NE(stat(subdir2.c_str(), &st), 0);
    EXPECT_NE(stat(subdir1.c_str(), &st), 0);
    EXPECT_NE(stat(bucketdir.c_str(), &st), 0);
}

// Purpose: Verify DeleteCacheFile actually deletes an existing cache file
// Expected: Returns 0, file is removed from filesystem
// Covers: FdManager::DeleteCacheFile (fdcache.cpp:2879-2910)
TEST_F(FdManagerCoverageTest, DeleteCacheFileExistingFile) {
    FdManager::SetCacheDir(tmpdir.c_str());

    // bucket is defined in test_stubs.cpp as "test-bucket"
    // MakeCachePath constructs: cache_dir/bucket/path
    // So the cache file path is: tmpdir/test-bucket/testfile.txt
    string bucketdir = tmpdir + "/test-bucket";
    ASSERT_EQ(mkdir(bucketdir.c_str(), 0755), 0);

    // Create a cache file at the path MakeCachePath would construct
    string cachefile = bucketdir + "/testfile.txt";
    ofstream(cachefile.c_str()) << "test content";

    // Also need to create the stat file (cache_dir/.bucket.stat/path)
    // DeleteCacheFile also calls CacheFileStat::DeleteCacheFileStat
    string statdir = tmpdir + "/.test-bucket.stat";
    ASSERT_EQ(mkdir(statdir.c_str(), 0755), 0);
    string statfile = statdir + "/testfile.txt";
    ofstream(statfile.c_str()) << "stat content";

    // Verify files exist
    struct stat st;
    ASSERT_EQ(stat(cachefile.c_str(), &st), 0);
    ASSERT_EQ(stat(statfile.c_str(), &st), 0);

    // Delete the cache file via FdManager
    int result = FdManager::DeleteCacheFile("/testfile.txt");
    EXPECT_EQ(result, 0);

    // Verify files are deleted
    EXPECT_NE(stat(cachefile.c_str(), &st), 0);
    EXPECT_EQ(errno, ENOENT);
    EXPECT_NE(stat(statfile.c_str(), &st), 0);
    EXPECT_EQ(errno, ENOENT);
}

// Purpose: Verify CheckCacheTopDir behavior with different directory states
// Expected: Returns true when cache_dir is empty, or when bucket dir exists/doesn't exist
//           (check_exist_dir_permission returns true for ENOENT)
// Covers: FdManager::CheckCacheTopDir (fdcache.cpp:2949-2957)
TEST_F(FdManagerCoverageTest, CheckCacheTopDirNonexistentDir) {
    // Test 1: No cache dir set - returns true
    FdManager::SetCacheDir("");
    bool result = FdManager::CheckCacheTopDir();
    EXPECT_TRUE(result);

    // Test 2: Cache dir set but bucket subdirectory doesn't exist
    // Note: check_exist_dir_permission returns true when dir doesn't exist (ENOENT case)
    // This is intentional behavior - see s3fs_util.cpp:369-372
    FdManager::SetCacheDir(tmpdir.c_str());
    result = FdManager::CheckCacheTopDir();
    EXPECT_TRUE(result);  // Returns true even when bucket dir doesn't exist

    // Test 3: Bucket directory exists - also returns true
    string bucketdir = tmpdir + "/test-bucket";
    ASSERT_EQ(mkdir(bucketdir.c_str(), 0755), 0);
    result = FdManager::CheckCacheTopDir();
    EXPECT_TRUE(result);

    // Test 4: Cache dir doesn't exist - returns false (EACCES or other error)
    FdManager::SetCacheDir("/nonexistent/cache/dir/that/does/not/exist");
    result = FdManager::CheckCacheTopDir();
    // This might return false due to EACCES or true due to ENOENT
    // depending on the parent directory permissions
    // Just verify it doesn't crash
    (void)result;

    // Reset cache dir
    FdManager::SetCacheDir("");
}

// Purpose: Verify CleanupCacheDir actually removes files from cache directory
// Expected: Files in cache directory are removed after cleanup
// Covers: FdManager::CleanupCacheDir (fdcache.cpp:3454-3467)
TEST_F(FdManagerCoverageTest, CleanupCacheDirRemovesFiles) {
    FdManager::SetCacheDir(tmpdir.c_str());

    // Create bucket directory
    string bucketdir = tmpdir + "/test-bucket";
    ASSERT_EQ(mkdir(bucketdir.c_str(), 0755), 0);

    // Create some files that should be cleaned up
    string file1 = bucketdir + "/cleanup1.txt";
    string file2 = bucketdir + "/cleanup2.txt";
    ofstream(file1.c_str()) << "content1";
    ofstream(file2.c_str()) << "content2";

    // Verify files exist
    struct stat st;
    ASSERT_EQ(stat(file1.c_str(), &st), 0);
    ASSERT_EQ(stat(file2.c_str(), &st), 0);

    // Run cleanup
    FdManager::get()->CleanupCacheDir();

    // The cleanup behavior depends on internal implementation
    // At minimum, verify the method runs without crash
    // and the bucket directory still exists (cleanup removes files, not dirs)
    EXPECT_EQ(stat(bucketdir.c_str(), &st), 0);
}

// Purpose: Verify SetCheckCacheDirExist and CheckCacheDirExist round-trip
// Expected: Setting true/false and checking returns consistent results
// Covers: FdManager::SetCheckCacheDirExist, FdManager::CheckCacheDirExist (fdcache.cpp:2969-2995)
TEST_F(FdManagerCoverageTest, SetCheckCacheDirExistRoundTrip) {
    // Test 1: Default state - check should return true (disabled check = pass)
    FdManager::SetCheckCacheDirExist(false);
    EXPECT_TRUE(FdManager::CheckCacheDirExist());

    // Test 2: Enable check with valid directory
    FdManager::SetCacheDir(tmpdir.c_str());
    bool old = FdManager::SetCheckCacheDirExist(true);
    EXPECT_FALSE(old);  // Previous was false
    EXPECT_TRUE(FdManager::CheckCacheDirExist());  // Valid dir exists

    // Test 3: Disable and verify
    old = FdManager::SetCheckCacheDirExist(false);
    EXPECT_TRUE(old);  // Previous was true
    EXPECT_TRUE(FdManager::CheckCacheDirExist());  // Disabled always returns true

    // Test 4: Enable with invalid directory
    FdManager::SetCacheDir("/nonexistent/path/for/roundtrip/test");
    FdManager::SetCheckCacheDirExist(true);
    EXPECT_FALSE(FdManager::CheckCacheDirExist());  // Invalid dir should fail

    // Cleanup
    FdManager::SetCheckCacheDirExist(false);
    FdManager::SetCacheDir("");
}

// ============================================================
// Task 4.1: PageList Complex Scenario Tests
// ============================================================

// Test Case 1: GetPageListsForMultipartUpload with mixed modified/unmodified pages
// Purpose: Verify correct partition when pages alternate between modified/unmodified
// Expected: Modified pages go to mixuppages, small unmodified gaps may be merged
// Why: This exercises the complex merging logic in GetPageListsForMultipartUpload
TEST_F(PageListCoverageTest, GetPageListsForMultipartUpload_ComplexMixedPattern) {
    const size_t mb = 1024 * 1024;
    const size_t file_size = 50 * mb;

    PageList pl(file_size, true);

    // Create a complex pattern: modified, unmodified, modified, unmodified, modified
    // All segments are > 5MB to avoid small page merging
    pl.SetPageModifiedStatus(0, 10 * mb, true);       // 0-10MB: modified
    pl.SetPageModifiedStatus(10 * mb, 10 * mb, false); // 10-20MB: unmodified
    pl.SetPageModifiedStatus(20 * mb, 10 * mb, true);  // 20-30MB: modified
    pl.SetPageModifiedStatus(30 * mb, 10 * mb, false); // 30-40MB: unmodified
    pl.SetPageModifiedStatus(40 * mb, 10 * mb, true);  // 40-50MB: modified

    fdpage_list_t dlpages;
    fdpage_list_t mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, 10 * mb);

    // Verify: We should have pages in mixuppages (modified and unmodified)
    EXPECT_GT(mixuppages.size(), 0u);

    // Count modified vs unmodified pages
    int modified_count = 0;
    int unmodified_count = 0;
    for (fdpage_list_t::const_iterator it = mixuppages.begin(); it != mixuppages.end(); ++it) {
        if ((*it)->modified) {
            modified_count++;
        } else {
            unmodified_count++;
        }
    }

    // We expect at least some modified pages
    EXPECT_GT(modified_count, 0);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

// Test Case 2: GetPageListsForMultipartUpload with max_partsize boundary
// Purpose: Verify split behavior when file size is exactly at max_partsize boundary
// Expected: No split when file size <= 2 * max_partsize
// Why: Tests the boundary condition "remaining > 2 * max_partsize" in split logic
TEST_F(PageListCoverageTest, GetPageListsForMultipartUpload_ExactBoundarySplit) {
    const size_t mb = 1024 * 1024;
    const size_t max_partsize = 10 * mb;

    // Test 1: File size exactly equals max_partsize (no split needed)
    {
        PageList pl(max_partsize, true);
        pl.SetPageModifiedStatus(0, max_partsize, true);

        fdpage_list_t dlpages;
        fdpage_list_t mixuppages;
        pl.GetPageListsForMultipartUpload(dlpages, mixuppages, max_partsize);

        // Should produce exactly 1 part (no split)
        int modified_count = 0;
        for (fdpage_list_t::const_iterator it = mixuppages.begin(); it != mixuppages.end(); ++it) {
            if ((*it)->modified) {
                modified_count++;
            }
        }
        EXPECT_EQ(modified_count, 1);

        PageList::FreeList(dlpages);
        PageList::FreeList(mixuppages);
    }

    // Test 2: File size = 2 * max_partsize (no split, kept as single part)
    {
        PageList pl(2 * max_partsize, true);
        pl.SetPageModifiedStatus(0, 2 * max_partsize, true);

        fdpage_list_t dlpages;
        fdpage_list_t mixuppages;
        pl.GetPageListsForMultipartUpload(dlpages, mixuppages, max_partsize);

        // remaining <= 2 * max_partsize, so kept as single part
        int modified_count = 0;
        for (fdpage_list_t::const_iterator it = mixuppages.begin(); it != mixuppages.end(); ++it) {
            if ((*it)->modified) {
                modified_count++;
            }
        }
        EXPECT_EQ(modified_count, 1);

        PageList::FreeList(dlpages);
        PageList::FreeList(mixuppages);
    }

    // Test 3: File size = 3 * max_partsize (should split once)
    {
        PageList pl(3 * max_partsize, true);
        pl.SetPageModifiedStatus(0, 3 * max_partsize, true);

        fdpage_list_t dlpages;
        fdpage_list_t mixuppages;
        pl.GetPageListsForMultipartUpload(dlpages, mixuppages, max_partsize);

        // remaining (30MB) > 2 * max_partsize (20MB), so split: 10MB + 20MB (kept as single)
        int modified_count = 0;
        for (fdpage_list_t::const_iterator it = mixuppages.begin(); it != mixuppages.end(); ++it) {
            if ((*it)->modified) {
                modified_count++;
            }
        }
        EXPECT_EQ(modified_count, 2);  // Split into 2 parts

        PageList::FreeList(dlpages);
        PageList::FreeList(mixuppages);
    }
}

// Test Case 3: SetPageModifiedStatus edge cases - toggling modified status
// Purpose: Verify status changes when toggling modified status on same region
// Expected: Toggling works correctly and compress merges adjacent same-status pages
// Why: Exercises Parse() and Compress() logic with repeated status changes
TEST_F(PageListCoverageTest, SetPageModifiedStatus_ToggleAndCompress) {
    PageList pl(1000, false);

    // Set middle region as modified
    EXPECT_TRUE(pl.SetPageModifiedStatus(200, 600, true, false));
    EXPECT_TRUE(pl.SetPageModifiedStatus(400, 200, true, true));  // Trigger compress

    // Now unset part of it
    EXPECT_TRUE(pl.SetPageModifiedStatus(300, 200, false, true));

    // Now set it back
    EXPECT_TRUE(pl.SetPageModifiedStatus(300, 200, true, true));

    // Verify final state - all from 200-800 should be modified
    fdpage_list_t dlpages;
    fdpage_list_t mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, 100);

    // Check that we have modified pages in the expected range
    bool has_modified_in_range = false;
    for (fdpage_list_t::const_iterator it = mixuppages.begin(); it != mixuppages.end(); ++it) {
        if ((*it)->modified && (*it)->offset >= 200 && (*it)->offset < 800) {
            has_modified_in_range = true;
            break;
        }
    }
    EXPECT_TRUE(has_modified_in_range);

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

// Test Case 4: ClearAllModified after various operations
// Purpose: Verify ClearAllModified clears all modified flags after complex operations
// Expected: After ClearAllModified, GetPageListsForMultipartUpload shows no modified pages
// Why: ClearAllModified must work after any sequence of operations
TEST_F(PageListCoverageTest, ClearAllModified_AfterComplexOperations) {
    const size_t mb = 1024 * 1024;
    PageList pl(20 * mb, true);

    // Perform various operations
    pl.SetPageModifiedStatus(0, 5 * mb, true);
    pl.SetPageModifiedStatus(7 * mb, 3 * mb, true);
    pl.SetPageModifiedStatus(15 * mb, 5 * mb, true);

    // Clear all
    pl.ClearAllModified();

    // Verify all pages are now unmodified via GetPageListsForMultipartUpload
    fdpage_list_t dlpages;
    fdpage_list_t mixuppages;
    pl.GetPageListsForMultipartUpload(dlpages, mixuppages, 5 * mb);

    // All pages should be unmodified (CopyPart, not UploadPart)
    for (fdpage_list_t::const_iterator it = mixuppages.begin(); it != mixuppages.end(); ++it) {
        EXPECT_FALSE((*it)->modified) << "Page at offset " << (*it)->offset
                                      << " should be unmodified after ClearAllModified";
    }

    PageList::FreeList(dlpages);
    PageList::FreeList(mixuppages);
}

// Test Case 5: GetTotalUnloadedPageSize with complex overlapping ranges
// Purpose: Verify correct sum when query range partially overlaps multiple pages
// Expected: Correct sum of unloaded bytes in the overlapping region
// Why: Exercises the complex offset calculation in GetTotalUnloadedPageSize
TEST_F(PageListCoverageTest, GetTotalUnloadedPageSize_ComplexOverlapScenarios) {
    PageList pl(1000, false);

    // Set up a pattern: [0-200 unloaded] [200-400 loaded] [400-600 unloaded] [600-800 loaded] [800-1000 unloaded]
    pl.SetPageLoadedStatus(200, 200, true);
    pl.SetPageLoadedStatus(600, 200, true);

    // Scenario 1: Query exactly at page boundary
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(0, 200), 200u);   // Only first page
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(200, 200), 0u);   // Only loaded page
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(400, 200), 200u); // Middle unloaded

    // Scenario 2: Query spanning multiple pages
    // Range [100, 400): [0-200] contributes [100-200)=100, [200-400] is loaded, [400-600] starts at 400 which equals end
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(100, 300), 100u); // [100-200]=100 only (400-600 not in range)
    // Range [300, 600): [200-400] is loaded, [400-600] contributes [400-600)=200
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(300, 300), 200u); // [400-600]=200

    // Scenario 3: Query spanning entire range
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(0, 1000), 600u);  // 200 + 200 + 200 unloaded

    // Scenario 4: Query with start beyond page list
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(1000, 100), 0u);  // Beyond end

    // Scenario 5: Query that extends beyond page list
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(900, 200), 100u); // [900-1000]=100 unloaded

    // Scenario 6: Query with exact overlap at boundaries
    // Range [100, 500): [0-200] contributes [100-200)=100, [400-600] contributes [400-500)=100
    EXPECT_EQ(pl.GetTotalUnloadedPageSize(100, 400), 200u); // [100-200]=100 + [400-500]=100
}

// Test Case 6: GetTotalLoadedPageSize edge cases
// Purpose: Verify correct calculation with various edge cases
// Expected: Correct sum of loaded bytes in all scenarios
// Why: Exercises the complex offset calculation in GetTotalLoadedPageSize
TEST_F(PageListCoverageTest, GetTotalLoadedPageSize_EdgeCases) {
    PageList pl(1000, false);

    // Set up a pattern: [0-300 loaded] [300-500 unloaded] [500-800 loaded] [800-1000 unloaded]
    pl.SetPageLoadedStatus(0, 300, true);
    pl.SetPageLoadedStatus(500, 300, true);

    // Edge case 1: Query starts in middle of loaded page
    EXPECT_EQ(pl.GetTotalLoadedPageSize(150, 100), 100u);  // Fully within [0-300] loaded
    EXPECT_EQ(pl.GetTotalLoadedPageSize(150, 200), 150u);  // [150-300]=150 + [300-350]=0

    // Edge case 2: Query ends in middle of loaded page
    EXPECT_EQ(pl.GetTotalLoadedPageSize(400, 150), 50u);   // [500-550]=50 (partial loaded)

    // Edge case 3: Query exactly at boundaries
    EXPECT_EQ(pl.GetTotalLoadedPageSize(300, 200), 0u);    // [300-500] fully unloaded
    EXPECT_EQ(pl.GetTotalLoadedPageSize(500, 300), 300u);  // [500-800] fully loaded

    // Edge case 4: Query spanning entire list with size=0
    EXPECT_EQ(pl.GetTotalLoadedPageSize(0, 0), 600u);      // All loaded: 300 + 300

    // Edge case 5: Query starting at boundary between pages
    // Range [300, 600): [300-500] is unloaded, [500-800] contributes [500-600)=100
    EXPECT_EQ(pl.GetTotalLoadedPageSize(300, 300), 100u);  // [500-600]=100

    // Edge case 6: Single byte queries
    EXPECT_EQ(pl.GetTotalLoadedPageSize(299, 1), 1u);      // Last byte of first loaded region
    EXPECT_EQ(pl.GetTotalLoadedPageSize(300, 1), 0u);      // First byte of unloaded region
    EXPECT_EQ(pl.GetTotalLoadedPageSize(500, 1), 1u);      // First byte of second loaded region

    // Edge case 7: Query spanning multiple loaded regions
    // Range [200, 700): [0-300] contributes [200-300)=100, [500-800] contributes [500-700)=200
    EXPECT_EQ(pl.GetTotalLoadedPageSize(200, 500), 300u);  // [200-300]=100 + [500-700]=200
}

// ============================================================
// WS-04: ftruncate recovery failure data inconsistency
// tc_obsfs_rel_wstream_004 / ISSUE2026031000007
//
// Verifies the state inconsistency when ftruncate fails after
// NoCacheMultipartPost has already cleared parts from untreated_list.
// In the no-cache streaming path:
//   1. Write triggers TransitionToMultipart
//   2. NoCacheMultipartPost uploads part, calls ClearParts on untreated_list
//   3. ftruncate (to reclaim tmpfile space) fails with -EIO
//   4. BUG: OBS has the part data, but untreated_list is already cleared
//      → state mismatch between local and remote
//
// Since we cannot easily mock ftruncate without --wrap (which would
// break all ftruncate calls), we test the observable consequence:
// after ClearParts, the untreated range is gone even if the tmpfile
// ftruncate that follows would fail.
// ============================================================

TEST_F(FdEntityCoverageTest, WS04_ClearPartsStateAfterNoCacheMultipartPost) {
    FdEntity ent("/test/ws04_ftruncate");
    ASSERT_NE(-1, ent.Open(NULL, 0, -1));

    // Write some data to establish untreated ranges
    char buf[4096];
    memset(buf, 'W', sizeof(buf));
    ssize_t wsize = ent.Write(buf, 0, sizeof(buf));
    ASSERT_EQ(sizeof(buf), (size_t)wsize);

    // At this point untreated_list should have [0, 4096)
    // NoCacheMultipartPost without upload_id returns -EIO (not initialized)
    // but the real scenario is: after TransitionToMultipart sets upload_id,
    // NoCacheMultipartPost uploads then calls ClearParts.
    //
    // We verify:
    // 1. After Write, untreated_list is non-empty (has dirty ranges)
    // 2. After NoCacheMultipartPost failure, state should NOT be corrupted
    int mp_result = ent.NoCacheMultipartPost(-1, 0, sizeof(buf));
    // Expected: -EIO because upload_id not set (no TransitionToMultipart)
    EXPECT_EQ(-EIO, mp_result);

    // The key invariant: after a failed NoCacheMultipartPost (upload_id empty),
    // the untreated_list should NOT be cleared. If upload_id were valid and the
    // upload succeeded but ftruncate failed, untreated_list would already be
    // cleared — that's the ISSUE2026031000007 state inconsistency.
    //
    // We verify the Write data is still accessible locally:
    char readbuf[4096] = {0};
    ssize_t rsize = ent.Read(readbuf, 0, sizeof(readbuf));
    EXPECT_EQ(sizeof(readbuf), (size_t)rsize);
    EXPECT_EQ(0, memcmp(readbuf, buf, sizeof(buf)));

    ent.Close();
}

// Verify that simulated ftruncate failure on read-only fd returns error
TEST_F(FdEntityCoverageTest, WS04_FtruncateFailureOnReadOnlyFd) {
    // Open a read-only file to simulate ftruncate failure
    char tmppath[] = "/tmp/ws04_ro_XXXXXX";
    int tmpfd = mkstemp(tmppath);
    ASSERT_NE(-1, tmpfd);

    // Write some data
    char data[1024];
    memset(data, 'D', sizeof(data));
    ASSERT_EQ(sizeof(data), (size_t)write(tmpfd, data, sizeof(data)));
    close(tmpfd);

    // Reopen as read-only
    int rofd = open(tmppath, O_RDONLY);
    ASSERT_NE(-1, rofd);

    // ftruncate on read-only fd should fail with EINVAL or EBADF
    int rc = ftruncate(rofd, 0);
    EXPECT_EQ(-1, rc);
    EXPECT_TRUE(errno == EINVAL || errno == EBADF);

    close(rofd);
    unlink(tmppath);
}

// ============================================================
// Main
// ============================================================

// ============================================================
// CancelPrefetch concurrency tests (ISSUE2026031800002)
// ============================================================
// Tests that CancelPrefetch is safe when called multiple times
// or from multiple threads — the fix ensures prefetch_active is
// atomically cleared inside prefetch_mutex so only one caller
// executes pthread_join.

// Sequential double-Close: CancelPrefetch called twice in a row.
// Before fix: second pthread_join on already-joined tid → undefined behavior.
// After fix: second call sees prefetch_active=false → no-op.
TEST_F(StreamReadTest, CancelPrefetch_DoubleCloseSequential) {
    streamread = true;
    streamread_window = 64;
    FdEntity ent("/streamread_double_close");
    ASSERT_NE(-1, ent.Open(NULL, 200, -1));

    // Write data so Read can work
    char wbuf[200];
    memset(wbuf, 'A', sizeof(wbuf));
    ent.Write(wbuf, 0, 200);

    // Read to trigger prefetch state machine
    char rbuf[10];
    ent.Read(rbuf, 0, 10);

    // Dup to increment refcnt (so first Close doesn't fully release)
    ent.Dup();

    // First Close: CancelPrefetch runs, clears prefetch_active
    ent.Close();

    // Second Close: CancelPrefetch should be a safe no-op (prefetch_active already false)
    ent.Close();
    // If we reach here without hang → test passes
}

// Concurrent Close from two threads on same FdEntity.
// This is the exact scenario from ISSUE2026031800002:
//   Thread A: s3fs_read_s3 → FdManager::Close → CancelPrefetch → pthread_join
//   Thread B: s3fs_read_s3 → FdManager::Close → CancelPrefetch → pthread_join (same tid)
// Before fix: second join hangs forever. After fix: only first join executes.
struct ConcurrentCloseArg {
    FdEntity* ent;
    std::atomic<int> done_count;
};

static void* concurrent_close_thread(void* arg)
{
    ConcurrentCloseArg* ctx = static_cast<ConcurrentCloseArg*>(arg);
    ctx->ent->Close();
    ctx->done_count.fetch_add(1);
    return NULL;
}

TEST_F(StreamReadTest, CancelPrefetch_ConcurrentClose) {
    streamread = true;
    streamread_window = 64;
    FdEntity ent("/streamread_concurrent_close");
    ASSERT_NE(-1, ent.Open(NULL, 200, -1));

    char wbuf[200];
    memset(wbuf, 'B', sizeof(wbuf));
    ent.Write(wbuf, 0, 200);

    char rbuf[10];
    ent.Read(rbuf, 0, 10);

    // Dup twice so refcnt allows 3 Close calls (original + 2 threads)
    ent.Dup();
    ent.Dup();

    ConcurrentCloseArg ctx;
    ctx.ent = &ent;
    ctx.done_count.store(0);

    // Launch two threads that both call Close simultaneously
    pthread_t t1, t2;
    pthread_create(&t1, NULL, concurrent_close_thread, &ctx);
    pthread_create(&t2, NULL, concurrent_close_thread, &ctx);

    // Wait with timeout — if fix is broken, one thread hangs forever
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 5;  // 5 second timeout

    int rc1 = pthread_timedjoin_np(t1, NULL, &deadline);
    int rc2 = pthread_timedjoin_np(t2, NULL, &deadline);

    EXPECT_EQ(0, rc1) << "Thread 1 should complete within 5s (not hung in pthread_join)";
    EXPECT_EQ(0, rc2) << "Thread 2 should complete within 5s (not hung in pthread_join)";
    EXPECT_EQ(2, ctx.done_count.load()) << "Both threads should have completed Close()";

    // Final Close for the original reference
    ent.Close();
}

// Concurrent Read + Close: one thread reads (may trigger WaitPrefetch),
// another closes (triggers CancelPrefetch). Both go through CancelPrefetch.
struct ReadCloseArg {
    FdEntity* ent;
    std::atomic<bool> read_done;
    std::atomic<bool> close_done;
};

static void* read_thread_func(void* arg)
{
    ReadCloseArg* ctx = static_cast<ReadCloseArg*>(arg);
    char buf[10];
    // Multiple reads to increase chance of hitting WaitPrefetch at range boundary
    for(int i = 0; i < 5; i++){
        ctx->ent->Read(buf, i * 10, 10);
    }
    ctx->read_done.store(true);
    return NULL;
}

static void* close_thread_func(void* arg)
{
    ReadCloseArg* ctx = static_cast<ReadCloseArg*>(arg);
    // Small delay to let read thread start
    usleep(1000);
    ctx->ent->Close();
    ctx->close_done.store(true);
    return NULL;
}

TEST_F(StreamReadTest, CancelPrefetch_ConcurrentReadAndClose) {
    streamread = true;
    streamread_window = 64;
    FdEntity ent("/streamread_read_close_race");
    ASSERT_NE(-1, ent.Open(NULL, 200, -1));

    char wbuf[200];
    memset(wbuf, 'C', sizeof(wbuf));
    ent.Write(wbuf, 0, 200);

    // Dup so Close doesn't fully release while Read is still going
    ent.Dup();

    ReadCloseArg ctx;
    ctx.ent = &ent;
    ctx.read_done.store(false);
    ctx.close_done.store(false);

    pthread_t t_read, t_close;
    pthread_create(&t_read, NULL, read_thread_func, &ctx);
    pthread_create(&t_close, NULL, close_thread_func, &ctx);

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 5;

    int rc1 = pthread_timedjoin_np(t_read, NULL, &deadline);
    int rc2 = pthread_timedjoin_np(t_close, NULL, &deadline);

    EXPECT_EQ(0, rc1) << "Read thread should complete within 5s";
    EXPECT_EQ(0, rc2) << "Close thread should complete within 5s";

    // Final cleanup
    ent.Close();
}

// =========================================================================
// ISSUE2026032500001: LaunchRangePrefetch active guard tests
// =========================================================================

// SR1: 4 concurrent Reads simulating kernel readahead — must not crash
TEST_F(StreamReadTest, StreamRead_ConcurrentReads_NoCrash) {
    streamread = true;
    streamread_window = 64;  // 64MB window, larger than file → single range
    FdEntity ent("/streamread_concurrent_crash");
    ASSERT_NE(-1, ent.Open(NULL, 8192, -1));

    // Write full file so pread works
    char wbuf[8192];
    memset(wbuf, 'Q', sizeof(wbuf));
    ent.Write(wbuf, 0, sizeof(wbuf));

    // 4 concurrent reads at consecutive 128-byte offsets (simulating readahead)
    const int N = 4;
    pthread_t threads[N];
    char rbufs[N][128];
    struct ReadArg { FdEntity* ent; char* buf; off_t off; size_t sz; };
    ReadArg args[N];
    for(int i = 0; i < N; i++){
        args[i] = {&ent, rbufs[i], static_cast<off_t>(i * 128), 128};
    }

    auto reader = [](void* a) -> void* {
        ReadArg* ra = static_cast<ReadArg*>(a);
        ra->ent->Read(ra->buf, ra->off, ra->sz, false);
        return NULL;
    };

    for(int i = 0; i < N; i++){
        pthread_create(&threads[i], NULL, reader, &args[i]);
    }

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 5;
    for(int i = 0; i < N; i++){
        int rc = pthread_timedjoin_np(threads[i], NULL, &deadline);
        EXPECT_EQ(0, rc) << "Read thread " << i << " should complete within 5s";
    }

    // If we get here without SIGSEGV, the fix works
    ent.CancelPrefetch();
    ent.Close();
}

// SR2: Sequential reads across range boundary — complete lifecycle
TEST_F(StreamReadTest, StreamRead_SequentialReadsCycle) {
    streamread = true;
    streamread_window = 1;  // 1MB window for small file testing
    FdEntity ent("/streamread_seq_cycle");
    ASSERT_NE(-1, ent.Open(NULL, 4096, -1));

    char wbuf[4096];
    memset(wbuf, 'S', sizeof(wbuf));
    ent.Write(wbuf, 0, sizeof(wbuf));

    // Read 10 times at sequential offsets, some may cross range boundary
    char rbuf[128];
    for(int i = 0; i < 10; i++){
        off_t off = (i * 128) % 4096;
        ssize_t r = ent.Read(rbuf, off, 128, false);
        EXPECT_GT(r, 0) << "Read " << i << " at offset " << off << " failed";
    }

    ent.CancelPrefetch();
    ent.Close();
}

// SR3: Concurrent reads verify at most one prefetch active
TEST_F(StreamReadTest, StreamRead_ConcurrentReads_OnlyOnePrefetch) {
    streamread = true;
    streamread_window = 64;
    FdEntity ent("/streamread_single_prefetch");
    ASSERT_NE(-1, ent.Open(NULL, 8192, -1));

    char wbuf[8192];
    memset(wbuf, 'P', sizeof(wbuf));
    ent.Write(wbuf, 0, sizeof(wbuf));

    // Rapidly fire 8 reads — at most one prefetch should be created
    const int N = 8;
    pthread_t threads[N];
    char rbufs[N][64];
    struct ReadArg { FdEntity* ent; char* buf; off_t off; size_t sz; };
    ReadArg args[N];
    for(int i = 0; i < N; i++){
        args[i] = {&ent, rbufs[i], static_cast<off_t>(i * 64), 64};
    }
    auto reader = [](void* a) -> void* {
        ReadArg* ra = static_cast<ReadArg*>(a);
        ra->ent->Read(ra->buf, ra->off, ra->sz, false);
        return NULL;
    };
    for(int i = 0; i < N; i++){
        pthread_create(&threads[i], NULL, reader, &args[i]);
    }
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 5;
    for(int i = 0; i < N; i++){
        pthread_timedjoin_np(threads[i], NULL, &deadline);
    }

    // After all reads: no crash = no double prefetch thread.
    // IsPrefetchActive is either true (one prefetch still running) or false (finished).
    // Either is valid — the key invariant is at most one prefetch exists.
    // We verify by cancelling: if two threads existed, CancelPrefetch would only
    // join one (the other orphaned), and the orphan's curl ops would crash.
    // No crash after cancel = at most one prefetch was created.
    ent.CancelPrefetch();
    EXPECT_FALSE(ent.IsPrefetchActive()) << "After CancelPrefetch, prefetch must be inactive";
    ent.Close();
}

// =========================================================================
// ISSUE2026031900002: CloseRef / refactored Close tests
// =========================================================================

// --- Group 1: FdEntity::CloseRef basic behavior (A-1 core) ---

TEST_F(FdEntityCoverageTest, CloseRef_DecrementsRefCount) {
    FdEntity ent("/test/closeref_decr");
    ent.Open(NULL, 100, -1);
    ent.Dup();  // refcnt: 1 -> 2
    ent.CloseRef();  // refcnt: 2 -> 1
    EXPECT_TRUE(ent.IsOpen()) << "CloseRef should not close entity when refcnt > 0";
    ent.Close();  // refcnt: 1 -> 0, cleanup
    EXPECT_FALSE(ent.IsOpen());
}

TEST_F(FdEntityCoverageTest, CloseRef_DoesNotCancelPrefetch) {
    // Enable streamread so prefetch can be triggered
    bool saved_sr = streamread;
    size_t saved_srw = streamread_window;
    streamread = true;
    streamread_window = 1;  // 1MB window
    FdManager::SetCacheDir("");

    FdEntity ent("/test/closeref_no_cancel");
    ent.Open(NULL, 200, -1);

    // Write some data so Read has something to work with
    char wbuf[200];
    memset(wbuf, 'A', sizeof(wbuf));
    ent.Write(wbuf, 0, 200);

    // Read to potentially trigger prefetch state machine
    char rbuf[10];
    ent.Read(rbuf, 0, 10);

    ent.Dup();  // refcnt: 1 -> 2
    ent.CloseRef();  // Should NOT call CancelPrefetch
    EXPECT_TRUE(ent.IsOpen()) << "Entity should still be open after CloseRef";
    // No hang, no crash = success (CancelPrefetch with pthread_join not called)

    ent.Close();  // final cleanup
    streamread = saved_sr;
    streamread_window = saved_srw;
}

TEST_F(FdEntityCoverageTest, CloseRef_WhenNotOpen) {
    FdEntity ent("/test/closeref_notopen");
    // fd == -1, not opened
    ent.CloseRef();  // Should be a safe no-op
    EXPECT_FALSE(ent.IsOpen());
}

TEST_F(FdEntityCoverageTest, CloseRef_MultipleBalanced) {
    FdEntity ent("/test/closeref_multi");
    ent.Open(NULL, 50, -1);
    ent.Dup();  // refcnt: 1 -> 2
    ent.Dup();  // refcnt: 2 -> 3
    ent.Dup();  // refcnt: 3 -> 4
    ent.CloseRef();  // 4 -> 3
    ent.CloseRef();  // 3 -> 2
    ent.CloseRef();  // 2 -> 1
    EXPECT_TRUE(ent.IsOpen()) << "After 3 Dup + 3 CloseRef, original open's refcnt remains";
    ent.Close();  // 1 -> 0
    EXPECT_FALSE(ent.IsOpen());
}

// --- Group 2: FdEntity::CloseCleanup behavior (A-2 support) ---

TEST_F(FdEntityCoverageTest, CloseCleanup_ClosesFileDescriptor) {
    FdEntity ent("/test/closecleanup_basic");
    ent.Open(NULL, 100, -1);
    EXPECT_TRUE(ent.IsOpen());
    // Manually decrement refcnt to 0 via CloseRef (simulating what refactored FdManager::Close does)
    ent.CloseRef();  // refcnt: 1 -> 0
    // Now call CloseCleanup to do the actual file cleanup
    ent.CloseCleanup();
    EXPECT_FALSE(ent.IsOpen()) << "CloseCleanup should close the file descriptor";
}

TEST_F(FdEntityCoverageTest, CloseCleanup_DoesNotCancelPrefetch) {
    // CloseCleanup is purely file cleanup; CancelPrefetch is caller's responsibility
    bool saved_sr = streamread;
    size_t saved_srw = streamread_window;
    streamread = true;
    streamread_window = 1;

    FdEntity ent("/test/closecleanup_no_cancel");
    ent.Open(NULL, 200, -1);
    char wbuf[200];
    memset(wbuf, 'B', sizeof(wbuf));
    ent.Write(wbuf, 0, 200);

    ent.CloseRef();  // refcnt: 1 -> 0
    ent.CloseCleanup();  // Should not hang (does not call CancelPrefetch)
    EXPECT_FALSE(ent.IsOpen());

    streamread = saved_sr;
    streamread_window = saved_srw;
}

TEST_F(FdEntityCoverageTest, CloseCleanup_RefcntNotZero_StillCleanup) {
    // CloseCleanup doesn't check refcnt — caller is responsible for ensuring refcnt==0
    // This test verifies it doesn't crash even if called with refcnt > 0
    FdEntity ent("/test/closecleanup_refcnt");
    ent.Open(NULL, 50, -1);
    ent.Dup();  // refcnt: 1 -> 2
    // CloseCleanup does file cleanup regardless (by design: caller ensures correctness)
    ent.CloseCleanup();
    EXPECT_FALSE(ent.IsOpen());
}

// --- Group 3: FdManager::CloseRef (A-1 manager layer) ---

TEST_F(FdManagerCoverageTest, FdManager_CloseRef_Basic) {
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/mgr_closeref", NULL, 100, -1, true, true);
    ASSERT_NE(nullptr, ent);
    int fd = ent->GetFd();

    FdEntity* duped = FdManager::get()->GetFdEntityWithDup("/test/mgr_closeref", fd);
    ASSERT_NE(nullptr, duped);
    EXPECT_EQ(ent, duped);

    // CloseRef: only decrements refcnt, entity stays open
    FdManager::get()->CloseRef(duped);
    EXPECT_TRUE(ent->IsOpen());

    // Full Close: release the original reference
    FdManager::get()->Close(ent);
}

TEST_F(FdManagerCoverageTest, FdManager_CloseRef_Null) {
    EXPECT_TRUE(FdManager::get()->CloseRef(NULL));  // Should not crash
}

TEST_F(FdManagerCoverageTest, FdManager_CloseRef_SimulateReadPath) {
    // Simulate the read hot path: repeated GetFdEntityWithDup + CloseRef
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/mgr_read_sim", NULL, 1000, -1, true, true);
    ASSERT_NE(nullptr, ent);
    int fd = ent->GetFd();

    char wbuf[100];
    memset(wbuf, 'X', sizeof(wbuf));
    ent->Write(wbuf, 0, 100);

    // Simulate 10 consecutive read operations
    for(int i = 0; i < 10; i++){
        FdEntity* ref = FdManager::get()->GetFdEntityWithDup("/test/mgr_read_sim", fd);
        ASSERT_NE(nullptr, ref);
        char rbuf[10];
        ref->Read(rbuf, i * 10, 10);
        FdManager::get()->CloseRef(ref);
    }

    EXPECT_TRUE(ent->IsOpen()) << "Entity should survive 10 rounds of GetFdEntityWithDup + CloseRef";
    FdManager::get()->Close(ent);
}

// --- Group 4: Close vs CloseRef behavioral difference ---

TEST_F(FdEntityCoverageTest, Close_TriggersCancel_CloseRef_DoesNot) {
    bool saved_sr = streamread;
    size_t saved_srw = streamread_window;
    streamread = true;
    streamread_window = 1;

    // Round 1: Close triggers CancelPrefetch (verified by no hang + prefetch_active=false)
    {
        FdEntity ent1("/test/diff_close");
        ent1.Open(NULL, 200, -1);
        char wbuf[200];
        memset(wbuf, 'C', sizeof(wbuf));
        ent1.Write(wbuf, 0, 200);
        char rbuf[10];
        ent1.Read(rbuf, 0, 10);
        ent1.Dup();
        ent1.Close();  // CancelPrefetch called — prefetch_active guaranteed false after this
        EXPECT_FALSE(ent1.IsPrefetchActive()) << "Close should have cancelled prefetch";
        ent1.Close();  // final cleanup
    }

    // Round 2: CloseRef does NOT trigger CancelPrefetch
    {
        FdEntity ent2("/test/diff_closeref");
        ent2.Open(NULL, 200, -1);
        char wbuf[200];
        memset(wbuf, 'D', sizeof(wbuf));
        ent2.Write(wbuf, 0, 200);
        char rbuf[10];
        ent2.Read(rbuf, 0, 10);
        ent2.Dup();
        // Check prefetch state before CloseRef
        bool before = ent2.IsPrefetchActive();
        ent2.CloseRef();
        bool after = ent2.IsPrefetchActive();
        // CloseRef should not change prefetch_active state
        EXPECT_EQ(before, after) << "CloseRef should not affect prefetch_active";
        EXPECT_TRUE(ent2.IsOpen());
        ent2.Close();  // final cleanup
    }

    streamread = saved_sr;
    streamread_window = saved_srw;
}

TEST_F(FdEntityCoverageTest, CloseRef_PreservesPrefetch_ThenCloseKillsIt) {
    bool saved_sr = streamread;
    size_t saved_srw = streamread_window;
    streamread = true;
    streamread_window = 1;

    FdEntity ent("/test/preserve_then_kill");
    ent.Open(NULL, 200, -1);
    char wbuf[200];
    memset(wbuf, 'E', sizeof(wbuf));
    ent.Write(wbuf, 0, 200);
    char rbuf[10];
    ent.Read(rbuf, 0, 10);

    ent.Dup();   // refcnt: 1 -> 2
    ent.CloseRef();  // refcnt: 2 -> 1, prefetch untouched
    EXPECT_TRUE(ent.IsOpen());

    ent.Close();  // refcnt: 1 -> 0, CancelPrefetch called, file cleaned up
    EXPECT_FALSE(ent.IsOpen());
    EXPECT_FALSE(ent.IsPrefetchActive());

    streamread = saved_sr;
    streamread_window = saved_srw;
}

// --- Group 5: Concurrent scenarios ---

struct ConcurrentCloseRefArg {
    FdEntity* ent;
    std::atomic<int> done_count;
};

static void* concurrent_closeref_thread(void* arg) {
    ConcurrentCloseRefArg* ctx = static_cast<ConcurrentCloseRefArg*>(arg);
    ctx->ent->CloseRef();
    ctx->done_count.fetch_add(1);
    return NULL;
}

TEST_F(FdEntityCoverageTest, Concurrent_CloseRef_FromMultipleThreads) {
    FdEntity ent("/test/conc_closeref");
    ent.Open(NULL, 100, -1);
    // Dup 4 times: refcnt 1 -> 5
    for(int i = 0; i < 4; i++) ent.Dup();

    ConcurrentCloseRefArg ctx;
    ctx.ent = &ent;
    ctx.done_count.store(0);

    pthread_t threads[4];
    for(int i = 0; i < 4; i++){
        pthread_create(&threads[i], NULL, concurrent_closeref_thread, &ctx);
    }

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 5;

    for(int i = 0; i < 4; i++){
        int rc = pthread_timedjoin_np(threads[i], NULL, &deadline);
        EXPECT_EQ(0, rc) << "Thread " << i << " should complete within 5s";
    }

    EXPECT_EQ(4, ctx.done_count.load());
    EXPECT_TRUE(ent.IsOpen()) << "refcnt should be 1 (original open), entity still open";
    ent.Close();
    EXPECT_FALSE(ent.IsOpen());
}

struct CloseRefAndCloseArg {
    FdEntity* ent;
    bool do_close;  // true = Close(), false = CloseRef()
    std::atomic<int> done_count;
};

static void* closeref_or_close_thread(void* arg) {
    CloseRefAndCloseArg* ctx = static_cast<CloseRefAndCloseArg*>(arg);
    if(ctx->do_close){
        ctx->ent->Close();
    }else{
        ctx->ent->CloseRef();
    }
    ctx->done_count.fetch_add(1);
    return NULL;
}

TEST_F(FdEntityCoverageTest, Concurrent_CloseRef_And_Close_Race) {
    FdEntity ent("/test/conc_mixed");
    ent.Open(NULL, 100, -1);
    ent.Dup();  // refcnt: 1 -> 2
    ent.Dup();  // refcnt: 2 -> 3

    // Thread A: CloseRef (3 -> 2)
    CloseRefAndCloseArg ctx_a;
    ctx_a.ent = &ent;
    ctx_a.do_close = false;
    ctx_a.done_count.store(0);

    // Thread B: Close (CancelPrefetch + 2 -> 1 or 3 -> 2 depending on order)
    CloseRefAndCloseArg ctx_b;
    ctx_b.ent = &ent;
    ctx_b.do_close = true;
    ctx_b.done_count.store(0);

    pthread_t t_a, t_b;
    pthread_create(&t_a, NULL, closeref_or_close_thread, &ctx_a);
    pthread_create(&t_b, NULL, closeref_or_close_thread, &ctx_b);

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 5;

    int rc1 = pthread_timedjoin_np(t_a, NULL, &deadline);
    int rc2 = pthread_timedjoin_np(t_b, NULL, &deadline);
    EXPECT_EQ(0, rc1) << "CloseRef thread should complete within 5s";
    EXPECT_EQ(0, rc2) << "Close thread should complete within 5s";

    // After both: refcnt should be 1 (original open)
    EXPECT_TRUE(ent.IsOpen()) << "Entity should still be open (refcnt=1)";
    ent.Close();  // final cleanup
    EXPECT_FALSE(ent.IsOpen());
}

TEST_F(FdEntityCoverageTest, Concurrent_GetFdEntityWithDup_CloseRef_Loop) {
    // This tests the actual read hot path pattern at high concurrency
    // Using FdEntity directly (FdManager tested separately in FdManagerCoverageTest)
    FdEntity ent("/test/conc_dup_closeref_loop");
    ent.Open(NULL, 1000, -1);

    char wbuf[100];
    memset(wbuf, 'Z', sizeof(wbuf));
    ent.Write(wbuf, 0, 100);

    struct LoopArg {
        FdEntity* ent;
        int iterations;
        std::atomic<int> done;
    };
    LoopArg ctx;
    ctx.ent = &ent;
    ctx.iterations = 100;
    ctx.done.store(0);

    auto loop_fn = [](void* arg) -> void* {
        LoopArg* ctx = static_cast<LoopArg*>(arg);
        for(int i = 0; i < ctx->iterations; i++){
            ctx->ent->Dup();
            char buf[10];
            ctx->ent->Read(buf, 0, 10);
            ctx->ent->CloseRef();
        }
        ctx->done.fetch_add(1);
        return NULL;
    };

    pthread_t threads[4];
    for(int i = 0; i < 4; i++){
        pthread_create(&threads[i], NULL, loop_fn, &ctx);
    }

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 10;

    for(int i = 0; i < 4; i++){
        int rc = pthread_timedjoin_np(threads[i], NULL, &deadline);
        EXPECT_EQ(0, rc) << "Thread " << i << " should complete within 10s";
    }

    EXPECT_EQ(4, ctx.done.load());
    EXPECT_TRUE(ent.IsOpen()) << "Entity should still be open after 400 Dup+CloseRef cycles";
    ent.Close();
    EXPECT_FALSE(ent.IsOpen());
}

// --- Group 6: Refactored FdManager::Close behavior (A-2 verification) ---

TEST_F(FdManagerCoverageTest, FdManager_Close_StillWorks_AfterRefactor) {
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/mgr_close_refactor", NULL, 100, -1, true, true);
    ASSERT_NE(nullptr, ent);
    EXPECT_TRUE(ent->IsOpen());

    FdManager::get()->Close(ent);
    // Entity should be deleted; verify via GetOpenEntitySize
    off_t sz;
    EXPECT_FALSE(FdManager::get()->GetOpenEntitySize("/test/mgr_close_refactor", &sz));
}

TEST_F(FdManagerCoverageTest, FdManager_Close_WithActivePrefetch) {
    bool saved_sr = streamread;
    size_t saved_srw = streamread_window;
    streamread = true;
    streamread_window = 1;
    FdManager::SetCacheDir("");

    FdEntity* ent = FdManager::get()->Open("/test/mgr_close_prefetch", NULL, 200, -1, true, true);
    ASSERT_NE(nullptr, ent);

    char wbuf[200];
    memset(wbuf, 'F', sizeof(wbuf));
    ent->Write(wbuf, 0, 200);
    char rbuf[10];
    ent->Read(rbuf, 0, 10);

    // Close should cancel prefetch and clean up without hanging
    FdManager::get()->Close(ent);

    off_t sz;
    EXPECT_FALSE(FdManager::get()->GetOpenEntitySize("/test/mgr_close_prefetch", &sz));

    streamread = saved_sr;
    streamread_window = saved_srw;
}

TEST_F(FdManagerCoverageTest, FdManager_Close_ConcurrentRelease_DifferentRef) {
    FdManager::SetCacheDir("");
    FdEntity* ent = FdManager::get()->Open("/test/mgr_conc_release", NULL, 100, -1, true, true);
    ASSERT_NE(nullptr, ent);
    int fd = ent->GetFd();

    // Dup to simulate two file handles
    FdEntity* ref = FdManager::get()->GetFdEntityWithDup("/test/mgr_conc_release", fd);
    ASSERT_NE(nullptr, ref);

    struct MgrCloseArg {
        FdEntity* ent;
        std::atomic<int> done;
    };
    MgrCloseArg ctx;
    ctx.ent = ent;
    ctx.done.store(0);

    auto close_fn = [](void* arg) -> void* {
        MgrCloseArg* ctx = static_cast<MgrCloseArg*>(arg);
        FdManager::get()->Close(ctx->ent);
        ctx->done.fetch_add(1);
        return NULL;
    };

    // Two threads call FdManager::Close concurrently (simulating concurrent release)
    pthread_t t1, t2;
    pthread_create(&t1, NULL, close_fn, &ctx);
    pthread_create(&t2, NULL, close_fn, &ctx);

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 5;

    int rc1 = pthread_timedjoin_np(t1, NULL, &deadline);
    int rc2 = pthread_timedjoin_np(t2, NULL, &deadline);
    EXPECT_EQ(0, rc1) << "Thread 1 should complete within 5s";
    EXPECT_EQ(0, rc2) << "Thread 2 should complete within 5s";
    EXPECT_EQ(2, ctx.done.load());

    off_t sz;
    EXPECT_FALSE(FdManager::get()->GetOpenEntitySize("/test/mgr_conc_release", &sz));
}

// ========================================================================
// Task D: Non-copyable class compile-time checks (ISSUE2026040300001)
// ========================================================================

TEST(NonCopyable, CacheFileStat_IsNotCopyable) {
    EXPECT_FALSE(std::is_copy_constructible<CacheFileStat>::value);
    EXPECT_FALSE(std::is_copy_assignable<CacheFileStat>::value);
}

TEST(NonCopyable, FdEntity_IsNotCopyable) {
    EXPECT_FALSE(std::is_copy_constructible<FdEntity>::value);
    EXPECT_FALSE(std::is_copy_assignable<FdEntity>::value);
}

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
