/*
 * s3fs - FUSE-based file system backed by Amazon S3
 *
 * Unit tests for fdpage struct and related utilities
 */

#include "gtest.h"
#include "fdcache.h"
#include <string>
#include <cstring>
#include <sys/types.h>

// Test fdpage default constructor
TEST(FdPage, DefaultConstructor) {
    fdpage page;

    EXPECT_EQ(page.offset, 0);
    EXPECT_EQ(page.bytes, 0);
    EXPECT_FALSE(page.loaded);
}

// Test fdpage parameterized constructor
TEST(FdPage, ParameterizedConstructor) {
    fdpage page(1024, 4096, true);

    EXPECT_EQ(page.offset, 1024);
    EXPECT_EQ(page.bytes, 4096);
    EXPECT_TRUE(page.loaded);
}

// Test fdpage with all default parameters
TEST(FdPage, ConstructorWithDefaults) {
    fdpage page(2048, 8192);

    EXPECT_EQ(page.offset, 2048);
    EXPECT_EQ(page.bytes, 8192);
    EXPECT_FALSE(page.loaded);  // default is_loaded = false
}

// Test fdpage next() method - returns offset + bytes
TEST(FdPage, NextMethod) {
    fdpage page(1024, 4096);

    // next() should return offset + bytes
    EXPECT_EQ(page.next(), 1024 + 4096);
    EXPECT_EQ(page.next(), 5120);
}

// Test fdpage next() with zero bytes
TEST(FdPage, NextMethodZeroBytes) {
    fdpage page(1024, 0);

    EXPECT_EQ(page.next(), 1024);
}

// Test fdpage end() method - returns offset + bytes - 1
TEST(FdPage, EndMethod) {
    fdpage page(1024, 4096);

    // end() should return offset + bytes - 1
    EXPECT_EQ(page.end(), 1024 + 4096 - 1);
    EXPECT_EQ(page.end(), 5119);
}

// Test fdpage end() with zero bytes - returns 0
TEST(FdPage, EndMethodZeroBytes) {
    fdpage page(1024, 0);

    // When bytes is 0, end() returns 0
    EXPECT_EQ(page.end(), 0);
}

// Test fdpage end() with one byte
TEST(FdPage, EndMethodOneByte) {
    fdpage page(100, 1);

    // end() should be offset + bytes - 1 = 100
    EXPECT_EQ(page.end(), 100);
}

// Test fdpage with offset 0
TEST(FdPage, ZeroOffset) {
    fdpage page(0, 4096, true);

    EXPECT_EQ(page.offset, 0);
    EXPECT_EQ(page.bytes, 4096);
    EXPECT_EQ(page.next(), 4096);
    EXPECT_EQ(page.end(), 4095);
}

// Test fdpage with large offset
TEST(FdPage, LargeOffset) {
    off_t large_offset = 1024LL * 1024 * 1024;  // 1 GB
    size_t large_size = 1024 * 1024;  // 1 MB

    fdpage page(large_offset, large_size);

    EXPECT_EQ(page.offset, large_offset);
    EXPECT_EQ(page.bytes, large_size);
    EXPECT_EQ(page.next(), large_offset + large_size);
    EXPECT_EQ(page.end(), large_offset + large_size - 1);
}

// Test fdpage with maximum reasonable size
TEST(FdPage, MaxSize) {
    size_t max_size = 10 * 1024 * 1024;  // 10 MB
    fdpage page(0, max_size);

    EXPECT_EQ(page.bytes, max_size);
    EXPECT_EQ(page.next(), max_size);
    EXPECT_EQ(page.end(), max_size - 1);
}

// Test fdpage loaded flag variations
TEST(FdPage, LoadedFlagVariations) {
    fdpage page1(0, 4096, true);
    fdpage page2(0, 4096, false);

    EXPECT_TRUE(page1.loaded);
    EXPECT_FALSE(page2.loaded);
}

// Test fdpage alignment for typical page sizes
TEST(FdPage, PageAlignment) {
    size_t page_size = 4096;  // typical 4KB page

    fdpage page(0, page_size);

    // Page should be aligned
    EXPECT_EQ(page.offset % page_size, 0);
    EXPECT_EQ(page.bytes, page_size);
}

// Test fdpage for sequential pages
TEST(FdPage, SequentialPages) {
    fdpage page1(0, 4096, true);
    fdpage page2(4096, 4096, false);
    fdpage page3(8192, 4096, true);

    // Verify sequential alignment
    EXPECT_EQ(page1.next(), page2.offset);
    EXPECT_EQ(page2.next(), page3.offset);
    EXPECT_EQ(page1.end() + 1, page2.offset);
    EXPECT_EQ(page2.end() + 1, page3.offset);
}

// Test fdpage for overlapping scenario detection
TEST(FdPage, OverlappingPages) {
    fdpage page1(0, 4096, true);
    fdpage page2(2048, 4096, true);

    // Pages overlap: page2 starts in the middle of page1
    EXPECT_LT(page2.offset, page1.next());
    EXPECT_GT(page2.end(), page1.end());
}

// Test fdpage for non-overlapping scenario
TEST(FdPage, NonOverlappingPages) {
    fdpage page1(0, 4096, true);
    fdpage page2(4096, 4096, true);

    // Pages don't overlap: page2 starts exactly where page1 ends
    EXPECT_EQ(page2.offset, page1.next());
}

// Test fdpage with odd sizes
TEST(FdPage, OddSizes) {
    fdpage page(0, 12345);

    EXPECT_EQ(page.bytes, 12345);
    EXPECT_EQ(page.next(), 12345);
    EXPECT_EQ(page.end(), 12344);
}

// Test fdpage with off_t signed types
TEST(FdPage, SignedOffsetTypes) {
    off_t offset = -1;  // Invalid offset test
    fdpage page(offset, 4096);

    EXPECT_EQ(page.offset, -1);
    EXPECT_EQ(page.bytes, 4096);
    EXPECT_EQ(page.next(), 4095);  // -1 + 4096 = 4095
}

// Test fdpage size_t unsigned type
TEST(FdPage, UnsignedSizeType) {
    size_t max_size = SIZE_MAX - 1000;
    fdpage page(0, 1000);

    EXPECT_EQ(page.bytes, 1000);
    EXPECT_LE(page.bytes, max_size);
}

// Test fdpage copy semantics
TEST(FdPage, CopySemantics) {
    fdpage page1(1024, 4096, true);
    fdpage page2 = page1;

    EXPECT_EQ(page2.offset, page1.offset);
    EXPECT_EQ(page2.bytes, page1.bytes);
    EXPECT_EQ(page2.loaded, page1.loaded);

    // Modify page2, page1 should remain unchanged
    page2.loaded = false;
    EXPECT_TRUE(page1.loaded);
    EXPECT_FALSE(page2.loaded);
}

// Test fdpage with small pages (less than typical block size)
TEST(FdPage, SmallPages) {
    fdpage page(0, 512);

    EXPECT_EQ(page.bytes, 512);
    EXPECT_EQ(page.next(), 512);
    EXPECT_EQ(page.end(), 511);
}

// Test fdpage with single byte
TEST(FdPage, SingleBytePage) {
    fdpage page(100, 1);

    EXPECT_EQ(page.bytes, 1);
    EXPECT_EQ(page.next(), 101);
    EXPECT_EQ(page.end(), 100);  // offset + 1 - 1 = offset
}

// Test fdpage boundary calculations
TEST(FdPage, BoundaryCalculations) {
    off_t offset = 4095;
    size_t size = 2;

    fdpage page(offset, size);

    EXPECT_EQ(page.offset, 4095);
    EXPECT_EQ(page.bytes, 2);
    EXPECT_EQ(page.next(), 4097);
    EXPECT_EQ(page.end(), 4096);
}

// Test fdpage for boundary crossing scenario
TEST(FdPage, BoundaryCrossing) {
    size_t boundary = 4096;
    fdpage page(4095, 10);  // Page crosses 4KB boundary

    EXPECT_EQ(page.offset, 4095);
    EXPECT_EQ(page.bytes, 10);
    EXPECT_EQ(page.next(), 4105);
    EXPECT_EQ(page.end(), 4104);

    // Verify page crosses boundary
    EXPECT_LT(page.offset, boundary);
    EXPECT_GE(page.end(), boundary);
}

// Test fdpage at maximum file size range
TEST(FdPage, MaximumFileSize) {
    // Test near typical max file size limits
    off_t large_offset = INT64_MAX - 10000;
    size_t safe_size = 5000;

    fdpage page(large_offset, safe_size);

    EXPECT_EQ(page.offset, large_offset);
    EXPECT_EQ(page.bytes, safe_size);
    // Note: next() and end() might overflow, which is expected behavior
    // at extreme boundaries
}

// Test fdpage minimum valid page
TEST(FdPage, MinimumValidPage) {
    fdpage page(0, 1, true);

    EXPECT_EQ(page.offset, 0);
    EXPECT_EQ(page.bytes, 1);
    EXPECT_TRUE(page.loaded);
    EXPECT_EQ(page.next(), 1);
    EXPECT_EQ(page.end(), 0);
}

// Test fdpage for page list scenario
TEST(FdPage, PageListScenario) {
    // Simulate a list of pages
    fdpage pages[4];
    pages[0] = fdpage(0, 4096, true);
    pages[1] = fdpage(4096, 4096, true);
    pages[2] = fdpage(8192, 4096, false);
    pages[3] = fdpage(12288, 4096, false);

    // Verify sequential pages
    for (int i = 1; i < 4; i++) {
        EXPECT_EQ(pages[i].offset, pages[i-1].next());
    }

    // Verify loaded status
    EXPECT_TRUE(pages[0].loaded);
    EXPECT_TRUE(pages[1].loaded);
    EXPECT_FALSE(pages[2].loaded);
    EXPECT_FALSE(pages[3].loaded);
}

// Test fdpage gap detection
TEST(FdPage, GapDetection) {
    fdpage page1(0, 4096, true);
    fdpage page2(8192, 4096, true);  // Gap: 4096-8191

    off_t gap_start = page1.next();
    off_t gap_end = page2.offset - 1;

    EXPECT_EQ(gap_start, 4096);
    EXPECT_EQ(gap_end, 8191);
    EXPECT_EQ(gap_end - gap_start + 1, 4096);  // 4KB gap
}

// Test fdpage contiguous pages
TEST(FdPage, ContiguousPages) {
    fdpage page1(0, 4096, true);
    fdpage page2(4096, 4096, true);

    // Pages are contiguous
    EXPECT_EQ(page1.next(), page2.offset);
    EXPECT_EQ(page2.offset - page1.end(), 1);
}

// Test fdpage with mixed loaded status
TEST(FdPage, MixedLoadedStatus) {
    fdpage pages[3];
    pages[0] = fdpage(0, 4096, true);
    pages[1] = fdpage(4096, 4096, false);
    pages[2] = fdpage(8192, 4096, true);

    // Verify alternating loaded status
    EXPECT_TRUE(pages[0].loaded);
    EXPECT_FALSE(pages[1].loaded);
    EXPECT_TRUE(pages[2].loaded);
}

// Test fdpage default values match expectations
TEST(FdPage, DefaultValuesMatchExpectations) {
    fdpage page;

    // Verify all fields are zero/false
    EXPECT_EQ(page.offset, 0);
    EXPECT_EQ(page.bytes, 0);
    EXPECT_FALSE(page.loaded);

    // Verify methods handle zero bytes correctly
    EXPECT_EQ(page.next(), 0);
    EXPECT_EQ(page.end(), 0);
}

// Test fdpage size calculations
TEST(FdPage, SizeCalculations) {
    fdpage page(1024, 100);

    EXPECT_EQ(page.bytes, 100);
    EXPECT_EQ(page.next() - page.offset, 100);
    EXPECT_EQ(page.end() - page.offset + 1, 100);
}

// Test fdpage for typical file operations
TEST(FdPage, TypicalFileOperations) {
    // Simulate reading a file in 4KB chunks
    size_t chunk_size = 4096;
    off_t file_offset = 0;

    fdpage chunk1(file_offset, chunk_size, true);
    file_offset += chunk_size;

    fdpage chunk2(file_offset, chunk_size, true);
    file_offset += chunk_size;

    fdpage chunk3(file_offset, chunk_size, true);

    EXPECT_EQ(chunk1.offset, 0);
    EXPECT_EQ(chunk2.offset, 4096);
    EXPECT_EQ(chunk3.offset, 8192);
}

// Test fdpage for sparse file representation
TEST(FdPage, SparseFileRepresentation) {
    // Sparse file: only certain regions are loaded
    fdpage region1(0, 4096, true);      // Loaded
    fdpage region2(4096, 8192, false);  // Not loaded (hole)
    fdpage region3(12288, 4096, true);  // Loaded

    EXPECT_TRUE(region1.loaded);
    EXPECT_FALSE(region2.loaded);
    EXPECT_TRUE(region3.loaded);
}

// Main function to run the tests
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
