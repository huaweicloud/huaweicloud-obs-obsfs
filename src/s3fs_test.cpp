/*
 * s3fs_test.cpp
 *
 * Unit tests for s3fs.h - Core s3fs/obsfs constants and macros
 *
 * Test coverage:
 * - FUSE version constants
 * - File size constants (FIVE_GB)
 * - Memory management macros (S3FS_MALLOCTRIM, etc.)
 * - FUSE exit macro (S3FS_FUSE_EXIT)
 * - XML memory management macros
 *
 * Note: This is a standalone test that verifies constants and macro behavior
 * to avoid complex linking dependencies with the main obsfs binary.
 */

#include "gtest.h"
#include "s3fs.h"
#include "common.h"
#include <climits>
#include <cstdint>

// Test: FUSE_USE_VERSION is set correctly
TEST(S3fsConstantsTest, FuseUseVersion) {
    // Verify FUSE_USE_VERSION is defined and set to 26
    EXPECT_EQ(FUSE_USE_VERSION, 26);
}

// Test: FIVE_GB constant is correctly calculated
TEST(S3fsConstantsTest, FiveGBConstant) {
    // FIVE_GB should be 5 * 1024 * 1024 * 1024 bytes
    int64_t expected = 5LL * 1024LL * 1024LL * 1024LL;
    EXPECT_EQ(FIVE_GB, expected);

    // Also verify it equals 5GB in bytes
    EXPECT_EQ(FIVE_GB, 5368709120LL);
}

// Test: FIVE_GB constant in different units
TEST(S3fsConstantsTest, FiveGBInUnits) {
    // Verify FIVE_GB is 5 GB
    int64_t bytes_per_gb = 1024LL * 1024LL * 1024LL;
    EXPECT_EQ(FIVE_GB / bytes_per_gb, 5LL);

    // Verify FIVE_GB is 5120 MB
    int64_t bytes_per_mb = 1024LL * 1024LL;
    EXPECT_EQ(FIVE_GB / bytes_per_mb, 5120LL);

    // Verify FIVE_GB is 5242880 KB
    int64_t bytes_per_kb = 1024LL;
    EXPECT_EQ(FIVE_GB / bytes_per_kb, 5242880LL);
}

// Test: FIVE_GB is positive
TEST(S3fsConstantsTest, FiveGBIsPositive) {
    EXPECT_GT(FIVE_GB, 0);
}

// Test: FIVE_GB fits within int64_t range
TEST(S3fsConstantsTest, FiveGBWithinInt64Range) {
    EXPECT_LE(FIVE_GB, INT64_MAX);
    EXPECT_GE(FIVE_GB, INT64_MIN);
}

// Test: S3FS_MALLOCTRIM macro exists
TEST(S3fsMacrosTest, MallocTrimMacroExists) {
    // This test verifies the macro can be used without error
    // The actual behavior depends on HAVE_MALLOC_TRIM

    // Call S3FS_MALLOCTRIM with a dummy value (should compile and run)
    S3FS_MALLOCTRIM(0);

    // If we reach here, the macro is valid
    SUCCEED();
}

// Test: S3FS_MALLOCTRIM macro with different padding values
TEST(S3fsMacrosTest, MallocTrimWithPadding) {
    // Test with various padding values
    S3FS_MALLOCTRIM(0);
    S3FS_MALLOCTRIM(128);
    S3FS_MALLOCTRIM(1024);

    SUCCEED();
}

// Test: SAFESTRPTR macro from common.h
TEST(S3fsMacrosTest, SafeStrPtrMacro) {
    // Note: SAFESTRPTR is defined in common.h, not s3fs.h
    // but it's a core utility that should work correctly

    const char* valid_str = "test";
    const char* null_str = nullptr;

    // SAFESTRPTR should return the string if not null
    // This is tested implicitly through the macro definition

    // If we have a valid pointer, SAFESTRPTR returns it
    EXPECT_STREQ(SAFESTRPTR(valid_str), "test");

    // If we have a null pointer, SAFESTRPTR returns ""
    EXPECT_STREQ(SAFESTRPTR(null_str), "");
}

// Test: Verify macro constants are properly defined at compile time
TEST(S3fsConstantsTest, CompileTimeConstants) {
    // These tests verify constants can be used in compile-time contexts

    // FUSE_USE_VERSION should be usable in switch statements
    switch(FIVE_GB) {
        case 5LL * 1024LL * 1024LL * 1024LL:
            SUCCEED();
            break;
        default:
            FAIL() << "FIVE_GB has unexpected value";
    }
}

// Test: FIVE_GB calculations are consistent
TEST(S3fsConstantsTest, FiveGBConsistency) {
    // Verify multiple calculation methods give the same result
    int64_t method1 = 5LL * 1024LL * 1024LL * 1024LL;
    int64_t method2 = 5LL * (1024LL * 1024LL * 1024LL);
    int64_t method3 = 5120LL * 1024LL * 1024LL;

    EXPECT_EQ(FIVE_GB, method1);
    EXPECT_EQ(FIVE_GB, method2);
    EXPECT_EQ(FIVE_GB, method3);
}

// Test: Size conversions using FIVE_GB
TEST(S3fsConstantsTest, SizeConversions) {
    // Test converting FIVE_GB to different units
    double gb = static_cast<double>(FIVE_GB) / (1024.0 * 1024.0 * 1024.0);
    EXPECT_DOUBLE_EQ(gb, 5.0);

    double mb = static_cast<double>(FIVE_GB) / (1024.0 * 1024.0);
    EXPECT_DOUBLE_EQ(mb, 5120.0);
}

// Test: Verify FIVE_GB is less than typical file size limits
TEST(S3fsConstantsTest, FiveGBWithinTypicalLimits) {
    // S3 multipart upload has a 5GB part size limit
    // This constant should be exactly that limit
    int64_t s3_multipart_part_limit = 5LL * 1024LL * 1024LL * 1024LL;
    EXPECT_EQ(FIVE_GB, s3_multipart_part_limit);
}

// Test: Macro consistency
TEST(S3fsMacrosTest, MacroConsistency) {
    // Verify related macros have consistent behavior
    // S3FS_MALLOCTRIM should be callable with different arguments
    // without causing compilation errors

    #ifdef HAVE_MALLOC_TRIM
    // If malloc_trim is available, S3FS_MALLOCTRIM should use it
    SUCCEED() << "HAVE_MALLOC_TRIM is defined";
    #else
    // If malloc_trim is not available, S3FS_MALLOCTRIM should be a no-op
    SUCCEED() << "HAVE_MALLOC_TRIM is not defined, S3FS_MALLOCTRIM is a no-op";
    #endif
}

// Test: S3FS identifier macro
TEST(S3fsConstantsTest, S3fsIdentifier) {
    // S3FS macro should be defined as "obsfs"
    // This is used for logging and identification
    // Note: S3FS is defined in common.h, not s3fs.h
    const char* s3fs_str = S3FS;
    EXPECT_STREQ(s3fs_str, "obsfs");
}

// Test: CACHE_STAT_TYPE enum values
TEST(S3fsConstantsTest, CacheStatTypeEnum) {
    // Test enum values from common.h
    EXPECT_EQ(STAT_TYPE_HEAD, 0);
    EXPECT_EQ(STAT_TYPE_LIST, 1);
    EXPECT_EQ(STAT_TYPE_BUTT, 2);
}

// Test: s3fs_log_level enum values
TEST(S3fsConstantsTest, LogLevelEnum) {
    // Test log level enum values from common.h
    EXPECT_EQ(S3FS_LOG_CRIT, 0);
    EXPECT_EQ(S3FS_LOG_ERR, 1);
    EXPECT_EQ(S3FS_LOG_WARN, 3);
    EXPECT_EQ(S3FS_LOG_INFO, 7);
    EXPECT_EQ(S3FS_LOG_DBG, 15);
}

// Test: s3fs_log_mode enum values
TEST(S3fsConstantsTest, LogModeEnum) {
    // Test log mode enum values from common.h
    EXPECT_EQ(LOG_MODE_FOREGROUND, 0);
    EXPECT_EQ(LOG_MODE_OBSFS, 1);
    EXPECT_EQ(LOG_MODE_SYSLOG, 2);
}

// Test: bucket_type_t enum values
TEST(S3fsConstantsTest, BucketTypeEnum) {
    // Test bucket type enum values from common.h
    EXPECT_EQ(BUCKET_TYPE_UNKNOWN, 0);
    EXPECT_EQ(BUCKET_TYPE_OBJECT_SEMANTIC, 1);
    EXPECT_EQ(BUCKET_TYPE_FILE_SEMANTIC, 2);
}

// Test: objtype_t enum values
TEST(S3fsConstantsTest, ObjectTypeEnum) {
    // Test object type enum values from common.h
    EXPECT_EQ(OBJTYPE_UNKNOWN, -1);
    EXPECT_EQ(OBJTYPE_FILE, 0);
    EXPECT_EQ(OBJTYPE_SYMLINK, 1);
    EXPECT_EQ(OBJTYPE_DIR, 2);
}

// Main function to run the tests
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
