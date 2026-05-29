/*
 * hws_s3fs_statis_api_test.cpp - Unit tests for hws_s3fs_statis_api
 *
 * Tests the actual statistics API implementation by linking against
 * the real source code (hws_s3fs_statis_api.o, hws_configure.o, obsfs_log.o)
 */

#include "gtest.h"
#include "hws_s3fs_statis_api.h"
#include "common.h"
#include <cstring>
#include <sys/time.h>
#include <string>
#include <unistd.h>

// Stub variables and functions required for linking
// NOTE: test_stubs.cpp provides most stubs - only define test-specific ones here
// NOTE: IndexLogEntry is defined in obsfs_log.cpp, which is linked in
// We don't need to define it here - doing so would cause a duplicate symbol error

s3fs_log_level debug_level = S3FS_LOG_INFO;
const char* s3fs_log_nest[S3FS_LOG_NEST_MAX] = {"", "  ", "    ", "      "};
s3fs_log_mode debug_log_mode = LOG_MODE_FOREGROUND;
int foreground = 0;

// Stub config values - these affect when statistics are printed
// HWS_CFG_STAT_PRINT_SECONDS: Print period in seconds (default 180)
// HWS_CFG_STAT_PRINT_COUNT: Print after N operations (default 5000)
// HWS_CFG_STATIS_OPER_LONG_MS: Warn about operations longer than this (default 1000ms)

using namespace std;

namespace {

// Global test setup - initialize statistics before all tests
class HwsS3fsStatisApiTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Ensure statistics are initialized
    InitStatis();
  }

  void TearDown() override {
    // Clean up after tests
    clearStatis();
    // Reinitialize for next test
    InitStatis();
  }
};

// ============================================================================
// Tests for getDiffUs function
// ============================================================================

TEST_F(HwsS3fsStatisApiTest, GetDiffUs_BasicTimeDiff) {
    struct timeval begin, end;

    begin.tv_sec = 1;
    begin.tv_usec = 500000;  // 1.5 seconds
    end.tv_sec = 3;
    end.tv_usec = 250000;    // 3.25 seconds

    unsigned long long diff = getDiffUs(&begin, &end);
    EXPECT_EQ(diff, 1750000ULL);  // 1.75 seconds = 1,750,000 microseconds
}

TEST_F(HwsS3fsStatisApiTest, GetDiffUs_ZeroDiff) {
    struct timeval begin, end;

    begin.tv_sec = 5;
    begin.tv_usec = 100000;
    end.tv_sec = 5;
    end.tv_usec = 100000;

    unsigned long long diff = getDiffUs(&begin, &end);
    EXPECT_EQ(diff, 0ULL);
}

TEST_F(HwsS3fsStatisApiTest, GetDiffUs_MicrosecondsOverflow) {
    struct timeval begin, end;

    begin.tv_sec = 1;
    begin.tv_usec = 800000;
    end.tv_sec = 2;
    end.tv_usec = 100000;  // End has fewer microseconds

    unsigned long long diff = getDiffUs(&begin, &end);
    EXPECT_EQ(diff, 300000ULL);  // 0.3 seconds
}

TEST_F(HwsS3fsStatisApiTest, GetDiffUs_LargeTimeDiff) {
    struct timeval begin, end;

    begin.tv_sec = 0;
    begin.tv_usec = 0;
    end.tv_sec = 100;
    end.tv_usec = 0;

    unsigned long long diff = getDiffUs(&begin, &end);
    EXPECT_EQ(diff, 100000000ULL);  // 100 seconds = 100,000,000 microseconds
}

TEST_F(HwsS3fsStatisApiTest, GetDiffUs_SmallTimeDiff) {
    struct timeval begin, end;

    begin.tv_sec = 10;
    begin.tv_usec = 100000;
    end.tv_sec = 10;
    end.tv_usec = 100100;  // 100 microseconds difference

    unsigned long long diff = getDiffUs(&begin, &end);
    EXPECT_EQ(diff, 100ULL);
}

TEST_F(HwsS3fsStatisApiTest, GetDiffUs_OneSecond) {
    struct timeval begin, end;

    begin.tv_sec = 0;
    begin.tv_usec = 0;
    end.tv_sec = 1;
    end.tv_usec = 0;

    unsigned long long diff = getDiffUs(&begin, &end);
    EXPECT_EQ(diff, 1000000ULL);  // 1 second = 1,000,000 microseconds
}

TEST_F(HwsS3fsStatisApiTest, GetDiffUs_NegativeMicroseconds) {
    struct timeval begin, end;

    begin.tv_sec = 2;
    begin.tv_usec = 900000;
    end.tv_sec = 3;
    end.tv_usec = 100000;  // End time has fewer microseconds (wraps around)

    unsigned long long diff = getDiffUs(&begin, &end);
    EXPECT_EQ(diff, 200000ULL);  // 0.2 seconds
}

TEST_F(HwsS3fsStatisApiTest, GetDiffUs_MixedValues) {
    struct timeval begin, end;

    begin.tv_sec = 10;
    begin.tv_usec = 999999;
    end.tv_sec = 11;
    end.tv_usec = 1;

    unsigned long long diff = getDiffUs(&begin, &end);
    EXPECT_EQ(diff, 2ULL);  // Just 2 microseconds
}

// ============================================================================
// Tests for s3fsStatisStart function
// ============================================================================

TEST_F(HwsS3fsStatisApiTest, S3fsStatisStart_ValidInput) {
    struct timeval tv;

    // Normal case - first valid statis type
    s3fsStatisStart(MIN_STEP_STATIS, &tv);

    // Verify tv was set (should be recent, non-negative)
    EXPECT_GE(tv.tv_sec, 0);
    EXPECT_GE(tv.tv_usec, 0);
    EXPECT_LT(tv.tv_usec, 1000000);

    // Test with a different statis type
    s3fsStatisStart(GETATTR_OBJECT_TOTAL, &tv);
    EXPECT_GE(tv.tv_sec, 0);
}

TEST_F(HwsS3fsStatisApiTest, S3fsStatisStart_NullPointer) {
    // Should handle NULL pointer gracefully (prints error and returns)
    s3fsStatisStart(GETATTR_OBJECT_TOTAL, NULL);
    // If we get here without crashing, the test passes
    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, S3fsStatisStart_InvalidStatisId) {
    struct timeval tv;

    // Invalid statis ID (>= MAX_STEP_STATIS)
    s3fsStatisStart(static_cast<statis_type_t>(MAX_STEP_STATIS), &tv);

    // Should handle invalid ID gracefully
    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, S3fsStatisStart_AllValidTypes) {
    struct timeval tv;

    // Test several major statis types
    s3fsStatisStart(MIN_STEP_STATIS, &tv);
    EXPECT_GE(tv.tv_sec, 0);

    s3fsStatisStart(GETATTR_OBJECT_TOTAL, &tv);
    EXPECT_GE(tv.tv_sec, 0);

    s3fsStatisStart(WRITE_FILE_TOTAL, &tv);
    EXPECT_GE(tv.tv_sec, 0);

    s3fsStatisStart(READ_FILE_TOTAL, &tv);
    EXPECT_GE(tv.tv_sec, 0);

    s3fsStatisStart(CREATE_FILE_TOTAL, &tv);
    EXPECT_GE(tv.tv_sec, 0);

    s3fsStatisStart(FLUSH_TOTAL, &tv);
    EXPECT_GE(tv.tv_sec, 0);

    s3fsStatisStart(RENAME_TOTAL, &tv);
    EXPECT_GE(tv.tv_sec, 0);
}

TEST_F(HwsS3fsStatisApiTest, S3fsStatisStart_LastValidType) {
    struct timeval tv;

    // Test last valid type (MAX_STEP_STATIS - 1)
    s3fsStatisStart(static_cast<statis_type_t>(MAX_STEP_STATIS - 1), &tv);
    EXPECT_GE(tv.tv_sec, 0);
}

// ============================================================================
// Tests for s3fsStatisEnd function
// ============================================================================

TEST_F(HwsS3fsStatisApiTest, S3fsStatisEnd_ValidInput) {
    struct timeval begin;

    s3fsStatisStart(GETATTR_OBJECT_TOTAL, &begin);
    usleep(1000);  // Sleep for 1ms to ensure some time passes

    // Normal case - without path string
    s3fsStatisEnd(GETATTR_OBJECT_TOTAL, &begin, NULL);

    // With path string
    s3fsStatisStart(WRITE_FILE_TOTAL, &begin);
    usleep(500);
    s3fsStatisEnd(WRITE_FILE_TOTAL, &begin, "/test/path");

    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, S3fsStatisEnd_NullBeginTime) {
    // Should handle NULL pointer gracefully
    s3fsStatisEnd(GETATTR_OBJECT_TOTAL, NULL, "/test/path");
    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, S3fsStatisEnd_InvalidStatisId) {
    struct timeval begin;
    s3fsStatisStart(GETATTR_OBJECT_TOTAL, &begin);

    // Invalid statis ID
    s3fsStatisEnd(static_cast<statis_type_t>(MAX_STEP_STATIS), &begin, NULL);
    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, S3fsStatisEnd_WithLongOperation) {
    struct timeval begin;

    s3fsStatisStart(GETATTR_OBJECT_TOTAL, &begin);
    usleep(2000);  // 2ms - below warning threshold of 1000ms
    s3fsStatisEnd(GETATTR_OBJECT_TOTAL, &begin, "/test/long/path");

    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, S3fsStatisEnd_CompleteTimingCycle) {
    struct timeval begin;

    // Test complete start/end cycle
    s3fsStatisStart(READ_FILE_TOTAL, &begin);
    usleep(100);
    s3fsStatisEnd(READ_FILE_TOTAL, &begin, "/path/to/file");

    s3fsStatisStart(WRITE_FILE_TOTAL, &begin);
    usleep(150);
    s3fsStatisEnd(WRITE_FILE_TOTAL, &begin, NULL);

    s3fsStatisStart(FLUSH_TOTAL, &begin);
    usleep(50);
    s3fsStatisEnd(FLUSH_TOTAL, &begin, "/another/path");

    SUCCEED();
}

// ============================================================================
// Tests for s3fsShowStatis function
// ============================================================================

TEST_F(HwsS3fsStatisApiTest, S3fsShowStatis_EmptyStatistics) {
    // Show statistics with no data (should not crash)
    s3fsShowStatis();
    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, S3fsShowStatis_AfterRecording) {
    // Record some statistics
    struct timeval begin, end;
    gettimeofday(&begin, NULL);
    usleep(1000);
    gettimeofday(&end, NULL);
    s3fsStatis(GETATTR_OBJECT_TOTAL, 1000, &end);

    // Show statistics (should not crash)
    s3fsShowStatis();

    SUCCEED();
}

// ============================================================================
// Tests for statistics accumulation
// ============================================================================

TEST_F(HwsS3fsStatisApiTest, S3fsStatis_MultipleCalls) {
    struct timeval end;
    gettimeofday(&end, NULL);

    // Record statistics multiple times (fewer than print threshold)
    for (int i = 0; i < 10; i++) {
        s3fsStatis(GETATTR_OBJECT_TOTAL, 1000, &end);
    }

    // The function should accumulate without triggering print
    // (threshold is 5000 operations or 180 seconds)
    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, S3fsStatis_DifferentTypes) {
    struct timeval end;
    gettimeofday(&end, NULL);

    // Record different types of statistics
    s3fsStatis(GETATTR_OBJECT_TOTAL, 100, &end);
    s3fsStatis(WRITE_FILE_TOTAL, 200, &end);
    s3fsStatis(READ_FILE_TOTAL, 150, &end);
    s3fsStatis(FLUSH_TOTAL, 50, &end);
    s3fsStatis(RENAME_TOTAL, 300, &end);

    SUCCEED();
}

// ============================================================================
// Tests for InitStatis and clearStatis functions
// ============================================================================

TEST_F(HwsS3fsStatisApiTest, InitStatis_CompletesSuccessfully) {
    // Clear any previous state
    clearStatis();

    // Initialize statistics
    InitStatis();

    // The function should complete without crashing
    // We can't directly verify internal state without access to globals
    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, InitStatis_CalledTwice) {
    clearStatis();
    InitStatis();
    // Calling InitStatis twice should not crash
    InitStatis();
    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, ClearStatis_CompletesSuccessfully) {
    InitStatis();
    // Clear should not crash
    clearStatis();
    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, ClearStatis_CalledTwice) {
    InitStatis();
    clearStatis();
    // Calling clearStatis twice should not crash
    clearStatis();
    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, InitAndClearStatis_Cycle) {
    // Initialize
    InitStatis();

    // Add some statistics via Start/End
    struct timeval begin;
    s3fsStatisStart(GETATTR_OBJECT_TOTAL, &begin);
    usleep(100);
    s3fsStatisEnd(GETATTR_OBJECT_TOTAL, &begin, NULL);

    // Clear
    clearStatis();

    // Re-initialize
    InitStatis();

    // Should complete cycle without issues
    SUCCEED();
}

// ============================================================================
// Tests for statis_type_t enum values
// ============================================================================

TEST_F(HwsS3fsStatisApiTest, StatisTypeEnum_ValuesAreValid) {
    // Test that enum values are in expected range
    EXPECT_GE(MIN_STEP_STATIS, 0);
    EXPECT_LT(MIN_STEP_STATIS, MAX_STEP_STATIS);

    EXPECT_GE(GETATTR_OBJECT_TOTAL, 0);
    EXPECT_LT(GETATTR_OBJECT_TOTAL, MAX_STEP_STATIS);

    EXPECT_GE(WRITE_FILE_TOTAL, 0);
    EXPECT_LT(WRITE_FILE_TOTAL, MAX_STEP_STATIS);

    EXPECT_GE(READ_FILE_TOTAL, 0);
    EXPECT_LT(READ_FILE_TOTAL, MAX_STEP_STATIS);

    EXPECT_EQ(MAX_STEP_STATIS, 64);
}

TEST_F(HwsS3fsStatisApiTest, StatisType_BoundaryConditions) {
    struct timeval tv;

    // Test first valid type
    s3fsStatisStart(MIN_STEP_STATIS, &tv);
    EXPECT_GE(tv.tv_sec, 0);

    // Test last valid type (MAX_STEP_STATIS - 1)
    s3fsStatisStart(static_cast<statis_type_t>(MAX_STEP_STATIS - 1), &tv);
    EXPECT_GE(tv.tv_sec, 0);

    // Test invalid type (MAX_STEP_STATIS)
    s3fsStatisStart(static_cast<statis_type_t>(MAX_STEP_STATIS), &tv);
    // Should handle gracefully without crashing
}

// ============================================================================
// Tests for complete operation cycles
// ============================================================================

TEST_F(HwsS3fsStatisApiTest, CompleteOperation_Getattr) {
    struct timeval begin;

    s3fsStatisStart(GETATTR_OBJECT_TOTAL, &begin);
    usleep(100);
    s3fsStatisEnd(GETATTR_OBJECT_TOTAL, &begin, "/path/to/getattr");

    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, CompleteOperation_Write) {
    struct timeval begin;

    s3fsStatisStart(WRITE_FILE_TOTAL, &begin);
    usleep(200);
    s3fsStatisEnd(WRITE_FILE_TOTAL, &begin, "/path/to/write");

    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, CompleteOperation_Read) {
    struct timeval begin;

    s3fsStatisStart(READ_FILE_TOTAL, &begin);
    usleep(150);
    s3fsStatisEnd(READ_FILE_TOTAL, &begin, "/path/to/read");

    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, CompleteOperation_Rename) {
    struct timeval begin;

    s3fsStatisStart(RENAME_TOTAL, &begin);
    usleep(300);
    s3fsStatisEnd(RENAME_TOTAL, &begin, "/old/path");

    SUCCEED();
}

TEST_F(HwsS3fsStatisApiTest, CompleteOperation_Truncate) {
    struct timeval begin;

    s3fsStatisStart(TRUNCATE_TOTAL, &begin);
    usleep(80);
    s3fsStatisEnd(TRUNCATE_TOTAL, &begin, "/path/to/truncate");

    SUCCEED();
}

}  // namespace

// Main function to run the tests
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
