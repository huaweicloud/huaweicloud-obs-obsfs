/*
 * hws_configure_test.cpp - Unit tests for hws_configure.cpp
 *
 * Tests the actual hws_configure implementation by compiling hws_configure.cpp
 * directly into this test binary with HWS_CONFIGURE_TEST_PATH defined to use
 * a temporary config file path for testing file I/O.
 *
 * Coverage targets:
 * - HwsGetIntConfigValue (global function)
 * - HwsConfigure::HwsGetIntConfigInClass
 * - HwsConfigure::setObsFsLogLevel
 * - HwsConfigure::setFuseIntfLogLevel
 * - HwsConfigure::set_data_cache_log_level
 * - HwsConfigure::hwsApplyConfigParam
 * - HwsConfigure::hwsAnalyseConfigFile (with temp config file)
 * - HwsConfigure::hwsAnalyseConfigLine_Int (via hwsAnalyseConfigFile)
 * - HwsConfigure::hwsAnalyseConfigLine_Str (via hwsAnalyseConfigFile)
 * - HwsConfigure::getIntByParamName (via hwsAnalyseConfigFile)
 * - HwsConfigure::getStrByParamName (via hwsAnalyseConfigFile)
 * - g_hwsConfigIntTable / g_hwsConfigStrTable initialization
 */

#include "gtest.h"
#include "common.h"
#include "hws_configure.h"
#include "hws_index_cache.h"

#include <cstring>
#include <cstdio>
#include <fstream>
#include <string>
#include <unistd.h>

// External globals defined in hws_configure.cpp
extern HwsConfigIntItem_s g_hwsConfigIntTable[];
extern HwsConfigStrItem_s g_hwsConfigStrTable[];
extern s3fs_log_level fuse_intf_log_level;  // declared in hws_fd_cache.h
// g_logConfigurePath declared in hws_configure.h

// External globals from test stubs / other modules
extern s3fs_log_level debug_level;
extern s3fs_log_mode debug_log_mode;
extern bool cache_assert;
extern int gMetaCacheSize;
extern s3fs_log_level data_cache_log_level;
extern struct timespec hws_s3fs_start_ts;
extern bool g_s3fs_start_flag;

// ============================================================================
// Test fixture
// ============================================================================
class HwsConfigureRealTest : public ::testing::Test {
protected:
    // Save original config table values to restore after each test
    struct SavedIntConfig {
        int intValue;
    };
    struct SavedStrConfig {
        char strValue[HWS_CONFIG_VALUE_STR_LEN];
    };

    SavedIntConfig savedIntConfigs[HWS_CFG_INT_END];
    SavedStrConfig savedStrConfigs[HWS_CFG_STR_END];
    s3fs_log_level saved_debug_level;
    s3fs_log_mode saved_debug_log_mode;
    bool saved_cache_assert;
    int saved_gMetaCacheSize;
    s3fs_log_level saved_data_cache_log_level;
    s3fs_log_level saved_fuse_intf_log_level;
    struct timespec saved_hws_s3fs_start_ts;

    void SetUp() override {
        // Save int config table values
        for (int i = 0; i < HWS_CFG_INT_END; i++) {
            savedIntConfigs[i].intValue = g_hwsConfigIntTable[i].intValue;
        }
        // Save str config table values
        for (int i = 0; i < HWS_CFG_STR_END; i++) {
            strncpy(savedStrConfigs[i].strValue, g_hwsConfigStrTable[i].strValue, HWS_CONFIG_VALUE_STR_LEN);
        }
        saved_debug_level = debug_level;
        saved_debug_log_mode = debug_log_mode;
        saved_cache_assert = cache_assert;
        saved_gMetaCacheSize = gMetaCacheSize;
        saved_data_cache_log_level = data_cache_log_level;
        saved_fuse_intf_log_level = fuse_intf_log_level;
        saved_hws_s3fs_start_ts = hws_s3fs_start_ts;
    }

    void TearDown() override {
        // Restore int config table values
        for (int i = 0; i < HWS_CFG_INT_END; i++) {
            g_hwsConfigIntTable[i].intValue = savedIntConfigs[i].intValue;
        }
        // Restore str config table values
        for (int i = 0; i < HWS_CFG_STR_END; i++) {
            strncpy(g_hwsConfigStrTable[i].strValue, savedStrConfigs[i].strValue, HWS_CONFIG_VALUE_STR_LEN);
        }
        debug_level = saved_debug_level;
        debug_log_mode = saved_debug_log_mode;
        cache_assert = saved_cache_assert;
        gMetaCacheSize = saved_gMetaCacheSize;
        data_cache_log_level = saved_data_cache_log_level;
        fuse_intf_log_level = saved_fuse_intf_log_level;
        hws_s3fs_start_ts = saved_hws_s3fs_start_ts;

        // Remove temp config file if it exists
        std::remove(g_logConfigurePath.c_str());
    }

    // Helper to write a config file with given contents
    void writeConfigFile(const std::string& contents) {
        std::ofstream ofs(g_logConfigurePath);
        ASSERT_TRUE(ofs.is_open()) << "Failed to create config file at: " << g_logConfigurePath;
        ofs << contents;
        ofs.close();
    }
};

// ============================================================================
// Tests for g_hwsConfigIntTable initialization
// ============================================================================

TEST_F(HwsConfigureRealTest, IntTableInitialization_DefaultValues) {
    // Verify default values of all config entries
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].intValue, HWS_CONFIG_INVALID_VALUE);
    EXPECT_STREQ(g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].paramName, "dbglogmode");
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].minValue, 0);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].maxValue, 2);

    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, 180);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_COUNT].intValue, 5000);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_FUSE_INTF_LOG_LEVEL].intValue, 7);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_DATA_CACHE_LOG_LEVEL].intValue, 7);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_CACHE_ASSERT].intValue, 0);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_COULDNT_RESOLVE_HOST].intValue, 10);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_MAX_PRINT_LOG_NUM_PERIOD].intValue, 3);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STATIS_OPER_LONG_MS].intValue, 1000);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_CACHE_CHECK_CRC_OPEN].intValue, 0);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_CACHE_ATTR_SWITCH_OPEN].intValue, 0);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_CACHE_ATTR_VALID_MS].intValue, 7200000);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_CACHE_ATTR_VALID_4_LIST_MS].intValue, 5000);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_META_CACHE_CAPACITY].intValue, 20000);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_READ_PAGE_CLEAN_MS].intValue, 3000);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_MAX_CACHE_MEM_SIZE_MB].intValue, 1024);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_READ_AHEAD_STAT_DIFF_LONG].intValue, 50000);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_READ_AHEAD_STAT_DIFF_SHORT].intValue, 10000);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_FREE_CACHE_THRESHOLD_PERCENT].intValue, 20);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_READ_STAT_VEC_SIZE].intValue, 32);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_READ_STAT_SEQUENTIAL_SIZE].intValue, 24);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_READ_STAT_SIZE_THRESHOLD].intValue, 4194304);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_INTERSECT_WRITE_MERGE].intValue, 0);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_READ_PAGE_NUM].intValue, 12);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WRITE_PAGE_NUM].intValue, 12);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_SWITCH].intValue, 0);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_MAX_MEM_SIZE].intValue, 10);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_STATIS_PERIOD].intValue, 5);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_READ_TIMES].intValue, 300);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_READ_SIZE].intValue, 5);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_READ_INTERVAL_MS].intValue, 50);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_MAX_RECYCLE_TIME].intValue, 10800);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_MAX_HIT_TIMES].intValue, 500);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_MIN_READ_WRITE_PAGE_BY_CACHE_SIZE].intValue, 0);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_REQUEST_WITH_INODENO].intValue, 1);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_PERIOD_CHECK_AK_SK_CHANGE].intValue, 1);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_LIST_MAX_KEY].intValue, 110);
}

TEST_F(HwsConfigureRealTest, StrTableInitialization_DefaultValues) {
    EXPECT_STREQ(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].paramName, "dbglevel");
    EXPECT_STREQ(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "");
    EXPECT_EQ(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].minLen, 3u);
    EXPECT_EQ(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].maxLen, 11u);
}

// ============================================================================
// Tests for HwsGetIntConfigValue / HwsGetIntConfigInClass
// ============================================================================

TEST_F(HwsConfigureRealTest, HwsGetIntConfigValue_ValidEnum) {
    // Test reading a value via the global function
    int val = HwsGetIntConfigValue(HWS_CFG_STAT_PRINT_SECONDS);
    EXPECT_EQ(val, 180);
}

TEST_F(HwsConfigureRealTest, HwsGetIntConfigValue_AllEnums) {
    // Test that all enum values return their default
    EXPECT_EQ(HwsGetIntConfigValue(HWS_CFG_DEBUG_LOG_MODE), HWS_CONFIG_INVALID_VALUE);
    EXPECT_EQ(HwsGetIntConfigValue(HWS_CFG_STAT_PRINT_SECONDS), 180);
    EXPECT_EQ(HwsGetIntConfigValue(HWS_CFG_STAT_PRINT_COUNT), 5000);
    EXPECT_EQ(HwsGetIntConfigValue(HWS_CFG_FUSE_INTF_LOG_LEVEL), 7);
    EXPECT_EQ(HwsGetIntConfigValue(HWS_CFG_DATA_CACHE_LOG_LEVEL), 7);
    EXPECT_EQ(HwsGetIntConfigValue(HWS_CFG_CACHE_ASSERT), 0);
    EXPECT_EQ(HwsGetIntConfigValue(HWS_READ_PAGE_NUM), 12);
    EXPECT_EQ(HwsGetIntConfigValue(HWS_WRITE_PAGE_NUM), 12);
    EXPECT_EQ(HwsGetIntConfigValue(HWS_LIST_MAX_KEY), 110);
}

TEST_F(HwsConfigureRealTest, HwsGetIntConfigInClass_InvalidEnum) {
    // Test out-of-range enum returns invalid value
    int val = HwsConfigure::GetInstance().HwsGetIntConfigInClass(HWS_CFG_INT_END);
    EXPECT_EQ(val, HWS_CONFIG_INVALID_VALUE);
}

TEST_F(HwsConfigureRealTest, HwsGetIntConfigInClass_LargeEnum) {
    // Test very large enum value
    int val = HwsConfigure::GetInstance().HwsGetIntConfigInClass(static_cast<HwsConfigIntEnum>(999));
    EXPECT_EQ(val, HWS_CONFIG_INVALID_VALUE);
}

TEST_F(HwsConfigureRealTest, HwsGetIntConfigValue_AfterModification) {
    // Modify a value in the global table and verify it's returned
    g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue = 500;
    EXPECT_EQ(HwsGetIntConfigValue(HWS_CFG_STAT_PRINT_SECONDS), 500);
}

// ============================================================================
// Tests for setObsFsLogLevel
// ============================================================================

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_Null) {
    s3fs_log_level before = debug_level;
    HwsConfigure::GetInstance().setObsFsLogLevel(NULL);
    // Should not change debug_level
    EXPECT_EQ(debug_level, before);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_EmptyString) {
    s3fs_log_level before = debug_level;
    HwsConfigure::GetInstance().setObsFsLogLevel("");
    // Should not change debug_level
    EXPECT_EQ(debug_level, before);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_Silent) {
    HwsConfigure::GetInstance().setObsFsLogLevel("silent");
    EXPECT_EQ(debug_level, S3FS_LOG_CRIT);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_Critical) {
    HwsConfigure::GetInstance().setObsFsLogLevel("critical");
    EXPECT_EQ(debug_level, S3FS_LOG_CRIT);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_Crit) {
    HwsConfigure::GetInstance().setObsFsLogLevel("crit");
    EXPECT_EQ(debug_level, S3FS_LOG_CRIT);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_Error) {
    HwsConfigure::GetInstance().setObsFsLogLevel("error");
    EXPECT_EQ(debug_level, S3FS_LOG_ERR);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_Err) {
    HwsConfigure::GetInstance().setObsFsLogLevel("err");
    EXPECT_EQ(debug_level, S3FS_LOG_ERR);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_Wan) {
    HwsConfigure::GetInstance().setObsFsLogLevel("wan");
    EXPECT_EQ(debug_level, S3FS_LOG_WARN);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_Warn) {
    HwsConfigure::GetInstance().setObsFsLogLevel("warn");
    EXPECT_EQ(debug_level, S3FS_LOG_WARN);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_Warning) {
    HwsConfigure::GetInstance().setObsFsLogLevel("warning");
    EXPECT_EQ(debug_level, S3FS_LOG_WARN);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_Inf) {
    debug_level = S3FS_LOG_CRIT;
    HwsConfigure::GetInstance().setObsFsLogLevel("inf");
    EXPECT_EQ(debug_level, S3FS_LOG_INFO);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_Info) {
    debug_level = S3FS_LOG_CRIT;
    HwsConfigure::GetInstance().setObsFsLogLevel("info");
    EXPECT_EQ(debug_level, S3FS_LOG_INFO);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_Information) {
    debug_level = S3FS_LOG_CRIT;
    HwsConfigure::GetInstance().setObsFsLogLevel("information");
    EXPECT_EQ(debug_level, S3FS_LOG_INFO);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_Dbg) {
    HwsConfigure::GetInstance().setObsFsLogLevel("dbg");
    EXPECT_EQ(debug_level, S3FS_LOG_DBG);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_Debug) {
    HwsConfigure::GetInstance().setObsFsLogLevel("debug");
    EXPECT_EQ(debug_level, S3FS_LOG_DBG);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_UnknownLevel) {
    s3fs_log_level before = debug_level;
    HwsConfigure::GetInstance().setObsFsLogLevel("foobar");
    // Unknown level should not change debug_level
    EXPECT_EQ(debug_level, before);
}

TEST_F(HwsConfigureRealTest, SetObsFsLogLevel_CaseInsensitive) {
    HwsConfigure::GetInstance().setObsFsLogLevel("DEBUG");
    EXPECT_EQ(debug_level, S3FS_LOG_DBG);

    HwsConfigure::GetInstance().setObsFsLogLevel("WARN");
    EXPECT_EQ(debug_level, S3FS_LOG_WARN);

    HwsConfigure::GetInstance().setObsFsLogLevel("Error");
    EXPECT_EQ(debug_level, S3FS_LOG_ERR);
}

// ============================================================================
// Tests for setFuseIntfLogLevel
// ============================================================================

TEST_F(HwsConfigureRealTest, SetFuseIntfLogLevel_Default) {
    // Default FUSE_INTF_LOG_LEVEL is 7 (S3FS_LOG_INFO)
    HwsConfigure::GetInstance().setFuseIntfLogLevel();
    EXPECT_EQ(fuse_intf_log_level, (s3fs_log_level)7);
}

TEST_F(HwsConfigureRealTest, SetFuseIntfLogLevel_AfterModification) {
    g_hwsConfigIntTable[HWS_CFG_FUSE_INTF_LOG_LEVEL].intValue = 15;
    HwsConfigure::GetInstance().setFuseIntfLogLevel();
    EXPECT_EQ(fuse_intf_log_level, (s3fs_log_level)15);
}

// ============================================================================
// Tests for set_data_cache_log_level
// ============================================================================

TEST_F(HwsConfigureRealTest, SetDataCacheLogLevel_Default) {
    // Default DATA_CACHE_LOG_LEVEL is 7
    HwsConfigure::GetInstance().set_data_cache_log_level();
    EXPECT_EQ(data_cache_log_level, (s3fs_log_level)7);
}

TEST_F(HwsConfigureRealTest, SetDataCacheLogLevel_AfterModification) {
    g_hwsConfigIntTable[HWS_CFG_DATA_CACHE_LOG_LEVEL].intValue = 0;
    HwsConfigure::GetInstance().set_data_cache_log_level();
    EXPECT_EQ(data_cache_log_level, (s3fs_log_level)0);
}

// ============================================================================
// Tests for hwsApplyConfigParam
// ============================================================================

TEST_F(HwsConfigureRealTest, ApplyConfigParam_DebugLogModeInvalid) {
    // Default debug_log_mode value is -1 (HWS_CONFIG_INVALID_VALUE)
    // so the if-block should not be entered
    g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].intValue = HWS_CONFIG_INVALID_VALUE;
    s3fs_log_mode before = debug_log_mode;
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    // debug_log_mode should remain unchanged
    EXPECT_EQ(debug_log_mode, before);
}

TEST_F(HwsConfigureRealTest, ApplyConfigParam_DebugLogModeOutOfRange) {
    // Value > LOG_MODE_SYSLOG (2) should not enter the block
    g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].intValue = 5;
    s3fs_log_mode before = debug_log_mode;
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    EXPECT_EQ(debug_log_mode, before);
}

TEST_F(HwsConfigureRealTest, ApplyConfigParam_DebugLogModeForeground) {
    // Set to LOG_MODE_FOREGROUND (0)
    // When new_log_mode == LOG_MODE_FOREGROUND, the inner block is not entered
    // so debug_log_mode is NOT explicitly set (the if branch with delay is skipped)
    g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].intValue = LOG_MODE_FOREGROUND;
    debug_log_mode = LOG_MODE_SYSLOG;
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    // With LOG_MODE_FOREGROUND, the "new_log_mode != LOG_MODE_FOREGROUND" check is false
    // so debug_log_mode is NOT changed by this path
    EXPECT_EQ(debug_log_mode, LOG_MODE_SYSLOG);
}

TEST_F(HwsConfigureRealTest, ApplyConfigParam_DebugLogModeChange_WithDelay) {
    // Set to LOG_MODE_OBSFS (1), which != LOG_MODE_FOREGROUND
    // Start time is 0, so diff_in_ms will be large (> 5000ms) since system has been running
    // Set start time to current time to trigger the delay path
    clock_gettime(CLOCK_MONOTONIC_COARSE, &hws_s3fs_start_ts);
    g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].intValue = LOG_MODE_OBSFS;
    debug_log_mode = LOG_MODE_FOREGROUND;
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    // Since start time is ~now, diff_in_ms < 5000, so it should set to LOG_MODE_SYSLOG
    EXPECT_EQ(debug_log_mode, LOG_MODE_SYSLOG);
}

TEST_F(HwsConfigureRealTest, ApplyConfigParam_DebugLogModeChange_AfterDelay) {
    // Set start time far in the past so diff_in_ms > 5000
    hws_s3fs_start_ts.tv_sec = 0;
    hws_s3fs_start_ts.tv_nsec = 0;
    g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].intValue = LOG_MODE_OBSFS;
    debug_log_mode = LOG_MODE_FOREGROUND;
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    // Since start time is epoch 0, diff_in_ms will be very large (> 5000ms)
    // so it should set to the requested mode (LOG_MODE_OBSFS)
    EXPECT_EQ(debug_log_mode, LOG_MODE_OBSFS);
}

TEST_F(HwsConfigureRealTest, ApplyConfigParam_DebugLogModeSyslog) {
    // Test changing to LOG_MODE_SYSLOG (2)
    hws_s3fs_start_ts.tv_sec = 0;
    hws_s3fs_start_ts.tv_nsec = 0;
    g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].intValue = LOG_MODE_SYSLOG;
    debug_log_mode = LOG_MODE_FOREGROUND;
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    EXPECT_EQ(debug_log_mode, LOG_MODE_SYSLOG);
}

TEST_F(HwsConfigureRealTest, ApplyConfigParam_DebugLogModeSameValue) {
    // When new_log_mode == debug_log_mode, the "change" warning should not fire
    // but the mode should still be applied
    hws_s3fs_start_ts.tv_sec = 0;
    hws_s3fs_start_ts.tv_nsec = 0;
    g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].intValue = LOG_MODE_SYSLOG;
    debug_log_mode = LOG_MODE_SYSLOG;
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    EXPECT_EQ(debug_log_mode, LOG_MODE_SYSLOG);
}

TEST_F(HwsConfigureRealTest, ApplyConfigParam_CacheAssertChange) {
    // Set cache_assert to 0, config to 1 -> should change
    cache_assert = false;
    g_hwsConfigIntTable[HWS_CFG_CACHE_ASSERT].intValue = 1;
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    EXPECT_TRUE(cache_assert);
}

TEST_F(HwsConfigureRealTest, ApplyConfigParam_CacheAssertNoChange) {
    // Set cache_assert to 0, config to 0 -> no change
    cache_assert = false;
    g_hwsConfigIntTable[HWS_CFG_CACHE_ASSERT].intValue = 0;
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    EXPECT_FALSE(cache_assert);
}

TEST_F(HwsConfigureRealTest, ApplyConfigParam_MetaCacheCapacityIncrease) {
    // When config capacity > gMetaCacheSize, resizeMetaCacheCapacity is called
    gMetaCacheSize = 1000;
    g_hwsConfigIntTable[HWS_CFG_META_CACHE_CAPACITY].intValue = 50000;
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    // After resize, gMetaCacheSize should be updated
    EXPECT_GE(gMetaCacheSize, 50000);
}

TEST_F(HwsConfigureRealTest, ApplyConfigParam_MetaCacheCapacityNoChange) {
    // When config capacity <= gMetaCacheSize, no resize
    int before = gMetaCacheSize;
    g_hwsConfigIntTable[HWS_CFG_META_CACHE_CAPACITY].intValue = before - 1;
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    EXPECT_EQ(gMetaCacheSize, before);
}

TEST_F(HwsConfigureRealTest, ApplyConfigParam_SetsLogLevels) {
    // Verify that apply also calls setFuseIntfLogLevel and set_data_cache_log_level
    g_hwsConfigIntTable[HWS_CFG_FUSE_INTF_LOG_LEVEL].intValue = 3;
    g_hwsConfigIntTable[HWS_CFG_DATA_CACHE_LOG_LEVEL].intValue = 1;
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    EXPECT_EQ(fuse_intf_log_level, (s3fs_log_level)3);
    EXPECT_EQ(data_cache_log_level, (s3fs_log_level)1);
}

TEST_F(HwsConfigureRealTest, ApplyConfigParam_SetsObsFsLogLevel) {
    // When strValue is a valid level string, setObsFsLogLevel should apply it
    strncpy(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "debug", HWS_CONFIG_VALUE_STR_LEN - 1);
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    EXPECT_EQ(debug_level, S3FS_LOG_DBG);
}

// ============================================================================
// Tests for hwsAnalyseConfigFile - file not found
// ============================================================================

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_FileNotFound) {
    // Ensure config file does not exist
    std::remove(g_logConfigurePath.c_str());
    // Should return without crashing
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(false);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_FileNotFoundMultipleCalls) {
    // Ensure config file does not exist
    std::remove(g_logConfigurePath.c_str());
    // Call multiple times - should only log once
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(false);
}

// ============================================================================
// Tests for hwsAnalyseConfigFile - with config file (integer parsing)
// ============================================================================

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseIntValues) {
    writeConfigFile("statisprintseconds=500\nread_page_num=100\nwrite_page_num=200\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, 500);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_READ_PAGE_NUM].intValue, 100);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WRITE_PAGE_NUM].intValue, 200);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseIntOutOfRange) {
    // Value above max should be rejected
    writeConfigFile("statisprintseconds=9999\n");
    int before = g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue;
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, before);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseIntBelowMin) {
    // Value below min should be rejected
    writeConfigFile("statisprintseconds=0\n");
    int before = g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue;
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, before);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseIntAtMinBoundary) {
    writeConfigFile("statisprintseconds=1\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, 1);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseIntAtMaxBoundary) {
    writeConfigFile("statisprintseconds=1800\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, 1800);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseIntNoEquals) {
    // Line without = should not change values
    writeConfigFile("statisprintseconds 500\n");
    int before = g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue;
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, before);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseBooleanConfig) {
    writeConfigFile("cache_check_crc_open=1\ncache_attr_switch_open=1\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_CACHE_CHECK_CRC_OPEN].intValue, 1);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_CACHE_ATTR_SWITCH_OPEN].intValue, 1);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseDbgLogMode) {
    writeConfigFile("dbglogmode=1\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].intValue, 1);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseMultipleIntLines) {
    writeConfigFile(
        "dbglogmode=0\n"
        "statisprintseconds=300\n"
        "statisprintcount=1000\n"
        "fuse_intf_log_level=15\n"
        "obsfs_data_cache_log_level=3\n"
        "cacheassert=1\n"
        "can_not_resolve_host_retrycnt=50\n"
        "max_print_log_num_period=10\n"
        "statis_operate_long_ms=2000\n"
    );
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].intValue, 0);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, 300);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_COUNT].intValue, 1000);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_FUSE_INTF_LOG_LEVEL].intValue, 15);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_DATA_CACHE_LOG_LEVEL].intValue, 3);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_CACHE_ASSERT].intValue, 1);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_COULDNT_RESOLVE_HOST].intValue, 50);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_MAX_PRINT_LOG_NUM_PERIOD].intValue, 10);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STATIS_OPER_LONG_MS].intValue, 2000);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseCacheParams) {
    // Note: cache_attr_valid_ms is a substring of list_cache_attr_valid_ms,
    // so both get matched on the list_cache_attr_valid_ms line.
    // Test them separately to avoid substring matching interference.
    writeConfigFile(
        "meta_cache_capacity=50000\n"
        "read_page_clean_ms=5000\n"
        "max_cache_mem_size_mb=2048\n"
    );
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_META_CACHE_CAPACITY].intValue, 50000);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_READ_PAGE_CLEAN_MS].intValue, 5000);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_MAX_CACHE_MEM_SIZE_MB].intValue, 2048);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseCacheAttrValidMs) {
    writeConfigFile("cache_attr_valid_ms=1000\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    // cache_attr_valid_ms is also matched by list_cache_attr_valid_ms substring,
    // but since the line value is 1000, list param gets 1000 too (within its range)
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_CACHE_ATTR_VALID_MS].intValue, 1000);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseListCacheAttrValidMs) {
    writeConfigFile("list_cache_attr_valid_ms=2000\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_CACHE_ATTR_VALID_4_LIST_MS].intValue, 2000);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseReadAheadParams) {
    writeConfigFile(
        "read_ahead_stat_diff_long_ms=100000\n"
        "read_ahead_stat_diff_short_ms=20000\n"
        "free_cache_threshold_percent=50\n"
        "read_stat_vec_size=16\n"
        "read_stat_sequential_size=8\n"
        "read_stat_size_threshold=8388608\n"
    );
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_READ_AHEAD_STAT_DIFF_LONG].intValue, 100000);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_READ_AHEAD_STAT_DIFF_SHORT].intValue, 20000);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_FREE_CACHE_THRESHOLD_PERCENT].intValue, 50);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_READ_STAT_VEC_SIZE].intValue, 16);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_READ_STAT_SEQUENTIAL_SIZE].intValue, 8);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_READ_STAT_SIZE_THRESHOLD].intValue, 8388608);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseWholeCacheParams) {
    writeConfigFile(
        "whole_cache_switch=1\n"
        "whole_cache_max_mem_gb=20\n"
        "whole_cache_statis_period_second=10\n"
        "whole_cache_not_hit_times=500\n"
        "whole_cache_not_hit_size_mb=10\n"
        "whole_cache_pre_read_interval_ms=100\n"
        "whole_cache_recycle_time_second=86400\n"
        "whole_cache_max_hit_times_thousand=1000\n"
    );
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_SWITCH].intValue, 1);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_MAX_MEM_SIZE].intValue, 20);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_STATIS_PERIOD].intValue, 10);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_READ_TIMES].intValue, 500);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_READ_SIZE].intValue, 10);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_READ_INTERVAL_MS].intValue, 100);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_MAX_RECYCLE_TIME].intValue, 86400);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_WHOLE_CACHE_MAX_HIT_TIMES].intValue, 1000);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseOtherParams) {
    writeConfigFile(
        "intersect_write_merge=1\n"
        "min_read_write_page_by_cache_size=5\n"
        "request_with_inodeno=0\n"
        "period_check_ak_sk_change=0\n"
        "list_max_key=500\n"
    );
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_INTERSECT_WRITE_MERGE].intValue, 1);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_MIN_READ_WRITE_PAGE_BY_CACHE_SIZE].intValue, 5);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_REQUEST_WITH_INODENO].intValue, 0);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_PERIOD_CHECK_AK_SK_CHANGE].intValue, 0);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_LIST_MAX_KEY].intValue, 500);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_EmptyLines) {
    // Empty lines should be handled gracefully
    writeConfigFile("\n\nstatisprintseconds=300\n\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, 300);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_UnrecognizedLines) {
    // Lines not matching any param name should be ignored
    writeConfigFile("unknown_param=123\nsome_other_thing=abc\n");
    // Save values before
    int beforeStat = g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue;
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, beforeStat);
}

// ============================================================================
// Tests for hwsAnalyseConfigFile - string parsing
// ============================================================================

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseStrValues) {
    writeConfigFile("dbglevel=debug\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(false);
    EXPECT_STREQ(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "debug");
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseStrMinLength) {
    writeConfigFile("dbglevel=err\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(false);
    EXPECT_STREQ(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "err");
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseStrMaxLength) {
    writeConfigFile("dbglevel=information\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(false);
    EXPECT_STREQ(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "information");
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseStrTooShort) {
    // "ab" has length 2 < minLen (3), should be rejected
    strncpy(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "old", HWS_CONFIG_VALUE_STR_LEN - 1);
    writeConfigFile("dbglevel=ab\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(false);
    EXPECT_STREQ(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "old");
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseStrTooLong) {
    // "toolongstring123" has length > 11 (maxLen), should be rejected
    strncpy(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "old", HWS_CONFIG_VALUE_STR_LEN - 1);
    writeConfigFile("dbglevel=toolongstring123\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(false);
    EXPECT_STREQ(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "old");
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseStrNoEquals) {
    // Line without = should not change string values
    strncpy(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "old", HWS_CONFIG_VALUE_STR_LEN - 1);
    writeConfigFile("dbglevel debug\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(false);
    EXPECT_STREQ(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "old");
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_ParseStrEmptyValue) {
    // Empty value has length 0 < minLen (3), should be rejected
    strncpy(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "old", HWS_CONFIG_VALUE_STR_LEN - 1);
    writeConfigFile("dbglevel=\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(false);
    EXPECT_STREQ(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "old");
}

// ============================================================================
// Tests for hwsAnalyseConfigFile - file creation and deletion
// ============================================================================

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_FileCreatedAfterFirstCall) {
    // First call: file doesn't exist
    std::remove(g_logConfigurePath.c_str());
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);

    // Create the file
    writeConfigFile("statisprintseconds=500\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, 500);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_EmptyFile) {
    writeConfigFile("");
    int before = g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue;
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, before);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_IntThenStr) {
    // Parse int params first, then str params from same file
    writeConfigFile("statisprintseconds=500\ndbglevel=debug\n");

    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);  // int params
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, 500);

    HwsConfigure::GetInstance().hwsAnalyseConfigFile(false); // str params
    EXPECT_STREQ(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "debug");
}

// ============================================================================
// Tests for GetInstance (singleton)
// ============================================================================

TEST_F(HwsConfigureRealTest, GetInstance_ReturnsSameInstance) {
    HwsConfigure& inst1 = HwsConfigure::GetInstance();
    HwsConfigure& inst2 = HwsConfigure::GetInstance();
    EXPECT_EQ(&inst1, &inst2);
}

// ============================================================================
// Tests for config constants
// ============================================================================

TEST_F(HwsConfigureRealTest, ConfigConstants) {
    EXPECT_EQ(HWS_CONFIG_INVALID_VALUE, -1);
    EXPECT_EQ(HWS_CONFIG_VALUE_STR_LEN, 128);
    EXPECT_EQ(READ_LOG_CONFIGURE_INTERVAL, 5);
    EXPECT_EQ(OBSFS_LOG_MODE_SET_DELAY_MS, 5000);
}

// ============================================================================
// Integration test: full config file parse + apply
// ============================================================================

TEST_F(HwsConfigureRealTest, FullConfigParseAndApply) {
    hws_s3fs_start_ts.tv_sec = 0;
    hws_s3fs_start_ts.tv_nsec = 0;
    cache_assert = false;

    writeConfigFile(
        "dbglogmode=2\n"
        "cacheassert=1\n"
        "fuse_intf_log_level=15\n"
        "obsfs_data_cache_log_level=3\n"
        "dbglevel=warn\n"
    );

    // Parse both int and str params
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(false);

    // Apply all params
    HwsConfigure::GetInstance().hwsApplyConfigParam();

    // Verify all effects
    EXPECT_EQ(debug_log_mode, LOG_MODE_SYSLOG);  // dbglogmode=2
    EXPECT_TRUE(cache_assert);                     // cacheassert=1
    EXPECT_EQ(fuse_intf_log_level, (s3fs_log_level)15);  // fuse_intf_log_level=15
    EXPECT_EQ(data_cache_log_level, (s3fs_log_level)3);  // obsfs_data_cache_log_level=3
    EXPECT_EQ(debug_level, S3FS_LOG_WARN);         // dbglevel=warn
}

TEST_F(HwsConfigureRealTest, FullConfigParseAndApply_MultiplePasses) {
    hws_s3fs_start_ts.tv_sec = 0;
    hws_s3fs_start_ts.tv_nsec = 0;

    // First config
    writeConfigFile("statisprintseconds=500\ndbglevel=error\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(false);
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, 500);
    EXPECT_EQ(debug_level, S3FS_LOG_ERR);

    // Update config
    writeConfigFile("statisprintseconds=900\ndbglevel=debug\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(false);
    HwsConfigure::GetInstance().hwsApplyConfigParam();
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, 900);
    EXPECT_EQ(debug_level, S3FS_LOG_DBG);
}

// ============================================================================
// Edge case tests
// ============================================================================

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_MultipleEqualsInLine) {
    // find_last_of("=") should use the last = sign
    writeConfigFile("statisprintseconds=500=600\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    // atoi("600") = 600
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, 600);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_StrMultipleEquals) {
    // For string parsing, find_last_of("=") takes value after last =
    writeConfigFile("dbglevel=debug=warn\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(false);
    // "warn" has length 4 which is within [3, 11]
    EXPECT_STREQ(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue, "warn");
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_NegativeIntValue) {
    // Negative values should be rejected if below minValue
    // statisprintseconds has minValue=1, so -1 should be rejected
    int before = g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue;
    writeConfigFile("statisprintseconds=-1\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, before);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_NonNumericValue) {
    // atoi("abc") returns 0, which may or may not be in range
    // For statisprintseconds (min=1), 0 is out of range
    int before = g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue;
    writeConfigFile("statisprintseconds=abc\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, before);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_WhitespaceInValue) {
    // " 500" -> atoi returns 500
    writeConfigFile("statisprintseconds= 500\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, 500);
}

TEST_F(HwsConfigureRealTest, AnalyseConfigFile_TrailingContent) {
    // "500abc" -> atoi returns 500
    writeConfigFile("statisprintseconds=500abc\n");
    HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);
    EXPECT_EQ(g_hwsConfigIntTable[HWS_CFG_STAT_PRINT_SECONDS].intValue, 500);
}

// ============================================================================
// Test g_logConfigurePath override
// ============================================================================

TEST_F(HwsConfigureRealTest, ConfigPathIsTestPath) {
    // Verify that the test override of g_logConfigurePath is active
    EXPECT_NE(g_logConfigurePath.find("/tmp/"), std::string::npos);
}

// Verify g_logConfigurePath declared via hws_configure.h (not inline extern)
TEST_F(HwsConfigureRealTest, ConfigPath_NotEmpty) {
    EXPECT_FALSE(g_logConfigurePath.empty());
}

TEST_F(HwsConfigureRealTest, ConfigPath_EndsWithConfigFilename) {
    // Both test and production paths should end with a config filename
    std::string::size_type slash = g_logConfigurePath.rfind('/');
    ASSERT_NE(std::string::npos, slash);
    std::string filename = g_logConfigurePath.substr(slash + 1);
    EXPECT_FALSE(filename.empty());
}

// Main function
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
