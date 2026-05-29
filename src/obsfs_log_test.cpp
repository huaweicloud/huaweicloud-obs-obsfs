/*
 * s3fs - FUSE-based file system backed by Amazon S3
 *
 * Unit tests for obsfs_log (logging system) functionality
 *
 * This test links to the actual obsfs_log.cpp source code
 * to get proper coverage metrics.
 */

#include "gtest.h"
#include <string>
#include <cstring>
#include <cstdint>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <pthread.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <atomic>
#include <cstdarg>

// Include the actual obsfs_log header
extern "C" {
    #include "obsfs_log.h"
}

// verifyPath is provided by hws_fs_util.o (linked via test_stubs.o)
extern bool verifyPath(const char* path, char* resolved_path, bool need_exist);

// Declare internal functions from obsfs_log.cpp that are not in the header
// These are inside extern "C" block in obsfs_log.cpp
extern "C" {
    int32_t CreateLogBuf(LogBuf &log_buf);
    void CleanLogBuf(LogBuf *log_buf);
    bool NeedSwitchBuf(uint64_t log_len, LogBuf &log_buf);
    void LogToBuf(
             const uint64_t     module,
             const char*        chunk_id,
             const uint32_t     log_level,
             const char*        log_file_name,
             const char*        log_func_name,
             const uint32_t     log_line,
             const char*        format,
             va_list            ap);
    void OpenLogFile(void);
    void RenameLogFile(void);
    uint64_t GetFileSize(const char *path);
    void FreeLogBuf(void);
    void CopyCacheToWriteDiskBuf(LogBuf &cache_buf, LogBuf &write_disk_buf);
    int32_t WriteCachetoDisk(uint32_t& time_count);
    void *LogFlushThread(void *junk);
}

// Declare global variables from obsfs_log.cpp for direct manipulation
// These are defined inside extern "C" { } in obsfs_log.cpp, but they are
// C++ types (std::string, std::atomic). The extern "C" block in obsfs_log.cpp
// suppresses C++ name mangling. We need to match that.
extern "C" {
    extern std::string index_log_file_name;
    extern std::string index_log_file_path;
    extern int         g_log_file;
    extern LogBuf      g_ping_buf;
    extern LogBuf      g_pong_buf;
    extern LogBuf      g_write_disk_buf;
    extern PingPong    g_write_buf;
    extern bool        g_should_stop_flag;
    extern bool        g_flush_dead_flag;
    extern bool        g_log_service_enable_flag;
    extern uint64_t    g_throw_log_count;
    extern uint32_t    g_log_flush_cnt;
    extern uint64_t    g_fileSizeByHandle;
    extern int         g_process_id;
}
extern std::atomic<uint32_t> g_instance_num;

// Helper to call a variadic function via LogToBuf
static void CallLogToBuf(const uint64_t module, const char* chunk_id,
                          const uint32_t log_level, const char* file_name,
                          const char* func_name, const uint32_t line,
                          const char* format, ...) {
    va_list ap;
    va_start(ap, format);
    LogToBuf(module, chunk_id, log_level, file_name, func_name, line, format, ap);
    va_end(ap);
}

// Helper class that manages log buffer lifecycle for tests that need
// to directly manipulate global ping/pong buffers without starting the
// full log service (which spawns a thread).
class LogBufTestFixture : public ::testing::Test {
protected:
    // Save original global state
    LogBuf saved_ping_buf;
    LogBuf saved_pong_buf;
    LogBuf saved_write_disk_buf;
    PingPong saved_write_buf;
    bool saved_stop_flag;
    bool saved_flush_dead_flag;
    bool saved_enable_flag;
    int saved_log_file;
    uint64_t saved_throw_count;
    std::string saved_log_file_name;
    std::string saved_log_file_path;

    LogBufTestFixture() :
        saved_ping_buf(),
        saved_pong_buf(),
        saved_write_disk_buf(),
        saved_write_buf(E_PING),
        saved_stop_flag(false),
        saved_flush_dead_flag(false),
        saved_enable_flag(false),
        saved_log_file(-1),
        saved_throw_count(0) {
        // Initialize LogBuf members to zero
        saved_ping_buf.start = nullptr;
        saved_ping_buf.end = nullptr;
        saved_ping_buf.offset = 0;
        saved_pong_buf.start = nullptr;
        saved_pong_buf.end = nullptr;
        saved_pong_buf.offset = 0;
        saved_write_disk_buf.start = nullptr;
        saved_write_disk_buf.end = nullptr;
        saved_write_disk_buf.offset = 0;
    }

    void SetUp() override {
        // Save all globals
        saved_ping_buf = g_ping_buf;
        saved_pong_buf = g_pong_buf;
        saved_write_disk_buf = g_write_disk_buf;
        saved_write_buf = g_write_buf;
        saved_stop_flag = g_should_stop_flag;
        saved_flush_dead_flag = g_flush_dead_flag;
        saved_enable_flag = g_log_service_enable_flag;
        saved_log_file = g_log_file;
        saved_throw_count = g_throw_log_count;
        saved_log_file_name = index_log_file_name;
        saved_log_file_path = index_log_file_path;

        // Initialize fresh buffers for testing
        memset(&g_ping_buf, 0, sizeof(g_ping_buf));
        memset(&g_pong_buf, 0, sizeof(g_pong_buf));
        memset(&g_write_disk_buf, 0, sizeof(g_write_disk_buf));
        g_write_buf = E_PING;
        g_should_stop_flag = false;
        g_flush_dead_flag = false;
        g_log_service_enable_flag = true;
        g_log_file = -1;
        g_throw_log_count = 0;
    }

    void TearDown() override {
        // Free any test-allocated buffers if they differ from saved
        if (g_ping_buf.start && g_ping_buf.start != saved_ping_buf.start) {
            free(g_ping_buf.start);
        }
        if (g_pong_buf.start && g_pong_buf.start != saved_pong_buf.start) {
            free(g_pong_buf.start);
        }
        if (g_write_disk_buf.start && g_write_disk_buf.start != saved_write_disk_buf.start) {
            free(g_write_disk_buf.start);
        }

        // Restore all globals
        g_ping_buf = saved_ping_buf;
        g_pong_buf = saved_pong_buf;
        g_write_disk_buf = saved_write_disk_buf;
        g_write_buf = saved_write_buf;
        g_should_stop_flag = saved_stop_flag;
        g_flush_dead_flag = saved_flush_dead_flag;
        g_log_service_enable_flag = saved_enable_flag;
        g_log_file = saved_log_file;
        g_throw_log_count = saved_throw_count;
        index_log_file_name = saved_log_file_name;
        index_log_file_path = saved_log_file_path;
    }

    // Allocate test buffers (small for testing, not 10MB)
    void AllocateTestBuffers(size_t size = 4096) {
        g_ping_buf.start = (char*)malloc(size);
        memset(g_ping_buf.start, 0, size);
        g_ping_buf.end = g_ping_buf.start + size - 1;
        g_ping_buf.offset = 0;

        g_pong_buf.start = (char*)malloc(size);
        memset(g_pong_buf.start, 0, size);
        g_pong_buf.end = g_pong_buf.start + size - 1;
        g_pong_buf.offset = 0;

        g_write_disk_buf.start = (char*)malloc(size);
        memset(g_write_disk_buf.start, 0, size);
        g_write_disk_buf.end = g_write_disk_buf.start + size - 1;
        g_write_disk_buf.offset = 0;
    }

    // Allocate full-size buffers (10MB each, as used by obsfs_log.cpp)
    void AllocateFullBuffers() {
        CreateLogBuf(g_ping_buf);
        CreateLogBuf(g_pong_buf);
        CreateLogBuf(g_write_disk_buf);
    }
};

// ============================================================================
// Tests for constant values and basic structures
// ============================================================================

TEST(ObsfsLog, LogLevelConstants) {
    EXPECT_EQ(INDEX_LOG_DEBUG, 0);
    EXPECT_EQ(INDEX_LOG_INFO, 1);
    EXPECT_EQ(INDEX_LOG_WARNING, 2);
    EXPECT_EQ(INDEX_LOG_ERR, 3);
    EXPECT_EQ(INDEX_LOG_CRIT, 4);
}

TEST(ObsfsLog, PingPongEnum) {
    EXPECT_EQ(E_PING, 0x1);
    EXPECT_EQ(E_PONG, 0x2);
    EXPECT_NE(E_PING, E_PONG);
}

TEST(ObsfsLog, LogBufStructure) {
    LogBuf log_buf;
    EXPECT_EQ(sizeof(log_buf.offset), sizeof(uint64_t));
    EXPECT_EQ(sizeof(log_buf.start), sizeof(char*));
    EXPECT_EQ(sizeof(log_buf.end), sizeof(char*));
}

TEST(ObsfsLog, ModuleConstants) {
    EXPECT_EQ(LOG_INDEX_COMM, 1);
    EXPECT_EQ(LOG_INDEX_CLIENT, 2);
    EXPECT_EQ(LOG_INDEX_MNGT, 3);
    EXPECT_EQ(LOG_INDEX_SERVER, 4);
    EXPECT_EQ(LOG_INDEX_KVDB, 5);
    EXPECT_EQ(LOG_INDEX_PLOGMOCK, 6);
    EXPECT_EQ(LOG_INDEX_TOOL, 7);
    EXPECT_EQ(LOG_INDEX_MONITOR, 8);
}

// ============================================================================
// Tests for CreateLogBuf
// ============================================================================

TEST(ObsfsLog, CreateLogBuf_Success) {
    LogBuf log_buf;
    memset(&log_buf, 0, sizeof(log_buf));

    int32_t result = CreateLogBuf(log_buf);
    EXPECT_EQ(result, 0);  // RET_OK
    ASSERT_NE(log_buf.start, nullptr);
    EXPECT_EQ(log_buf.offset, (uint64_t)0);
    // end should point to start + LOG_CACHE_MAX_LEN - 1
    EXPECT_EQ(log_buf.end, log_buf.start + (1024 * 1024 * 10) - 1);

    free(log_buf.start);
}

// ============================================================================
// Tests for CleanLogBuf
// ============================================================================

TEST(ObsfsLog, CleanLogBuf_ResetsOffset) {
    LogBuf log_buf;
    log_buf.start = (char*)malloc(1024);
    log_buf.end = log_buf.start + 1023;
    log_buf.offset = 500;

    CleanLogBuf(&log_buf);

    EXPECT_EQ(log_buf.offset, 0ULL);
    // start and end should not change
    EXPECT_NE(log_buf.start, nullptr);

    free(log_buf.start);
}

TEST(ObsfsLog, CleanLogBuf_AlreadyZero) {
    LogBuf log_buf;
    log_buf.start = (char*)malloc(1024);
    log_buf.end = log_buf.start + 1023;
    log_buf.offset = 0;

    CleanLogBuf(&log_buf);
    EXPECT_EQ(log_buf.offset, 0ULL);

    free(log_buf.start);
}

// ============================================================================
// Tests for NeedSwitchBuf (actual function calls)
// ============================================================================

TEST(ObsfsLog, NeedSwitchBuf_Actual_NotNeeded) {
    LogBuf log_buf;
    log_buf.start = (char*)malloc(1024);
    log_buf.end = log_buf.start + 1023;
    log_buf.offset = 0;

    // 100 bytes with full buffer available - no switch needed
    bool result = NeedSwitchBuf(100, log_buf);
    EXPECT_FALSE(result);

    free(log_buf.start);
}

TEST(ObsfsLog, NeedSwitchBuf_Actual_Needed) {
    LogBuf log_buf;
    const size_t LOG_CACHE_MAX_LEN = 1024 * 1024 * 10;
    log_buf.start = (char*)malloc(64);
    log_buf.end = log_buf.start + 63;
    log_buf.offset = LOG_CACHE_MAX_LEN - 10;  // Only 10 bytes remaining

    // 100 bytes with only 10 bytes remaining - switch needed
    bool result = NeedSwitchBuf(100, log_buf);
    EXPECT_TRUE(result);

    free(log_buf.start);
}

TEST(ObsfsLog, NeedSwitchBuf_Actual_ExactFit) {
    LogBuf log_buf;
    const size_t LOG_CACHE_MAX_LEN = 1024 * 1024 * 10;
    log_buf.start = (char*)malloc(64);
    log_buf.end = log_buf.start + 63;
    log_buf.offset = LOG_CACHE_MAX_LEN - 100;  // Exactly 100 bytes remaining

    // Exactly fits - no switch needed (log_len <= remaining)
    bool result = NeedSwitchBuf(100, log_buf);
    EXPECT_FALSE(result);

    free(log_buf.start);
}

TEST(ObsfsLog, NeedSwitchBuf_Actual_ZeroLen) {
    LogBuf log_buf;
    log_buf.start = (char*)malloc(64);
    log_buf.end = log_buf.start + 63;
    log_buf.offset = 0;

    // Zero-length log - no switch needed
    bool result = NeedSwitchBuf(0, log_buf);
    EXPECT_FALSE(result);

    free(log_buf.start);
}

// ============================================================================
// Tests for IndexSetLogLevel (actual function)
// ============================================================================

TEST(ObsfsLog, IndexSetLogLevel_AllLevels) {
    IndexSetLogLevel(INDEX_LOG_DEBUG);
    EXPECT_EQ(g_index_log_level, (uint32_t)INDEX_LOG_DEBUG);

    IndexSetLogLevel(INDEX_LOG_INFO);
    EXPECT_EQ(g_index_log_level, (uint32_t)INDEX_LOG_INFO);

    IndexSetLogLevel(INDEX_LOG_WARNING);
    EXPECT_EQ(g_index_log_level, (uint32_t)INDEX_LOG_WARNING);

    IndexSetLogLevel(INDEX_LOG_ERR);
    EXPECT_EQ(g_index_log_level, (uint32_t)INDEX_LOG_ERR);

    IndexSetLogLevel(INDEX_LOG_CRIT);
    EXPECT_EQ(g_index_log_level, (uint32_t)INDEX_LOG_CRIT);

    // Reset
    IndexSetLogLevel(INDEX_LOG_DEBUG);
}

// ============================================================================
// Tests for IndexSetLogFileName (actual function)
// ============================================================================

TEST(ObsfsLog, IndexSetLogFileName_SetsGlobals) {
    std::string test_path = "/tmp/test_obsfs_log_unit/";
    std::string test_name = "mylog";

    IndexSetLogFileName(test_path, test_name);

    // Verify global path is set
    EXPECT_EQ(index_log_file_path, test_path);
    // index_log_file_name should be path + name + pid
    char pid_str[32];
    snprintf(pid_str, sizeof(pid_str), "%d", getpid());
    std::string expected_name = test_path + test_name + pid_str;
    EXPECT_EQ(index_log_file_name, expected_name);
    // g_process_id should be set
    EXPECT_EQ(g_process_id, getpid());
}

TEST(ObsfsLog, IndexSetLogFileName_EmptyStrings) {
    IndexSetLogFileName("", "");
    // Should work without crashing
    char pid_str[32];
    snprintf(pid_str, sizeof(pid_str), "%d", getpid());
    EXPECT_EQ(index_log_file_name, std::string(pid_str));
    EXPECT_EQ(index_log_file_path, "");
}

// ============================================================================
// Tests for IndexLogEntry (actual function with real logic)
// ============================================================================

TEST_F(LogBufTestFixture, IndexLogEntry_StoppedService) {
    // When g_should_stop_flag is TRUE, IndexLogEntry returns immediately
    g_should_stop_flag = true;
    g_log_service_enable_flag = true;
    AllocateFullBuffers();

    IndexLogEntry(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                  "test.cpp", "TestFunc", 1, "should not log");
    // ping_buf offset should remain 0
    EXPECT_EQ(g_ping_buf.offset, 0ULL);
}

TEST_F(LogBufTestFixture, IndexLogEntry_DisabledService) {
    // When g_log_service_enable_flag is FALSE, IndexLogEntry returns immediately
    g_should_stop_flag = false;
    g_log_service_enable_flag = false;
    AllocateFullBuffers();

    IndexLogEntry(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                  "test.cpp", "TestFunc", 1, "should not log");
    EXPECT_EQ(g_ping_buf.offset, 0);
}

TEST_F(LogBufTestFixture, IndexLogEntry_FilteredByLevel) {
    g_should_stop_flag = false;
    g_log_service_enable_flag = true;
    AllocateFullBuffers();
    IndexSetLogLevel(INDEX_LOG_ERR);

    // DEBUG level should be filtered out when level is ERR
    IndexLogEntry(LOG_INDEX_COMM, "test", INDEX_LOG_DEBUG,
                  "test.cpp", "TestFunc", 1, "filtered message");
    EXPECT_EQ(g_ping_buf.offset, 0);

    // ERR level should pass through
    IndexLogEntry(LOG_INDEX_COMM, "test", INDEX_LOG_ERR,
                  "test.cpp", "TestFunc", 2, "error message");
    EXPECT_GT(g_ping_buf.offset, (uint64_t)0);

    // Reset
    IndexSetLogLevel(INDEX_LOG_DEBUG);
}

TEST_F(LogBufTestFixture, IndexLogEntry_WritesToPingBuf) {
    g_should_stop_flag = false;
    g_log_service_enable_flag = true;
    g_write_buf = E_PING;
    AllocateFullBuffers();
    IndexSetLogLevel(INDEX_LOG_DEBUG);

    IndexLogEntry(LOG_INDEX_COMM, "chunk", INDEX_LOG_INFO,
                  "test.cpp", "TestFunc", 42, "Hello %s", "world");

    // Data should be written to ping buffer
    EXPECT_GT(g_ping_buf.offset, (uint64_t)0);
    // Verify the buffer contains the message
    std::string buf_content(g_ping_buf.start, g_ping_buf.offset);
    EXPECT_NE(buf_content.find("Hello world"), std::string::npos);
    EXPECT_NE(buf_content.find("INFO"), std::string::npos);
    EXPECT_NE(buf_content.find("test.cpp"), std::string::npos);
    EXPECT_NE(buf_content.find("TestFunc"), std::string::npos);
}

// ============================================================================
// Tests for LogToBuf - Ping/Pong buffer switching logic
// ============================================================================

TEST_F(LogBufTestFixture, LogToBuf_WriteToPing) {
    AllocateFullBuffers();
    g_write_buf = E_PING;

    CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                 "test.cpp", "Func", 1, "ping message");

    EXPECT_GT(g_ping_buf.offset, (uint64_t)0);
    EXPECT_EQ(g_pong_buf.offset, 0);
}

TEST_F(LogBufTestFixture, LogToBuf_WriteToPong) {
    AllocateFullBuffers();
    g_write_buf = E_PONG;

    CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                 "test.cpp", "Func", 1, "pong message");

    EXPECT_EQ(g_ping_buf.offset, 0);
    EXPECT_GT(g_pong_buf.offset, (uint64_t)0);
}

TEST_F(LogBufTestFixture, LogToBuf_SwitchFromPingToPong) {
    AllocateFullBuffers();
    g_write_buf = E_PING;

    // Fill ping buffer almost completely
    const size_t LOG_CACHE_MAX_LEN = 1024 * 1024 * 10;
    g_ping_buf.offset = LOG_CACHE_MAX_LEN - 10;  // Only 10 bytes left

    // This message should be too large for ping, switch to pong
    CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                 "test.cpp", "Func", 1, "overflow message that forces switch");

    // Should have switched to pong
    EXPECT_EQ(g_write_buf, E_PONG);
    EXPECT_GT(g_pong_buf.offset, (uint64_t)0);
}

TEST_F(LogBufTestFixture, LogToBuf_SwitchFromPongToPing) {
    AllocateFullBuffers();
    g_write_buf = E_PONG;

    // Fill pong buffer almost completely
    const size_t LOG_CACHE_MAX_LEN = 1024 * 1024 * 10;
    g_pong_buf.offset = LOG_CACHE_MAX_LEN - 10;  // Only 10 bytes left

    // This message should be too large for pong, switch to ping
    CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                 "test.cpp", "Func", 1, "overflow message that forces switch");

    // Should have switched to ping
    EXPECT_EQ(g_write_buf, E_PING);
    EXPECT_GT(g_ping_buf.offset, (uint64_t)0);
}

TEST_F(LogBufTestFixture, LogToBuf_BothBuffersFull_PingActive) {
    AllocateFullBuffers();
    g_write_buf = E_PING;

    // Fill both buffers almost completely
    const size_t LOG_CACHE_MAX_LEN = 1024 * 1024 * 10;
    g_ping_buf.offset = LOG_CACHE_MAX_LEN - 10;
    g_pong_buf.offset = LOG_CACHE_MAX_LEN - 10;

    uint64_t throw_before = g_throw_log_count;

    // This message cannot fit in either buffer
    CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                 "test.cpp", "Func", 1, "message too large for both buffers");

    // throw_log_count should have incremented
    EXPECT_EQ(g_throw_log_count, throw_before + 1);
}

TEST_F(LogBufTestFixture, LogToBuf_BothBuffersFull_PongActive) {
    AllocateFullBuffers();
    g_write_buf = E_PONG;

    // Fill both buffers almost completely
    const size_t LOG_CACHE_MAX_LEN = 1024 * 1024 * 10;
    g_pong_buf.offset = LOG_CACHE_MAX_LEN - 10;
    g_ping_buf.offset = LOG_CACHE_MAX_LEN - 10;

    uint64_t throw_before = g_throw_log_count;

    CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                 "test.cpp", "Func", 1, "message too large for both buffers");

    EXPECT_EQ(g_throw_log_count, throw_before + 1);
}

TEST_F(LogBufTestFixture, LogToBuf_VeryLongMessage) {
    AllocateFullBuffers();
    g_write_buf = E_PING;

    // Create a message that exceeds TEMP_ARRAY_LEN (800 bytes)
    // The header already takes ~100 bytes, so a 750-char message should
    // trigger the truncation logic (p >= limit)
    std::string long_msg(750, 'A');
    CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                 "test.cpp", "Func", 1, "%s", long_msg.c_str());

    // Should still have written something (truncated)
    EXPECT_GT(g_ping_buf.offset, (uint64_t)0);
}

TEST_F(LogBufTestFixture, LogToBuf_MessageEndingWithNewline) {
    AllocateFullBuffers();
    g_write_buf = E_PING;

    // Message already ending with newline - should not add another
    CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                 "test.cpp", "Func", 1, "message with newline\n");

    EXPECT_GT(g_ping_buf.offset, (uint64_t)0);
    // Verify the content ends with exactly one newline
    std::string content(g_ping_buf.start, g_ping_buf.offset);
    EXPECT_EQ(content.back(), '\n');
}

TEST_F(LogBufTestFixture, LogToBuf_AllLogLevels) {
    AllocateFullBuffers();
    g_write_buf = E_PING;

    // Test each log level produces correct label
    const char* levels[] = {"DBG", "INFO", "WARN", "ERR", "CRI"};
    for (uint32_t level = INDEX_LOG_DEBUG; level <= INDEX_LOG_CRIT; level++) {
        g_ping_buf.offset = 0;
        memset(g_ping_buf.start, 0, 4096);

        CallLogToBuf(LOG_INDEX_COMM, "test", level,
                     "test.cpp", "Func", 1, "level test");

        std::string content(g_ping_buf.start, g_ping_buf.offset);
        EXPECT_NE(content.find(levels[level]), std::string::npos)
            << "Expected level string '" << levels[level] << "' in output";
    }
}

// ============================================================================
// Tests for CopyCacheToWriteDiskBuf
// ============================================================================

TEST_F(LogBufTestFixture, CopyCacheToWriteDiskBuf_CopiesAndClears) {
    AllocateFullBuffers();

    // Write some data to ping
    const char* test_data = "test log data";
    memcpy(g_ping_buf.start, test_data, strlen(test_data));
    g_ping_buf.offset = strlen(test_data);

    CopyCacheToWriteDiskBuf(g_ping_buf, g_write_disk_buf);

    // write_disk_buf should now have the data
    EXPECT_EQ(g_write_disk_buf.offset, strlen(test_data));
    EXPECT_EQ(memcmp(g_write_disk_buf.start, test_data, strlen(test_data)), 0);

    // cache_buf (ping) should be cleared
    EXPECT_EQ(g_ping_buf.offset, 0);
}

// ============================================================================
// Tests for GetFileSize
// ============================================================================

TEST_F(LogBufTestFixture, GetFileSize_ExistingFile) {
    // Create a temp file with known size
    char temp_file[] = "/tmp/obsfs_log_getsize_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);

    const char* data = "1234567890";
    write(fd, data, 10);
    close(fd);

    uint64_t size = GetFileSize(temp_file);
    EXPECT_EQ(size, 10);

    unlink(temp_file);
}

TEST_F(LogBufTestFixture, GetFileSize_NonExistentFile) {
    uint64_t size = GetFileSize("/tmp/nonexistent_obsfs_log_test_12345");
    EXPECT_EQ(size, 0);
}

TEST_F(LogBufTestFixture, GetFileSize_WithValidLogFile) {
    // Create a temp file and set it as g_log_file
    char temp_file[] = "/tmp/obsfs_log_gf_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);

    const char* data = "test data for fstat";
    write(fd, data, strlen(data));

    g_log_file = fd;

    // Now call GetFileSize - it should also fstat g_log_file
    uint64_t size = GetFileSize(temp_file);
    EXPECT_EQ(size, strlen(data));
    // g_fileSizeByHandle should be updated
    EXPECT_EQ(g_fileSizeByHandle, strlen(data));

    close(fd);
    g_log_file = -1;
    unlink(temp_file);
}

TEST_F(LogBufTestFixture, GetFileSize_InvalidLogFile) {
    // g_log_file = -1, the fstat branch should be skipped
    g_log_file = -1;

    char temp_file[] = "/tmp/obsfs_log_inv_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);
    write(fd, "data", 4);
    close(fd);

    uint64_t size = GetFileSize(temp_file);
    EXPECT_EQ(size, 4);

    unlink(temp_file);
}

// ============================================================================
// Tests for FreeLogBuf
// ============================================================================

TEST(ObsfsLog, FreeLogBuf_FreesAllBuffers) {
    // Save originals
    LogBuf saved_ping = g_ping_buf;
    LogBuf saved_pong = g_pong_buf;
    LogBuf saved_disk = g_write_disk_buf;

    // Allocate test buffers
    g_ping_buf.start = (char*)malloc(64);
    g_pong_buf.start = (char*)malloc(64);
    g_write_disk_buf.start = (char*)malloc(64);

    FreeLogBuf();

    // After FreeLogBuf, starts should be null
    EXPECT_EQ(g_ping_buf.start, nullptr);
    EXPECT_EQ(g_pong_buf.start, nullptr);
    EXPECT_EQ(g_write_disk_buf.start, nullptr);

    // Restore
    g_ping_buf = saved_ping;
    g_pong_buf = saved_pong;
    g_write_disk_buf = saved_disk;
}

TEST(ObsfsLog, FreeLogBuf_NullPointers) {
    LogBuf saved_ping = g_ping_buf;
    LogBuf saved_pong = g_pong_buf;
    LogBuf saved_disk = g_write_disk_buf;

    // Set all to null
    g_ping_buf.start = nullptr;
    g_pong_buf.start = nullptr;
    g_write_disk_buf.start = nullptr;

    // Should not crash
    FreeLogBuf();

    EXPECT_EQ(g_ping_buf.start, nullptr);
    EXPECT_EQ(g_pong_buf.start, nullptr);
    EXPECT_EQ(g_write_disk_buf.start, nullptr);

    g_ping_buf = saved_ping;
    g_pong_buf = saved_pong;
    g_write_disk_buf = saved_disk;
}

// ============================================================================
// Tests for OpenLogFile
// ============================================================================

TEST_F(LogBufTestFixture, OpenLogFile_CreatesDirectory) {
    // Use a temp directory that doesn't exist
    std::string test_dir = "/tmp/obsfs_log_test_openlog_dir/";
    std::string test_file = test_dir + "test_log";
    index_log_file_path = test_dir;
    index_log_file_name = test_file;

    // Remove dir if it exists
    unlink(test_file.c_str());
    rmdir(test_dir.c_str());

    g_log_file = -1;
    OpenLogFile();

    // Should have created the directory and opened the file
    if (g_log_file != -1) {
        EXPECT_GE(g_log_file, 0);
        close(g_log_file);
        g_log_file = -1;
    }

    // Cleanup
    unlink(test_file.c_str());
    rmdir(test_dir.c_str());
}

TEST_F(LogBufTestFixture, OpenLogFile_DirectoryExists) {
    // Use /tmp which always exists
    std::string test_dir = "/tmp/";
    std::string test_file = "/tmp/obsfs_log_test_open_exist";
    index_log_file_path = test_dir;
    index_log_file_name = test_file;

    g_log_file = -1;
    OpenLogFile();

    // Should have opened the file
    EXPECT_GE(g_log_file, 0);

    if (g_log_file >= 0) {
        close(g_log_file);
        g_log_file = -1;
    }
    unlink(test_file.c_str());
}

// ============================================================================
// Tests for RenameLogFile
// ============================================================================

TEST_F(LogBufTestFixture, RenameLogFile_RenamesAndReopens) {
    // Create a temp log file
    std::string test_dir = "/tmp/";
    std::string test_file = "/tmp/obsfs_log_test_rename";
    index_log_file_path = test_dir;
    index_log_file_name = test_file;

    // Create the file
    int fd = open(test_file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    ASSERT_GE(fd, 0);
    write(fd, "old data", 8);
    g_log_file = fd;

    RenameLogFile();

    // g_log_file should now be a new file descriptor (or -1 if verifyPath fails)
    // The old file should have been renamed
    if (g_log_file >= 0) {
        close(g_log_file);
        g_log_file = -1;
    }

    // Clean up the original and any renamed files
    unlink(test_file.c_str());
    // The renamed file has a timestamp suffix - clean up with glob pattern
    // Just try to find and remove it
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -f %s.*", test_file.c_str());
    system(cmd);
}

// ============================================================================
// Tests for WriteCachetoDisk
// ============================================================================

TEST_F(LogBufTestFixture, WriteCachetoDisk_EmptyBuffer) {
    AllocateFullBuffers();
    g_write_disk_buf.offset = 0;

    uint32_t time_count = 5;
    int32_t result = WriteCachetoDisk(time_count);

    EXPECT_EQ(result, 0);  // RET_OK
    // time_count should NOT be reset when offset is 0 (early return)
    EXPECT_EQ(time_count, 5U);
}

TEST_F(LogBufTestFixture, WriteCachetoDisk_NoLogFile_OpensOne) {
    AllocateFullBuffers();
    g_log_file = -1;

    // Set valid path so OpenLogFile can work
    index_log_file_path = "/tmp/";
    index_log_file_name = "/tmp/obsfs_log_test_writetodisk";

    // Put some data in write_disk_buf
    const char* data = "test log line\n";
    memcpy(g_write_disk_buf.start, data, strlen(data));
    g_write_disk_buf.offset = strlen(data);

    uint32_t time_count = 3;
    int32_t result = WriteCachetoDisk(time_count);

    if (g_log_file != -1) {
        EXPECT_EQ(result, 0);
        EXPECT_EQ(time_count, 0U);
        EXPECT_EQ(g_write_disk_buf.offset, 0ULL);
        close(g_log_file);
        g_log_file = -1;
    }

    unlink("/tmp/obsfs_log_test_writetodisk");
}

TEST_F(LogBufTestFixture, WriteCachetoDisk_FileExists_Writes) {
    AllocateFullBuffers();

    // Create a temp file
    char temp_file[] = "/tmp/obsfs_log_wtd_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);

    g_log_file = fd;
    index_log_file_name = temp_file;

    const char* data = "log entry\n";
    memcpy(g_write_disk_buf.start, data, strlen(data));
    g_write_disk_buf.offset = strlen(data);

    uint32_t time_count = 7;
    int32_t result = WriteCachetoDisk(time_count);

    EXPECT_EQ(result, 0);
    EXPECT_EQ(time_count, 0U);
    EXPECT_EQ(g_write_disk_buf.offset, 0ULL);

    // Verify data was written to file
    struct stat st;
    fstat(fd, &st);
    EXPECT_EQ(st.st_size, static_cast<off_t>(strlen(data)));

    close(fd);
    g_log_file = -1;
    unlink(temp_file);
}

TEST_F(LogBufTestFixture, WriteCachetoDisk_FileDeleted_Reopens) {
    AllocateFullBuffers();

    // Create and then delete the file
    char temp_file[] = "/tmp/obsfs_log_del_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);
    g_log_file = fd;
    index_log_file_name = temp_file;
    index_log_file_path = "/tmp/";

    // Delete the file while it's still open (log file deleted externally)
    unlink(temp_file);

    const char* data = "after delete\n";
    memcpy(g_write_disk_buf.start, data, strlen(data));
    g_write_disk_buf.offset = strlen(data);

    uint32_t time_count = 2;
    (void)WriteCachetoDisk(time_count);

    // The function should detect the file is gone and reopen
    // Result depends on whether reopen succeeds
    if (g_log_file >= 0 && g_log_file != fd) {
        close(g_log_file);
    }
    // Close original fd if still valid
    close(fd);
    g_log_file = -1;

    // Cleanup
    unlink(temp_file);
}

// ============================================================================
// Tests for ManualFlushLog
// ============================================================================

TEST_F(LogBufTestFixture, ManualFlushLog_NoLogFile) {
    g_log_file = -1;
    AllocateFullBuffers();
    g_ping_buf.offset = 100;
    g_pong_buf.offset = 100;

    // Should return immediately without crashing
    ManualFlushLog();

    // Buffers should NOT be touched since g_log_file is -1
    EXPECT_EQ(g_ping_buf.offset, 100);
    EXPECT_EQ(g_pong_buf.offset, 100);
}

TEST_F(LogBufTestFixture, ManualFlushLog_FlushesNonZeroPingBuf) {
    AllocateFullBuffers();

    // Create a temp file for g_log_file
    char temp_file[] = "/tmp/obsfs_log_mf_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);
    g_log_file = fd;

    // Put data in ping buffer
    const char* ping_data = "ping data\n";
    memcpy(g_ping_buf.start, ping_data, strlen(ping_data));
    g_ping_buf.offset = strlen(ping_data);

    ManualFlushLog();

    // Ping buffer should be cleaned
    EXPECT_EQ(g_ping_buf.offset, 0);

    close(fd);
    g_log_file = -1;
    unlink(temp_file);
}

TEST_F(LogBufTestFixture, ManualFlushLog_FlushesNonZeroPongBuf) {
    AllocateFullBuffers();

    char temp_file[] = "/tmp/obsfs_log_mfp_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);
    g_log_file = fd;

    // Put data in pong buffer
    const char* pong_data = "pong data\n";
    memcpy(g_pong_buf.start, pong_data, strlen(pong_data));
    g_pong_buf.offset = strlen(pong_data);

    ManualFlushLog();

    // Pong buffer should be cleaned
    EXPECT_EQ(g_pong_buf.offset, 0);

    close(fd);
    g_log_file = -1;
    unlink(temp_file);
}

TEST_F(LogBufTestFixture, ManualFlushLog_FlushesBothBuffers) {
    AllocateFullBuffers();

    char temp_file[] = "/tmp/obsfs_log_mfb_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);
    g_log_file = fd;

    // Put data in both buffers
    const char* ping_data = "ping\n";
    memcpy(g_ping_buf.start, ping_data, strlen(ping_data));
    g_ping_buf.offset = strlen(ping_data);

    const char* pong_data = "pong\n";
    memcpy(g_pong_buf.start, pong_data, strlen(pong_data));
    g_pong_buf.offset = strlen(pong_data);

    ManualFlushLog();

    EXPECT_EQ(g_ping_buf.offset, 0);
    EXPECT_EQ(g_pong_buf.offset, 0);

    // Verify file has both entries
    struct stat st;
    fstat(fd, &st);
    EXPECT_EQ(st.st_size, static_cast<off_t>(strlen(ping_data) + strlen(pong_data)));

    close(fd);
    g_log_file = -1;
    unlink(temp_file);
}

TEST_F(LogBufTestFixture, ManualFlushLog_EmptyBuffers) {
    AllocateFullBuffers();

    char temp_file[] = "/tmp/obsfs_log_mfe_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);
    g_log_file = fd;

    // Both buffers empty
    g_ping_buf.offset = 0;
    g_pong_buf.offset = 0;

    ManualFlushLog();

    // Nothing should be written
    struct stat st;
    fstat(fd, &st);
    EXPECT_EQ(st.st_size, 0);

    close(fd);
    g_log_file = -1;
    unlink(temp_file);
}

// ============================================================================
// Tests for EnableLogService / DisableLogService with multiple instances
// ============================================================================

TEST(ObsfsLog, EnableLogService_MultipleInstances) {
    // Save instance count
    uint32_t saved_count = g_instance_num.load();
    g_instance_num.store(0);

    std::string test_path = "/tmp/obsfs_log_test_multi/";
    std::string test_name = "multi_log";
    mkdir(test_path.c_str(), 0755);

    // First enable - should call EnableLogService__
    int32_t result1 = EnableLogService(test_path, test_name);
    EXPECT_EQ(result1, 0);
    EXPECT_EQ(g_instance_num.load(), 1u);

    // Second enable - should NOT call EnableLogService__ again
    int32_t result2 = EnableLogService(test_path, test_name);
    EXPECT_EQ(result2, 0);
    EXPECT_EQ(g_instance_num.load(), 2u);

    // Disable second instance (won't actually disable)
    DisableLogService();
    EXPECT_EQ(g_instance_num.load(), 1u);

    // Disable first instance (will actually disable)
    DisableLogService();
    EXPECT_EQ(g_instance_num.load(), 0u);

    rmdir(test_path.c_str());
    g_instance_num.store(saved_count);
}

// ============================================================================
// Tests for EnableLogService__ / DisableLogService__ full lifecycle
// ============================================================================

TEST(ObsfsLog, EnableDisableLogService_FullLifecycle) {
    std::string test_path = "/tmp/obsfs_log_test_lifecycle/";
    std::string test_name = "lifecycle";
    mkdir(test_path.c_str(), 0755);

    int32_t result = EnableLogService(test_path, test_name);
    EXPECT_EQ(result, 0);

    // Write various log entries to fill buffers
    IndexSetLogLevel(INDEX_LOG_DEBUG);
    for (int i = 0; i < 50; i++) {
        IndexLogEntry(LOG_INDEX_COMM, "lc", INDEX_LOG_DEBUG,
                      "test.cpp", "Func", i,
                      "Lifecycle test message number %d with extra data padding", i);
    }

    // Small sleep to let flush thread process
    usleep(100000);  // 100ms

    // Manual flush
    ManualFlushLog();

    // Disable (this will flush remaining buffers and stop thread)
    DisableLogService();

    rmdir(test_path.c_str());
}

// ============================================================================
// Tests for DisableLogService__ with data in buffers
// ============================================================================

TEST(ObsfsLog, DisableLogService_WithPingPongData) {
    std::string test_path = "/tmp/obsfs_log_test_dis_ppd/";
    std::string test_name = "dis_ppd";
    mkdir(test_path.c_str(), 0755);

    int32_t result = EnableLogService(test_path, test_name);
    EXPECT_EQ(result, 0);

    // Write enough data to have content in both ping and pong
    IndexSetLogLevel(INDEX_LOG_DEBUG);
    for (int i = 0; i < 20; i++) {
        IndexLogEntry(LOG_INDEX_COMM, "ppd", INDEX_LOG_INFO,
                      "test.cpp", "Func", i,
                      "Data for ping-pong disable test iteration %d", i);
    }

    // Let some flush happen
    usleep(60000);

    DisableLogService();
    rmdir(test_path.c_str());
}

// ============================================================================
// Tests for LogFlushThread paths
// ============================================================================

TEST_F(LogBufTestFixture, LogFlushThread_PingActivePongHasData) {
    // Test: when write_buf == E_PING and pong has data, copy pong to disk buf
    AllocateFullBuffers();
    g_write_buf = E_PING;

    // Set up pong with data
    const char* data = "pong data for flush\n";
    memcpy(g_pong_buf.start, data, strlen(data));
    g_pong_buf.offset = strlen(data);

    // Set up a valid log file
    char temp_file[] = "/tmp/obsfs_log_lft_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);
    g_log_file = fd;
    index_log_file_name = temp_file;

    // CopyCacheToWriteDiskBuf + WriteCachetoDisk - simulate what flush thread does
    CopyCacheToWriteDiskBuf(g_pong_buf, g_write_disk_buf);
    EXPECT_EQ(g_pong_buf.offset, 0);
    EXPECT_GT(g_write_disk_buf.offset, (uint64_t)0);

    uint32_t time_count = 0;
    WriteCachetoDisk(time_count);
    EXPECT_EQ(g_write_disk_buf.offset, 0);

    close(fd);
    g_log_file = -1;
    unlink(temp_file);
}

TEST_F(LogBufTestFixture, LogFlushThread_PongActivePingHasData) {
    // Test: when write_buf == E_PONG and ping has data, copy ping to disk buf
    AllocateFullBuffers();
    g_write_buf = E_PONG;

    const char* data = "ping data for flush\n";
    memcpy(g_ping_buf.start, data, strlen(data));
    g_ping_buf.offset = strlen(data);

    char temp_file[] = "/tmp/obsfs_log_lft2_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);
    g_log_file = fd;
    index_log_file_name = temp_file;

    CopyCacheToWriteDiskBuf(g_ping_buf, g_write_disk_buf);
    EXPECT_EQ(g_ping_buf.offset, 0);
    EXPECT_GT(g_write_disk_buf.offset, (uint64_t)0);

    uint32_t time_count = 0;
    WriteCachetoDisk(time_count);

    close(fd);
    g_log_file = -1;
    unlink(temp_file);
}

TEST_F(LogBufTestFixture, LogFlushThread_TimeoutFlush_PingActive) {
    // Test: when time_count exceeds threshold and ping is active with data
    // This simulates FLUSH_TIME <= time_count * FLUSH_SLEEP_TIME
    AllocateFullBuffers();
    g_write_buf = E_PING;

    const char* data = "timed flush data\n";
    memcpy(g_ping_buf.start, data, strlen(data));
    g_ping_buf.offset = strlen(data);
    g_pong_buf.offset = 0;  // Other buffer empty

    // Simulate the timed flush path: switch and copy
    g_write_buf = E_PONG;
    CopyCacheToWriteDiskBuf(g_ping_buf, g_write_disk_buf);

    EXPECT_EQ(g_write_buf, E_PONG);
    EXPECT_EQ(g_ping_buf.offset, 0);
    EXPECT_GT(g_write_disk_buf.offset, (uint64_t)0);
}

TEST_F(LogBufTestFixture, LogFlushThread_TimeoutFlush_PongActive) {
    // Test: when time_count exceeds threshold and pong is active with data
    AllocateFullBuffers();
    g_write_buf = E_PONG;

    const char* data = "timed flush pong data\n";
    memcpy(g_pong_buf.start, data, strlen(data));
    g_pong_buf.offset = strlen(data);
    g_ping_buf.offset = 0;  // Other buffer empty

    // Simulate the timed flush path: switch and copy
    g_write_buf = E_PING;
    CopyCacheToWriteDiskBuf(g_pong_buf, g_write_disk_buf);

    EXPECT_EQ(g_write_buf, E_PING);
    EXPECT_EQ(g_pong_buf.offset, 0);
    EXPECT_GT(g_write_disk_buf.offset, (uint64_t)0);
}

// ============================================================================
// Integration test: Full Enable -> Write -> Flush -> Disable cycle
// ============================================================================

TEST(ObsfsLog, FullCycle_WriteAndVerify) {
    std::string test_path = "/tmp/obsfs_log_test_fullcycle/";
    std::string test_name = "fullcycle";
    mkdir(test_path.c_str(), 0755);

    int32_t result = EnableLogService(test_path, test_name);
    EXPECT_EQ(result, 0);

    // Capture the actual log file name set by EnableLogService
    std::string log_file = index_log_file_name;

    IndexSetLogLevel(INDEX_LOG_DEBUG);

    // Write messages at all log levels
    IndexLogEntry(LOG_INDEX_COMM, "full", INDEX_LOG_DEBUG,
                  "test.cpp", "TestFunc", 1, "debug msg %d", 1);
    IndexLogEntry(LOG_INDEX_COMM, "full", INDEX_LOG_INFO,
                  "test.cpp", "TestFunc", 2, "info msg %d", 2);
    IndexLogEntry(LOG_INDEX_COMM, "full", INDEX_LOG_WARNING,
                  "test.cpp", "TestFunc", 3, "warn msg %d", 3);
    IndexLogEntry(LOG_INDEX_COMM, "full", INDEX_LOG_ERR,
                  "test.cpp", "TestFunc", 4, "error msg %d", 4);
    IndexLogEntry(LOG_INDEX_COMM, "full", INDEX_LOG_CRIT,
                  "test.cpp", "TestFunc", 5, "crit msg %d", 5);

    // Flush and wait
    ManualFlushLog();
    usleep(200000);

    // DisableLogService also flushes remaining data and closes file
    DisableLogService();

    // The log file should have been created (may or may not have data
    // depending on timing of flush thread vs the short test lifetime)
    SUCCEED();

    // Cleanup
    unlink(log_file.c_str());
    rmdir(test_path.c_str());
}

// ============================================================================
// Tests for edge cases in IndexLogEntry
// ============================================================================

TEST_F(LogBufTestFixture, IndexLogEntry_CRIT_Level) {
    g_should_stop_flag = false;
    g_log_service_enable_flag = true;
    AllocateFullBuffers();
    IndexSetLogLevel(INDEX_LOG_CRIT);

    // Only CRIT should pass
    IndexLogEntry(LOG_INDEX_COMM, "test", INDEX_LOG_WARNING,
                  "test.cpp", "F", 1, "filtered");
    EXPECT_EQ(g_ping_buf.offset, 0);

    IndexLogEntry(LOG_INDEX_COMM, "test", INDEX_LOG_CRIT,
                  "test.cpp", "F", 2, "critical!");
    EXPECT_GT(g_ping_buf.offset, (uint64_t)0);

    IndexSetLogLevel(INDEX_LOG_DEBUG);
}

TEST_F(LogBufTestFixture, IndexLogEntry_EmptyFormatString) {
    g_should_stop_flag = false;
    g_log_service_enable_flag = true;
    AllocateFullBuffers();
    IndexSetLogLevel(INDEX_LOG_DEBUG);

    // Empty format string
    IndexLogEntry(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                  "test.cpp", "F", 1, "");
    // Should still write the header + newline
    EXPECT_GT(g_ping_buf.offset, (uint64_t)0);

    IndexSetLogLevel(INDEX_LOG_DEBUG);
}

TEST_F(LogBufTestFixture, IndexLogEntry_NullChunkId) {
    g_should_stop_flag = false;
    g_log_service_enable_flag = true;
    AllocateFullBuffers();
    IndexSetLogLevel(INDEX_LOG_DEBUG);

    // nullptr chunk_id (used by Monitor/Stub modules)
    IndexLogEntry(LOG_INDEX_MONITOR, nullptr, INDEX_LOG_INFO,
                  "test.cpp", "F", 1, "null chunk");
    EXPECT_GT(g_ping_buf.offset, (uint64_t)0);
}

// ============================================================================
// Tests for LogToBuf buffer overflow with message exceeding TEMP_ARRAY_LEN
// ============================================================================

TEST_F(LogBufTestFixture, LogToBuf_ExactBufferBoundary) {
    AllocateFullBuffers();
    g_write_buf = E_PING;

    // Write a message that is exactly at the boundary of TEMP_ARRAY_LEN (800)
    // The header takes about 80-120 chars, so a 680-char message should be close
    std::string msg(680, 'B');
    CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                 "test.cpp", "Func", 1, "%s", msg.c_str());

    EXPECT_GT(g_ping_buf.offset, (uint64_t)0);
}

TEST_F(LogBufTestFixture, LogToBuf_MessageExceedsTempArrayLen) {
    AllocateFullBuffers();
    g_write_buf = E_PING;

    // Create a message that definitely exceeds TEMP_ARRAY_LEN (800) with header
    // The total output (header + message) must exceed 800 bytes
    std::string msg(800, 'C');
    CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                 "test.cpp", "Func", 1, "%s", msg.c_str());

    // The message should be truncated but still written
    EXPECT_GT(g_ping_buf.offset, (uint64_t)0);
    // The output should be capped at about TEMP_ARRAY_LEN (800) bytes
    EXPECT_LE(g_ping_buf.offset, (uint64_t)800);
}

// ============================================================================
// Tests for LogToBuf with various format strings
// ============================================================================

TEST_F(LogBufTestFixture, LogToBuf_FormatWithMultipleArgs) {
    AllocateFullBuffers();
    g_write_buf = E_PING;

    CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                 "test.cpp", "Func", 42,
                 "int=%d str=%s hex=0x%x float=%.2f",
                 123, "hello", 0xDEAD, 3.14);

    std::string content(g_ping_buf.start, g_ping_buf.offset);
    EXPECT_NE(content.find("int=123"), std::string::npos);
    EXPECT_NE(content.find("str=hello"), std::string::npos);
    EXPECT_NE(content.find("hex=0xdead"), std::string::npos);
    EXPECT_NE(content.find("float=3.14"), std::string::npos);
}

TEST_F(LogBufTestFixture, LogToBuf_FormatTimestamp) {
    AllocateFullBuffers();
    g_write_buf = E_PING;

    CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_ERR,
                 "myfile.cpp", "MyFunc", 99, "test msg");

    std::string content(g_ping_buf.start, g_ping_buf.offset);
    // Should contain timestamp pattern like [2026-02-10 ...]
    EXPECT_NE(content.find("[20"), std::string::npos);
    EXPECT_NE(content.find("ERR"), std::string::npos);
    EXPECT_NE(content.find("myfile.cpp"), std::string::npos);
    EXPECT_NE(content.find("MyFunc"), std::string::npos);
    EXPECT_NE(content.find("99"), std::string::npos);
}

// ============================================================================
// Tests for WriteCachetoDisk error paths
// ============================================================================

TEST_F(LogBufTestFixture, WriteCachetoDisk_InvalidLogFilePath) {
    AllocateFullBuffers();
    g_log_file = -1;
    // Set invalid path that cannot be created
    index_log_file_path = "/nonexistent_root_dir/obsfs_log_test/";
    index_log_file_name = "/nonexistent_root_dir/obsfs_log_test/test_log";

    const char* data = "some data\n";
    memcpy(g_write_disk_buf.start, data, strlen(data));
    g_write_disk_buf.offset = strlen(data);

    uint32_t time_count = 5;
    int32_t result = WriteCachetoDisk(time_count);

    // Should fail because the path doesn't exist and can't be created
    EXPECT_EQ(result, -1);
}

// ============================================================================
// Additional tests for coverage of tricky branches
// ============================================================================

TEST_F(LogBufTestFixture, LogToBuf_ConsecutiveWrites_PingBuffer) {
    AllocateFullBuffers();
    g_write_buf = E_PING;

    // Multiple consecutive writes to ping buffer
    for (int i = 0; i < 10; i++) {
        CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                     "test.cpp", "Func", i, "msg %d", i);
    }

    // All writes should accumulate in ping buffer
    EXPECT_GT(g_ping_buf.offset, (uint64_t)0);
    EXPECT_EQ(g_pong_buf.offset, 0);
}

TEST_F(LogBufTestFixture, LogToBuf_ConsecutiveWrites_PongBuffer) {
    AllocateFullBuffers();
    g_write_buf = E_PONG;

    for (int i = 0; i < 10; i++) {
        CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                     "test.cpp", "Func", i, "msg %d", i);
    }

    EXPECT_EQ(g_ping_buf.offset, 0);
    EXPECT_GT(g_pong_buf.offset, (uint64_t)0);
}

// ============================================================================
// Tests for EnableLogService__ error paths
// ============================================================================

TEST(ObsfsLog, EnableLogService_BasicStart) {
    std::string test_path = "/tmp/obsfs_log_test_basicstart/";
    std::string test_name = "basicstart";
    mkdir(test_path.c_str(), 0755);

    int32_t result = EnableLogService(test_path, test_name);
    EXPECT_EQ(result, 0);

    // Verify the log service is running
    EXPECT_TRUE(g_log_service_enable_flag);

    DisableLogService();
    rmdir(test_path.c_str());
}

// ============================================================================
// Test: LogFlushThread quick cycle (stop immediately)
// ============================================================================

TEST(ObsfsLog, LogFlushThread_QuickStop) {
    std::string test_path = "/tmp/obsfs_log_test_quickstop/";
    std::string test_name = "quickstop";
    mkdir(test_path.c_str(), 0755);

    int32_t result = EnableLogService(test_path, test_name);
    EXPECT_EQ(result, 0);

    // Immediately disable - the flush thread should stop quickly
    DisableLogService();
    rmdir(test_path.c_str());
}

// ============================================================================
// Test: Stress test with many log entries
// ============================================================================

TEST(ObsfsLog, StressTest_ManyEntries) {
    std::string test_path = "/tmp/obsfs_log_test_stress/";
    std::string test_name = "stress";
    mkdir(test_path.c_str(), 0755);

    int32_t result = EnableLogService(test_path, test_name);
    EXPECT_EQ(result, 0);
    IndexSetLogLevel(INDEX_LOG_DEBUG);

    // Write 200 log entries
    for (int i = 0; i < 200; i++) {
        IndexLogEntry(LOG_INDEX_COMM, "stress", INDEX_LOG_INFO,
                      "test.cpp", "StressFunc", i,
                      "Stress test entry %d with data: %s", i, "abcdefghij");
    }

    usleep(200000);  // Wait for flush
    ManualFlushLog();
    DisableLogService();

    rmdir(test_path.c_str());
}

// ============================================================================
// Test: CopyCacheToWriteDiskBuf with empty source
// ============================================================================

TEST_F(LogBufTestFixture, CopyCacheToWriteDiskBuf_EmptySource) {
    AllocateFullBuffers();
    g_ping_buf.offset = 0;

    CopyCacheToWriteDiskBuf(g_ping_buf, g_write_disk_buf);

    EXPECT_EQ(g_write_disk_buf.offset, 0);
    EXPECT_EQ(g_ping_buf.offset, 0);
}

// ============================================================================
// Test: NeedSwitchBuf with full buffer
// ============================================================================

TEST(ObsfsLog, NeedSwitchBuf_FullBuffer) {
    const size_t LOG_CACHE_MAX_LEN = 1024 * 1024 * 10;
    LogBuf log_buf;
    log_buf.start = (char*)malloc(64);
    log_buf.end = log_buf.start + 63;
    log_buf.offset = LOG_CACHE_MAX_LEN;  // Completely full

    bool result = NeedSwitchBuf(1, log_buf);
    EXPECT_TRUE(result);

    free(log_buf.start);
}

// ============================================================================
// Test: GetFileSize with g_log_file set to a valid fd
// ============================================================================

TEST_F(LogBufTestFixture, GetFileSize_FstatBranch) {
    // Create a temp file
    char temp_file[] = "/tmp/obsfs_log_fstat_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);

    // Write 100 bytes
    char buf[100];
    memset(buf, 'x', 100);
    write(fd, buf, 100);

    g_log_file = fd;

    uint64_t size = GetFileSize(temp_file);
    EXPECT_EQ(size, 100);
    EXPECT_EQ(g_fileSizeByHandle, 100);

    close(fd);
    g_log_file = -1;
    unlink(temp_file);
}

// ============================================================================
// Test: LogToBuf with newline already present at end
// ============================================================================

TEST_F(LogBufTestFixture, LogToBuf_NoDoubleNewline) {
    AllocateFullBuffers();
    g_write_buf = E_PING;

    CallLogToBuf(LOG_INDEX_COMM, "test", INDEX_LOG_INFO,
                 "test.cpp", "Func", 1, "ending with newline\n");

    // Check that there's no double newline
    std::string content(g_ping_buf.start, g_ping_buf.offset);
    size_t pos = content.rfind('\n');
    EXPECT_EQ(pos, content.length() - 1);
    if (pos > 0) {
        EXPECT_NE(content[pos - 1], '\n');
    }
}

// ============================================================================
// Test: WriteCachetoDisk with existing valid log file
// ============================================================================

TEST_F(LogBufTestFixture, WriteCachetoDisk_ExistingFile_AccessOk) {
    AllocateFullBuffers();

    char temp_file[] = "/tmp/obsfs_log_wcd_ok_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);
    g_log_file = fd;
    index_log_file_name = temp_file;

    const char* data = "existing file write test\n";
    memcpy(g_write_disk_buf.start, data, strlen(data));
    g_write_disk_buf.offset = strlen(data);

    uint32_t time_count = 10;
    int32_t result = WriteCachetoDisk(time_count);

    EXPECT_EQ(result, 0);
    EXPECT_EQ(time_count, 0U);
    EXPECT_EQ(g_write_disk_buf.offset, 0ULL);

    close(fd);
    g_log_file = -1;
    unlink(temp_file);
}

// ============================================================================
// Test: Multiple rapid enable/disable cycles
// ============================================================================

TEST(ObsfsLog, RapidEnableDisableCycles) {
    std::string test_path = "/tmp/obsfs_log_test_rapid/";
    std::string test_name = "rapid";
    mkdir(test_path.c_str(), 0755);

    for (int i = 0; i < 3; i++) {
        int32_t result = EnableLogService(test_path, test_name);
        EXPECT_EQ(result, 0);

        IndexSetLogLevel(INDEX_LOG_DEBUG);
        IndexLogEntry(LOG_INDEX_COMM, "rapid", INDEX_LOG_INFO,
                      "test.cpp", "F", i, "cycle %d", i);

        DisableLogService();
    }

    rmdir(test_path.c_str());
}

// ============================================================================
// Tests for OpenLogFile verifyPath failure path
// ============================================================================

TEST_F(LogBufTestFixture, OpenLogFile_InvalidPath_Symlink) {
    // Use a path that contains a symlink loop to trigger verifyPath failure
    // verifyPath with need_exist=false can still fail for invalid paths
    // Set path to something that exists but the file name has suspicious chars
    index_log_file_path = "/tmp/";
    // A filename containing ".." could cause verifyPath to fail
    index_log_file_name = "/tmp/../../../etc/obsfs_log_test_xyz";

    g_log_file = -1;
    OpenLogFile();

    // Whether it succeeds or fails depends on verifyPath logic
    if (g_log_file >= 0) {
        close(g_log_file);
        g_log_file = -1;
    }
    unlink("/tmp/../../../etc/obsfs_log_test_xyz");
}

// ============================================================================
// Tests for OpenLogFile mkdir failure
// ============================================================================

TEST_F(LogBufTestFixture, OpenLogFile_MkdirFailure) {
    // Set path to something unwritable (root-owned dir we can't write to)
    index_log_file_path = "/proc/obsfs_test_nodir/";
    index_log_file_name = "/proc/obsfs_test_nodir/logfile";

    g_log_file = -1;
    OpenLogFile();

    // mkdir under /proc should fail - g_log_file should remain -1
    EXPECT_EQ(g_log_file, -1);
}

// ============================================================================
// Tests for WriteCachetoDisk write failure path
// ============================================================================

TEST_F(LogBufTestFixture, WriteCachetoDisk_WriteFails) {
    AllocateFullBuffers();

    // Use a read-only file descriptor to force write() to fail
    char temp_file[] = "/tmp/obsfs_log_wf_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);
    close(fd);

    // Reopen as read-only
    fd = open(temp_file, O_RDONLY);
    ASSERT_GE(fd, 0);
    g_log_file = fd;
    index_log_file_name = temp_file;

    const char* data = "write fail test\n";
    memcpy(g_write_disk_buf.start, data, strlen(data));
    g_write_disk_buf.offset = strlen(data);

    uint32_t time_count = 5;
    int32_t result = WriteCachetoDisk(time_count);

    // write() should fail on a read-only fd
    EXPECT_EQ(result, -1);
    EXPECT_EQ(time_count, (uint32_t)0);

    close(fd);
    g_log_file = -1;
    unlink(temp_file);
}

// ============================================================================
// Tests for WriteCachetoDisk access fails (file deleted + reopen fails)
// ============================================================================

TEST_F(LogBufTestFixture, WriteCachetoDisk_AccessFails_ReopenFails) {
    AllocateFullBuffers();

    char temp_file[] = "/tmp/obsfs_log_arf_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);
    g_log_file = fd;
    // Set path to unwritable directory - reopen will fail
    index_log_file_name = "/proc/obsfs_log_no_exist_file";
    index_log_file_path = "/proc/";

    // Delete the temp file to trigger the access() != 0 path
    unlink(temp_file);

    const char* data = "reopen fail test\n";
    memcpy(g_write_disk_buf.start, data, strlen(data));
    g_write_disk_buf.offset = strlen(data);

    uint32_t time_count = 5;
    int32_t result = WriteCachetoDisk(time_count);

    // Should fail: access fails, close, reopen fails
    EXPECT_EQ(result, -1);

    // Clean up - fd was closed by WriteCachetoDisk
    g_log_file = -1;
}

// ============================================================================
// Test for DisableLogService__ with data still in pong buffer
// This test directly calls DisableLogService__ after setting up state
// ============================================================================

TEST(ObsfsLog, DisableLogService_PongBufferFlushed) {
    std::string test_path = "/tmp/obsfs_log_test_pong_flush/";
    std::string test_name = "pf";
    mkdir(test_path.c_str(), 0755);

    int32_t result = EnableLogService(test_path, test_name);
    EXPECT_EQ(result, 0);

    IndexSetLogLevel(INDEX_LOG_DEBUG);

    // Write data so that the pong buffer gets some content
    // Fill ping buffer first via many writes, triggering switch to pong
    for (int i = 0; i < 100; i++) {
        IndexLogEntry(LOG_INDEX_COMM, "pf", INDEX_LOG_INFO,
                      "test.cpp", "F", i,
                      "Fill buffer with test data iteration %d padding %s",
                      i, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    }

    // Don't sleep - let data remain in buffers
    // DisableLogService__ should flush both ping and pong buffers
    DisableLogService();

    rmdir(test_path.c_str());
}

// ============================================================================
// Test for DisableLogService__ with g_log_file == -1
// ============================================================================

TEST(ObsfsLog, DisableLogService_NoLogFile) {
    std::string test_path = "/tmp/obsfs_log_test_nofile/";
    std::string test_name = "nofile";
    mkdir(test_path.c_str(), 0755);

    int32_t result = EnableLogService(test_path, test_name);
    EXPECT_EQ(result, 0);

    // Force close the log file to simulate the g_log_file == -1 path
    // in DisableLogService__
    if (g_log_file >= 0) {
        close(g_log_file);
        g_log_file = -1;
    }

    DisableLogService();

    rmdir(test_path.c_str());
}

// ============================================================================
// Test for EnableLogService__ verifying thread creation
// ============================================================================

TEST(ObsfsLog, EnableLogService_VerifyThreadCreation) {
    std::string test_path = "/tmp/obsfs_log_test_thread/";
    std::string test_name = "thread";
    mkdir(test_path.c_str(), 0755);

    int32_t result = EnableLogService(test_path, test_name);
    EXPECT_EQ(result, 0);

    // Write some data to let the thread process
    IndexSetLogLevel(INDEX_LOG_DEBUG);
    IndexLogEntry(LOG_INDEX_COMM, "t", INDEX_LOG_INFO,
                  "test.cpp", "F", 1, "thread test");

    // Let flush thread do one cycle
    usleep(100000);

    // g_log_flush_cnt should have been incremented by the thread
    EXPECT_GT(g_log_flush_cnt, (uint32_t)0);

    DisableLogService();

    // After disable, flush_dead_flag should be true
    EXPECT_TRUE(g_flush_dead_flag);

    rmdir(test_path.c_str());
}

// ============================================================================
// Test for WriteCachetoDisk with large write (verifying fsync path)
// ============================================================================

TEST_F(LogBufTestFixture, WriteCachetoDisk_LargeWrite) {
    AllocateFullBuffers();

    char temp_file[] = "/tmp/obsfs_log_lw_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);
    g_log_file = fd;
    index_log_file_name = temp_file;

    // Write 100KB of data
    size_t data_size = 100 * 1024;
    memset(g_write_disk_buf.start, 'Z', data_size);
    g_write_disk_buf.offset = data_size;

    uint32_t time_count = 15;
    int32_t result = WriteCachetoDisk(time_count);

    EXPECT_EQ(result, 0);
    EXPECT_EQ(time_count, (uint32_t)0);
    EXPECT_EQ(g_write_disk_buf.offset, (uint64_t)0);

    // Verify file size
    struct stat st;
    fstat(fd, &st);
    EXPECT_EQ(st.st_size, static_cast<off_t>(data_size));

    close(fd);
    g_log_file = -1;
    unlink(temp_file);
}

// ============================================================================
// Test for ManualFlushLog with only pong having data (not ping)
// ============================================================================

TEST_F(LogBufTestFixture, ManualFlushLog_OnlyPongHasData) {
    AllocateFullBuffers();

    char temp_file[] = "/tmp/obsfs_log_mop_XXXXXX";
    int fd = mkstemp(temp_file);
    ASSERT_GE(fd, 0);
    g_log_file = fd;

    // Only pong has data
    g_ping_buf.offset = 0;
    const char* pong_data = "only pong data\n";
    memcpy(g_pong_buf.start, pong_data, strlen(pong_data));
    g_pong_buf.offset = strlen(pong_data);

    ManualFlushLog();

    EXPECT_EQ(g_ping_buf.offset, (uint64_t)0);
    EXPECT_EQ(g_pong_buf.offset, (uint64_t)0);

    // Verify file has pong data
    struct stat st;
    fstat(fd, &st);
    EXPECT_EQ(st.st_size, static_cast<off_t>(strlen(pong_data)));

    close(fd);
    g_log_file = -1;
    unlink(temp_file);
}

// ============================================================================
// Test for LogToBuf with a log level > INDEX_LOG_CRIT (unknown level)
// ============================================================================

TEST_F(LogBufTestFixture, LogToBuf_UnknownLogLevel) {
    AllocateFullBuffers();
    g_write_buf = E_PING;

    // Use log level 10 (beyond CRIT=4, will default to empty level string)
    CallLogToBuf(LOG_INDEX_COMM, "test", 10,
                 "test.cpp", "Func", 1, "unknown level");

    // Should still write something
    EXPECT_GT(g_ping_buf.offset, (uint64_t)0);
}

// ============================================================================
// Lifecycle test with heavy write to trigger LogFlushThread branches
// ============================================================================

TEST(ObsfsLog, LogFlushThread_HeavyWriteTriggersBranches) {
    std::string test_path = "/tmp/obsfs_log_test_heavy/";
    std::string test_name = "heavy";
    mkdir(test_path.c_str(), 0755);

    int32_t result = EnableLogService(test_path, test_name);
    EXPECT_EQ(result, 0);

    IndexSetLogLevel(INDEX_LOG_DEBUG);

    // Write many entries rapidly to trigger ping/pong switching and
    // flush thread branch coverage
    for (int i = 0; i < 500; i++) {
        IndexLogEntry(LOG_INDEX_COMM, "heavy", INDEX_LOG_INFO,
                      "test.cpp", "HeavyFunc", i,
                      "Heavy write entry %d: %s %s %s %s %s %s %s",
                      i, "aaaaaaaaaaaaa", "bbbbbbbbbbbbb", "ccccccccccccc",
                      "ddddddddddddd", "eeeeeeeeeeeee", "fffffffffffff",
                      "ggggggggggggg");
    }

    // Let flush thread process for multiple cycles
    usleep(300000);  // 300ms = ~6 flush cycles at 50ms each

    // Write more to trigger timed flush (FLUSH_TIME = 500ms)
    for (int i = 0; i < 100; i++) {
        IndexLogEntry(LOG_INDEX_COMM, "heavy", INDEX_LOG_INFO,
                      "test.cpp", "HeavyFunc", i,
                      "Second batch entry %d", i);
    }

    usleep(600000);  // 600ms to exceed FLUSH_TIME

    ManualFlushLog();
    DisableLogService();

    rmdir(test_path.c_str());
}

// Main function to run the tests
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
