#ifndef __BASE_LOG_H__
#define __BASE_LOG_H__

#include <string>
#include <string.h>
#include <stdio.h>
#include <stdint.h>


#ifdef __cplusplus
#if __cplusplus
extern "C"{
#endif
#endif /* __cplusplus */

struct  LogBuf
{
    uint64_t    offset;
    char*       start;
    char*       end;
};

enum PingPong
{
    E_PING  = 0x1,
    E_PONG  = 0x2
};

#define INDEX_LOG_DEBUG   0
#define INDEX_LOG_INFO    1
#define INDEX_LOG_WARNING 2
#define INDEX_LOG_ERR     3
#define INDEX_LOG_CRIT    4

#define __FILENAME__ (strrchr(__FILE__, '/') ? (strrchr(__FILE__, '/') + 1):__FILE__)
#define IndexLog(module, chunk_id, level, pszFormat...)  \
    IndexLogEntry((module), (chunk_id), (level), (char*)__FILENAME__, (char*)__FUNCTION__, __LINE__, ##pszFormat)

void  IndexLogEntry(
         const uint64_t     module,
         const char*        chunk_id,
         const uint32_t     log_level,
         const char*        log_file_name,
         const char*        log_func_name,
         const uint32_t     log_line,
         const char*        pszFormat, ...)
         __attribute__ ((format (printf, 7, 8)));

void IndexSetLogLevel(uint32_t log_level);
void IndexSetLogFileName(const std::string& log_file_path, const std::string& log_file_name);
int32_t EnableLogService(const std::string& log_file_path, const std::string& log_file_name);
int32_t EnableLogService__(const std::string& log_file_path, const std::string& log_file_name);
void DisableLogService(void);
void DisableLogService__(void);
void ManualFlushLog(void);


#define LOG_INDEX_COMM      1   // common Sub-module
#define LOG_INDEX_CLIENT    2   // inedex client Sub-module
#define LOG_INDEX_MNGT      3   // inedex manager Sub-module
#define LOG_INDEX_SERVER    4   // inedex server Sub-module
#define LOG_INDEX_KVDB      5   // inedex KVDB Sub-module
#define LOG_INDEX_PLOGMOCK  6   // inedex PLOG mock-module
#define LOG_INDEX_TOOL      7   // inedex tool
#define LOG_INDEX_MONITOR   8   // inedex component monitor

//PlogEnv support multiple instance
#define IndexCommLog(chunk_id, level, fmt...)      IndexLog( LOG_INDEX_COMM, (chunk_id), level,             ##fmt)
#define IndexCommLogDbg(chunk_id, fmt...)          IndexLog( LOG_INDEX_COMM, (chunk_id), INDEX_LOG_DEBUG,   ##fmt)
#define IndexCommLogInfo(chunk_id, fmt...)         IndexLog( LOG_INDEX_COMM, (chunk_id), INDEX_LOG_INFO,    ##fmt)
#define IndexCommLogWarn(chunk_id, fmt...)         IndexLog( LOG_INDEX_COMM, (chunk_id), INDEX_LOG_WARNING, ##fmt)
#define IndexCommLogErr(chunk_id, fmt...)          IndexLog( LOG_INDEX_COMM, (chunk_id), INDEX_LOG_ERR,     ##fmt)
#define IndexCommLogCrit(chunk_id, fmt...)         IndexLog( LOG_INDEX_COMM, (chunk_id), INDEX_LOG_CRIT,    ##fmt)


// 下列接口需要在类 的成员函数中使用
#define IndexKvdbLog(level, fmt...)      IndexLog( LOG_INDEX_KVDB, chunk_id_.c_str(), level,             ##fmt)
#define IndexKvdbLogDbg(fmt...)          IndexLog( LOG_INDEX_KVDB, chunk_id_.c_str(), INDEX_LOG_DEBUG,   ##fmt)
#define IndexKvdbLogInfo(fmt...)         IndexLog( LOG_INDEX_KVDB, chunk_id_.c_str(), INDEX_LOG_INFO,    ##fmt)
#define IndexKvdbLogWarn(fmt...)         IndexLog( LOG_INDEX_KVDB, chunk_id_.c_str(), INDEX_LOG_WARNING, ##fmt)
#define IndexKvdbLogErr(fmt...)          IndexLog( LOG_INDEX_KVDB, chunk_id_.c_str(), INDEX_LOG_ERR,     ##fmt)
#define IndexKvdbLogCrit(fmt...)         IndexLog( LOG_INDEX_KVDB, chunk_id_.c_str(), INDEX_LOG_CRIT,    ##fmt)


#define IndexLogErrIf(condi, fmt...) do{ IndexLog( LOG_INDEX_KVDB, chunk_id_.c_str(), ((TRUE==(condi)) ? INDEX_LOG_INFO : INDEX_LOG_ERR),  ##fmt);}while(0)

#define IndexLogErrDbgIf(condi, fmt...) do{ IndexLog( LOG_INDEX_KVDB, chunk_id_.c_str(), ((TRUE==(condi)) ? INDEX_LOG_DEBUG : INDEX_LOG_ERR),  ##fmt);}while(0)

#define IndexStubLogDbg(fmt...)          IndexLog(LOG_INDEX_PLOGMOCK, nullptr, INDEX_LOG_DEBUG,   ##fmt)
#define IndexStubLogDbgIf(is_err,fmt...) IndexLog(LOG_INDEX_PLOGMOCK, nullptr, ((is_err) ? INDEX_LOG_ERR : INDEX_LOG_DEBUG),     ##fmt)
#define IndexStubLogInfo(fmt...)         IndexLog(LOG_INDEX_PLOGMOCK, nullptr, INDEX_LOG_INFO,    ##fmt)
#define IndexStubLogWarn(fmt...)         IndexLog(LOG_INDEX_PLOGMOCK, nullptr, INDEX_LOG_WARNING, ##fmt)
#define IndexStubLogErr(fmt...)          IndexLog(LOG_INDEX_PLOGMOCK, nullptr, INDEX_LOG_ERR,     ##fmt)
#define IndexStubLogErrIf(is_err,fmt...) IndexLog(LOG_INDEX_PLOGMOCK, nullptr, ((is_err) ? INDEX_LOG_ERR : INDEX_LOG_DEBUG),     ##fmt)
#define IndexStubLogCrit(fmt...)         IndexLog(LOG_INDEX_PLOGMOCK, nullptr, INDEX_LOG_CRIT,    ##fmt)

#define IndexMonitorLogDbg(fmt...)          IndexLog(LOG_INDEX_MONITOR, nullptr, INDEX_LOG_DEBUG,   ##fmt)
#define IndexMonitorLogDbgIf(is_err,fmt...) IndexLog(LOG_INDEX_MONITOR, nullptr, ((is_err) ? INDEX_LOG_ERR : INDEX_LOG_DEBUG),     ##fmt)
#define IndexMonitorLogInfo(fmt...)         IndexLog(LOG_INDEX_MONITOR, nullptr, INDEX_LOG_INFO,    ##fmt)
#define IndexMonitorLogWarn(fmt...)         IndexLog(LOG_INDEX_MONITOR, nullptr, INDEX_LOG_WARNING, ##fmt)
#define IndexMonitorLogErr(fmt...)          IndexLog(LOG_INDEX_MONITOR, nullptr, INDEX_LOG_ERR,     ##fmt)
#define IndexMonitorLogErrIf(is_err,fmt...) IndexLog(LOG_INDEX_MONITOR, nullptr, ((is_err) ? INDEX_LOG_ERR : INDEX_LOG_DEBUG),     ##fmt)
#define IndexMonitorLogCrit(fmt...)         IndexLog(LOG_INDEX_MONITOR, nullptr, INDEX_LOG_CRIT,    ##fmt)

extern void __IndexCrashIf(
    bool condition,
    const uint64_t     module,
    const char*        chunk_id,
    const char*        log_file_name,
    const char*        log_func_name,
    const uint32_t     log_line,
    const char*        pszFormat, ...);

extern std::string IndexData2HexString(const char *pdata, int size);
extern void IndexDataHexDump(const char *buf, int len);
extern uint32_t g_index_log_level; //to avoid naming confict with third party


#ifdef __cplusplus
#if __cplusplus
}
#endif
#endif /* __cplusplus */

#endif
