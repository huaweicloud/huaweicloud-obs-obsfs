#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <signal.h>
#include <time.h>
#include <mutex>
#include <thread>
#include <atomic>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include "obsfs_log.h"


#ifdef __cplusplus
#if __cplusplus
extern "C"{
#endif
#endif

#define INDEX_CONDITION_TRUE(condition)           (condition)
#define IndexMemFree(ptr)                      do{if(nullptr==(ptr)) {break;} free(ptr); (ptr) = nullptr;}while(0)
#define IndexMemCopy(p_dst, dst_max, p_src, size)       memcpy((p_dst), (p_src), (size))
#define IndexMemZero(p_dst, size)              memset((p_dst), 0, (size))
#define IndexMemSet(p_dst, dst_max,byte_value, size)   memset((p_dst), (byte_value), (size))
#define IndexMemCmp(ptr1, ptr2, size)          memcmp((ptr1), (ptr2), (size))
#define IndexStrCopy(p_dst, size, p_src)       strcpy((p_dst), (p_src))
#define IndexSnprintf(p_dst,dst_max, size, format...)  snprintf((p_dst), (dst_max),##format)
#define IndexVsnprintf(p_dst,dst_max, size, format, p_src)  vsnprintf((p_dst), (dst_max), (format), (p_src))
#define IndexStrnCpy(p_dst,dst_max, p_src, size)       strncpy((p_dst), (p_src), (size))
#define IndexSprintf(p_dst, dst_max, format...)   sprintf((p_dst),##format)

const uint32_t      FALSE                       = 0;
const uint32_t      TRUE                        = 1;
const int32_t       RET_OK                      = 0;
const int32_t       RET_ERROR                   = -1;
const uint32_t      TRY_MAX_TIEM                = 3;

#define gettid() syscall(__NR_gettid)

using namespace std;

string              index_log_file_name("/var/log/obsfs/obsfs_log");//lint !e102 !e2
string              index_log_file_path("/var/log/obsfs/");
mutex               g_log_mutex;
int                 g_log_file                     = -1;
LogBuf              g_ping_buf                     = {0};
LogBuf              g_pong_buf                     = {0};
LogBuf              g_write_disk_buf               = {0};
PingPong            g_write_buf                    = E_PING;
bool                g_should_stop_flag             = FALSE;
bool                g_flush_dead_flag              = FALSE;
bool                g_log_service_enable_flag      = FALSE;
uint64_t            g_throw_log_count              = 0;
uint32_t            g_log_flush_cnt                = 0;
uint64_t            g_fileSizeByHandle = 0;
uint64_t           g_process_id = 0;

// info log support multiple instances
std::atomic<uint32_t> g_instance_num{0};
#define             LOG_CACHE_MAX_LEN              (1024 * 1024 * 10)
#define             MAX_LOG_SIZE                   (1024 * 1024 * 512)
#define             LOG_NEW_NAME_LEN               256
#define             FORMAT_TIME_STR_LEN            32
#define             TEMP_ARRAY_LEN                 800
#define             FLUSH_SLEEP_TIME               50000   //50 ms
#define             FLUSH_TIME                     500000 //us

static const char* g_sLogLevel[] =
{
    "DBG", "INFO", "WARN", "ERR", "CRI"
};

uint32_t g_index_log_level = INDEX_LOG_DEBUG;

/*****************************************************************************
*   Prototype    : CreateLogBuf
*   Description  : create and init log buffer
*   Input        : LogBuf &log_buf
*   Output       : None
*   Return Value : int32
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/11
*           Author       :
*           Modification : Created function
*
*****************************************************************************/
int32_t CreateLogBuf(LogBuf &log_buf)
{//lint !e832
    log_buf.start = (char *)malloc(LOG_CACHE_MAX_LEN);
    IndexMemZero(log_buf.start, LOG_CACHE_MAX_LEN);

    if (!log_buf.start)
    {
        return RET_ERROR;
    }

    log_buf.end    = log_buf.start + LOG_CACHE_MAX_LEN - 1;
    log_buf.offset = 0;

    return RET_OK;
}

/*****************************************************************************
*   Prototype    : CleanLogBuf
*   Description  : clean log cache
*   Input        : LogBuf *log_buf
*   Output       : None
*   Return Value : void
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/11
*           Author       :
*           Modification : Created function
*
*****************************************************************************/
void CleanLogBuf(LogBuf *log_buf)
{//lint !e832
    log_buf->offset = 0;
}

/*****************************************************************************
*   Prototype    : NeedSwitchBuf
*   Description  : determine if it is needed switching ping with pong buffer
*   Input        : IN uint64 log_len
*                  IN LogBuf &log_buf
*   Output       : None
*   Return Value : bool
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/11
*           Author       :
*           Modification : Created function
*
*****************************************************************************/
bool NeedSwitchBuf(uint64_t log_len, LogBuf &log_buf)
{
    uint64_t remaining_len = LOG_CACHE_MAX_LEN - log_buf.offset;
    if (log_len > remaining_len)
    {
        return TRUE;
    }
    else
    {
        return FALSE;
    }
}

/*****************************************************************************
*   Prototype    : LogToBuf
*   Description  : write log to buffer
*   Input        : IN const uint64     module
*                  IN const uint32     log_level
*                  IN const char*      log_file_name
*                  IN const char*      log_func_name
*                  IN const uint32     log_line
*                  IN const char*      format
*                  IN va_list          ap
*   Output       : None
*   Return Value : void
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/11
*           Author       :
*           Modification : Created function
*
*****************************************************************************/
void LogToBuf(
         const uint64_t     module,
         const char*        chunk_id,
         const uint32_t     log_level,
         const char*        log_file_name,
         const char*        log_func_name,
         const uint32_t     log_line,
         const char*        format,
         va_list            ap)
{
    char*           base        = nullptr;
    uint64_t        bufsize     = 0;
    char*           p           = nullptr;
    char*           limit       = nullptr;
    uint64_t        len         = 0;
    const uint64_t  thread_id   = gettid();
    char            buffer[TEMP_ARRAY_LEN];
    char            timeZone[8];

    bufsize = sizeof(buffer);
    base = buffer;

    p = base;
    limit = base + bufsize;

    struct timeval now_tv;
    gettimeofday(&now_tv, nullptr);
    const time_t seconds = now_tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    strftime(timeZone, sizeof(timeZone), "%z", &t);
    p += snprintf(p, limit - p,
                "[%04d-%02d-%02d %02d:%02d:%02d.%06d%s][%llu][%s][%s,%s,%u] ",
                t.tm_year + 1900,
                t.tm_mon + 1,
                t.tm_mday,
                t.tm_hour,
                t.tm_min,
                t.tm_sec,
                static_cast<int>(now_tv.tv_usec),   //lint !e26
                timeZone,
                (long long unsigned int)thread_id,
                g_sLogLevel[log_level],
                log_file_name,
                log_func_name,
                log_line);

    // Print the message
    if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += IndexVsnprintf(p, limit - p, limit - p - 1, format, backup_ap);
        va_end(backup_ap);
    }

    // Truncate to available space if necessary
    if (p >= limit) {
        p = limit - 2;
    }

    // Add newline if necessary
    if (p == base || p[-1] != '\n') {
        *p++ = '\n';
    }

    if (p > limit)
    {
       return;
    }
    len = p - base;

    g_log_mutex.lock();
    if (E_PING == g_write_buf)
    {
        if (NeedSwitchBuf(len, g_ping_buf))
        {
            if (NeedSwitchBuf(len, g_pong_buf))
            {
                g_throw_log_count++;
                g_log_mutex.unlock();
                return;
            }

            IndexMemCopy(g_pong_buf.start + g_pong_buf.offset, LOG_CACHE_MAX_LEN - g_pong_buf.offset, base, len);
            g_write_buf       = E_PONG;
            g_pong_buf.offset = g_pong_buf.offset + len;
        }
        else
        {
            IndexMemCopy(g_ping_buf.start + g_ping_buf.offset,LOG_CACHE_MAX_LEN - g_ping_buf.offset, base, len);
            g_ping_buf.offset = g_ping_buf.offset + len;
        }
    }
    else
    {
        if (NeedSwitchBuf(len, g_pong_buf))
        {
            if (NeedSwitchBuf(len, g_ping_buf))
            {
                g_throw_log_count++;
                g_log_mutex.unlock();
                return;
            }

            IndexMemCopy(g_ping_buf.start + g_ping_buf.offset,LOG_CACHE_MAX_LEN - g_ping_buf.offset, base, len);
            g_write_buf       = E_PING;
            g_ping_buf.offset = g_ping_buf.offset + len;
        }
        else
        {
            IndexMemCopy(g_pong_buf.start + g_pong_buf.offset,LOG_CACHE_MAX_LEN - g_pong_buf.offset, base, len);
            g_pong_buf.offset = g_pong_buf.offset + len;
        }
    }
    g_log_mutex.unlock();

    return;
}

/**************************************************************************************************
Prototype    : OpenLogFile
Description  : open log file
Input        : None
Output       : None
Return Value : None
History:

1. Date         : 2017/7/8
   Author       :
   Modification : Created function

**************************************************************************************************/
void OpenLogFile(void)
{
    int32_t   result = 0;

    //if path no exist ,create it
    result = access((const char*)(index_log_file_path.c_str()), F_OK);
    if(0 != result)
    {
         if (0 != mkdir((const char*)(index_log_file_path.c_str()), 0750)) {
           return ;
        }
    }

    umask(S_IXOTH);
    g_log_file = open((const char*)(index_log_file_name.c_str()), O_RDWR | O_APPEND | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP);

    return;
}

/*****************************************************************************
*   Prototype    : RenameLogFile
*   Description  : rename log file
*   Input        : None
*   Output       : None
*   Return Value : void
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/11
*           Author       :
*           Modification : Created function
*
*****************************************************************************/
void RenameLogFile()
{
   char    new_name[LOG_NEW_NAME_LEN]           = {0};
   char    format_time_str[FORMAT_TIME_STR_LEN] = {0};
   time_t  current_time;

   time(&current_time);

   close(g_log_file);
   struct tm *ptm = localtime(&current_time);
   if (nullptr != ptm)
   {
        strftime(format_time_str, FORMAT_TIME_STR_LEN, "%Y-%m-%dT%H-%M-%S", ptm);
   }
   IndexSnprintf(new_name, sizeof(new_name), (index_log_file_name.length() + FORMAT_TIME_STR_LEN + 1), "%s.%s", index_log_file_name.c_str(), format_time_str);
   (void)rename((const char*)(index_log_file_name.c_str()), new_name);
   umask(S_IXOTH);
   g_log_file = open((const char*)(index_log_file_name.c_str()), O_RDWR | O_APPEND | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP);
}

/*****************************************************************************
*   Prototype    : GetFileSize
*   Description  : get size of the file
*   Input        : const char *path
*   Output       : None
*   Return Value : uint64_t
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/6/22
*           Author       :
*           Modification : Created function
*
*****************************************************************************/
uint64_t GetFileSize(const char *path)
{
    uint64_t fileSize = 0;
    struct stat statbuff;
    int    log_file_fd = 0;

    if (stat(path, &statbuff) < 0)
    {
        fileSize = 0;
    }
    else
    {
        fileSize = statbuff.st_size;
    }

    if (-1 != g_log_file && fstat(g_log_file,&statbuff) >= 0)
    {
        g_fileSizeByHandle = statbuff.st_size;
        if (g_fileSizeByHandle > (uint64_t)MAX_LOG_SIZE * 10)
        {
            /*if current file size larger than 5 GB£¬change file handle*/
            close(g_log_file);
            log_file_fd = open((const char*)(index_log_file_name.c_str()), O_RDWR | O_APPEND, S_IRUSR | S_IWUSR | S_IRGRP);
            if (log_file_fd > 0)
            {
                g_log_file = log_file_fd;
            }
        }
    }

    return fileSize;
}

/*****************************************************************************
*   Prototype    : FreeLogBuf
*   Description  : free ping pong buffer
*   Input        : void
*   Output       : None
*   Return Value : void
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/14
*           Author       :
*           Modification : Created function
*
*****************************************************************************/
void FreeLogBuf(void)
{
    IndexMemFree(g_ping_buf.start);
    IndexMemFree(g_pong_buf.start);
    IndexMemFree(g_write_disk_buf.start);
}


// copy ping or pong cache to write disk buf.
void CopyCacheToWriteDiskBuf(LogBuf &cache_buf, LogBuf &write_disk_buf)
{
    write_disk_buf.offset = cache_buf.offset;
    IndexMemCopy(write_disk_buf.start, LOG_CACHE_MAX_LEN, cache_buf.start, LOG_CACHE_MAX_LEN);
    cache_buf.offset = 0;
    IndexMemZero(cache_buf.start,LOG_CACHE_MAX_LEN);
}

// write ping or pong cache to disk
int32_t WriteCachetoDisk(uint32_t& time_count)
{
    int         result    = 0;
    int32_t     len       = 0;

    if (g_write_disk_buf.offset <= 0)
    {
        return RET_OK;
    }

    if (-1 == g_log_file)
    {
        OpenLogFile();
        if (-1 == g_log_file)
        {
            return RET_ERROR;
        }
    }
    else
    {
        result = access((const char*)(index_log_file_name.c_str()), F_OK);
        if (0 != result)
        {
            close(g_log_file);
            g_log_file = -1;
            OpenLogFile();
            if (-1 == g_log_file)
            {
                return RET_ERROR;
            }
        }
    }

    len = write(g_log_file, g_write_disk_buf.start, g_write_disk_buf.offset);
    if (0 >= len)
    {
        time_count = 0;
        return RET_ERROR;
    }
    (void)fsync(g_log_file);
    g_write_disk_buf.offset = 0;
    time_count = 0;

    return RET_OK;
}


/*****************************************************************************
*   Prototype    : LogFlushThread
*   Description  : write log to file from buffer through the resident thread
*   Input        : void *junk
*   Output       : None
*   Return Value : void *
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/11
*           Author       :
*           Modification : Created function
*
*****************************************************************************/
void *LogFlushThread(void *junk)
{
    uint32_t    result      = RET_OK;
    uint32_t    time_count  = 0;

    g_log_flush_cnt++;
    pthread_setname_np(pthread_self(), "s3fs_logd");
    while(!g_should_stop_flag)
    {
        if (MAX_LOG_SIZE <= GetFileSize((const char*)(index_log_file_name.c_str())))
        {
            RenameLogFile();
        }

        time_count++;
        g_log_flush_cnt++;
        g_log_mutex.lock();
        if (INDEX_CONDITION_TRUE(E_PING == g_write_buf && 0 != g_pong_buf.offset))
        {
            CopyCacheToWriteDiskBuf(g_pong_buf,g_write_disk_buf);
        }
        else if (INDEX_CONDITION_TRUE(E_PONG == g_write_buf && 0 != g_ping_buf.offset))
        {
            CopyCacheToWriteDiskBuf(g_ping_buf,g_write_disk_buf);
        }
        else if (INDEX_CONDITION_TRUE(FLUSH_TIME <= time_count * FLUSH_SLEEP_TIME))
        {
            if (INDEX_CONDITION_TRUE(E_PING == g_write_buf && 0 != g_ping_buf.offset))
            {
                g_write_buf = E_PONG;
                CopyCacheToWriteDiskBuf(g_ping_buf,g_write_disk_buf);
            }
            else if (INDEX_CONDITION_TRUE(E_PONG == g_write_buf && 0 != g_pong_buf.offset))
            {
                g_write_buf = E_PING;
                CopyCacheToWriteDiskBuf(g_pong_buf,g_write_disk_buf);
            }
        }
        g_log_mutex.unlock();

        result = WriteCachetoDisk(time_count);
        if (RET_OK != result)
        {
            continue;
        }
        usleep(FLUSH_SLEEP_TIME);
    }
    g_flush_dead_flag = TRUE;

    return nullptr;
}

// info log support multiple instances
void DisableLogService(void)
{
    uint32_t tmp = g_instance_num.fetch_sub(1, std::memory_order_relaxed);
    IndexCommLogErr("", "DisableLogService num(%u)", tmp);
    if(1 == tmp)
    {
       return DisableLogService__();
    }
    else
    {
       return ;
    }
}


/*****************************************************************************
*   Prototype    : DisableLogService
*   Description  : disable log service
*   Input        : void
*   Output       : None
*   Return Value : void
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/11
*           Author       :
*           Modification : Created function
*
*****************************************************************************/
void DisableLogService__(void)
{
    uint32_t loop = 0;
    uint32_t len  = 0;
    int32_t  iRet = 0;
    g_should_stop_flag = TRUE;//exit flush thread

    while (!g_flush_dead_flag)
    {
        struct timespec sleepTime;
        sleepTime.tv_sec  = 0;
        sleepTime.tv_nsec = FLUSH_SLEEP_TIME * 1000;
        nanosleep(&sleepTime, NULL);
    }
    if (-1 == g_log_file)
    {
        OpenLogFile();
        if (-1 == g_log_file)
        {
            return;
        }
    }

    if (0 != g_ping_buf.offset)
    {
        do
        {
            len = write(g_log_file, g_ping_buf.start, g_ping_buf.offset);
            if (0 < len)
            {
                break;
            }
            loop++;
        } while (TRY_MAX_TIEM > loop);
    }

    if (0 != g_pong_buf.offset)
    {
        loop = 0;
        do
        {
            len = write(g_log_file, g_pong_buf.start, g_pong_buf.offset);
            if (0 < len)
            {
                break;
            }
            loop++;
        } while (TRY_MAX_TIEM > loop);
    }

    loop = 0;
    do
    {
        iRet = fsync(g_log_file);
        if (RET_OK == iRet)
        {
            break;
        }
        loop++;
    } while (TRY_MAX_TIEM > loop);

    loop = 0;
    do
    {
        iRet = close(g_log_file);
        if (RET_OK == iRet)
        {
            break;
        }
        loop++;
    } while (TRY_MAX_TIEM > loop);

    g_log_file = -1;
    FreeLogBuf();

    return;
}

/**************************************************************************************************
Prototype    : IndexLogEntry
Description  : print log
Input        : None
Output       : None
Return Value : None
History:

1. Date         : 2017/4/11
   Author       :
   Modification : Created function

**************************************************************************************************/
void  IndexLogEntry(
         const uint64_t     module,
         const char*        chunk_id,
         const uint32_t     log_level,
         const char*        log_file_name,
         const char*        log_func_name,
         const uint32_t     log_line,
         const char*        pszFormat, ...)
{
   if (TRUE == g_should_stop_flag || FALSE == g_log_service_enable_flag)//lint !e731
   {
       return;
   }

   if (log_level < g_index_log_level)
   {
      return;
   }

   va_list ap;
   va_start(ap, pszFormat);
   LogToBuf(module, chunk_id, log_level, log_file_name, log_func_name, log_line, pszFormat, ap);
   va_end(ap);
   return;
}

/**************************************************************************************************
Prototype    : IndexLogEntry
Description  : set log level
Input        : None
Output       : None
Return Value : None
History:

1. Date         : 2017/4/11
   Author       :
   Modification : Created function

**************************************************************************************************/
void IndexSetLogLevel(uint32_t log_level)
{
    g_index_log_level = log_level;
}

/**************************************************************************************************
Prototype    : IndexLogEntry
Description  : set log file name and path
Input        : log_file_path
Output       : None
Return Value : None
History:

1. Date         : 2017/4/21
   Author       :
   Modification : Created function

**************************************************************************************************/
void IndexSetLogFileName(const std::string& log_file_path, const std::string& log_file_name)
{
    char    process_id_str[LOG_NEW_NAME_LEN]           = {0};

    g_process_id = (long long )getpid();
    snprintf(process_id_str, sizeof(process_id_str), "%ld",g_process_id);
    /* BEGIN: Modified for fix log lost, 2017/6/8 */
    index_log_file_path = log_file_path;
    index_log_file_name = index_log_file_path + log_file_name + process_id_str;
    /* END:  2017/6/8 */
}

// info log support multiple instances
int32_t EnableLogService(const std::string& log_file_path, const std::string& log_file_name)
{
    uint32_t tmp = g_instance_num.fetch_add(1, std::memory_order_relaxed);
    if(0 == tmp)
    {
       return EnableLogService__(log_file_path, log_file_name);
    }
    else
    {
       IndexCommLogErr("", "DB(%s)EnableLogService num(%u)", log_file_path.c_str(), tmp);
       return RET_OK;
    }
}
/*****************************************************************************
*   Prototype    : EnableLogService
*   Description  : enable log service
*   Input        : IN const string log_file_path
*   Output       : None
*   Return Value : void
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/11
*           Author       :
*           Modification : Created function
*
*****************************************************************************/
int32_t EnableLogService__(const std::string& log_file_path, const std::string& log_file_name)
{
    int32_t   result = RET_OK;

    result = CreateLogBuf(g_ping_buf);
    if (RET_OK != result)
    {
        return result;
    }
    result = CreateLogBuf(g_pong_buf);
    if (RET_OK != result)
    {
        IndexMemFree(g_ping_buf.start);
        return result;
    }

    result = CreateLogBuf(g_write_disk_buf);
    if (RET_OK != result)
    {
        IndexMemFree(g_ping_buf.start);
        IndexMemFree(g_pong_buf.start);
        return result;
    }

    IndexSetLogFileName(log_file_path, log_file_name);

    pthread_t pthread_flush;
    result = pthread_create(&pthread_flush, nullptr, LogFlushThread, (void*)nullptr);
    if (0 != result)
    {
        return result;
    }

    g_log_service_enable_flag = TRUE;
    return result;
}


/*****************************************************************************
*   Prototype    : ManualFlushLog
*   Description  :
*   Input        : void
*   Output       : None
*   Return Value : void
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/9/11
*           Author       :
*           Modification : Created function
*
*****************************************************************************/
void ManualFlushLog(void)
{

    if (-1 == g_log_file)
    {
        return;
    }

    g_log_mutex.lock();

    if (0 != g_ping_buf.offset)
    {
        (void)write(g_log_file, g_ping_buf.start, g_ping_buf.offset);
        CleanLogBuf(&g_ping_buf);
    }

    if (0 != g_pong_buf.offset)
    {
        (void)write(g_log_file, g_pong_buf.start, g_pong_buf.offset);
        CleanLogBuf(&g_pong_buf);
    }
    g_log_mutex.unlock();

    (void)fsync(g_log_file);

    return;
}
//convert byte to hex
void IndexByte2HexAsic(char byte1, char hexbuf[3])
{
    unsigned char byte = (unsigned char)byte1;
    unsigned char high4bit = byte >> 4, low4bit = (byte & 0x0F);
    hexbuf[0] = (high4bit <= 9) ? (high4bit + 0x30) : (high4bit + 0x61-0xa);
    hexbuf[1] = (low4bit <= 9) ? (low4bit + 0x30) : (low4bit + 0x61-0xa);
    hexbuf[2] = 0;
}
//binary data 2 hexstring
std::string IndexData2HexString(const char *pdata, int size)
{
    std::string  hexstring;
    char  hexbuf[3] = {0};
    int   i = 0;

    while(size > 0)
    {
        IndexByte2HexAsic(pdata[i], hexbuf);
        hexstring.append(hexbuf);
        size--;
        i++;
    }
    return hexstring;
}

// Dump data to log with hex format
void IndexDataHexDump(const char *buf, int len) {
    int i = 0, j = 0, k = 0;
    int max_size = 16*1024*1024; // 16MB
    char *binStr = NULL;

    if (0 >= len || len > max_size) {
        IndexCommLogWarn("", "bson length is invalid for dump! len(%d)max(%d)",
            len, max_size);
        return;
    }

    binStr = (char*)malloc(len);
    IndexMemZero(binStr,len);

    if (NULL == binStr) {
        IndexCommLogWarn("", "malloc fail! size(%d)", len);
        return;
    }

    for (i=0; i<len; i++) {
        if (0==(i%16)) {
            IndexSprintf(binStr, len, "%08x -", (unsigned int)i);
            IndexSprintf(binStr, len, "%s %02x", binStr,(unsigned char)buf[i]);
        } else if (15 == (i%16)) {
            IndexSprintf(binStr, len, "%s %02x", binStr,(unsigned char)buf[i]);
            IndexSprintf(binStr, len, "%s  ", binStr);
            for (j=i-15; j<=i; j++) {
                IndexSprintf(binStr, len, "%s%c", binStr, ('!'<buf[j] && buf[j]<='~') ? buf[j] : '.');
            }
            IndexCommLogWarn("", "%s", binStr);
        } else {
            IndexSprintf(binStr, len, "%s %02x", binStr, (unsigned char)buf[i]);
        }
    }

    if (0 != (i%16)) {
        k = 16 - (i%16);
        for (j=0; j<k; j++) {
            IndexSprintf(binStr, len, "%s   ", binStr);
        }
        IndexSprintf(binStr, len, "%s  ", binStr);
        k = 16-k;
        for (j=i-k; j<i;j ++) {
            IndexSprintf(binStr, len, "%s%c", binStr, ('!'<buf[j] && buf[j]<='~') ? buf[j] : '.');
        }
        IndexCommLogWarn("", "%s", binStr);
    }

    IndexMemFree(binStr);
    return;
}


#ifdef __cplusplus
#if __cplusplus
}
#endif
#endif /* __cplusplus */

