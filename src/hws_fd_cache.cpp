#include <string.h>
#include <assert.h>
#include <time.h>
#include <syslog.h>

#include <cstdint>
#include <string>
#include <iostream>
#include <sstream>
#include <map>
#include <list>
#include <vector>
#include <mutex>
#include <memory>

#include "common.h"
#include "hws_fd_cache.h"
#include "crc32c.h"
#include "hws_configure.h"

#ifdef HWS_FD_CACHE_TEST
extern int s3fs_write_hw_obs_proc_test(const char* buf, size_t size, off_t offset);
extern int s3fs_read_hw_obs_proc_test(char* buf, size_t size, off_t offset);
extern void mock_invalid_read_page(struct HwsFdReadPage* page, int read_size);

extern unsigned int g_test_simulate_alloc_fail;
extern std::atomic<int> g_test_read_page_disable_step;
#else
extern int s3fs_write_hw_obs_proc(const char* path, const char* buf, size_t size, off_t offset);
extern int s3fs_read_hw_obs_proc(const char* path, char* buf, size_t size, off_t offset);
#endif

#define HWS_CACHE_ADJUST_SIZE (500*HWS_MB_SIZE)
#define HWS_LOG_FLOW_CONTROL_MS 3000  /*err log flow control,3 second print one*/

s3fs_log_level data_cache_log_level;

bool gIsReadWaitCache = true;
size_t gWritePageSize = 6 * 1024 * 1024 - 128 * 1024;
unsigned int gWritePageNum = 12;
size_t gReadPageSize = 6 * 1024 *1024;
unsigned int gReadPageNum = 12;
long gReadPageCleanMs = 1000;
int gReadStatSizeMax = 32;
int gReadStatSize = 32;
int gReadStatSeqSize = 24;
off64_t gReadStatSizeThreshold = 4 * 1024 * 1024;
long gReadStatDiffLongMs = 120000;   /*120 seconds*/
/*10 seconds,one read 0.2s,24 read need 4.8s,must larger than 4.8s*/
long gReadStatDiffShortMs = 10000;   
int  gFreeCacheThresholdPercent = 20;  /*if lower than this percent,use gReadStatDiffShortMs*/
bool gIsCheckCRC = true;
unsigned int gWriteStatSeqNum = 3;     /*if sequential modify 3 times,merge write*/
bool g_bIntersectWriteMerge = false;   /*if merge intersect write to write cache*/
size_t gFuseMaxReadSize = 128 * 1024;
off64_t gMaxCacheMemSize = 1024 * HWS_MB_SIZE;
long gCheckCacheSizePeriodMs = 10000;   /*10 seconds*/
unsigned int g_ReadAllHitNum = 0;   /*for UT*/
unsigned int g_ReadNotHitNum = 0;   /*for UT*/
long gPrintCacheStatisMs =  300000;  /*print statis every 300 second*/
/*num for alloc write page retry beyond 3 second*/
unsigned int g_AllocWritePageTimeoutNum = 0;   
int  g_aulPrintLogCnt[HWS_LOG_HASH_BUCKET_NUM] = {0};    //for print log flow control

bool g_wholeFileCacheSwitch = false; //default false
off64_t g_wholeFileCacheMaxMemSize = 1024 * HWS_MB_SIZE;
int g_wholeFileCachePreReadStatisPeriod = 5; //seconds
int g_wholeFileCacheNotHitTimesPerPeriod = 300;
off64_t g_wholeFileCacheNotHitSizePerPeriod = 5 * HWS_MB_SIZE;
int g_wholeFileCachePreReadInterval = 50; //ms
int g_wholeFileCacheMaxRecycleTime = 10800; //seconds
off64_t g_wholeFileCacheMaxHitTimes = 500000;
off64_t g_MinReadWritePageByCacheSize = 0;
std::atomic<int> g_wholeCacheReadTasks{0};
std::mutex g_wholeCacheReadThreadsMutex;
std::atomic<off64_t> g_wholeCacheTotalMemSize{0};

// TODO: move function to Utility
#define TIME_FORMAT_LENGTH 20

static void get_current_time(char* buffer, unsigned int length)
{
    memset(buffer, '\0', length);
    if (length < TIME_FORMAT_LENGTH)
    {
        return;
    }

    time_t time_now;
    struct tm* time_info;
    time(&time_now);
    time_info = localtime(&time_now);
    strftime(buffer, length, "%04Y%02m%02d %H:%M:%S", time_info);
}

const char* print_err_type(hws_write_err_type_e err_type){
    switch (err_type)
    {
        HWS_ENUM_TO_STR(CRC_ERR)
        HWS_ENUM_TO_STR(SEND_OBS_ERR)
        case BUTT_ERR:
            break;
    }
    return "invalid_err";
}

int sendto_obs(const char* path, const char* data, size_t bytes, off64_t offset)
{
#ifdef HWS_FD_CACHE_TEST
    return s3fs_write_hw_obs_proc_test(data, bytes, offset);
#else
    return s3fs_write_hw_obs_proc(path, data, bytes, offset);
#endif
}

int recvfrom_obs(const char* path, char* data, size_t bytes, off64_t offset)
{
#ifdef HWS_FD_CACHE_TEST
    return s3fs_read_hw_obs_proc_test(data, bytes, offset);
#else
    return s3fs_read_hw_obs_proc(path, data, bytes, offset);
#endif
}

long diff_in_ms(struct timespec *start, struct timespec *end)
{
    return (end->tv_sec - start->tv_sec)*1000 + ((long)end->tv_nsec - (long)start->tv_nsec)/1000000;
}
/*print statis result and clear statis*/
void clearPrintLogNum()       
{
    static struct timespec prev_clear_ts;
    struct timespec now_ts;
    
    clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);
    /*print every 30 seconds*/
    long clearDiffMs  = diff_in_ms(&prev_clear_ts, &now_ts);
    if (clearDiffMs < 30000)
    {
        return;
    }
    memset((void *)g_aulPrintLogCnt, 0, sizeof(int) * HWS_LOG_HASH_BUCKET_NUM);
    prev_clear_ts = now_ts;
    
    S3FS_PRN_DBG("clearDiffMs=%ld",clearDiffMs);
}

bool can_pint_log_with_fc(int uiLineNum)
{    
#ifdef HWS_FD_CACHE_TEST
        return true;
#else    
    int ulHashIndex = 0;
    bool bCanPrint = true;
    
    ulHashIndex = uiLineNum % HWS_LOG_HASH_BUCKET_NUM;
    
    if  (g_aulPrintLogCnt[ulHashIndex] >= HwsGetIntConfigValue(HWS_CFG_MAX_PRINT_LOG_NUM_PERIOD))
    {
        bCanPrint = false;
    }

    if ( true == bCanPrint )
    {
        g_aulPrintLogCnt[ulHashIndex] += 1;
    }

    S3FS_PRN_DBG("bCanPrint=%d,printCnt=%d,cfgMax=%d,uiLineNum=%d", 
        bCanPrint,g_aulPrintLogCnt[ulHashIndex],HwsGetIntConfigValue(HWS_CFG_MAX_PRINT_LOG_NUM_PERIOD),
        uiLineNum);

    return bCanPrint;        
    
#endif
}

bool is_new_merge_happened(struct HwsFdWritePage* page)
{
    struct timespec now_ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);
    return page->state_ != HWS_FD_WRITE_STATE_NOTIFY &&
           diff_in_ms(&(page->last_write_ts_), &now_ts) < HWS_WRITE_PAGE_WAIT_TIME_MS;
}


void write_thread_task(struct HwsFdWritePage* page)
{
    ssize_t result = 0;    
    
    struct timespec begin_ts;    
    clock_gettime(CLOCK_MONOTONIC_COARSE, &begin_ts);
    
    std::unique_lock<std::mutex> lock(page->write_merge_mutex_);
    while(page->state_ == HWS_FD_WRITE_STATE_WAIT)
    {
        if(page->cv_.wait_for(lock, std::chrono::milliseconds(HWS_WRITE_PAGE_WAIT_TIME_MS))==std::cv_status::timeout)
    	{
    	    // timed out
            if (!is_new_merge_happened(page))
    	    {
    	        S3FS_PRN_INFO("hws fd cache write path(%s) offset(%ld)bytes(%lu) timeout",
                    page->path_.c_str(), page->offset_, page->bytes_);
                break;
    	    }
    	    S3FS_PRN_INFO("hws fd cache write path(%s) offset(%ld)bytes(%lu) continue wait",
                    page->path_.c_str(), page->offset_, page->bytes_);
    	}
    	else{
    	    S3FS_PRN_INFO("hws fd cache write path(%s) offset(%ld)bytes(%lu) is notified",
                page->path_.c_str(), page->offset_, page->bytes_);
            break;

    	}
    }
    page->state_ = HWS_FD_WRITE_STATE_SENDING;
    lock.unlock();

    bool need_send = true;
    if(gIsCheckCRC)
    {
        uint32_t crc = rocksdb::crc32c::Value(page->data_, page->bytes_);
        if(crc != page->crc_)
        {
            need_send = false;
            page->err_type_ = CRC_ERR;
            result = -1;
        }
    }
    if(need_send)
    {
        result = sendto_obs(page->path_.c_str(), page->data_, page->bytes_, page->offset_);
    }

    HwsCacheStatis::GetInstance().AddStatisNum(WRITE_MERGE_OBS);
    HwsCacheStatis::GetInstance().AddStatisLen(MERGE_WRITE_OBS_LEN, 
        (unsigned int) page->bytes_);
    
    lock.lock();
    if(result >= 0)
    {
        page->state_ = HWS_FD_WRITE_STATE_FINISH;
    }
    else
    {
        page->state_ = HWS_FD_WRITE_STATE_ERROR;
        if (page->err_type_ == BUTT_ERR)
        {
            page->err_type_ = SEND_OBS_ERR;
        }
        S3FS_PRN_ERR("write page error path(%s) offset(%ld)bytes(%lu)errType(%s)",
            page->path_.c_str(), page->offset_, page->bytes_, print_err_type(page->err_type_));
    }
    page->cv_.notify_all();

    struct timespec end_ts;    
    clock_gettime(CLOCK_MONOTONIC_COARSE, &end_ts);
    long diffMs  = diff_in_ms(&begin_ts, &end_ts);
    S3FS_DATA_CACHE_PRN_LOG(
        "hws cache write path(%s)off(%ld)bytes(%lu)freeCache(%ld)maxCache(%ld)diff(%ld)", 
        page->path_.c_str(), page->offset_,page->bytes_,
        HwsFdManager::GetInstance().GetFreeCacheMemSize(),
        gMaxCacheMemSize,diffMs);
    return;
}

void read_thread_task(struct HwsFdReadPage* page)
{
    struct timespec begin_ts;    
    clock_gettime(CLOCK_MONOTONIC_COARSE, &begin_ts);

    ssize_t read_size = 0;
    read_size = recvfrom_obs(page->path_.c_str(), page->data_, page->read_page_size_, page->offset_);
    page->GenerateCrc(read_size);    
    HwsCacheStatis::GetInstance().AddStatisNum(READ_AHEAD_PAGE);
    HwsCacheStatis::GetInstance().AddStatisLen(READ_AHEAD_LEN, 
        (unsigned int) read_size);

#ifdef HWS_FD_CACHE_TEST
    mock_invalid_read_page(page, read_size);
#endif
    std::lock_guard<std::mutex> lock(page->read_ahead_mutex_);
    clock_gettime(CLOCK_MONOTONIC_COARSE, &page->got_ts_);
    if(read_size > 0)
    {
        page->read_size_ = read_size;
        if(page->state_ == HWS_FD_READ_STATE_RECVING)
        {
            page->state_ = HWS_FD_READ_STATE_RECVED;
        }
    }
    else
    {
        page->state_ = HWS_FD_READ_STATE_ERROR;
    }

    page->is_read_finish_ = true;
    page->cv_.notify_all();
    
    struct timespec end_ts;    
    clock_gettime(CLOCK_MONOTONIC_COARSE, &end_ts);
    long diffMs  = diff_in_ms(&begin_ts, &end_ts);
    S3FS_DATA_CACHE_PRN_LOG("hws cache read,path(%s)off(%ld)read_size(%lu),freeCache(%ld)"
        "maxCache(%ld),state_(%d),got_ts_(%ld)diff(%ld)",
        page->path_.c_str(), page->offset_,read_size,
        HwsFdManager::GetInstance().GetFreeCacheMemSize(),gMaxCacheMemSize,page->state_,
        page->got_ts_.tv_sec,diffMs);
    return;
}

void whole_file_cache_task(long long inodeNo)
{
    off64_t offset = 0;

    std::shared_ptr<HwsFdEntity> ent = HwsFdManager::GetInstance().Get(inodeNo);
    if (nullptr == ent)
    {
        S3FS_PRN_ERR("get instance failed, inodeNo(%lld)", inodeNo);
        return;
    }

    std::string& path = ent->GetPath();
    std::mutex& entity_mutex = ent->GetEntityMutex();
    HwsFdReadWholePageList& wholePageList = ent->GetReadWholePageList();

    /* pre-read the whole file data to cache */
    off64_t fileSize = ent->GetFileSize();
    while ((offset < fileSize) && (g_wholeCacheTotalMemSize.load() <= g_wholeFileCacheMaxMemSize))
    {
        if (ent->IsWholeWriteConflict())
        {
            S3FS_PRN_WARN("meet wirte conflict, stop pre-read, inodeNo(%lld)", inodeNo);
            break;
        }

        struct HwsFdReadPage *page = new(std::nothrow) HwsFdReadPage(path.c_str(), 
                offset, gReadPageSize, entity_mutex, NULL);
        if (nullptr == page)
        {
            S3FS_PRN_ERR("alloc read page fail! path(%s)offset(%ld)(%lld)", path.c_str(), offset, inodeNo);
            break;
        }
        ssize_t read_size = 0;
        read_size = recvfrom_obs(page->path_.c_str(), page->data_, page->read_page_size_, page->offset_);

        std::lock_guard<std::mutex> lock(page->read_ahead_mutex_);
        if(read_size > 0)
        {
            page->read_size_ = read_size;
            if(page->state_ == HWS_FD_READ_STATE_RECVING)
            {
                page->state_ = HWS_FD_READ_STATE_RECVED;
            }
        }
        else
        {
            S3FS_PRN_ERR("read (%s) failed, offset(%ld)inodeNo(%lld)", path.c_str(), offset, inodeNo);
            delete page;
            break;
        }
        page->is_read_finish_ = true;
        offset += read_size;
        wholePageList.pages_.push_back(page);
        g_wholeCacheTotalMemSize.fetch_add(read_size);
        std::this_thread::sleep_for(std::chrono::milliseconds(g_wholeFileCachePreReadInterval));
    }

    off64_t cur_cache_size = g_wholeCacheTotalMemSize.load();
    S3FS_PRN_WARN("whole_file_cache_task finished, path(%s)inodeNo(%lld)refs(%d)fileSize(%ld)cacheSize(%ld)",
                path.c_str(), inodeNo, ent->GetRefs(), fileSize, cur_cache_size);

    /* wait to read hit */
    clock_gettime(CLOCK_MONOTONIC_COARSE, &wholePageList.cacheEndTs);
    long diffSec = 0;
    struct timespec now_ts;
    while ((diffSec < g_wholeFileCacheMaxRecycleTime
            && wholePageList.cacheHitCnt < g_wholeFileCacheMaxHitTimes)
            && (!ent->IsWholeWriteConflict()))
    {
        std::this_thread::sleep_for(std::chrono::seconds(READ_LOG_CONFIGURE_INTERVAL)); //10 sec
        clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);
        diffSec = diff_in_ms(&wholePageList.cacheEndTs, &now_ts) / 1000;
    }

    cur_cache_size = g_wholeCacheTotalMemSize.load();
    S3FS_PRN_WARN("try to release cache pages, path(%s)inodeNo(%lld)"
                "refs(%d)cacheSize(%ld)diffSec(%ld)cacheHitCnt(%ld)conflict(%d)",
                path.c_str(), inodeNo, ent->GetRefs(), cur_cache_size, diffSec, 
                wholePageList.cacheHitCnt, ent->IsWholeWriteConflict());
    /* release cache data */
    std::lock_guard<std::mutex> lock(entity_mutex);
    auto iter = wholePageList.pages_.begin();
    while(iter != wholePageList.pages_.end())
    {
        HwsFdReadPage* page = *iter;
        do
        {
            if (page->CanRelease())
            {
                iter = wholePageList.pages_.erase(iter);
                g_wholeCacheTotalMemSize.fetch_sub(page->read_size_);
                delete page;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        } while (true);
    }

    wholePageList.cacheHitCnt = 0;
    ent->ClearWholeReadCacheStatis();
    ent->ClearWholeReadConflictFlag();
    ent->SetWholeCacheStartedFlag(false);
    cur_cache_size = g_wholeCacheTotalMemSize.load();
    S3FS_PRN_WARN("read whole file cache task exited, path(%s)inodeNo(%lld)refs(%d)cacheSize(%ld)",
                            path.c_str(), inodeNo, ent->GetRefs(), cur_cache_size);
    //decrease Entity's ref and erase it from HwsEntityMap when 'ref == 0'.
    HwsFdManager::GetInstance().Close(inodeNo);

    return;
}

void setFdWholeFileCacheParasByCfg()
{
#ifdef HWS_FD_CACHE_TEST  //hws_fd_cache_test can not compile config function
        return;
#else
    int value = 0;
    value = HwsGetIntConfigValue(HWS_WHOLE_CACHE_SWITCH);
    g_wholeFileCacheSwitch = (value == 0) ? false : true;
    g_wholeFileCacheMaxMemSize = (off64_t)HwsGetIntConfigValue(HWS_WHOLE_CACHE_MAX_MEM_SIZE) * 1024 * HWS_MB_SIZE;
    g_wholeFileCachePreReadStatisPeriod = HwsGetIntConfigValue(HWS_WHOLE_CACHE_STATIS_PERIOD);
    g_wholeFileCacheNotHitTimesPerPeriod = HwsGetIntConfigValue(HWS_WHOLE_CACHE_READ_TIMES);
    g_wholeFileCacheNotHitSizePerPeriod = (off64_t)HwsGetIntConfigValue(HWS_WHOLE_CACHE_READ_SIZE) * HWS_MB_SIZE;
    g_wholeFileCachePreReadInterval = HwsGetIntConfigValue(HWS_WHOLE_CACHE_READ_INTERVAL_MS);
    g_wholeFileCacheMaxRecycleTime = HwsGetIntConfigValue(HWS_WHOLE_CACHE_MAX_RECYCLE_TIME);
    g_wholeFileCacheMaxHitTimes = (off64_t)HwsGetIntConfigValue(HWS_WHOLE_CACHE_MAX_HIT_TIMES) * 1000;

    S3FS_DATA_CACHE_PRN_LOG(
        "g_wholeFileCacheSwitch=%d,g_wholeFileCacheMaxMemSize=%ld,g_wholeFileCachePreReadStatisPeriod=%ds,"
        "g_wholeFileCacheNotHitTimesPerPeriod=%d,g_wholeFileCacheNotHitSizePerPeriod=%ld,"
        "g_wholeFileCachePreReadInterval=%dms,g_wholeFileCacheMaxRecycleTime=%ds,g_wholeFileCacheMaxHitTimes=%ld",
        g_wholeFileCacheSwitch, g_wholeFileCacheMaxMemSize, g_wholeFileCachePreReadStatisPeriod,
        g_wholeFileCacheNotHitTimesPerPeriod, g_wholeFileCacheNotHitSizePerPeriod,
        g_wholeFileCachePreReadInterval, g_wholeFileCacheMaxRecycleTime, g_wholeFileCacheMaxHitTimes);
#endif
}

void setFdCacheGlobalVariableByCfg()
{
    static struct timespec prev_set_by_cfg_ts;
    struct timespec now_ts;
    
    clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);
    /*print every 60 seconds*/
    long clearDiffMs  = diff_in_ms(&prev_set_by_cfg_ts, &now_ts);
    if (clearDiffMs < 60000)
    {
        return;
    }
    prev_set_by_cfg_ts = now_ts;
    
#ifdef HWS_FD_CACHE_TEST  //hws_fd_cache_test can not compile config function
    return;
#else    
    gReadPageCleanMs = HwsGetIntConfigValue(HWS_CFG_READ_PAGE_CLEAN_MS); 
    gReadStatDiffLongMs = HwsGetIntConfigValue(HWS_READ_AHEAD_STAT_DIFF_LONG); 
    gReadStatDiffShortMs = HwsGetIntConfigValue(HWS_READ_AHEAD_STAT_DIFF_SHORT); 
    gMaxCacheMemSize = (long)HwsGetIntConfigValue(HWS_CFG_MAX_CACHE_MEM_SIZE_MB) * HWS_MB_SIZE;
    gIsCheckCRC = (bool)HwsGetIntConfigValue(HWS_CFG_CACHE_CHECK_CRC_OPEN); 
    int cfgStatVecSize     = HwsGetIntConfigValue(HWS_READ_STAT_VEC_SIZE); 
    if (cfgStatVecSize < gReadStatSizeMax)
    {
        gReadStatSize = cfgStatVecSize;
    }
    int cfgStatSequentialSize = HwsGetIntConfigValue(HWS_READ_STAT_SEQUENTIAL_SIZE); 
    if (cfgStatSequentialSize < gReadStatSize)
    {
        gReadStatSeqSize  = cfgStatSequentialSize; 
    }
    gReadStatSizeThreshold = (off64_t)HwsGetIntConfigValue(HWS_READ_STAT_SIZE_THRESHOLD); 
    S3FS_DATA_CACHE_PRN_LOG(
        "gReadPageCleanMs=%ld,gReadStatDiffLongMs=%ld,gReadStatDiffShortMs=%ld",
        gReadPageCleanMs,gReadStatDiffLongMs,gReadStatDiffShortMs);
    S3FS_DATA_CACHE_PRN_LOG(
        "gMaxCacheMemSize=%ld,gIsCheckCRC=%d",gMaxCacheMemSize,gIsCheckCRC);
    S3FS_DATA_CACHE_PRN_LOG(
        "gReadStatSize=%d,gReadStatSeqSize=%d,StatSizeThreshold=%ld",
        gReadStatSize,gReadStatSeqSize,gReadStatSizeThreshold);
    gWritePageNum = HwsGetIntConfigValue(HWS_WRITE_PAGE_NUM);
    gReadPageNum = HwsGetIntConfigValue(HWS_READ_PAGE_NUM);
    S3FS_DATA_CACHE_PRN_LOG(
         "gWritePageNum=%d, gReadPageNum=%d,", gWritePageNum, gReadPageNum);
    gFreeCacheThresholdPercent = HwsGetIntConfigValue(HWS_FREE_CACHE_THRESHOLD_PERCENT);        
    int cfgIntersectWriteMerge = HwsGetIntConfigValue(HWS_INTERSECT_WRITE_MERGE); 
    g_bIntersectWriteMerge = (cfgIntersectWriteMerge == 0) ? false : true;
    if (g_bIntersectWriteMerge)
    {
        gIsCheckCRC = false;
        S3FS_DATA_CACHE_PRN_LOG(
            "change checkCrc false for intersectMerge,gIsCheckCRC=%d",gIsCheckCRC);
    }
    S3FS_DATA_CACHE_PRN_LOG(
        "FreeCachePercent=%d,g_bIntersectWriteMerge=%d",
        gFreeCacheThresholdPercent,g_bIntersectWriteMerge);
    setFdWholeFileCacheParasByCfg();

    g_MinReadWritePageByCacheSize = 
        (off64_t)HwsGetIntConfigValue(HWS_MIN_READ_WRITE_PAGE_BY_CACHE_SIZE);
    S3FS_DATA_CACHE_PRN_LOG(
        "min_read_write_page_by_cache_size=%ld",g_MinReadWritePageByCacheSize);
    
#endif    
}

/*daemon thread function,adjust cache mem size*/
void hws_cache_daemon_task()
{
    pthread_setname_np(pthread_self(), "datacache_d");
    
    while (!HwsFdManager::GetInstance().GetDaemonStop() && g_s3fs_start_flag)
    {  
         HwsFdManager::GetInstance().CheckCacheMemSize();
         HwsCacheStatis::GetInstance().PrintStatisAndClear();
         std::this_thread::sleep_for(std::chrono::milliseconds(1000));
         clearPrintLogNum();
         setFdCacheGlobalVariableByCfg();
         HwsFdManager::GetInstance().DestroyWholeFileCacheTask();
    }
}
/*wait write page write to obs;
  must lock write_merge_mutex_ in caller*/
void wait_write_task_finish(struct HwsFdWritePage* page,std::unique_lock<std::mutex>& lock)
{
    page->WritePageRef();
    if(page->state_ == HWS_FD_WRITE_STATE_WAIT)
    {
        page->state_ = HWS_FD_WRITE_STATE_NOTIFY;
        page->cv_.notify_all();
    }
    while(page->state_ != HWS_FD_WRITE_STATE_FINISH && page->state_ != HWS_FD_WRITE_STATE_ERROR)
    {
        page->cv_.wait(lock);
    }
    page->WritePageUnref();
    return;
}
/*wait page read from obs;
  must lock read_ahead_mutex_ in caller*/
void wait_read_task_finish(struct HwsFdReadPage* page,std::unique_lock<std::mutex>& lock)
{
    page->ReadPageRef();
    while(page->state_ == HWS_FD_READ_STATE_RECVING)
    {
        page->cv_.wait(lock);
    }
    page->ReadPageUnref();
    return;
}
/*alloc mem,if fail,retry until succeed*/
void* AllocMemWithRetry(size_t size)
{  
    static off64_t retryNum = 0;
    
    void* buffer = nullptr;
    do
    {
        buffer = malloc(size);

#ifdef HWS_FD_CACHE_TEST  /*UT simulate alloc fail*/
        if (g_test_simulate_alloc_fail > 0)
        {
            free(buffer);
            buffer = nullptr;
            g_test_simulate_alloc_fail--;
            S3FS_PRN_INFO("simulate alloc fail,retry! simulate(%u)", 
                g_test_simulate_alloc_fail);                
        }
#endif        

        if (nullptr == buffer)
        {
            if (can_pint_log_with_fc(__LINE__))
            {
                S3FS_PRN_ERR("alloc mem fail! size(%zu),total retry(%ld)",
                    size,retryNum);                
            }
            /*delay 40 ms and retry*/
            std::this_thread::sleep_for(std::chrono::milliseconds(HWS_ALLOC_MEM_RETRY_MS));              
        }        
    }
    while (nullptr == buffer);

    return buffer;
}
/*copy intersect write to last page;  
  must lock write_merge_mutex_ in caller*/
size_t HwsFdWritePage::IntersectWriteMergeToLastpage(
        const char *buf,off64_t wrtOffset, size_t wrtSize)
{
    // if state_ != HWS_FD_WRITE_STATE_WAIT 
    //or size > write_page_size_ - bytes_, return in Append function

    bool canMerge = false;    
    if ((wrtOffset <= offset_ + (off64_t)bytes_) &&
        (wrtOffset >= offset_))
    {
        //write offset in page data range
        canMerge = true;
    }
    if (canMerge == false)
    {
        S3FS_PRN_WARN(
            "canMerge is false,path(%s)offset(%ld),wrtSize(%lu)",
            path_.c_str(),wrtOffset,wrtSize);
        return 0;
    }
    size_t copy_offset_in_page = wrtOffset - offset_;
    if (copy_offset_in_page + wrtSize >= write_page_size_)
    {
        //write end beyond page size
        S3FS_PRN_WARN(
            "beyond last page,path(%s)offset(%ld),wrtSize(%lu)pageoff(%ld)pgSize(%lu)",
            path_.c_str(),wrtOffset,wrtSize,offset_,write_page_size_);
        return 0;
    }

    memcpy(data_+copy_offset_in_page, buf, wrtSize);
    bytes_ = max(bytes_,copy_offset_in_page+wrtSize);
    // update last_write_ts
    clock_gettime(CLOCK_MONOTONIC_COARSE, &last_write_ts_);
    S3FS_DATA_CACHE_PRN_LOG(
        "merge to last,path(%s)offset(%ld),size(%lu)pageoff(%ld)"
        "bytes(%lu)copyOffset(%lu)",
        path_.c_str(),wrtOffset,wrtSize,offset_,bytes_,copy_offset_in_page);
    return wrtSize;    
}
/*append buf to page list;  pbIsSequential is output para
  must lock write_merge_mutex_ in caller*/
size_t HwsFdWritePage::Append(const char *buf,off64_t offset, size_t size,
            bool* pbIsSequential)
{
    *pbIsSequential = true;
    if(state_ != HWS_FD_WRITE_STATE_WAIT)
    {
        return 0;
    }
    else if(size > write_page_size_ - bytes_)
    {
        state_ = HWS_FD_WRITE_STATE_NOTIFY;
        cv_.notify_all();
        return 0;
    }    
    else if (offset != offset_ + (off64_t)bytes_)  
    {
        if (g_bIntersectWriteMerge)
        {
            *pbIsSequential = false; //intersect write not retry
            size_t intersectMergeSize = 
                    IntersectWriteMergeToLastpage(buf,offset,size);
            return intersectMergeSize;
        }
        else  // not support intersect write merge,must sequential
        {
            S3FS_PRN_ERR("write not sequential,path(%s)offset(%ld),pageoff(%ld)bytes(%lu)", 
                path_.c_str(),offset,offset_,bytes_);
            return 0;
        }
    }
    
    memcpy(data_+bytes_, buf, size);
    if(gIsCheckCRC)
    {
        crc_ = rocksdb::crc32c::Extend(crc_, buf, size);
    }
    bytes_ += size;
    if(write_page_size_ == bytes_){
        state_ = HWS_FD_WRITE_STATE_NOTIFY;
        cv_.notify_all();
    }
    // update last_write_ts
    clock_gettime(CLOCK_MONOTONIC_COARSE, &last_write_ts_);
    S3FS_PRN_INFO(
        "append to page,path(%s)offset(%ld),pageoff(%ld)bytes(%lu)crcswitch=%u"
        " bIsSequential=%d",
        path_.c_str(),offset,offset_,bytes_,gIsCheckCRC,*pbIsSequential);
    return size;
}

void HwsFdWritePage::WritePageUnref()
{
    if (refs_ > 0)
    {
        refs_--;
        return;
    }
    S3FS_PRN_ERR("page ref err! path(%s)pageoff(%ld)bytes(%lu)",
            path_.c_str(),offset_,bytes_);
    refs_ = 0;
}

HwsFdWritePage* HwsFdWritePageList::NewWritePage(const char *path,
                off64_t offset, size_t write_page_size,
                std::mutex& write_merge_mutex,
                WRITE_PAGE_THREAD_FUNC thread_task)
{
    HwsFdWritePage* newPage = nullptr;

    S3FS_PRN_INFO("new writePage,path(%s)off(%ld)write_page_size(%lu)", 
        path, offset,write_page_size);                

#ifdef HWS_FD_CACHE_TEST  /*UT simulate alloc fail*/
                if (g_test_simulate_alloc_fail > 0)
                {
                    /*modify write_page_size too large for new fail*/
                    write_page_size = 102400 * write_page_size;  
                    g_test_simulate_alloc_fail--;
                    S3FS_PRN_INFO("simulate new page fail!path(%s)simulate(%u)"
                        "write_page_size(%lu)", 
                        path, g_test_simulate_alloc_fail,write_page_size);                
                }
#endif        

    newPage = new HwsFdWritePage(path, offset, write_page_size,
        write_merge_mutex_, write_thread_task);   
    if (nullptr == newPage->GetDataBuf())
    {
        delete newPage;
        newPage = nullptr;
    }
    return newPage;
}

/*append buf to page list;  pbIsSequential is output para
  must lock write_merge_mutex_ in caller
  return Value: write to page bytes*/
size_t HwsFdWritePageList::Append(const char *path, const char *buf, off64_t offset, 
            size_t size,bool* pbIsSequential)
{
    size_t write_size = 0;
    HwsFdWritePage* newPage = nullptr;
	HwsFdWritePage* writePage = nullptr;
    unsigned int cur_page_num = pages_.size();
    bool bIsSequential = true;
    
    do{
        if (IsFull())
        {
            S3FS_PRN_INFO("writePage full,retry!path(%s)off(%ld)pageNum(%u)", 
                path, offset,cur_page_num);                
            return 0;            
        }
        if(pages_.size() == 0 || pages_.back()->Full())
        {
            newPage = NewWritePage(path, offset, write_page_size_,
                write_merge_mutex_, write_thread_task);            
            if (nullptr == newPage)
            {
                S3FS_PRN_WARN("alloc page fail!path(%s)off(%ld)pageNum(%u)", 
                    path, offset,cur_page_num);                
                return 0;
            }
            writePage = newPage;
            HwsFdManager::GetInstance().AddCacheMemSize(write_page_size_);            
        }
        else
        {
            writePage = pages_.back();
        }
        

        write_size = writePage->Append(buf,offset,size,pbIsSequential);
        if (nullptr != newPage)
        {
            pages_.push_back(newPage);
        }        
        bIsSequential = *pbIsSequential;
    }while(write_size == 0 && !IsFull() && bIsSequential);
    return write_size;
}

/*clean finish and error page;  
  must lock write_merge_mutex_ in caller*/
void HwsFdWritePageList::Clean()
{
    unsigned int cur_page_num = pages_.size();
    auto iter = pages_.begin();
    while(iter != pages_.end())
    {        
        HwsFdWritePage* page = *iter;
        if (HWS_FD_WRITE_STATE_ERROR == page->state_ )
        {
            err_type_ = page->err_type_;
            bWritePageErr = true;

            S3FS_PRN_ERR("page err, path(%s)ref(%d)offset(%ld)bytes(%lu),pageNum(%u),lastErrType(%s)",
                page->path_.c_str(), page->refs_, page->offset_,page->bytes_,cur_page_num,print_err_type(err_type_));
        }
        if (page->state_ != HWS_FD_WRITE_STATE_FINISH && page->state_ != HWS_FD_WRITE_STATE_ERROR)
        {
            iter++;
            continue;
        }
        if (!page->CanRelease())
        {
            S3FS_PRN_WARN("try to release page but ref error, path(%s)ref(%d)offset(%ld)bytes(%lu),pageNum(%u)",
                        page->path_.c_str(), page->refs_, page->offset_,page->bytes_,cur_page_num);
            iter++;
            continue;
        }

        S3FS_PRN_INFO("free write page,path(%s)offset(%ld)bytes(%lu),pageNum(%u)",
            page->path_.c_str(), page->offset_,page->bytes_,cur_page_num);
        
        iter = pages_.erase(iter);
        delete page;
        HwsFdManager::GetInstance().SubCacheMemSize(write_page_size_);
    }
    return;
}

void HwsFdWritePageList::WaitAllPageFinish(std::unique_lock<std::mutex>& lock)
{
    for(auto iter = pages_.begin(); iter != pages_.end(); iter++)
    {
        HwsFdWritePage* page = *iter;
        wait_write_task_finish(page,lock);
    }
}

/*get overlap page;
  must lock write_merge_mutex_ in caller*/
HwsFdWritePage* HwsFdWritePageList::OverlapPage(off64_t offset, size_t size)
{
    for(auto iter = pages_.begin(); iter != pages_.end(); iter++)
    {
        HwsFdWritePage* page = *iter;
        if((page->state_ != HWS_FD_WRITE_STATE_FINISH) &&
           (page->state_ != HWS_FD_WRITE_STATE_ERROR) &&
           (offset < page->offset_ + (off64_t)page->write_page_size_) &&
           (page->offset_ < offset + (off64_t)size))
        {
            return page;
        }
    }
    return nullptr;
}
/*if page list overlap with offset;  
  must lock write_merge_mutex_ in caller*/
bool HwsFdWritePageList::IsOverlap(off64_t offset, size_t size)
{
    for(auto iter = pages_.begin(); iter != pages_.end(); iter++)
    {
        HwsFdWritePage* page = *iter;
        if((page->state_ != HWS_FD_WRITE_STATE_FINISH) &&
           (page->state_ != HWS_FD_WRITE_STATE_ERROR) &&
           (offset < page->offset_ + (off64_t)page->write_page_size_) &&
           (page->offset_ < offset + (off64_t)size))
        {
            return true;
        }
    }
    return false;
}
/*get overlap page exclude last page
must lock write_merge_mutex_ in caller*/
HwsFdWritePage* HwsFdWritePageList::OverlapExcludeLastPage(off64_t offset, size_t size)
{
	HwsFdWritePage* lastPage = nullptr;
    if (0 == pages_.size())
    {
        return nullptr;        
    }
	lastPage = pages_.back();
	for(auto iter = pages_.begin(); iter != pages_.end(); iter++)
    {
        HwsFdWritePage* page = *iter;
		if (page->offset_ == lastPage->offset_)
		{
			if((page->state_ != HWS_FD_WRITE_STATE_FINISH) &&
           (page->state_ != HWS_FD_WRITE_STATE_ERROR) &&
           (offset >= page->offset_))
			{
				/*if only right part overlap last page,flush last page;
                  if read extent front part overlap last,not flush last page*/
				continue;
			}
		}
		
        if((page->state_ != HWS_FD_WRITE_STATE_FINISH) &&
           (page->state_ != HWS_FD_WRITE_STATE_ERROR) &&
           (offset < page->offset_ + (off64_t)page->write_page_size_) &&
           (page->offset_ < offset + (off64_t)size))
        {
            return page;
        }
    }

	return nullptr;
}
/*if intersect write merge to last page  
  must lock write_merge_mutex_ in caller*/
bool HwsFdWritePageList::canIntersectWrtMergeToLastPage(
        const char* path,off64_t offset, size_t size)
{
    if (!g_bIntersectWriteMerge)  //not support intersect write merge
    {
        return false;
    }
    if (GetCurWrtPageNum() == 0)
	{
        // no write page,can copy
        return true;
	}
    bool canMerge = false;
	HwsFdWritePage* page = pages_.back();
	if ((page->state_ == HWS_FD_WRITE_STATE_WAIT) &&
        (offset <= page->offset_ + (off64_t)page->bytes_) &&
        (offset >= page->offset_))
	{
        //intersect with last page,
        //or sequential with last page(offset equal lastpage offset_ + bytes_)
        canMerge = true;
    }
    S3FS_PRN_INFO("canMerge=%d,path(%s)offset(%ld)size(%lu),pageOff(%lu),pageBytes(%lu)", 
        canMerge,path, offset,size,page->offset_, page->bytes_);
    return canMerge;
}
/*read overlap from last write page,
  must lock write_merge_mutex_ in caller;
  Return Value: read success size */
int HwsFdWritePageList::ReadLastOverLap(const char* path,
        HwsWriteLastPageRead_S *readInfo, size_t offset, size_t readSize)
{
	if (0 == pages_.size())
	{
        return 0;
	}
    readInfo->read_bytes_ = 0;
	readInfo->file_offset_ = offset;
	HwsFdWritePage* page = pages_.back();
	if ((page->state_ != HWS_FD_WRITE_STATE_FINISH) &&
        (page->state_ != HWS_FD_WRITE_STATE_ERROR) &&
        (readInfo->file_offset_ < page->offset_ + (off64_t)page->bytes_) &&
        (readInfo->file_offset_ >= page->offset_))
	{
		size_t copy_offset_in_page = readInfo->file_offset_ - page->offset_;
		readInfo->read_bytes_ = min(readSize, page->bytes_ - copy_offset_in_page);
		if (readInfo->read_bytes_ <= gFuseMaxReadSize)
		{
			memcpy(readInfo->buffer_, page->data_ + copy_offset_in_page, readInfo->read_bytes_);
		}
		else
		{
            S3FS_PRN_ERR("buff size too small!path(%s) offset(%ld) size(%lu)", 
                path, offset,readSize);
			readInfo->read_bytes_ = 0;
            return -EIO;
		}
        S3FS_PRN_INFO("read last write page!path(%s)offset(%ld)size(%lu),copySize(%lu)", 
            path, offset,readSize,readInfo->read_bytes_);
	}
    return readInfo->read_bytes_;
}
/*if write page list full,
  must lock write_merge_mutex_ in caller*/
bool HwsFdWritePageList::IsFull()
{
    unsigned int max_page_num = HwsFdManager::GetInstance().GetWritePageNumByCacheSize();

    if (0 == max_page_num) 
    {
        return true;
    }
    else 
    {    
        return pages_.size() >= max_page_num && pages_.back()->state_ > HWS_FD_WRITE_STATE_WAIT;
    }
}
/*get page list size,
  must lock write_merge_mutex_ in caller*/
unsigned int HwsFdWritePageList::GetCurWrtPageNum()
{
    return pages_.size();
}
HwsFdWritePageList::~HwsFdWritePageList()
{
    auto iter = pages_.begin();
    while(iter != pages_.end())
    {
        HwsFdWritePage* page = *iter;
        iter = pages_.erase(iter);
        delete page;
        HwsFdManager::GetInstance().SubCacheMemSize(write_page_size_);
    }
}

void HwsFdReadPage::GenerateCrc(ssize_t read_size)
{
    char* data = data_;
    if(!gIsCheckCRC)
    {
        return;
    }
    ssize_t crc_num = (read_size + HWS_READ_CRC_SIZE -1)/HWS_READ_CRC_SIZE;
    for(int i = 0; i < crc_num; i++, data+=HWS_READ_CRC_SIZE)
    {
        crc_[i] = rocksdb::crc32c::Value(data, HWS_READ_CRC_SIZE);
    }
}

bool HwsFdReadPage::CheckCrcErr(size_t offset, size_t size)
{
    if(!gIsCheckCRC)
    {
        return false;
    }
    if(size == 0)
    {
        return false;
    }
    int start_crc_no = offset/HWS_READ_CRC_SIZE;
    int end_crc_no = (offset + size - 1)/HWS_READ_CRC_SIZE;
    int real_crc_num = (read_size_ + HWS_READ_CRC_SIZE - 1)/HWS_READ_CRC_SIZE;
    for(int i = start_crc_no; i <= end_crc_no && i < real_crc_num; i++)
    {
        char *data = data_+HWS_READ_CRC_SIZE*i;
        if(crc_[i] != rocksdb::crc32c::Value(data, HWS_READ_CRC_SIZE))
        {
            return true;
        }
    }
    return false;
}

void HwsFdReadPage::ReadPageUnref()
{
    if (refs_ > 0)
    {
        refs_--;
        return;
    }
    S3FS_PRN_ERR("read page ref err! path(%s)pageoff(%ld)",
            path_.c_str(),offset_);
    refs_ = 0;
}

long HwsFdReadStatVec::GetReadStatDiffThreshold()
{
    long diffThresholdMs = gReadStatDiffLongMs;
    
    off64_t freeCacheSize = HwsFdManager::GetInstance().GetFreeCacheMemSize();
    //if free size less than 20%,use short diff
    if (freeCacheSize < gMaxCacheMemSize * gFreeCacheThresholdPercent / 100)
    {
        diffThresholdMs = gReadStatDiffShortMs;
    }
    return diffThresholdMs;
}
void HwsFdReadStatVec::Add(const char* path,
                        off64_t offset, size_t size)
{
    bool isSequentialOld = isSequential_;
    struct timespec now_ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);

    if(stat_.size() == 0)
    {
        clock_gettime(CLOCK_MONOTONIC_COARSE, &first_ts_);
        memcpy(&last_ts_,&first_ts_,sizeof(struct timespec));            
    }
    else
    {
        long lastToNowDiffMs = diff_in_ms(&last_ts_, &now_ts);    
        if (lastToNowDiffMs > gReadStatDiffLongMs)
        {
            //too long,clear stat queue and set set first_ts
            offset_ = 0;
            stat_.clear();
            clock_gettime(CLOCK_MONOTONIC_COARSE, &first_ts_);
            S3FS_DATA_CACHE_PRN_LOG(
                "too long,lastToNowDiffMs(%ld)path(%s)offset(%ld)",
                lastToNowDiffMs,path,offset);        
        }
    }
    clock_gettime(CLOCK_MONOTONIC_COARSE, &last_ts_);
    HwsFdReadStat stat;
    stat.offset = offset;
    stat.size = size;
    stat_.push_back(stat);
    if((int)stat_.size() < gReadStatSize)
    {
        return;
    }

    firstToLastDiffMs = diff_in_ms(&first_ts_, &last_ts_);    
    long diffThresholdMs = GetReadStatDiffThreshold();    
    off64_t freeCacheSize = HwsFdManager::GetInstance().GetFreeCacheMemSize();
    if(firstToLastDiffMs > diffThresholdMs)
    {
        //read too slow, don't read ahead
        isSequential_ = false;
        offset_ = 0;
        stat_.clear();
        S3FS_DATA_CACHE_PRN_LOG(
            "too slow not read ahead,path(%s)diff(%ld)threshold(%ld)freecache(%ld)"
            "lastRead(%ld)",
            path,firstToLastDiffMs,diffThresholdMs,freeCacheSize,offset);        
        return;
    }
    std::sort(stat_.begin(), stat_.end(),
          [] (HwsFdReadStat const& a, HwsFdReadStat const& b) { return a.offset < b.offset; });
    isSequential_ = false;
    offset_ = 0;
    //for [8,31],[7,30]...[0,23] to find the last 24 sequential read in 
    // a range in gReadStatSizeThreshold(4MB)
    int seq_size_ = gReadStatSeqSize;
    for(int loop = gReadStatSize - seq_size_; loop >= 0; loop--)
    {
        if(stat_[loop+seq_size_-1].offset - stat_[loop].offset <= gReadStatSizeThreshold)
        {
            isSequential_ = true;
            offset_ = stat_[loop+seq_size_-1].offset + stat_[loop+seq_size_-1].size;
            break;
        }
    }
    if (isSequentialOld != isSequential_)
    {
        S3FS_DATA_CACHE_PRN_LOG(
          "change isSequential=%d,path(%s)readAheadOff(%ld)",
        isSequential_,path,offset_);        
    }
    stat_.clear();
}
/*must lock read_ahead_mutex_ in caller*/
off64_t HwsFdReadPageList::GetFirstPageOffset() 
{
    unsigned int curPageNum = pages_.size();
    if (0 == curPageNum)
    {
        return 0;
    }
    off64_t firstPageOffset = pages_[0]->offset_;
    return firstPageOffset;
}
/*must lock read_ahead_mutex_ in caller*/
off64_t HwsFdReadPageList::GetLastPageEnd() 
{
    unsigned int curPageNum = pages_.size();
    if (0 == curPageNum)
    {
        return 0;
    }
    HwsFdReadPage* lastPage = pages_.back();
    off64_t lastPageEnd = lastPage->offset_ + lastPage->read_page_size_;
    return lastPageEnd;
}
/*must lock read_ahead_mutex_ in caller*/
bool HwsFdReadPageList::CheckHitMaxOffset() 
{
    off64_t firstPageOffset = GetFirstPageOffset();
    off64_t lastPageEnd = GetLastPageEnd();
    if (hit_max_offset_ < firstPageOffset)
    {
        return false;        
    }
    else if (hit_max_offset_ > lastPageEnd)
    {
        return false;        
    }
    else
    {
        return true;
    }
}
/*read overlap from page list;  
  must lock read_ahead_mutex_ in caller*/
int HwsFdReadPageList::Read(const char* path,char *buf, off64_t offset, size_t size)
{
    off64_t copy_offset = offset;
    size_t copy_left_size = size;   /*size not copy*/
    size_t copied_size = 0;
    size_t copy_offset_in_page = 0;
    size_t page_copy_size =0;
    unsigned int hit_page_num = 0;
    unsigned int cur_page_num = pages_.size();
    off64_t hit_file_offset = 0;
    for(auto iter = pages_.begin(); iter != pages_.end() && copy_left_size > 0; iter++)
    {
        HwsFdReadPage* page = *iter;
        if (HWS_FD_READ_STATE_RECVED != page->state_)
        {
            continue;
        }
        off64_t page_end = page->offset_ + page->read_size_;
        /*left part must in page; if only right part in page,read from obs*/
        if((copy_offset >= page->offset_) && (copy_offset < page_end))
        {
            copy_offset_in_page = copy_offset - page->offset_;
            //page not sequential,break
            if (copied_size > 0 && 0 != copy_offset_in_page)
            {
                S3FS_PRN_WARN("page not sequential path(%s)offset(%ld)"
                    "size(%lu)pageoff(%ld)copysize(%lu)", 
                    path, offset, size,page->offset_,copied_size);
                break;
            }
            page_copy_size = min(copy_left_size, page->read_size_ - copy_offset_in_page);
            if(page->CheckCrcErr(copy_offset_in_page, page_copy_size))
            {
                S3FS_PRN_ERR("hws fd cache read crc error path(%s) offset(%ld) size(%lu) read(%lu, %lu)", 
                    path, page->offset_, page->read_page_size_, copy_offset_in_page, page_copy_size);
                return 0;
            }
            memcpy(buf + copied_size, page->data_ + copy_offset_in_page, page_copy_size);
            copy_offset += page_copy_size;
            copied_size += page_copy_size;
            copy_left_size -= page_copy_size;
            hit_page_num++;
            hit_file_offset = page->offset_ + copy_offset_in_page + page_copy_size;
            page->hitSizeInPage += page_copy_size;
            if(hit_file_offset > hit_max_offset_)
            {
                SetHitMaxOffset(hit_file_offset);
            }
            clock_gettime(CLOCK_MONOTONIC_COARSE, &page->last_hit_ts_);            
        }
    }
    off64_t firstPageOffset = GetFirstPageOffset();
    if (copied_size == size)
    {
        S3FS_DATA_CACHE_PRN_LOG("read all hit,path(%s) offset(%ld) size(%lu)"
            "copied(%lu)lastCp(%lu)hitPg(%u)pgNum(%u)hitoff(%ld)firOff(%ld)ttlHit(%u)", 
            path,offset,size,copied_size,page_copy_size,hit_page_num,cur_page_num,
            hit_file_offset,firstPageOffset,total_hit_size); 
        g_ReadAllHitNum++;
    }
    else if (0 == copied_size)
    {
        unsigned int max_page_num = CalcMaxReadPageNum();
        S3FS_DATA_CACHE_PRN_LOG("read page not hit,path(%s)offset(%ld) size(%lu)"
            "pgNum(%u)firOff(%ld)maxPg(%u)ttlHit(%u)", 
            path,offset,size,cur_page_num,firstPageOffset,max_page_num,total_hit_size);      
        g_ReadNotHitNum++;
    }
    else
    {
        S3FS_DATA_CACHE_PRN_LOG("read page part hit,path(%s) offset(%ld) size(%lu)"
            "copy_left_size(%lu) copied_size(%lu) hitPage(%u)pageNum(%u)", 
            path,offset,size,copy_left_size,copied_size,hit_page_num,cur_page_num);      
    }
    total_hit_size += copied_size;
    lastReadOffset = offset;
    return copied_size;
}
bool HwsFdReadPageList::PageNeedFree(HwsFdReadPage* page)
{
    unsigned int cur_page_num = pages_.size();
    off64_t cleanPageOffset = 0;
    /*if hit_max_offset_ beyond page end + 1MB,
      then set cleanOffset to pageOff + 1MB*/
    off64_t pageEnd = 
      (off64_t)(page->offset_ + read_page_size_);
    if (hit_max_offset_ > (pageEnd + HWS_MB_SIZE))
    {
        cleanPageOffset = (off64_t)(page->offset_ + HWS_MB_SIZE);
    }
    
    if (!page->is_read_finish_)
    {
        S3FS_DATA_CACHE_PRN_LOG(
            "break because of not read_finish,path(%s)off(%ld)state(%d)",
            page->path_.c_str(),page->offset_, page->state_);
        return false;
    }
    
    if(page->state_ == HWS_FD_READ_STATE_RECVING)
    {
        S3FS_DATA_CACHE_PRN_LOG(
            "break because of READ_STATE_RECVING,path(%s)off(%ld)",
            page->path_.c_str(),page->offset_);
        return false;
    }
    if (!page->CanRelease())
    {
        S3FS_PRN_WARN(
            "page ref not zero,path(%s)ref(%d)off(%ld)bytes(%lu),pageNum(%u)clnOff(%ld)state_(%d)",
            page->path_.c_str(), page->refs_, page->offset_,page->read_size_,cur_page_num,
            cleanPageOffset,page->state_);
        return false;
    }
    //HWS_FD_READ_STATE_DISABLED/HWS_FD_READ_STATE_ERROR return true       
    if(page->state_ == HWS_FD_READ_STATE_DISABLED || 
        page->state_ == HWS_FD_READ_STATE_ERROR)
    {
        S3FS_PRN_WARN("free disabled or error page,path(%s)off(%ld)state(%d)",
            page->path_.c_str(),page->offset_, page->state_);
        return true;
    }
    //now state must be RECVED
    if(page->state_ != HWS_FD_READ_STATE_RECVED)
    {
        S3FS_PRN_ERR("free invalid state page,path(%s)off(%ld)state(%d)",
            page->path_.c_str(),page->offset_, page->state_);
        return true;
    }      
    //if 90% page is hit,and last read offset beyond page end,delete
    off64_t dataSize = 
        (page->read_size_ > 0)? page->read_size_:gReadPageSize;
    off64_t hitPercent = page->hitSizeInPage * 100 / dataSize;
    if (hitPercent > 90 && lastReadOffset > (pageEnd + HWS_MB_SIZE))
    {
        S3FS_DATA_CACHE_PRN_LOG(
          "free read page,path(%s)off(%ld)hitPer(%ld),lastRead(%ld)pgEnd(%ld)", 
            page->path_.c_str(), page->offset_,hitPercent,lastReadOffset,pageEnd);
        return true;
    }
    /*1¡¢if page timeout£¬delete
      2¡¢if page offset less than cleanPageOffset£¬then delete*/ 
    struct timespec now_ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);
    long diffMsRecved = diff_in_ms(&page->got_ts_, &now_ts);
    long diffMsLastHit = diff_in_ms(&page->last_hit_ts_, &now_ts);
    long diffMs = min(diffMsRecved,diffMsLastHit);
    if(diffMs <= gReadPageCleanMs &&
       (page->offset_ > cleanPageOffset))
    {
        S3FS_PRN_INFO(
            "break,STATE_RECVED,path(%s)diffMs(%ld)threshold(%ld)off(%ld)clnOff(%ld)hitMax(%ld)", 
            page->path_.c_str(), diffMs,gReadPageCleanMs,page->offset_, 
            cleanPageOffset,hit_max_offset_);
        return false;
    }
    S3FS_DATA_CACHE_PRN_LOG(
      "free read page,path(%s)off(%ld)bytes(%lu),pageNum(%u)clnOff(%ld)"
      "diffRecv(%ld)diffLast(%ld)thres(%ld)hitMax(%ld)hitSz(%ld)", 
        page->path_.c_str(), page->offset_,page->read_size_,cur_page_num,
        cleanPageOffset,diffMsRecved,diffMsLastHit,gReadPageCleanMs,
        hit_max_offset_,page->hitSizeInPage);
    return true;
}
/*clean timeout received page/disabled page/error page;  
  must lock read_ahead_mutex_ in caller*/
void HwsFdReadPageList::Clean()
{    
    unsigned int cur_page_num = pages_.size();
    if (0 == cur_page_num)
    {
        S3FS_PRN_INFO("cur_page_num = 0");
        return;
    }
    HwsFdReadPage* first_page = *(pages_.begin());
    struct timespec now_ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);
    //if last clean time to now less than 5 ms,return
    if (diff_in_ms(&lastCleanTs, &now_ts) < 5)
    {
        if (can_pint_log_with_fc(__LINE__))
        {
            S3FS_PRN_WARN("last clean to now time short and return,path(%s)", 
            first_page->path_.c_str());
        }
        S3FS_PRN_DBG("last clean to now time short and return,path(%s)", 
            first_page->path_.c_str());
        return;
    }
    clock_gettime(CLOCK_MONOTONIC_COARSE, &lastCleanTs);
    
    if (!CheckHitMaxOffset())
    {        
        S3FS_PRN_INFO("hit max invalid,path(%s)hitmax(%ld)pageNum(%u)firstOff(%ld)lastEnd(%ld)", 
            first_page->path_.c_str(),hit_max_offset_,cur_page_num,
            GetFirstPageOffset(),GetLastPageEnd());
        /*adjust hit_max_offset_*/
        hit_max_offset_ = GetFirstPageOffset();
        return;
    }
    auto iter = pages_.begin();
    while(iter != pages_.end())
    {        
        HwsFdReadPage* page = *iter;
        //print log in pageNeedFree
        if (PageNeedFree(page))
        {
            iter = pages_.erase(iter);
            delete page;
            HwsFdManager::GetInstance().SubCacheMemSize(read_page_size_);
        }
        else
        {
            //if first page can not free,break
            break;
        }
    }
}
/*read ahead one page 
  must lock read_ahead_mutex_ in caller*/
void HwsFdReadPageList::ReadAheadOnePage(const char* path, off64_t offset, 
            HwsFdWritePageList& writePage,off64_t fileSize)
{
    off64_t readAheadOffset = offset;
    off64_t nextPageOffset = -1;
    
    if (pages_.size() > 0)
    {
        struct HwsFdReadPage *last = pages_.back();
        nextPageOffset = last->offset_ + last->read_page_size_;
        readAheadOffset = nextPageOffset;
        if(offset > readAheadOffset)
        {
            readAheadOffset = offset;
        }
    }
    if(writePage.IsOverlap(readAheadOffset, read_page_size_))
    {
        if (can_pint_log_with_fc(__LINE__))
        {
            S3FS_PRN_WARN("read ahead overlap write,path(%s)ahead off(%ld)",
                path, readAheadOffset);
        }
        return;
    }
    if (readAheadOffset > fileSize)
    {
        S3FS_PRN_DBG("read ahead offset beyond filesize,path(%s)"
            "ahead off(%ld),filesize(%ld)",
            path, readAheadOffset,fileSize);
        read_ahead_beyond_filesize_num++;
        return;        
    }
    struct HwsFdReadPage *newPage = new(std::nothrow) HwsFdReadPage(path,
        readAheadOffset,read_page_size_,read_ahead_mutex_, read_thread_task);
    if (nullptr == newPage)
    {
        S3FS_PRN_ERR("alloc read page fail!path(%s) offset(%ld)", 
            path, offset);                
        return;
    }
    HwsFdManager::GetInstance().AddCacheMemSize(read_page_size_);
    pages_.push_back(newPage);
    S3FS_DATA_CACHE_PRN_LOG("read ahead one page,path(%s)off(%ld)"
        "readbeyond(%u),hitsize(%u)firToLast(%ld)inputOff(%ld)nextOff(%ld)",
        path, readAheadOffset,read_ahead_beyond_filesize_num,total_hit_size,
        getFirstToLastDiffMs(),offset,nextPageOffset);
}
/*calc max page num by total read ahead page num and cache size*/
unsigned int HwsFdReadPageList::CalcMaxReadPageNum()
{
    /*if total_hit_size is 0,only read ahead 1 page,
      if hit 4MB then read ahead 2 page,
      max_page_num increase with total_hit_size*/
    unsigned int max_page_from_hitsize = 
        1+ ((total_hit_size + 2 * HWS_MB_SIZE) / read_page_size_);
    unsigned int max_page_num = min(max_page_from_hitsize,
        HwsFdManager::GetInstance().GetReadPageNumByCacheSize());
    if (getFirstToLastDiffMs() > 1000)
    {
        //if read slow,max page num not beyond 2
        unsigned int max_page_slow = 2;
        max_page_num = min(max_page_num,max_page_slow);
    }
    return max_page_num;
}

/*read ahead multi page 
  must lock read_ahead_mutex_ in caller*/
void HwsFdReadPageList::ReadAhead(const char* path, off64_t offset, 
        HwsFdWritePageList& writePage,off64_t fileSize)
{        
    HwsFdReadPage* last_page = nullptr;
    /*calc max page num by total read ahead page num and cache size*/
    unsigned int max_page_num = CalcMaxReadPageNum();
    unsigned int cur_page_num = pages_.size();
    
    //print log when max page num equal with config min
    if (max_page_num <= g_MinReadWritePageByCacheSize && can_pint_log_with_fc(__LINE__))
    {
        S3FS_PRN_WARN("max page num small!path(%s)max_page_num(%u),usedCacheSize(%ld)", 
            path,max_page_num,HwsFdManager::GetInstance().GetUsedCacheMemSize());                
    }
    //page full
    if(cur_page_num >= max_page_num)
    {
        S3FS_PRN_DBG("reach max page,path(%s)curPage(%u)maxPage(%u)",
            path,cur_page_num,max_page_num);
        return;
    }
    unsigned int new_page_num = max_page_num - pages_.size();
    S3FS_PRN_INFO("read ahead,path(%s)new_page_num(%u)",
        path, new_page_num);
    if (new_page_num > max_page_num)
    {
        S3FS_PRN_ERR("path(%s),invalid new_page_num=%u,max_page_num(%u)",
            path,new_page_num,max_page_num);        
        return;
    }
    if (pages_.size() > 0)
    {
        last_page = pages_.back();
        /*read to file end,no need read ahead*/
        if(last_page->read_size_ < last_page->read_page_size_
            && HWS_FD_READ_STATE_RECVED == last_page->state_)
        {
            S3FS_PRN_INFO("read ahead to end,path(%s)PageNum(%lu)off(%ld)readsize(%lu)",
                path, pages_.size(),last_page->offset_,last_page->read_size_);
            return;            
        }
    }
    /*read ahead new_page_num*/
    for(unsigned int i = 0; i < new_page_num; i++)
    {        
        ReadAheadOnePage(path,offset,writePage,fileSize);
    }
    
    return;
}
/*disable overlap page with write extent;  
  must lock read_ahead_mutex_ in caller*/
void HwsFdReadPageList::Invalid(off64_t offset, size_t size)
{
    off64_t end = offset + size;
    for(auto iter = pages_.begin(); iter != pages_.end(); iter++)
    {
        HwsFdReadPage* page = *iter;
        if(offset < page->offset_ + (off64_t)page->read_page_size_ && page->offset_ < end)
        {
            page->state_ = HWS_FD_READ_STATE_DISABLED;
        }
    }
    return;
}
/*get overlap page;  
  must lock read_ahead_mutex_ in caller*/
HwsFdReadPage* HwsFdReadPageList::OverlapPage(off64_t offset, size_t size){
    for(auto iter = pages_.begin(); iter != pages_.end(); iter++)
    {
        HwsFdReadPage* page = *iter;
        if((page->state_ == HWS_FD_READ_STATE_RECVING) &&
           (offset < page->offset_ + (off64_t)page->read_page_size_) &&
           (page->offset_ < offset + (off64_t)size))
        {
            return page;
        }
    }
    return nullptr;
}
/*get page list size;  
  must lock read_ahead_mutex_ in caller*/
unsigned int HwsFdReadPageList::GetCurReadPageNum()
{
    return pages_.size();
}
HwsFdReadPageList::~HwsFdReadPageList()
{
    auto iter = pages_.begin();
    while(iter != pages_.end())
    {
        HwsFdReadPage* page = *iter;
        iter = pages_.erase(iter);
        delete page;
        HwsFdManager::GetInstance().SubCacheMemSize(read_page_size_);
    }
}

//must lock entity_mutex_ in caller
void HwsFdEntity::StartWholeFilePreRead(off64_t offset, size_t size)
{
    off64_t  cur_cache_size = g_wholeCacheTotalMemSize.load();
    off64_t freeCacheSize = HwsFdManager::GetInstance().GetFreeCacheMemSize();
    if ((false == g_wholeFileCacheSwitch) || (true == wholeCacheTaskStarted_)
        || ((cur_cache_size + fileSize_) > g_wholeFileCacheMaxMemSize)
        || (freeCacheSize * 2 < gMaxCacheMemSize))
    {
        if ((true == g_wholeFileCacheSwitch) && (false == wholeCacheTaskStarted_))
        {
            S3FS_PRN_INFO("cache memory is not enough," 
            "freeCache(%ld)maxCache(%ld)cur_cache_size(%ld)fileSize(%ld)maxWholeMem(%ld)",
            freeCacheSize, gMaxCacheMemSize, cur_cache_size, fileSize_, g_wholeFileCacheMaxMemSize);
        }
        return;
    }

    // record the time first not hit in pre-read
    if (wholeCacheStat_.notHitSize == 0)
    {
        clock_gettime(CLOCK_MONOTONIC_COARSE, &wholeCacheStat_.first_ts);
    }
    wholeCacheStat_.notHitSize += size;
    wholeCacheStat_.notHitTimes++;

    struct timespec now_ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);

    long firstToNowDiffMs = diff_in_ms(&wholeCacheStat_.first_ts, &now_ts);
    //not hit read size reach 5MB or not hit times reach 300 times during 5 seconds
    if ((firstToNowDiffMs > g_wholeFileCachePreReadStatisPeriod * 1000)
        && ((wholeCacheStat_.notHitSize > g_wholeFileCacheNotHitSizePerPeriod)
            || wholeCacheStat_.notHitTimes > g_wholeFileCacheNotHitTimesPerPeriod))
    {
        wholeCacheTaskStarted_ = true;
        Ref(); // inc ref here and dec ref in whole_file_cache_task
        S3FS_PRN_WARN("start whole_file_cache_task, inodeNo(%lld)offset(%ld)size(%lu)cur_cache_size(%ld)",
                            inodeNo_, offset, size, cur_cache_size);
        HwsFdManager::GetInstance().CreateWholeFileCacheTask(inodeNo_);
    }
    else if (firstToNowDiffMs > 5000) //clear the not hit statis
    {
        ClearWholeReadCacheStatis();
    }
}

//must lock entity_mutex_ in caller
int HwsFdEntity::ReadFromWholeFileCache(const char* path, char* buf, off64_t offset, size_t size)
{
    if ((false == g_wholeFileCacheSwitch) || (false == wholeCacheTaskStarted_))
    {
        return 0;
    }

    off64_t copy_offset = offset;
    size_t copy_left_size = size;   /*size not copy*/
    size_t copied_size = 0;
    size_t copy_offset_in_page = 0;
    size_t page_copy_size =0;

    std::deque<HwsFdReadPage*>& pages = wholeCachePage_.pages_;
    unsigned int cur_page_num = pages.size();
    for(auto iter = pages.begin(); iter != pages.end() && copy_left_size > 0; iter++)
    {
        HwsFdReadPage* page = *iter;
        if (HWS_FD_READ_STATE_RECVED != page->state_)
        {
            continue;
        }
        page->ReadPageRef();
        off64_t page_end = page->offset_ + page->read_size_;
        /*left part must in page; if only right part in page,read from obs*/
        if((copy_offset >= page->offset_) && (copy_offset < page_end))
        {
            copy_offset_in_page = copy_offset - page->offset_;
            //page not sequential,break
            if (copied_size > 0 && 0 != copy_offset_in_page)
            {
                S3FS_DATA_CACHE_PRN_LOG("page not sequential path(%s)offset(%ld)"
                    "size(%lu)pageoff(%ld)copysize(%lu)", 
                    path, offset, size,page->offset_,copied_size);
                page->ReadPageUnref();
                break;
            }
            page_copy_size = min(copy_left_size, page->read_size_ - copy_offset_in_page);
            memcpy(buf + copied_size, page->data_ + copy_offset_in_page, page_copy_size);
            copy_offset += page_copy_size;
            copied_size += page_copy_size;
            copy_left_size -= page_copy_size;
        }
        page->ReadPageUnref();
    }

    if (copied_size == size)
    {
        S3FS_DATA_CACHE_PRN_LOG("read all hit,path(%s)offset(%ld)size(%lu)copied(%lu)lastCp(%lu)pgNum(%u)", 
            path,offset,size,copied_size,page_copy_size,cur_page_num); 
    }
    else
    {
        S3FS_DATA_CACHE_PRN_LOG("read page not all hit,path(%s) offset(%ld) size(%lu)"
            "copy_left_size(%lu)copied_size(%lu)pgNum(%u)", 
            path,offset,size,copy_left_size,copied_size,cur_page_num);
        copied_size = 0;
    }
    return copied_size;
}

//lock entity_mutex_ in caller
void HwsFdEntity::CheckPreReadConflict(off64_t offset, size_t size)
{
    if (true == wholeCacheTaskStarted_)
    {
        wholeCacheWriteConflict_ = true;
        S3FS_PRN_WARN("set conflict for inode(%lld),offset(%ld)size(%lu)", 
            inodeNo_, offset,size); 
    }
}

/*append data to write page,if full delay and retry;  
  must lock read_ahead_mutex_ in caller*/
size_t HwsFdEntity::AppendToPageWithRetry(const char *path, 
    const char *buf, off64_t offset, size_t size,
    std::unique_lock<std::mutex>& entity_lock)
{
    size_t res = 0;
    struct timespec now_ts;
    struct timespec begin_ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &begin_ts);

    if (nullptr == buf)
    {
        S3FS_PRN_ERR("write buf para is null!path(%s) offset(%ld)", 
            path, offset);                
    }
    else if (0 == size)
    {
        S3FS_PRN_WARN("writePage size if zero!path(%s) offset(%ld)", 
            path, offset);                
        return 0;
    }
    
    bool bIsSequential = true;
    do 
    {
        res = writePage_.Append(path_.c_str(), buf, offset, size,&bIsSequential);
        if (res == 0)   /*page full*/
        {
            if (bIsSequential == false)  //not retry for intersect write merge           
            {
                break;
            }
            entity_lock.unlock();
            retryNum++;
            if (can_pint_log_with_fc(__LINE__))
            {
                S3FS_PRN_WARN("writePage full,retry!path(%s)off(%ld),retry(%ld)"
                    "usedCacheSize(%ld)", 
                    path, offset,retryNum,HwsFdManager::GetInstance().GetUsedCacheMemSize());                
            }
            /*delay 40 ms and retry*/
            std::this_thread::sleep_for(std::chrono::milliseconds(HWS_ALLOC_MEM_RETRY_MS));    
            /*must not process service between unlock and lock,
              ensure entity_lock must in lock state when return*/
            entity_lock.lock();

            clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);
            if (diff_in_ms(&begin_ts, &now_ts) > HWS_MAX_WRITE_RETRY_MS)
            {
                g_AllocWritePageTimeoutNum++;
                S3FS_PRN_WARN("writePage retry too long!path(%s)off(%ld)"
                    "allocTimeoutNum(%u)", 
                    path, offset,g_AllocWritePageTimeoutNum);                  
                break;
            }
            /*clean and read page check invalid when retry*/
            writePage_.Clean();
            readPage_.Invalid(offset, size);
            
            if(!IsNeedWriteMerge(path,offset, size, entity_lock))
            {
                S3FS_PRN_WARN("retry but no need merge!path(%s)off(%ld)", 
                    path, offset);                
                break;
            }
        }
    }while(res == 0);
    /*entity_lock must in lock state when return*/
    return res;
}
    
/*must lock write_merge_mutex_ in caller*/
bool HwsFdEntity::IsNeedWriteMerge(const char *path, 
    off64_t offset, size_t size, std::unique_lock<std::mutex>& lock)
{
    int oldWriteMode = curr_write_mode_;
    bool canIntersectMerge = 
            writePage_.canIntersectWrtMergeToLastPage(path,offset,size);
    // get current write mode
    if(IsAppend(offset, size))
    {
        curr_write_mode_ = APPEND;
    }
    else if(IsSequentialModify(offset, size))
    {
        curr_write_mode_ = MODIFY;
    }
    else if (canIntersectMerge)
    {
        curr_write_mode_ = MODIFY;
    }
    else
    {
        curr_write_mode_ = THROUGH;        
    }
    if (oldWriteMode != curr_write_mode_)
    {
        S3FS_PRN_INFO("write mode change,path(%s)off(%ld)oldMode(%d)newMode(%d)", 
            path,offset,oldWriteMode,curr_write_mode_);                  
    }

    if (canIntersectMerge)
    {
        //if can merge to last page,no need wait flush
        return true;
    }
    // if not Write-Through, then need decide whether to flush page.
    else if (THROUGH != curr_write_mode_)
    {
        if (!writePage_.IsSequential(offset))
        {
            writePage_.WaitAllPageFinish(lock);
        }
        return true;            
    }                        
    return false;
}

int HwsFdEntity::Write(const char* buf, off64_t offset, size_t size, const char* path)
{
    HwsFdWritePage* page = nullptr;
    int res = 0;
    /*statis*/
    HwsCacheStatis::GetInstance().AddStatisNum(TOTAL_WRITE);
    HwsCacheStatis::GetInstance().AddStatisLen(TOTAL_WRITE_LEN, 
        (unsigned int) size);
    
    std::unique_lock<std::mutex> entity_lock(entity_mutex_);
	if (offset > fileSize_)
	{
		S3FS_PRN_WARN("write offset bigger than file size(%ld),path(%s)offset(%ld)size(%lu)", 
            fileSize_, path_.c_str(), offset, size);
	}
	
    ChangeFilePathIfNeed(path);

    writePage_.Clean();
    readPage_.Invalid(offset, size);
    if (writePage_.IsWritePageErr())
    {
        S3FS_PRN_ERR("has write page error! path(%s)offset(%ld)size(%lu)lastErrType(%s)",
            path_.c_str(), offset, size, print_err_type(writePage_.GetWriteErrType()));
        return -EIO;
    }

    CheckPreReadConflict(offset, size);

    if(IsNeedWriteMerge(path_.c_str(),offset, size, entity_lock))
    {
        res = AppendToPageWithRetry(path_.c_str(),buf,offset,size,entity_lock);
        /*retry but not sequential,send to obs direct*/
        if(res < (int)size)
        {
            entity_lock.unlock();
            S3FS_PRN_ERR("write page retry error,path(%s)offset(%ld)size(%lu)res(%d)", 
                path_.c_str(), offset, size, res);
            res = sendto_obs(path_.c_str(), buf, size, offset);
            entity_lock.lock();
            UpdateFileSize(offset, res);
            return res;
        }
        S3FS_PRN_INFO("write merge succ,path(%s)offset(%ld)size(%lu)mode(%d)", 
            path_.c_str(), offset, size, GetWriteMode());
        
        UpdateFileSize(offset, res);
        HwsCacheStatis::GetInstance().AddStatisNum(MERGE_WRITE);

        return res;
    }

    while((page = writePage_.OverlapPage(offset, size))!= nullptr){
        wait_write_task_finish(page,entity_lock);
    }

    entity_lock.unlock();
    res = sendto_obs(path_.c_str(), buf, size, offset);
    entity_lock.lock();
    UpdateFileSize(offset, res);
    S3FS_PRN_INFO("hws fd cache write not append path(%s) offset(%ld) size(%lu)" 
                    "res(%d) filesize(%ld)", 
        path_.c_str(), offset, size, res, fileSize_);

    return res;
}

int HwsFdEntity::Flush()
{
    HwsFdWritePage* page = nullptr;
    std::unique_lock<std::mutex> lock(entity_mutex_);
    off64_t fileSize = fileSize_;

    while((page = writePage_.OverlapPage(0, fileSize))!= nullptr){
        wait_write_task_finish(page,lock);
    }

    return 0;
}
int HwsFdEntity::Read(char* buf, off64_t offset, size_t size, const char* path)
{
    int hit_read_size = 0;
    int lastWriteOverLapSize = 0;
    HwsFdWritePage* page = nullptr;
    /*1,init writePageLastReadStru and alloc buffer*/
    HwsWriteLastPageRead_S writePageLastReadStru;
    memset(&writePageLastReadStru, 0, sizeof(HwsWriteLastPageRead_S));
    /*alloc mem,if fail,retry until succeed*/
    writePageLastReadStru.buffer_ = (char *)AllocMemWithRetry(gFuseMaxReadSize);
    /*statis*/
    HwsCacheStatis::GetInstance().AddStatisNum(TOTAL_READ);
    HwsCacheStatis::GetInstance().AddStatisLen(TOTAL_READ_LEN, 
        (unsigned int) size);
    
    std::unique_lock<std::mutex> entity_lock(entity_mutex_);
    ChangeFilePathIfNeed(path);

    readStat_.Add(path_.c_str(),offset, size);
    readPage_.setFirstToLastDiffMs(readStat_.getFirstToLastDiffMs());
    /*2,for not break last page merge:
        if overlap with pageList exclude last,flush the overlap page;
        if only right part overlap last page,flush last page;
        if read extent front part overlap last,not flush last page*/
    while((page =writePage_.OverlapExcludeLastPage(offset, size)) != nullptr)
    {
        wait_write_task_finish(page,entity_lock);
    }
    
    /*3,read if overlap with last page*/
    lastWriteOverLapSize = writePage_.ReadLastOverLap(path_.c_str(),&writePageLastReadStru, offset, size);
    if (lastWriteOverLapSize < 0)
    {
        /*has print err in ReadLastOverLap,so return*/
        return lastWriteOverLapSize;
    }
    /*if read from last write page,read obs and return*/
    if (writePageLastReadStru.read_bytes_ > 0 )
    {
        /*read unlock,because not read from page*/
        entity_lock.unlock();
        
    	S3FS_PRN_DBG("hws fd cache read from write page sucess,path(%s),offset(%ld),size(%zu)", 
            path_.c_str(), offset, writePageLastReadStru.read_bytes_);
        HwsCacheStatis::GetInstance().AddStatisNum(READ_LAST_WRT_PAGE);
        /*read from obs*/
		size_t read_obs_size = 0;
		if (writePageLastReadStru.read_bytes_ < size)
		{
			read_obs_size = recvfrom_obs(path_.c_str(), buf, size, offset);
			S3FS_PRN_DBG("hws fd cache read from obs sucess,path(%s),offset(%ld),size(%zu)", 
                path_.c_str(),offset, read_obs_size);
		}
		
        /*copy writePageReadStru buf*/
		memcpy(buf, writePageLastReadStru.buffer_, writePageLastReadStru.read_bytes_);
		
        /*free writePageReadStru buf*/
        free(writePageLastReadStru.buffer_);
        return max(writePageLastReadStru.read_bytes_, read_obs_size);
    }
    /*free writePageReadStru buf*/
    free(writePageLastReadStru.buffer_);
    
    /*4,read page overlap£¬wait*/
    if(gIsReadWaitCache)
    {
        HwsFdReadPage* readPage = nullptr;
        while((readPage = readPage_.OverlapPage(offset, size))!= nullptr)
        {
            wait_read_task_finish(readPage,entity_lock);
			S3FS_PRN_INFO("wait read ahead finish,path(%s),offset(%ld),size(%zu)", 
                path_.c_str(),offset, size);
        }
    }

    /*5,read from readpage if hit; if part hit,only left part read from 
        page,right part read from obs*/
    hit_read_size = readPage_.Read(path_.c_str(), buf, offset, size);
    readPage_.Clean();
    if(readStat_.IsSequential())
    {
        readPage_.ReadAhead(path_.c_str(), readStat_.GetOffset(), 
                writePage_,fileSize_);
    }
    else
    {
        /*clear total read hit size when need not read ahead*/
        readPage_.ClearTotalHitSize();
    }
    if(hit_read_size == (int)size)
    {
        HwsCacheStatis::GetInstance().AddStatisNum(READ_HIT);
        return hit_read_size;
    }

    StartWholeFilePreRead(offset, size);

    /* 6, read from whole file cache */
    hit_read_size = ReadFromWholeFileCache(path_.c_str(), buf, offset, size);
    if (hit_read_size == (int)size)
    {
        HwsCacheStatis::GetInstance().AddStatisNum(READ_WHOLE_HIT);
        wholeCachePage_.cacheHitCnt++;
        return hit_read_size;
    }
    entity_lock.unlock();

    /*7,if  part hit,not just read left part,read all extent from obs,
         simplify process;   print info log in readPage_.Read,so not print*/
    return recvfrom_obs(path_.c_str(), buf, size, offset);
}

off64_t HwsFdEntity::GetFileSize(void)
{
    return fileSize_;
}
/*Clean write and read page,return write and read page used mem;
  lock entity in this function*/
unsigned int HwsFdEntity::CleanAndGetPageUsedMem()
{
    std::unique_lock<std::mutex> entity_lock(entity_mutex_);
    writePage_.Clean();
    readPage_.Clean();    

    unsigned int writePageMem = gWritePageSize * writePage_.GetCurWrtPageNum();
    unsigned int readPageMem = gReadPageSize * readPage_.GetCurReadPageNum();

    return (writePageMem + readPageMem);
}

void HwsFdEntity::ChangeFilePathIfNeed(const char* path)
{
    if (path == NULL)
    {
        return;
    }

    if (path_.compare(path) != 0)
    {
        S3FS_PRN_INFO("change file path[originalpath=%s][newpath=%s]", path_.c_str(), path);
        path_ = path;
    }
}

std::shared_ptr<HwsFdEntity> HwsFdManager::Open(const char* path, const uint64_t inodeNo, const off64_t fileSize)
{
    std::lock_guard<std::mutex> lock(mutex_);
    HwsEntityMap::iterator search = fent_.find(inodeNo);
    if (search == fent_.end())
    {
        fent_.emplace(inodeNo, std::make_shared<HwsFdEntity>(path, inodeNo, fileSize));
        search = fent_.find(inodeNo);
    }

    search->second->Ref();
    return search->second;
}

std::shared_ptr<HwsFdEntity> HwsFdManager::Get(const uint64_t inodeNo)
{
    std::lock_guard<std::mutex> lock(mutex_);
    HwsEntityMap::iterator search = fent_.find(inodeNo);
    if (search != fent_.end()) {
        return search->second;
    }
    return nullptr;
}

bool HwsFdManager::Close(const uint64_t inodeNo)
{
    // ent keeps a ref to ensure that destructor will not triggered within mutex
    std::shared_ptr<HwsFdEntity> ent(nullptr);
    bool last_reference = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        HwsEntityMap::iterator search = fent_.find(inodeNo);
        if(search == fent_.end()) {
            return false;
        }
        
        ent = search->second;
        last_reference = ent->Unref();
        if(last_reference) {
            fent_.erase(search);
        }
    }

    return true;
}

off64_t HwsFdManager::GetFreeCacheMemSize()
{
    off64_t freeCacheSize = 0;
    off64_t cacheUsedSize = GetUsedCacheMemSize();
    if (cacheUsedSize > gMaxCacheMemSize)
    {
        freeCacheSize = 0;
    }
    else
    {
        freeCacheSize = gMaxCacheMemSize - cacheUsedSize;
    }
    return freeCacheSize;
}

off64_t HwsFdManager::GetUsedCacheMemSize()
{
    off64_t cacheUsedSize = cache_mem_size.load();
    return cacheUsedSize;
}

unsigned int HwsFdManager::GetWritePageNumByCacheSize()
{
    off64_t freeCacheSize = GetFreeCacheMemSize();
    off64_t pageNum = gWritePageNum * freeCacheSize/gMaxCacheMemSize;
    pageNum = max(g_MinReadWritePageByCacheSize,pageNum);
    return (unsigned int)pageNum;
}

unsigned int HwsFdManager::GetReadPageNumByCacheSize()
{
    off64_t freeCacheSize = GetFreeCacheMemSize();
    struct timespec now_ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);
    /*print free cache size every 10 seconds*/
    if (diff_in_ms(&print_free_cache_ts_, &now_ts) > gReadStatDiffLongMs)
    {
        S3FS_PRN_INFO("free cache mem size(%lu)", freeCacheSize);
        clock_gettime(CLOCK_MONOTONIC_COARSE, &print_free_cache_ts_);        
    }
    
    off64_t pageNum = gReadPageNum * freeCacheSize/gMaxCacheMemSize;
    
    pageNum = max(g_MinReadWritePageByCacheSize,pageNum);
    return (unsigned int)pageNum;
}

void HwsFdManager::AddCacheMemSize(uint64_t add_size)
{
    cache_mem_size.fetch_add(add_size);
}
void HwsFdManager::SubCacheMemSize(uint64_t sub_size)
{
    cache_mem_size.fetch_sub(sub_size);
}

/*ReStatic cache size and adjust*/
void HwsFdManager::CheckCacheMemSize()       
{
    struct timespec now_ts;
    std::shared_ptr<HwsFdEntity> ent = nullptr;
    off64_t  restaticSize = 0;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);
    /*print free cache size every 30 seconds*/
    if (diff_in_ms(&prev_check_cache_size_ts_, &now_ts) < gCheckCacheSizePeriodMs)
    {
        return;
    }
    clock_gettime(CLOCK_MONOTONIC_COARSE, &prev_check_cache_size_ts_);  
    off64_t  cur_cache_size = cache_mem_size.load();
    /*if used half mem,print warn log*/
    if (cur_cache_size > gMaxCacheMemSize/2)
    {
        S3FS_PRN_WARN("half cache mem used,curSize(%lu),maxSize(%lu)",
            cur_cache_size,gMaxCacheMemSize);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    /*restatic cache size*/
    for( HwsEntityMap::iterator iter=fent_.begin();iter!=fent_.end();iter++ )
    {
        ent = iter->second;
        restaticSize += ent->CleanAndGetPageUsedMem();        
    }

    char print_buffer[TIME_FORMAT_LENGTH];
    get_current_time(print_buffer, TIME_FORMAT_LENGTH);
    S3FS_PRN_INFO("[%s]Restatic cache size(%lu)curSize(%lu)",print_buffer,restaticSize,cur_cache_size);
    /*adjust mem size with restaticSize*/
    if (abs(restaticSize - cur_cache_size) > HWS_CACHE_ADJUST_SIZE)
    {
        S3FS_PRN_WARN("adjust cache size,curSize(%lu),Restatic(%lu)",
            cur_cache_size,restaticSize);
        cache_mem_size.store(restaticSize);
    }        
}

void HwsFdManager::CreateWholeFileCacheTask(long long inode)
{
    std::lock_guard<std::mutex> lock(cache_mutex_);
    struct HwsFdWholeFileTask* task = new(std::nothrow)HwsFdWholeFileTask(inode, whole_file_cache_task);
    g_wholeCacheReadTasks.fetch_add(1);
    wholeFileCacheTasks_.push_back(task);
    S3FS_PRN_WARN("add a whole file cache task for inode(%lld)taskNum(%d)", 
                inode, g_wholeCacheReadTasks.load());
}

/* executed every 60 seconds */
void HwsFdManager::DestroyWholeFileCacheTask(bool immediately)
{
    static struct timespec prev_set_by_cfg_ts;
    struct timespec now_ts;

    clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);
    long clearDiffMs  = diff_in_ms(&prev_set_by_cfg_ts, &now_ts);
    if ((clearDiffMs < 60000) && (false == immediately))
    {
        return;
    }
    prev_set_by_cfg_ts = now_ts;

    std::lock_guard<std::mutex> lock(cache_mutex_);
    if (0 == g_wholeCacheReadTasks.load())
    {
        return;
    }

    auto iter = wholeFileCacheTasks_.begin();
    while (iter != wholeFileCacheTasks_.end())
    {
        HwsFdWholeFileTask* task = *iter;
        // nullptr means the whole pre-read task thread exits
        // and the Entity is erased from HwsEntityMap.
        if (nullptr != Get(task->inode_))
        {
            iter++;
            continue;
        }
        g_wholeCacheReadTasks.fetch_sub(1);
        S3FS_PRN_WARN("delete a whole file cache task for inode(%lld)taskNum(%d)",
                    task->inode_, g_wholeCacheReadTasks.load());
        iter = wholeFileCacheTasks_.erase(iter);
        delete task;
    }
}

void HwsCacheStatis::AddStatisNum(hws_cache_statis_type_e statisEnum)
{
    statisStru.statisNumArray[statisEnum] += 1;
}
unsigned long long HwsCacheStatis::GetStatisNum(hws_cache_statis_type_e statisEnum)
{
    return statisStru.statisNumArray[statisEnum];
}

void HwsCacheStatis::AddStatisLen(hws_cache_statis_len_e statisEnum,
        unsigned int  addLen)
{
    statisStru.statisLenArray[statisEnum] += addLen;
}
unsigned long long HwsCacheStatis::GetAllStatisNum()       
{
    unsigned long long allStatisNum = 0;
    
    for (int i = 0; i < MAX_CACHE_STATIS; i++)
    {
        allStatisNum += statisStru.statisNumArray[i];
    }

    return allStatisNum;
}
void HwsCacheStatis::PrintStatisNum()       
{
    int ulReturnLen         = 0;
    int ulPrintLen          = 0;
    
    for (int statisEnum = 0; statisEnum < MAX_CACHE_STATIS; statisEnum ++)
    {
        ulReturnLen = snprintf(printStrBuf + ulPrintLen,
            HWS_STATIS_PRINT_BUF_SIZE - ulPrintLen, "%s:%llu,",
            statis_enum_to_string((hws_cache_statis_type_e)statisEnum),
            statisStru.statisNumArray[statisEnum]);
        ulPrintLen += ulReturnLen;
        /*if ulPrintLen larger than 200,output*/
        if (ulPrintLen > 200)            
        {
            S3FS_PRN_WARN("cache statis %s",printStrBuf);
            ulReturnLen         = 0;
            ulPrintLen          = 0;
        }
    }
    /*print buf if less then 100*/
    if (ulPrintLen > 0)            
    {
        S3FS_PRN_WARN("cache statis %s",printStrBuf);            
    }
}
unsigned long long HwsCacheStatis::Calc_len_per_req(unsigned long long reqNum,
         unsigned long long statis_len)       
{
    unsigned long long len_per_req = 0;
    
    if (reqNum > 0)
    {
        len_per_req = statis_len / reqNum;
    }
    return len_per_req;
}
void HwsCacheStatis::PrintStatisLen()       
{
    unsigned long long entity_read_len_per  = Calc_len_per_req(
        statisStru.statisNumArray[TOTAL_READ],
        statisStru.statisLenArray[TOTAL_READ_LEN]);
    unsigned long long read_ahead_len_per  = Calc_len_per_req(
        statisStru.statisNumArray[READ_AHEAD_PAGE],
        statisStru.statisLenArray[READ_AHEAD_LEN]);
    unsigned long long entity_write_len_per  = Calc_len_per_req(
        statisStru.statisNumArray[TOTAL_WRITE],
        statisStru.statisLenArray[TOTAL_WRITE_LEN]);
    unsigned long long merge_write_obs_per = Calc_len_per_req(
        statisStru.statisNumArray[WRITE_MERGE_OBS],
        statisStru.statisLenArray[MERGE_WRITE_OBS_LEN]);

    S3FS_PRN_WARN("entity_read_per:%llu,read_ahead_per:%llu,"
        "entity_write_per:%llu,merge_write_obs:%llu",
        entity_read_len_per,read_ahead_len_per,
        entity_write_len_per,merge_write_obs_per);
    
}
/*print statis result and clear statis*/
void HwsCacheStatis::PrintStatisAndClear()       
{
    struct timespec now_ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);
    /*print every 300 seconds*/
    if (diff_in_ms(&prev_print_ts_, &now_ts) < gPrintCacheStatisMs)
    {
        return;
    }
    clock_gettime(CLOCK_MONOTONIC_COARSE, &prev_print_ts_);  
    if (0 == GetAllStatisNum())
    {
        return;
    }
    /*print statis num and len*/
    PrintStatisNum();
    PrintStatisLen();
    
    ClearStatis();
}
const char *HwsCacheStatis::statis_enum_to_string(hws_cache_statis_type_e statisEnum)
{
    switch (statisEnum)
    {
        HWS_ENUM_TO_STR(TOTAL_READ)      
        HWS_ENUM_TO_STR(READ_HIT)
        HWS_ENUM_TO_STR(READ_WHOLE_HIT)
        HWS_ENUM_TO_STR(READ_AHEAD_PAGE) 
        HWS_ENUM_TO_STR(READ_LAST_WRT_PAGE)
        HWS_ENUM_TO_STR(TOTAL_WRITE)
        HWS_ENUM_TO_STR(MERGE_WRITE)
        HWS_ENUM_TO_STR(WRITE_MERGE_OBS)   

        case MAX_CACHE_STATIS:
            break;
    }
    return "invalid_statis_enum";
}

