/*
 * Copyright (C) 2018. Huawei Technologies Co., Ltd.
 *
 * This program is free software; you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License version 2 and
 * only version 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 */

#ifndef _HWS_FD_CACHE_H_
#define _HWS_FD_CACHE_H_
#include "common.h"
#include <condition_variable>
#include <thread>
#include <chrono>
#include <algorithm>
#include <deque>
#include <unordered_map>
#include <atomic>

using namespace std;

extern bool gIsReadWaitCache;
extern size_t gWritePageSize;
extern unsigned int gWritePageNum;
extern size_t gReadPageSize;
extern unsigned int gReadPageNum;
extern long gReadPageCleanMs;
extern int gReadStatSizeMax;
extern int gReadStatSeqSize;
extern off64_t gReadStatSizeThreshold;
extern long gReadStatDiffLongMs;
extern long gReadStatDiffShortMs;
extern unsigned int gWriteStatSeqNum;
extern s3fs_log_level fuse_intf_log_level;

extern void hws_cache_daemon_task();

#define HWS_READ_CRC_SIZE (128*1024)
#define HWS_MB_SIZE (1024*1024)
#define HWS_MAX_WRITE_RETRY_MS 3000   /*write page retry max 3 second*/
#define HWS_ALLOC_MEM_RETRY_MS 40     /*alloc memory delay 40 ms and retry*/
#define HWS_WRITE_PAGE_WAIT_TIME_MS 3000 /*write page wait time max 3 second since last write*/
#define HWS_LOG_HASH_BUCKET_NUM   65536  //log hash num for log flow control 

#define HWS_STATIS_PRINT_BUF_SIZE (10*1024)

#define HWS_ENUM_TO_STR(x)   case x: return(#x);

enum hws_fd_write_state{
    HWS_FD_WRITE_STATE_WAIT,
    HWS_FD_WRITE_STATE_NOTIFY,
    HWS_FD_WRITE_STATE_SENDING,
    HWS_FD_WRITE_STATE_FINISH,
    HWS_FD_WRITE_STATE_ERROR
};

enum hws_fd_read_state{
    HWS_FD_READ_STATE_RECVING,
    HWS_FD_READ_STATE_RECVED,
    HWS_FD_READ_STATE_DISABLED,
    HWS_FD_READ_STATE_ERROR
};

enum hws_fd_write_mode{
    THROUGH,
    APPEND,
    MODIFY	
};

enum hws_cache_statis_type_e
{
  TOTAL_READ = 0,      
  READ_HIT,
  READ_WHOLE_HIT,
  READ_AHEAD_PAGE,   /*read ahead page num*/
  READ_LAST_WRT_PAGE,  /*read from last write page*/

  TOTAL_WRITE,
  MERGE_WRITE,
  WRITE_MERGE_OBS,   /*merge write to obs*/
  MAX_CACHE_STATIS              
};

enum hws_cache_statis_len_e 
{
  TOTAL_READ_LEN = 0,   /*entity read len*/   
  TOTAL_WRITE_LEN,      /*entity write len*/   
  READ_AHEAD_LEN,       /*read ahead from obs len*/
  MERGE_WRITE_OBS_LEN,      /*merge write obs len*/
  MAX_CACHE_STATIS_LEN_E              
};

enum hws_write_err_type_e
{
    CRC_ERR = 0,       /*write page crc error*/
    SEND_OBS_ERR,      /*write page send to obs error*/
    BUTT_ERR
};

typedef struct  HwsWriteLastPageRead_Stru
{
    off64_t  file_offset_;      //offset in the file
    size_t   buf_size_;         
    size_t   read_bytes_;       //read bytes from write page list
    uint32_t crc_;              /*crc of read buf*/
    char*    buffer_;    
}HwsWriteLastPageRead_S;
struct HwsFdWritePage
{
    const std::string path_;
    off64_t  offset_;      //offset in the file
    const size_t write_page_size_;    /*page size,default gWritePageSize*/
    size_t   bytes_;
    char*    data_;
    int      state_;
    uint32_t crc_;
    int      refs_;
    std::mutex& write_merge_mutex_;    /*point to write_merge_mutex_ in entity*/
    std::condition_variable cv_;
    std::thread thread_;
    bool thread_start;
    void (*thread_task_)(HwsFdWritePage*);
    struct timespec last_write_ts_;
    hws_write_err_type_e err_type_;

    HwsFdWritePage(const char *path, off64_t offset, size_t write_page_size,
                std::mutex& write_merge_mutex,
                void (*thread_task)(HwsFdWritePage*))
           : path_(path),
             offset_(offset),
             write_page_size_(write_page_size),
             write_merge_mutex_(write_merge_mutex),             
             thread_task_(thread_task)
     {
          bytes_ = 0;
          state_ = HWS_FD_WRITE_STATE_WAIT;
          crc_ = 0;
          refs_ = 0;
          data_ = nullptr;
          thread_start = false;
          err_type_ = BUTT_ERR;
          clock_gettime(CLOCK_MONOTONIC_COARSE, &last_write_ts_);
    }

    ~HwsFdWritePage()
    {
        if (thread_start)
        {
            thread_.join();
        }
        if (nullptr != data_)
        {
            delete[] data_;
        }
    }

    int Init();
    size_t IntersectWriteMergeToLastpage(
            const char *buf,off64_t wrtOffset, size_t wrtSize);
    size_t Append(const char *buf,off64_t offset, size_t size,
            bool* pbIsSequential);
    bool Full(){ return bytes_ >= write_page_size_||state_ > HWS_FD_WRITE_STATE_WAIT;}
    bool IsSequential(off64_t offset)
    {
        off64_t write_end = offset_ + (off64_t)(bytes_);
        return write_end == offset;
    }
    char* GetDataBuf()
    {
        return data_;
    }
    void WritePageRef()
    {
        refs_++;
    }
    void WritePageUnref();
    bool CanRelease()
    {
        return refs_ == 0;
    }
};

typedef void (*WRITE_PAGE_THREAD_FUNC)(HwsFdWritePage*);

class HwsFdWritePageList
{
  private:
    std::list<HwsFdWritePage*> pages_;
    const size_t write_page_size_;     /*page size,default gWritePageSize*/
    std::mutex& write_merge_mutex_;    /*point to write_merge_mutex_ in entity*/
    bool bWritePageErr;                /*if crc err or send obs err*/
    hws_write_err_type_e err_type_ = BUTT_ERR;   /*specific error type*/

  public:
    HwsFdWritePageList(size_t write_page_size,std::mutex& write_merge_mutex)
            :write_page_size_(write_page_size),write_merge_mutex_(write_merge_mutex)
            {
                bWritePageErr = false;
            }
    ~HwsFdWritePageList();
    HwsFdWritePageList(const HwsFdWritePageList&) = delete;
    HwsFdWritePageList& operator = (const HwsFdWritePageList&) = delete;
    bool IsFull();
    bool IsSequential(off64_t offset)
    {
        if(pages_.empty())
            return true;
        HwsFdWritePage* page = pages_.back();
        return page->IsSequential(offset);
    }
    HwsFdWritePage* OverlapPage(off64_t offset, size_t size);
    bool IsOverlap(off64_t offset, size_t size);
    HwsFdWritePage* OverlapExcludeLastPage(off64_t offset, size_t size);
    bool canIntersectWrtMergeToLastPage(
            const char* path,off64_t offset, size_t size);
    size_t Append(const char *path, const char *buf, off64_t offset, 
            size_t size,bool* pbIsSequential);
    int ReadLastOverLap(const char* path,
        HwsWriteLastPageRead_S *readInfo, off64_t offset, size_t readSize);
    void Clean();
    unsigned int GetCurWrtPageNum();
    bool IsWritePageErr()
    {
        return bWritePageErr;
    }
    hws_write_err_type_e GetWriteErrType()
    {
        return err_type_;
    }
    HwsFdWritePage* NewWritePage(const char *path,
                off64_t offset, size_t write_page_size,
                    std::mutex& write_merge_mutex,
                    WRITE_PAGE_THREAD_FUNC thread_task);    
    void WaitAllPageFinish(std::unique_lock<std::mutex>&);
};

struct HwsFdReadPage
{
    const std::string path_;
    off64_t  offset_;  //offset in the file
    const size_t read_page_size_;    /*page size,default gReadPageSize*/
    char*    data_;
    int      state_;
    std::vector<uint32_t> crc_;
    int      refs_;
    size_t   read_size_;   //data size in page
    size_t  hitSizeInPage;
    struct timespec got_ts_;
    struct timespec last_hit_ts_;
    std::mutex& read_ahead_mutex_;    /*for read page list lock*/    
    std::condition_variable cv_;
    std::thread thread_;
    bool     is_read_finish_;     //finish read from osc
    void (*thread_task_)(HwsFdReadPage*);

    HwsFdReadPage(const char *path, off64_t offset, size_t read_page_size,
                std::mutex& read_ahead_mutex,
                void (*thread_task)(HwsFdReadPage*) = NULL)
           : path_(path),
             offset_(offset),
             read_page_size_(read_page_size),
             read_ahead_mutex_(read_ahead_mutex),
             thread_task_(thread_task){
      state_ = HWS_FD_READ_STATE_RECVING;
      got_ts_ = {0, 0};
      last_hit_ts_ = {0, 0};
      refs_ = 0;
      read_size_ = 0;
      crc_.reserve(read_page_size/HWS_READ_CRC_SIZE);
      is_read_finish_ = false;
      data_ = nullptr;
      hitSizeInPage = 0;
    }

    ~HwsFdReadPage(){
        if (NULL != thread_task_ && thread_.joinable())
        {
            thread_.join();
        }
        delete[] data_;
    }

    int Init();
    void GenerateCrc(ssize_t read_size);
    bool CheckCrcErr(off64_t offset, size_t size);
    void ReadPageRef()
    {
        refs_++;
    }
    void ReadPageUnref();
    bool CanRelease()
    {
        return refs_ == 0;
    }
};

class HwsFdReadPageList
{
  private:
    std::deque<HwsFdReadPage*> pages_;
    const size_t read_page_size_;   /*page size,default gReadPageSize*/
    std::mutex& read_ahead_mutex_;    /*for read page list lock*/    
    off64_t   hit_max_offset_;         /*read hit max offset in file*/        
    unsigned int total_hit_size;  /*count total read hit size*/
    unsigned int read_ahead_beyond_filesize_num;
    long firstToLastDiffMs;
    off64_t lastReadOffset;    
    struct timespec lastCleanTs;
    
  public:
    HwsFdReadPageList(size_t read_page_size,
        std::mutex& read_ahead_mutex)
        :read_page_size_(read_page_size), 
        read_ahead_mutex_(read_ahead_mutex)
    {
        hit_max_offset_ = 0;
        total_hit_size = 0;
        read_ahead_beyond_filesize_num = 0;
        firstToLastDiffMs = 0;
        lastReadOffset = 0;
        lastCleanTs = {0, 0};
    }
    ~HwsFdReadPageList();
    HwsFdReadPageList(const HwsFdReadPageList&) = delete;
    HwsFdReadPageList& operator = (const HwsFdReadPageList&) = delete;
    int Read(const char* path,char *buf, off64_t offset, size_t size);
    void ReadAhead(const char* path, off64_t offset, 
        HwsFdWritePageList& writePage,off64_t fileSize);
    void ReadAheadOnePage(const char* path, off64_t offset, 
            HwsFdWritePageList& writePage,off64_t fileSize);
    bool PageNeedFree(HwsFdReadPage* page);
    void Clean();
    void Invalid(off64_t offset, size_t size);
    HwsFdReadPage* OverlapPage(off64_t offset, size_t size);
    unsigned int GetCurReadPageNum();
    void SetHitMaxOffset(off64_t hit_max_offset)
    {
        hit_max_offset_ = hit_max_offset;
    }
    bool CheckHitMaxOffset();  
    unsigned int CalcMaxReadPageNum();
    off64_t GetFirstPageOffset(); 
    off64_t GetLastPageEnd();     
    void ClearTotalHitSize()
    {
        total_hit_size = 0;
    }
    long getFirstToLastDiffMs()
    {
        return firstToLastDiffMs;
    }
    void setFirstToLastDiffMs(long firstToLastMs)
    {
        firstToLastDiffMs = firstToLastMs; 
    }
    
};

struct HwsFdReadWholePageList
{
    struct timespec cacheEndTs;
    std::deque<HwsFdReadPage*> pages_;
    off64_t cacheHitCnt;
};

struct HwsFdReadWholeCacheStat
{
    struct timespec first_ts;
    off64_t notHitTimes;
    size_t notHitSize;
};

struct HwsFdReadStat
{
    off64_t offset;
    size_t size;
};

class HwsFdReadStatVec
{
  private:
    std::vector<HwsFdReadStat> stat_;
    struct timespec first_ts_;
    struct timespec last_ts_;
    bool isSequential_;
    off64_t offset_;   //read ahead offset
    long firstToLastDiffMs;
  public:
    HwsFdReadStatVec(int size)
    {
        stat_.reserve(size);
        first_ts_ = {0,0};
        last_ts_ = {0,0};
        isSequential_ = false;
        offset_ = 0;
        firstToLastDiffMs = 0;
    }
    ~HwsFdReadStatVec(){}
    void Add(const char* path,off64_t offset, size_t size);
    long GetReadStatDiffThreshold();
    bool IsSequential(){return isSequential_;}
    off64_t GetOffset(){return offset_;}
    long getFirstToLastDiffMs()
    {
        return firstToLastDiffMs;
    }
};

class HwsFdEntity
{
  private:
    int32_t refs_;
    bool wholeCacheWriteConflict_;
    bool wholeCacheTaskStarted_;
    std::string path_;
    const long long inodeNo_;
    off64_t fileSize_;
    std::mutex entity_mutex_;        /*for write and read page list lock*/
    HwsFdWritePageList writePage_;
    HwsFdReadPageList readPage_;
    HwsFdReadStatVec readStat_;
    off64_t          retryNum;  
    uint32_t write_cache_count;
    uint32_t write_through_count;
    uint32_t write_full_count;
    uint32_t sequential_modify_count_;
    int curr_write_mode_;
    off64_t write_end_;   //last write end position
    HwsFdReadWholeCacheStat wholeCacheStat_;
    HwsFdReadWholePageList wholeCachePage_;

  private:
    bool IsAppend(off64_t offset, size_t size)
    {
        if(offset == 0 && fileSize_ == 0 && size < 128*1024)
        {
            return false;
        }
        return fileSize_ == offset; 
    }
    void UpdateFileSize(off64_t offset, size_t size){
        off64_t write_end = offset + (off64_t)(size);
        if(write_end > fileSize_)
        {
            fileSize_ = write_end;
        }
        // Update last write position
    	write_end_ = write_end;
    }

    bool IsSequentialModify (off64_t offset, size_t size){
        //fileSize_ is equal to the offset in isAppend. Protection again here.
        if(fileSize_ == offset){
            return false;
        }
        sequential_modify_count_ = (write_end_==offset)?sequential_modify_count_+1:0;

        return sequential_modify_count_ >= gWriteStatSeqNum;
    }
  bool IsNeedWriteMerge(const char *path, 
      off64_t offset, size_t size, std::unique_lock<std::mutex>& lock);

  public:
    HwsFdEntity(const char* path, const long long inodeNo, const off64_t fileSize)
            : path_(path), inodeNo_(inodeNo), fileSize_(fileSize),
              writePage_(gWritePageSize,entity_mutex_),
              readPage_(gReadPageSize, entity_mutex_),
              readStat_(gReadStatSizeMax)
    {
        refs_ = 0;
        retryNum = 0;
        write_cache_count = 0;
        write_through_count = 0;
        write_full_count = 0;
        curr_write_mode_ = THROUGH;
        write_end_ = 0;
        sequential_modify_count_ = 0;
        wholeCachePage_.cacheHitCnt = 0;
        wholeCachePage_.cacheEndTs = {0,0};
        wholeCacheWriteConflict_ = false;
        wholeCacheTaskStarted_ = false;
        wholeCacheStat_.first_ts = {0,0};
        ClearWholeReadCacheStatis();
    }
    ~HwsFdEntity(){};
    HwsFdEntity(const HwsFdEntity&) = delete;
    HwsFdEntity& operator = (const HwsFdEntity&) = delete;
    long long GetInodeNo(){return inodeNo_;}
    void Ref(){
        refs_++;
    }

    int32_t GetRefs()
    {
        return refs_;
    }
    bool Unref(){
        if(refs_ > 0) {
            refs_--;
        }
        return refs_ == 0;
    }
    std::string& GetPath()
    {
        return path_;
    }
    std::mutex& GetEntityMutex()
    {
        return entity_mutex_;
    }
    HwsFdReadWholePageList& GetReadWholePageList()
    {
        return wholeCachePage_;
    }
    bool IsWholeWriteConflict()
    {
        return wholeCacheWriteConflict_ == true;
    }
    void SetWholeCacheStartedFlag(bool flag)
    {
        wholeCacheTaskStarted_ = flag;
    }
    bool GetWholeCacheStartedFlag()
    {
        return wholeCacheTaskStarted_;
    }
    int Write(const char* buf, off64_t offset, size_t size, const char* path = NULL);
    int Flush();
    int Read(char* buf, off64_t offset, size_t size, const char* path = NULL);
    off64_t GetFileSize(void);
    unsigned int CleanAndGetPageUsedMem();    
    size_t AppendToPageWithRetry(const char *path, 
        const char *buf, off64_t offset, size_t size,
        std::unique_lock<std::mutex>& entity_lock);    
    int GetWriteMode()
    {
	return curr_write_mode_;
    }
    void ChangeFilePathIfNeed(const char* path); /*in lock*/
    void UpdateFileSizeForTruncate(size_t size){ /*not in lock*/
        std::lock_guard<std::mutex> entity_lock(entity_mutex_);
        fileSize_ = (off64_t)(size);
    }

    void ClearWholeReadCacheStatis()
    {
        wholeCacheStat_.notHitSize = 0;
        wholeCacheStat_.notHitTimes = 0;
    }

    void ClearWholeReadConflictFlag()
    {
        wholeCacheWriteConflict_ = false;
    }

    void StartWholeFilePreRead(off64_t offset, size_t size);
    int ReadFromWholeFileCache(const char* path,
                        char* buf, off64_t offset, size_t size);
    void CheckPreReadConflict(off64_t offset, size_t size);
};

typedef std::unordered_map<uint64_t, std::shared_ptr<HwsFdEntity>> HwsEntityMap;

struct HwsFdWholeFileTask
{
    long long inode_;
    std::thread task_;

    HwsFdWholeFileTask(long long inode, void (*thread_task)(long long))
    {
        inode_ = inode;
        task_ = std::thread(thread_task, inode_);
    }

    ~HwsFdWholeFileTask()
    {
        if (task_.joinable())
        {
            task_.join();
        }
    }
};

class HwsFdManager
{
  private:
    HwsEntityMap fent_;
    std::mutex mutex_;
    std::list<HwsFdWholeFileTask*> wholeFileCacheTasks_;
    std::mutex cache_mutex_;
    std::atomic<off64_t>  cache_mem_size;      /*write and read page mem size*/
    struct timespec print_free_cache_ts_;      /*print free cache size time*/
    struct timespec prev_check_cache_size_ts_;      /*previous check cache size time*/
    std::thread thread_;
    bool bDaemonStop;

  public:
    HwsFdManager()
    {
        cache_mem_size.store(0);
        print_free_cache_ts_ = {0,0};
        prev_check_cache_size_ts_ = {0,0};
        bDaemonStop = false;        
    }
    ~HwsFdManager()
    {
        bDaemonStop = true;
        if (thread_.joinable())
        {
            thread_.join();
        }
    }

    HwsFdManager(const HwsFdManager&) = delete;
    HwsFdManager& operator = (const HwsFdManager&) = delete;

    static HwsFdManager& GetInstance(){
        static HwsFdManager instance;
        return instance;
    }
    std::shared_ptr<HwsFdEntity> Open(const char* path, const uint64_t inodeNo, const off64_t fileSize);
    std::shared_ptr<HwsFdEntity> Get(const uint64_t inodeNo);
    bool Close(const uint64_t inodeNo);

    off64_t GetFreeCacheMemSize();
    off64_t GetUsedCacheMemSize();
    unsigned int GetWritePageNumByCacheSize();
    unsigned int GetReadPageNumByCacheSize();
    void AddCacheMemSize(uint64_t add_size);
    void SubCacheMemSize(uint64_t sub_size);
    void CheckCacheMemSize();        /*ReStatic cache size and adjust*/
    bool GetDaemonStop()
    {
        return bDaemonStop;
    }
    void StartDaemonThread()
    {
        thread_ = std::thread(hws_cache_daemon_task);
    }

    void CreateWholeFileCacheTask(long long inode);
    void DestroyWholeFileCacheTask(bool immediately = false);
};

typedef struct _tag_hws_cache_statis 
{
        unsigned long long statisNumArray[MAX_CACHE_STATIS];      /*read/write num statis*/
        unsigned long long statisLenArray[MAX_CACHE_STATIS_LEN_E];   /*read/write len statis*/
}tag_hws_cache_statis_t;

class HwsCacheStatis
{
  private:
    tag_hws_cache_statis_t statisStru;
    struct timespec prev_print_ts_;
    char printStrBuf[HWS_STATIS_PRINT_BUF_SIZE];
    
  public:
    HwsCacheStatis()
    {
        memset(printStrBuf, 0, sizeof(printStrBuf));
        ClearStatis();
    }
    ~HwsCacheStatis(){}
    static HwsCacheStatis& GetInstance()
    {
        static HwsCacheStatis instance;
        return instance;
    }
    
    void ClearStatis()
    {
        memset(&statisStru, 0, sizeof(tag_hws_cache_statis_t));        
        clock_gettime(CLOCK_MONOTONIC_COARSE, &prev_print_ts_);  
    }
    void AddStatisNum(hws_cache_statis_type_e statisEnum);      
    unsigned long long GetStatisNum(hws_cache_statis_type_e statisEnum);
    void AddStatisLen(hws_cache_statis_len_e statisEnum,
        unsigned int  addLen);     
    unsigned long long GetAllStatisNum();       
    void PrintStatisNum();
    unsigned long long Calc_len_per_req(unsigned long long reqNum,
         unsigned long long statis_len) ;    
    void PrintStatisLen();   
    void PrintStatisAndClear();       
    const char *statis_enum_to_string(hws_cache_statis_type_e statisEnum);    
};


#endif // _HWS_FD_CACHE_H_



