#include <string>
#include <pthread.h>
#include <assert.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>

#include <cstdint>
#include <string>
#include <iostream>
#include <sstream>
#include <map>
#include <unordered_map>
#include <list>
#include <deque>
#include <vector>
#include <mutex>
#include <memory>
#include <condition_variable>
#include <thread>
#include <atomic>

#include "gtest.h"
#include "hws_fd_cache.h"
#include "crc32c.h"

s3fs_log_level debug_level        = S3FS_LOG_INFO;
const char*    s3fs_log_nest[S3FS_LOG_NEST_MAX] = {"", "  ", "    ", "      "};
s3fs_log_mode debug_log_mode      = LOG_MODE_FOREGROUND;
unsigned int g_test_simulate_alloc_fail = 0;
std::atomic<int> g_test_read_page_disable_step{0};

std::atomic<long long> send_times;
std::atomic<long long> recv_times;

extern void write_thread_task(struct HwsFdWritePage* page);
extern void read_thread_task(struct HwsFdReadPage* page);
extern bool gIsCheckCRC;
extern size_t gWritePageSize;
extern bool g_bIntersectWriteMerge; 
extern size_t gReadPageSize;
extern size_t gFuseMaxReadSize;
extern long gCheckCacheSizePeriodMs;
extern unsigned int g_ReadAllHitNum;
extern off64_t gMaxCacheMemSize;
extern unsigned int g_ReadNotHitNum ;
extern unsigned int g_AllocWritePageTimeoutNum;
extern long gPrintCacheStatisMs;
extern int gReadStatSize;
extern int gReadStatSeqSize;
extern bool g_wholeFileCacheSwitch;
extern int g_wholeFileCachePreReadInterval;
extern int g_wholeFileCacheMaxRecycleTime;
extern off64_t g_wholeFileCacheMaxHitTimes;
extern off64_t g_MinReadWritePageByCacheSize;

#define WRITE_PAGE_SIZE (6 * 1024 * 1024)
#define READ_PAGE_SIZE (6 * 1024 * 1024)
#define TEST_SIMULATE_OSC_FILE     "/tmp/hws_test_simulate_osc"
#define TEST_TEMPLATE_SRC_FILE     "/tmp/hws_test_template"
#define TEST_TEMPLATE_FILE_SIZE    (120 * 1024 * 1024)
bool g_s3fs_start_flag = true;

//only for compile
void s3fsStatisLogModeNotObsfs()        
{
}

int s3fs_write_hw_obs_proc_test(const char* data, size_t bytes, off64_t offset)
{
    int out;
    out = open(TEST_SIMULATE_OSC_FILE, O_WRONLY|O_CREAT, 777);
    pwrite(out, data, bytes, offset);
    close(out);
    cout << "send " << bytes << " bytes at offset " << offset << ",bytes=" <<
            bytes << ",freeCache=" <<  HwsFdManager::GetInstance().GetFreeCacheMemSize()
            <<std::endl;
    send_times.fetch_add(1);
    return bytes;
}

int s3fs_read_hw_obs_proc_test(char* data, size_t bytes, off64_t offset)
{
    int in;
    size_t read_bytes;
    in = open(TEST_SIMULATE_OSC_FILE, O_RDONLY);
    read_bytes = pread(in, data, bytes, offset);
    close(in);
    cout << "recv " << bytes << " bytes at offset " << offset << " real bytes " << read_bytes <<
        ",freeCache=" <<  HwsFdManager::GetInstance().GetFreeCacheMemSize()
            <<std::endl;
    recv_times.fetch_add(1);
    return read_bytes;
}
/*生成TEST_TEMPLATE_SRC_FILE文件*/
int GenerateTemplateFile()
{
    int fd;
    off64_t totalSize = 0;
    off64_t file_offset = 0;
    off64_t buf_offset = 0;
    char buf[WRITE_PAGE_SIZE] = {0};

    struct timespec now_ts;
    struct stat stbuf;
    size_t  timespec_size = sizeof(struct timespec);

    fd = open(TEST_TEMPLATE_SRC_FILE, O_RDWR|O_CREAT, 777);
    if(0 > fd)
    {
        return -1;
    }
    if(-1 != fstat(fd, &stbuf))
    {
        if (stbuf.st_size > TEST_TEMPLATE_FILE_SIZE)
        {
            cout << "template file exist,file size=" << stbuf.st_size << std::endl;
            close(fd);
            return stbuf.st_size;
        }
    }
    file_offset = 0;
    while(file_offset < TEST_TEMPLATE_FILE_SIZE + WRITE_PAGE_SIZE)
    {
        buf_offset = 0;
        while(buf_offset < WRITE_PAGE_SIZE)
        {
            clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);
            memcpy(buf+buf_offset,&now_ts,timespec_size);
            buf_offset += timespec_size;
        }
        /*write buf to template file*/
        file_offset += pwrite(fd, buf, WRITE_PAGE_SIZE, file_offset);
    }

    if(-1 != fstat(fd, &stbuf)){
        totalSize = stbuf.st_size;
    }
    close(fd);

    return totalSize;
}

/*从指定文件读取数据到缓存
   buf : 读模板文件的输出buf
   buf_size : buf大小
   file_offset ：读取位置在文件内偏移量
   read_size : 读取的大小
*/
int ReadFromTemplate(char* buf, size_t buf_size,off64_t file_offset,
            size_t read_size)
{
    int fd;
    size_t readBytes;

    if(buf_size < read_size)
    {
        cout << " input para invalid,buf_size:" << buf_size << "read_size"
              << read_size << std::endl;
        return -1;
    }
    fd = open(TEST_TEMPLATE_SRC_FILE, O_RDONLY);
    if(0 > fd)
    {
        cout << " open test template file failed ! " << std::endl;
        return -2;
    }
    readBytes = pread(fd, buf, read_size, file_offset);
    if(readBytes < read_size)
    {
        cout << " test template file too small,offset=" << file_offset <<
            "readBytes=" <<readBytes <<
           "need read size=" << read_size << std::endl;
        return -3;
    }
    close(fd);
    return readBytes;
}

/*校验读出数据的正确性
   bufWrite and bufRead check crc from buf start to checksize
*/
bool CheckReadWithTemplate(const char* bufRead,off64_t file_offset,
                                    size_t read_size)
{
    uint32_t crcRead = 0;
    uint32_t crcReadTemplate = 0;
    int readTemplateBytes = 0;
    bool checkResult = true;

    char* bufReadTemplate = (char *)malloc(read_size);
    if (NULL == bufReadTemplate)
    {
        cout << " alloc mem fail,offset=" << file_offset <<
            "readTemplateBytes=" <<readTemplateBytes << std::endl;
        return false;
    }
    readTemplateBytes = ReadFromTemplate(bufReadTemplate,
                read_size,file_offset,read_size);
    if (readTemplateBytes < 0)
    {
        cout << " test template file fail,offset=" << file_offset <<
            " readTemplateBytes=" <<readTemplateBytes << std::endl;
        free(bufReadTemplate);
        return false;
    }

    crcReadTemplate = rocksdb::crc32c::Value(bufReadTemplate, read_size);
    crcRead = rocksdb::crc32c::Value(bufRead, read_size);
    if (crcRead == crcReadTemplate)
    {
        checkResult = true;
    }
    else
    {
        cout << "CheckReadWrite failed:offset=" <<file_offset << " size=" << read_size
            << " crcReadTemplate=" << crcReadTemplate <<" crcRead=" << crcRead << std::endl;
        checkResult = false;
    }
    free(bufReadTemplate);
    return checkResult;
}

namespace {
    // The fixture for testing class Foo.
    class HwsFdCacheTest : public ::testing::Test {
     protected:
      // You can remove any or all of the following functions if its body
      // is empty.

      HwsFdCacheTest() {
         // You can do set-up work for each test here.
      }

      ~HwsFdCacheTest() override {
         // You can do clean-up work that doesn't throw exceptions here.
      }

      // If the constructor and destructor are not enough for setting up
      // and cleaning up each test, you can define the following methods:

      void SetUp() override {
         // Code here will be called immediately after the constructor (right
         // before each test).
         unlink(TEST_SIMULATE_OSC_FILE);
         send_times.store(0);
         recv_times.store(0);
         g_test_read_page_disable_step.store(0);
         HwsFdManager::GetInstance().CheckCacheMemSize();
         gCheckCacheSizePeriodMs = 0;
         gReadStatSeqSize = 6;
         gReadStatDiffLongMs = 1000;
         gReadStatDiffShortMs = 500;
         g_MinReadWritePageByCacheSize = 1;
      }

      void TearDown() override {
         // Code here will be called immediately after each test (right
         // before the destructor).
      }

      // Objects declared here can be used by all tests in the test case for Foo.
    };

    TEST_F(HwsFdCacheTest, HwsFdMultiTimes) {
        gReadPageSize = READ_PAGE_SIZE;
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, READ_PAGE_SIZE*6);
        std::shared_ptr<HwsFdEntity> ent1 = HwsFdManager::GetInstance().Open("/home/test", 0, READ_PAGE_SIZE*6);
        ent0 = HwsFdManager::GetInstance().Get(0);
        HwsFdManager::GetInstance().Close(0);
        HwsFdManager::GetInstance().Close(0);
    }

    TEST_F(HwsFdCacheTest, HwsFdManagerFunction) {
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        EXPECT_EQ(ent0->GetInodeNo(), 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        EXPECT_EQ(ent0->GetInodeNo(), 0);
        std::shared_ptr<HwsFdEntity> ent1 = HwsFdManager::GetInstance().Open("/home/test", 1, 0);
        EXPECT_EQ(ent1->GetInodeNo(), 1);
        ent1 = HwsFdManager::GetInstance().Open("/home/test", 1, 0);
        EXPECT_EQ(ent1->GetInodeNo(), 1);
        ent1 = HwsFdManager::GetInstance().Get(1);
        EXPECT_EQ(ent1->GetInodeNo(), 1);
        EXPECT_EQ(HwsFdManager::GetInstance().Close(1), true);
        EXPECT_EQ(HwsFdManager::GetInstance().Close(1), true);
        EXPECT_EQ(HwsFdManager::GetInstance().Close(0), true);
        ent0 = HwsFdManager::GetInstance().Get(0);
        EXPECT_EQ(ent0, nullptr);
        ent1 = HwsFdManager::GetInstance().Get(1);
        EXPECT_EQ(ent1, nullptr);
    }

    #define TEST_INODE_NUM 10000
    #define TEST_THREAD_NUM 100
    void *thread_start(void *arg)
    {
        long long i, num = TEST_INODE_NUM;

        for(i = 0; i < num; i++)
        {
            HwsFdManager::GetInstance().Open("/home/test", i, 0);
        }
        for(i = 0; i < num; i++)
        {
            HwsFdManager::GetInstance().Get(i);
        }
        for(i = 0; i < num; i++)
        {
            HwsFdManager::GetInstance().Close(i);
        }
        return NULL;
    }

    TEST_F(HwsFdCacheTest, HwsFdManagerConcurrent) {
        int tnum, num_threads = TEST_THREAD_NUM;
        pthread_t *thread_id;

        thread_id = (pthread_t *)calloc(num_threads, sizeof(*thread_id));
        for (tnum = 0; tnum < num_threads; tnum++) {
            pthread_create(&thread_id[tnum], NULL,
                           &thread_start, NULL);
        }

        for (tnum = 0; tnum < num_threads; tnum++) {
            pthread_join(thread_id[tnum], NULL);
        }

        free(thread_id);

        long long i, num = TEST_INODE_NUM;
        for(i = 0; i < num; i++)
        {
            EXPECT_EQ(HwsFdManager::GetInstance().Get(i), nullptr);
        }

    }

    TEST_F(HwsFdCacheTest, HwsFdWrite001) {
        gWritePageSize = WRITE_PAGE_SIZE;
        char buf[WRITE_PAGE_SIZE] = {0};
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        ent0->Write(buf, 0, WRITE_PAGE_SIZE/2);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(send_times.load(), 0);
        ent0->Write(buf, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(send_times.load(), 2);
        ent0->Write(buf, WRITE_PAGE_SIZE/2*3, WRITE_PAGE_SIZE/2);
        ent0->Write(buf, WRITE_PAGE_SIZE*2, WRITE_PAGE_SIZE/2);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(send_times.load(), 3);
        ent0->Write(buf, WRITE_PAGE_SIZE/2*5, WRITE_PAGE_SIZE/2);
        std::this_thread::sleep_for(std::chrono::milliseconds(HWS_WRITE_PAGE_WAIT_TIME_MS+100));
        EXPECT_EQ(send_times.load(), 4);
        HwsFdManager::GetInstance().Close(0);
    }

    TEST_F(HwsFdCacheTest, HwsFdWrite002) {
        gWritePageSize = WRITE_PAGE_SIZE;
        char buf[WRITE_PAGE_SIZE] = {0};
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        ent0->Write(buf, 0, WRITE_PAGE_SIZE/2);
        EXPECT_EQ(send_times.load(), 0);
        ent0->Write(buf, 0, WRITE_PAGE_SIZE);
        EXPECT_EQ(send_times.load(), 2);
        HwsFdManager::GetInstance().Close(0);
    }

    TEST_F(HwsFdCacheTest, HwsFdWrite003) {
        gWritePageSize = WRITE_PAGE_SIZE;
        char buf[WRITE_PAGE_SIZE] = {0};
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        ent0->Write(buf, 0, WRITE_PAGE_SIZE);
        ent0->Write(buf, WRITE_PAGE_SIZE, WRITE_PAGE_SIZE/2);
        ent0->Write(buf, 0, WRITE_PAGE_SIZE/2);
        EXPECT_EQ(send_times.load(), 2);
        std::this_thread::sleep_for(std::chrono::milliseconds(HWS_WRITE_PAGE_WAIT_TIME_MS+100));
        EXPECT_EQ(send_times.load(), 3);
        HwsFdManager::GetInstance().Close(0);
    }

    TEST_F(HwsFdCacheTest, HwsFdWrite004) {
        gWritePageSize = WRITE_PAGE_SIZE;
        char buf[WRITE_PAGE_SIZE] = {0};
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        for(unsigned int i = 0; i < gWritePageNum; i++)
        {
            ent0->Write(buf, i*WRITE_PAGE_SIZE, WRITE_PAGE_SIZE);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(send_times.load(), gWritePageNum);
        ent0->Write(buf, gWritePageNum*WRITE_PAGE_SIZE, 512*1024);
        ent0->Write(buf, gWritePageNum*WRITE_PAGE_SIZE+WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(send_times.load(), gWritePageNum+2);
        ent0->Write(buf, (gWritePageNum+1)*WRITE_PAGE_SIZE+WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2);
        EXPECT_EQ(send_times.load(), gWritePageNum+2);
        ent0->Flush();
        EXPECT_EQ(send_times.load(), gWritePageNum+3);
        HwsFdManager::GetInstance().Close(0);
    }

    TEST_F(HwsFdCacheTest, HwsFdWrite005) {
        gWritePageSize = WRITE_PAGE_SIZE;
        char buf[WRITE_PAGE_SIZE] = {0};
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        ent0->Write(buf, 0, WRITE_PAGE_SIZE/2);
        std::this_thread::sleep_for(std::chrono::milliseconds(HWS_WRITE_PAGE_WAIT_TIME_MS+2000));
        EXPECT_EQ(send_times.load(), 1);
        ent0->Write(buf, WRITE_PAGE_SIZE/2, 1024*128);
        ent0->Write(buf, WRITE_PAGE_SIZE/2 + 1024*128, 1024*128);
        EXPECT_EQ(send_times.load(), 1);
        ent0->Flush();
        EXPECT_EQ(send_times.load(), 2);
        HwsFdManager::GetInstance().Close(0);
    }

    TEST_F(HwsFdCacheTest, HwsFdWrite006) {
        gWritePageSize = WRITE_PAGE_SIZE;
        char buf[WRITE_PAGE_SIZE] = {0};
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        ent0->Write(buf, 0, WRITE_PAGE_SIZE/2);
        EXPECT_EQ(send_times.load(), 0);
        ent0->Write(buf, WRITE_PAGE_SIZE/2 + 1024*128, 1024*128);
        EXPECT_EQ(send_times.load(), 2);
        HwsFdManager::GetInstance().Close(0);
    }

    TEST_F(HwsFdCacheTest, HwsFdWrite007) {
        /*check free cache size,delay 1500 ms for page timeout */
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));
        off64_t freeCacheSize = HwsFdManager::GetInstance().GetFreeCacheMemSize();
        if (freeCacheSize != gMaxCacheMemSize)
        {
            cout << "page not free,freeCacheSize=" << freeCacheSize <<
                ",maxCacheSize=" << gMaxCacheMemSize << std::endl;
            EXPECT_EQ(true, false);
        }
        gWritePageSize = WRITE_PAGE_SIZE;
        char buf[WRITE_PAGE_SIZE] = {0};
        unsigned int i;
        off64_t offset = 0;
        size_t size = 1024*128;
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        for(i = 0; i < WRITE_PAGE_SIZE/size*8; i++, offset+=size)
            ent0->Write(buf, offset, size);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(send_times.load(), 8);

        for(i = 0; i < WRITE_PAGE_SIZE/size*8; i++, offset+=size)
            ent0->Write(buf, offset, size);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(send_times.load(), 16);

        HwsFdManager::GetInstance().Close(0);
    }

    TEST_F(HwsFdCacheTest, HwsFdWrite008) {
        gWritePageSize = WRITE_PAGE_SIZE;
        char buf[WRITE_PAGE_SIZE] = {0};
        unsigned int i;
        off64_t offset = 0;
        size_t size = 1024*128;
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        ent0->Write(buf, 0, 1024*64);
        EXPECT_EQ(send_times.load(), 1);
        offset = 1024*64;
        for(i = 0; i < WRITE_PAGE_SIZE/size; i++, offset+=size)
            ent0->Write(buf, offset, size);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(send_times.load(), 2);

        HwsFdManager::GetInstance().Close(0);
    }


    TEST_F(HwsFdCacheTest, HwsFdWrite009) {
        gWritePageSize = WRITE_PAGE_SIZE;
        size_t size_per_write = WRITE_PAGE_SIZE/6;
        char buf[WRITE_PAGE_SIZE] = {0};
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        for (int i = 0; i < 5; i++)
        {
            ent0->Write(buf, size_per_write*i, size_per_write);
            std::this_thread::sleep_for(std::chrono::milliseconds(HWS_WRITE_PAGE_WAIT_TIME_MS/2));
        }
        EXPECT_EQ(send_times.load(), 0);

        std::this_thread::sleep_for(std::chrono::milliseconds(HWS_WRITE_PAGE_WAIT_TIME_MS/2));
        std::this_thread::sleep_for(std::chrono::milliseconds(HWS_WRITE_PAGE_WAIT_TIME_MS+200));
        EXPECT_EQ(send_times.load(), 1);

        HwsFdManager::GetInstance().Close(0);
    }

    //intersect write merge
    TEST_F(HwsFdCacheTest, HwsFdWrite010) {
        bool oldCheckCRC = gIsCheckCRC;
        gIsCheckCRC = false;
        g_bIntersectWriteMerge = true;
        
        gWritePageSize = WRITE_PAGE_SIZE;
        char buf[WRITE_PAGE_SIZE] = {0};
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        ent0->Write(buf, 0, 1024*8);
        ent0->Write(buf, 1024*4, 1024*8);
        ent0->Write(buf, 1024*12, 1024*4);        
        std::this_thread::sleep_for(std::chrono::milliseconds(HWS_WRITE_PAGE_WAIT_TIME_MS+200));
        EXPECT_EQ(send_times.load(), 1);

        HwsFdManager::GetInstance().Close(0);
        gIsCheckCRC = oldCheckCRC;
        g_bIntersectWriteMerge = false;
    }

    TEST_F(HwsFdCacheTest, HwsFdWriteCrc001) {
        bool oldCheckCRC = gIsCheckCRC;
        gIsCheckCRC = true;
        char buf[1024*1024] = {0};
        std::mutex write_merge_mutex;
        struct HwsFdWritePage testWritePage("/home/test", 0, 1024*1024,
            write_merge_mutex,write_thread_task);

        bool needRetry = true;        
        testWritePage.Append(buf,0, 512*1024,&needRetry);
        testWritePage.Append(buf,512*1024, 512*1024,&needRetry);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(testWritePage.state_, HWS_FD_WRITE_STATE_FINISH);
        gIsCheckCRC = oldCheckCRC;
    }

    TEST_F(HwsFdCacheTest, HwsFdWriteCrc002) {
        bool oldCheckCRC = gIsCheckCRC;
        gIsCheckCRC = true;
        char buf[1024*1024] = {0};
        std::mutex write_merge_mutex;
        struct HwsFdWritePage testWritePage("/home/test", 0, 1024*1024,
            write_merge_mutex,write_thread_task);
        bool needRetry = true;        
        testWritePage.Append(buf,0, 512*1024,&needRetry);
        testWritePage.data_[0] += 1;
        testWritePage.Append(buf,512*1024, 512*1024,&needRetry);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(testWritePage.state_, HWS_FD_WRITE_STATE_ERROR);
        gIsCheckCRC = oldCheckCRC;
    }

    TEST_F(HwsFdCacheTest, HwsFdReadCrc001) {
        bool oldCheckCRC = gIsCheckCRC;
        gIsCheckCRC = true;
        char buf[6*1024*1024] = {0};
        s3fs_write_hw_obs_proc_test(buf, 6*1024*1024, 0);
        std::mutex read_ahead_mutex_;
        struct HwsFdReadPage testReadPage("/home/test", 0,
            6*1024*1024,read_ahead_mutex_, read_thread_task);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(testReadPage.state_, HWS_FD_READ_STATE_RECVED);
        EXPECT_EQ(testReadPage.CheckCrcErr(64*1024, 128 * 1024), false);
        gIsCheckCRC = oldCheckCRC;
    }

    TEST_F(HwsFdCacheTest, HwsFdReadCrc002) {
        bool oldCheckCRC = gIsCheckCRC;
        gIsCheckCRC = true;
        char buf[4*1024*1024] = {0};
        s3fs_write_hw_obs_proc_test(buf, 4*1024*1024, 0);
        std::mutex read_ahead_mutex_;
        struct HwsFdReadPage testReadPage("/home/test", 0, 6*1024*1024,
                read_ahead_mutex_, read_thread_task);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(testReadPage.state_, HWS_FD_READ_STATE_RECVED);
        EXPECT_EQ(testReadPage.CheckCrcErr(64*1024, 128 * 1024), false);
        testReadPage.data_[0] = 1;
        EXPECT_EQ(testReadPage.CheckCrcErr(64*1024, 128 * 1024), true);
        testReadPage.data_[0] = 0;
        EXPECT_EQ(testReadPage.CheckCrcErr(64*1024, 128 * 1024), false);
        testReadPage.data_[256*1024-1] = 1;
        EXPECT_EQ(testReadPage.CheckCrcErr(64*1024, 128 * 1024), true);
        testReadPage.data_[256*1024-1] = 0;
        EXPECT_EQ(testReadPage.CheckCrcErr(64*1024, 128 * 1024), false);
        EXPECT_EQ(testReadPage.CheckCrcErr(4*1024*1024 - 64*1024, 128 * 1024), false);
        testReadPage.data_[4*1024*1024-1] = 1;
        EXPECT_EQ(testReadPage.CheckCrcErr(4*1024*1024 - 64*1024, 128 * 1024), true);
        testReadPage.data_[4*1024*1024-1] = 0;
        EXPECT_EQ(testReadPage.CheckCrcErr(4*1024*1024 - 64*1024, 128 * 1024), false);
        gIsCheckCRC = oldCheckCRC;
    }

    TEST_F(HwsFdCacheTest, HwsFdReadStat001) {
        class HwsFdReadStatVec testReadStat(8);
        gReadStatSize = 8;
        EXPECT_EQ(testReadStat.IsSequential(), false);
        testReadStat.Add("/home/test",0, 512*1024);
        testReadStat.Add("/home/test",1*512*1024, 512*1024);
        testReadStat.Add("/home/test",2*512*1024, 512*1024);
        testReadStat.Add("/home/test",3*512*1024, 512*1024);
        testReadStat.Add("/home/test",4*512*1024, 512*1024);
        testReadStat.Add("/home/test",9*512*1024, 512*1024);
        testReadStat.Add("/home/test",10*512*1024, 512*1024);
        EXPECT_EQ(testReadStat.IsSequential(), false);
        testReadStat.Add("/home/test",11*512*1024, 512*1024);
        EXPECT_EQ(testReadStat.IsSequential(), false);
        gReadStatSize = 32;
    }

    TEST_F(HwsFdCacheTest, HwsFdReadStat002) {
        class HwsFdReadStatVec testReadStat(8);
        gReadStatSize = 8;
        EXPECT_EQ(testReadStat.IsSequential(), false);
        testReadStat.Add("/home/test",1*512*1024, 512*1024);
        testReadStat.Add("/home/test",0, 512*1024);
        testReadStat.Add("/home/test",2*512*1024, 512*1024);
        testReadStat.Add("/home/test",3*512*1024, 512*1024);
        testReadStat.Add("/home/test",4*512*1024, 512*1024);
        testReadStat.Add("/home/test",8*512*1024, 512*1024);
        testReadStat.Add("/home/test",9*512*1024, 512*1024);
        EXPECT_EQ(testReadStat.IsSequential(), false);
        std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        testReadStat.Add("/home/test",10*512*1024, 512*1024);
        EXPECT_EQ(testReadStat.IsSequential(), false);
        gReadStatSize = 32;
    }

    TEST_F(HwsFdCacheTest, HwsFdReadStat003) {
        class HwsFdReadStatVec testReadStat(8);
        gReadStatSize = 8;
        EXPECT_EQ(testReadStat.IsSequential(), false);
        testReadStat.Add("/home/test",1*512*1024, 512*1024);
        testReadStat.Add("/home/test",0, 512*1024);
        testReadStat.Add("/home/test",2*512*1024, 512*1024);
        testReadStat.Add("/home/test",3*512*1024, 512*1024);
        testReadStat.Add("/home/test",4*512*1024, 512*1024);
        testReadStat.Add("/home/test",8*512*1024, 512*1024);
        testReadStat.Add("/home/test",9*512*1024, 512*1024);
        EXPECT_EQ(testReadStat.IsSequential(), false);
        testReadStat.Add("/home/test",10*512*1024, 512*1024);
        EXPECT_EQ(testReadStat.IsSequential(), true);
        EXPECT_EQ(testReadStat.GetOffset(), 11*512*1024);
        gReadStatSize = 32;
    }

    TEST_F(HwsFdCacheTest, HwsFdReadStat004) {
        class HwsFdReadStatVec testReadStat(8);
        gReadStatSize = 8;
        EXPECT_EQ(testReadStat.IsSequential(), false);
        testReadStat.Add("/home/test",0, 512*1024);
        testReadStat.Add("/home/test",1*512*1024, 512*1024);
        testReadStat.Add("/home/test",2*512*1024, 512*1024);
        testReadStat.Add("/home/test",3*512*1024, 512*1024);
        testReadStat.Add("/home/test",4*512*1024, 512*1024);
        testReadStat.Add("/home/test",8*512*1024, 512*1024);
        testReadStat.Add("/home/test",9*512*1024, 512*1024);
        EXPECT_EQ(testReadStat.IsSequential(), false);
        testReadStat.Add("/home/test",11*512*1024, 512*1024);
        EXPECT_EQ(testReadStat.IsSequential(), true);
        EXPECT_EQ(testReadStat.GetOffset(), 10*512*1024);
        gReadStatSize = 32;
    }

    TEST_F(HwsFdCacheTest, HwsFdReadStat005) {
        class HwsFdReadStatVec testReadStat(8);
        gReadStatSize = 8;
        EXPECT_EQ(testReadStat.IsSequential(), false);
        testReadStat.Add("/home/test",0, 512*1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        testReadStat.Add("/home/test",1*512*1024, 512*1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        testReadStat.Add("/home/test",2*512*1024, 512*1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        testReadStat.Add("/home/test",3*512*1024, 512*1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        testReadStat.Add("/home/test",4*512*1024, 512*1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        testReadStat.Add("/home/test",5*512*1024, 512*1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        testReadStat.Add("/home/test",6*512*1024, 512*1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        testReadStat.Add("/home/test",7*512*1024, 512*1024);
        EXPECT_EQ(testReadStat.IsSequential(), false);
        gReadStatSize = 32;
    }

    TEST_F(HwsFdCacheTest, HwsFdReadHit001) {
        char buf[1024*1024] = {0};
        s3fs_write_hw_obs_proc_test(buf, 1024*1024, 0);
        std::mutex write_merge_mutex_;
        std::mutex read_ahead_mutex_;
        class HwsFdReadPageList testPageList(1024*1024, read_ahead_mutex_);
        class HwsFdWritePageList testWritePageList(1024*1024,write_merge_mutex_);
        testPageList.ReadAhead("/home/test", 0, testWritePageList,1024*1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        testPageList.Clean();
    }

    TEST_F(HwsFdCacheTest, HwsFdRead001) {
        gReadPageSize = READ_PAGE_SIZE;
        char buf[READ_PAGE_SIZE] = {0};
        bool oldIsReadWaitCache = gIsReadWaitCache;
        gIsReadWaitCache = false;
        off64_t offset = 0;
        unsigned int expectHitNum = 0;
        g_ReadAllHitNum = 0;

        int loop = 0;
        for(loop = 0; loop < 12; loop++, offset += READ_PAGE_SIZE)
            s3fs_write_hw_obs_proc_test(buf, READ_PAGE_SIZE, offset);
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, READ_PAGE_SIZE*6);
        ent0 = HwsFdManager::GetInstance().Get(0);
        for(loop = 0, offset = 0; loop < 32; loop++) {
            ent0->Read(buf, offset, 1024*128);
            offset += 1024*128;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        cout << "HwsFdRead001 1 now read offset is: " << offset << std::endl;
        for(loop = 0; loop < READ_PAGE_SIZE/1024/128/2; loop++) {
            ent0->Read(buf, offset, 1024*128);
            offset += 1024*128;
            expectHitNum++;
        }
        EXPECT_EQ(expectHitNum, g_ReadAllHitNum);
        
        cout << "HwsFdRead001 2 now read offset is: " << offset << std::endl;
        expectHitNum = 0;
        g_ReadAllHitNum = 0;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        for(loop = 0; loop < READ_PAGE_SIZE/1024/128; loop++) {
            ent0->Read(buf, offset, 1024*128);
            offset += 1024*128;
            expectHitNum++;
        }
        /*gIsReadWaitCache is false,so g_ReadAllHitNum less than expectHitNum*/
        cout << "HwsFdRead001 g_ReadAllHitNum=" << g_ReadAllHitNum
           << ",expectHitNum=" << expectHitNum << std::endl;
        if (g_ReadAllHitNum > expectHitNum)
        {
            EXPECT_EQ(true, false);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        HwsFdManager::GetInstance().Close(0);
        gIsReadWaitCache = oldIsReadWaitCache;

    }

    TEST_F(HwsFdCacheTest, HwsFdRead002) {
        gReadPageSize = READ_PAGE_SIZE;
        char buf[READ_PAGE_SIZE] = {0};
        bool oldIsReadWaitCache = gIsReadWaitCache;
        gIsReadWaitCache = true;
        int loop = 0;
        off64_t offset = 0;
        unsigned int expectHitNum = 0;
        g_ReadAllHitNum = 0;
        for(loop = 0; loop < 12; loop++, offset += READ_PAGE_SIZE)
            s3fs_write_hw_obs_proc_test(buf, READ_PAGE_SIZE, offset);
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, READ_PAGE_SIZE*6);
        ent0 = HwsFdManager::GetInstance().Get(0);
        for(loop = 0, offset = 0; loop < 32; loop++) {
            ent0->Read(buf, offset, 1024*128);
            offset += 1024*128;
        }
        EXPECT_EQ(recv_times.load(), 32);
        ent0->Read(buf, offset, 1024*128);
        offset += 1024*128;
        expectHitNum++;
        EXPECT_EQ(expectHitNum, g_ReadAllHitNum);
        cout << "HwsFdRead002 1 now read end is: " << offset << std::endl;
        for(loop = 0; loop < 47; loop++) {
            ent0->Read(buf, offset, 1024*128);
            offset += 1024*128;
            expectHitNum++;
        }
        EXPECT_EQ(expectHitNum, g_ReadAllHitNum);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        cout << "HwsFdRead002 2 now read end is: " << offset << std::endl;
        for(loop = 0; loop < 48; loop++) {
            ent0->Read(buf, offset, 1024*128);
            offset += 1024*128;
            expectHitNum++;
        }
        EXPECT_EQ(expectHitNum, g_ReadAllHitNum);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        cout << "HwsFdRead002 3 now read end is: " << offset << std::endl;
        for(loop = 0; loop < 48; loop++) {
            ent0->Read(buf, offset, 1024*128);
            offset += 1024*128;
            expectHitNum++;
        }
        EXPECT_EQ(expectHitNum, g_ReadAllHitNum);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        cout << "HwsFdRead002 4 now read end is: " << offset << std::endl;
        for(loop = 0; loop < 48; loop++) {
            ent0->Read(buf, offset, 1024*128);
            offset += 1024*128;
            expectHitNum++;
        }
        EXPECT_EQ(expectHitNum, g_ReadAllHitNum);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        cout << "now recv_times=" << recv_times.load() << std::endl;
        if (recv_times.load() > 34 + gReadPageNum)
        {
            EXPECT_EQ(true, false);
        }
        HwsFdManager::GetInstance().Close(0);
        gIsReadWaitCache = oldIsReadWaitCache;
    }

    TEST_F(HwsFdCacheTest, HwsFdRead003) {
        gReadPageSize = READ_PAGE_SIZE;
        char buf[READ_PAGE_SIZE] = {0};
        int loop = 0;
        off64_t offset = 0;
        unsigned int expectHitNum = 0;
        g_ReadAllHitNum = 0;

        for(loop = 0; loop < 12; loop++, offset += READ_PAGE_SIZE)
            s3fs_write_hw_obs_proc_test(buf, READ_PAGE_SIZE, offset);
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, READ_PAGE_SIZE*2);

        ent0 = HwsFdManager::GetInstance().Get(0);
        size_t read_size = READ_PAGE_SIZE/32/2/4;
        offset = 0;
        for(loop = 0; loop < 32; loop++) {
            ent0->Read(buf, offset, read_size);
            offset += read_size;
        }
        EXPECT_EQ(recv_times.load(), 32);
        cout << "HwsFdRead003 1 now read end is: " << offset << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        for(loop = 0; loop < 32; loop++) {
            ent0->Read(buf, offset, read_size);
            offset += read_size;
            expectHitNum++;
        }
        EXPECT_EQ(expectHitNum, g_ReadAllHitNum);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        for(loop = 0; loop < 32; loop++) {
            ent0->Read(buf, offset, read_size);
            offset += read_size;
            expectHitNum++;
        }
        cout << "HwsFdRead003 2 now read end is: " << offset << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        for(loop = 0; loop < 32; loop++) {
            ent0->Read(buf, offset, read_size);
            offset += read_size;
            expectHitNum++;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(expectHitNum, g_ReadAllHitNum);
        cout << "HwsFdRead003 3 now read end is: " << offset << std::endl;
        for(loop = 0; loop < 32; loop++) {
            ent0->Read(buf, offset, read_size);
            offset += read_size;
            expectHitNum++;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(expectHitNum, g_ReadAllHitNum);
        HwsFdManager::GetInstance().Close(0);
        cout << "now recv_times=" << recv_times.load() << std::endl;
        if (recv_times.load() > 32 + gReadPageNum)
        {
            EXPECT_EQ(true, false);
        }
    }

    TEST_F(HwsFdCacheTest, HwsFdReadWrite001) {
        gWritePageSize = WRITE_PAGE_SIZE;
        char buf[WRITE_PAGE_SIZE] = {0};
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        ent0->Write(buf, 0, WRITE_PAGE_SIZE/2);
        EXPECT_EQ(send_times.load(), 0);
        ent0->Write(buf, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2);
        ent0->Write(buf, WRITE_PAGE_SIZE, gFuseMaxReadSize);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(send_times.load(), 1);
        ent0->Read(buf, 0, WRITE_PAGE_SIZE/2);
        EXPECT_EQ(recv_times.load(), 1);
        /*read from last page,not flush last page*/
        ent0->Read(buf, WRITE_PAGE_SIZE, gFuseMaxReadSize);
        EXPECT_EQ(send_times.load(), 1);
        EXPECT_EQ(recv_times.load(), 1);
        HwsFdManager::GetInstance().Close(0);
    }

    TEST_F(HwsFdCacheTest, HwsFdReadWrite002) {
        gWritePageSize = WRITE_PAGE_SIZE;
        gReadPageSize = READ_PAGE_SIZE;
        char buf[READ_PAGE_SIZE] = {0};
        off64_t offset = 0;
        unsigned int expectHitNum = 0;
        g_ReadAllHitNum = 0;
        g_ReadNotHitNum = 0;

        int loop = 0;
        for(loop = 0; loop < 4; loop++)
            s3fs_write_hw_obs_proc_test(buf, READ_PAGE_SIZE, offset += READ_PAGE_SIZE);
        send_times.store(0);
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, READ_PAGE_SIZE*4);
        ent0 = HwsFdManager::GetInstance().Get(0);

        offset = 0;
        size_t read_size = READ_PAGE_SIZE/48;
        for(loop = 0; loop < 32; loop++) {
            ent0->Read(buf, offset, read_size);
            offset += read_size;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        for(loop = 0; loop < 24; loop++) {
            ent0->Read(buf, offset, read_size);
            offset += read_size;
            expectHitNum++;
        }
        cout << "HwsFdReadWrite002 1 now read end is: " << offset << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(expectHitNum, g_ReadAllHitNum);
        /*write 4194304 page,disable 4194304 read page*/
        ent0->Write(buf,  4194304, 1024*128);
        EXPECT_EQ(send_times.load(), 1);

        g_ReadNotHitNum = 0;
        ent0->Read(buf,  4194304, 1024*128);
        EXPECT_EQ(g_ReadNotHitNum, 1);

        ent0->Write(buf,  10485760, 1024*128);
        EXPECT_EQ(send_times.load(), 2);
        ent0->Read(buf,  10485760, 1024*128);
        EXPECT_EQ(g_ReadNotHitNum, 2);
        HwsFdManager::GetInstance().Close(0);

        cout << "now recv_times=" << recv_times.load() << std::endl;
        if (recv_times.load() > 34 + gReadPageNum)
        {
            EXPECT_EQ(true, false);
        }
    }

    TEST_F(HwsFdCacheTest, HwsFdReadWrite003) {
        gWritePageSize = READ_PAGE_SIZE;
        gReadPageSize = READ_PAGE_SIZE;
        char buf[READ_PAGE_SIZE] = {0};

        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        ent0->Write(buf,  0, READ_PAGE_SIZE);
        ent0->Write(buf,  READ_PAGE_SIZE, READ_PAGE_SIZE/2);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(send_times.load(), 1);
        off64_t offset = 0;
        int loop = 0;
        unsigned int expectNotHitNum = 0;

        offset = 0;
        size_t read_size = 128 * 1024;
        for(loop = 0; loop < 32; loop++) {
            ent0->Read(buf, offset, read_size);
            offset += read_size;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        /*read head overlap with write,not read ahead*/
        g_ReadNotHitNum = 0;
        for(loop = 0; loop < 16; loop++) {
            ent0->Read(buf, offset, read_size);
            offset += read_size;
            expectNotHitNum++;
        }
        EXPECT_EQ(expectNotHitNum, g_ReadNotHitNum);
        HwsFdManager::GetInstance().Close(0);

    }
}

TEST_F(HwsFdCacheTest, HwsFdReadWrite004) {
	gWritePageSize = WRITE_PAGE_SIZE;
	char buf[WRITE_PAGE_SIZE] = {0};
	std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
	ent0 = HwsFdManager::GetInstance().Get(0);
	ent0->Write(buf, 0, WRITE_PAGE_SIZE/2);
	EXPECT_EQ(send_times.load(), 0);
	ent0->Write(buf, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2);
	ent0->Write(buf, WRITE_PAGE_SIZE, gFuseMaxReadSize);
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	EXPECT_EQ(send_times.load(), 1);
    /*if only right part overlap last page,flush last page*/
	ent0->Read(buf, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE);
	EXPECT_EQ(recv_times.load(), 1);
	EXPECT_EQ(send_times.load(), 2);
        HwsFdManager::GetInstance().Close(0);
}

    TEST_F(HwsFdCacheTest, HwsFdReadWrite005) {
        gWritePageSize = WRITE_PAGE_SIZE;

        char* bufW = (char *)malloc(WRITE_PAGE_SIZE);
        char* bufR = (char *)malloc(WRITE_PAGE_SIZE);
        memset(bufW, 0, WRITE_PAGE_SIZE);
        memset(bufR, 0, WRITE_PAGE_SIZE);
        ReadFromTemplate(bufW, WRITE_PAGE_SIZE, 0, WRITE_PAGE_SIZE);

       // std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/tmp/hws_fd_cache_file", 0, 0);
        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        ent0->Write(bufW, 0, WRITE_PAGE_SIZE);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(send_times.load(), 1);
        ent0->Read(bufR, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2);

        bool bRet = CheckReadWithTemplate(bufR, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2);
        free(bufW);
        free(bufR);
        EXPECT_EQ(bRet, true);
        HwsFdManager::GetInstance().Close(0);
    }

    TEST_F(HwsFdCacheTest, HwsFdSeqWrite001) {
        gWritePageSize = WRITE_PAGE_SIZE;

        char* bufWrite = (char *)malloc(WRITE_PAGE_SIZE);
        memset(bufWrite, 0, WRITE_PAGE_SIZE);

        char* bufRead = (char *)malloc(WRITE_PAGE_SIZE);
        memset(bufRead, 0, WRITE_PAGE_SIZE);

        ReadFromTemplate(bufWrite, WRITE_PAGE_SIZE, 0, WRITE_PAGE_SIZE);

        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        //write 6M data
        ent0->Write(bufWrite, 0, WRITE_PAGE_SIZE);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        //Sequential Modify
        ent0->Write(bufWrite + WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2, 128*1024);
        ent0->Write(bufWrite + WRITE_PAGE_SIZE/2+128*1024, WRITE_PAGE_SIZE/2+128*1024, 128*1024);
        ent0->Write(bufWrite + WRITE_PAGE_SIZE/2+2*128*1024, WRITE_PAGE_SIZE/2+2*128*1024, 128*1024);
        ent0->Write(bufWrite + WRITE_PAGE_SIZE/2+3*128*1024, WRITE_PAGE_SIZE/2+3*128*1024, 128*1024);
        ent0->Write(bufWrite + WRITE_PAGE_SIZE/2+4*128*1024, WRITE_PAGE_SIZE/2+4*128*1024, 128*1024);
        //timeout
        std::this_thread::sleep_for(std::chrono::milliseconds(HWS_WRITE_PAGE_WAIT_TIME_MS+100));

        EXPECT_EQ(send_times.load(), 5);
        EXPECT_EQ(ent0->GetWriteMode(), MODIFY);
        //read(3M,3M)
        ent0->Read(bufRead, 0, WRITE_PAGE_SIZE);
        EXPECT_EQ(recv_times.load(), 1);
        //check read and write
        bool bRet = CheckReadWithTemplate(bufRead, 0, WRITE_PAGE_SIZE);
        free(bufWrite);
        free(bufRead);
        EXPECT_EQ(bRet, true);

        HwsFdManager::GetInstance().Close(0);
    }

    TEST_F(HwsFdCacheTest, HwsFdSeqWrite002) {
        gWritePageSize = WRITE_PAGE_SIZE;
        char buf[WRITE_PAGE_SIZE] = {0};

        ReadFromTemplate(buf, WRITE_PAGE_SIZE, 0, WRITE_PAGE_SIZE);

        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        //append 5M data in 3 times
        ent0->Write(buf, 0, WRITE_PAGE_SIZE/2);
        ent0->Write(buf + WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/6);
        ent0->Write(buf + WRITE_PAGE_SIZE/2 + WRITE_PAGE_SIZE/6, WRITE_PAGE_SIZE/2 + WRITE_PAGE_SIZE/6, WRITE_PAGE_SIZE/6);
        EXPECT_EQ(ent0->GetWriteMode(), APPEND);
        // 2 seq random modify write
        ent0->Write(buf + WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2, 128*1024);
        ent0->Write(buf + WRITE_PAGE_SIZE/2 + 128*1024, WRITE_PAGE_SIZE/2 + 128*1024, 128*1024);

        EXPECT_EQ(ent0->GetWriteMode(), THROUGH);
        EXPECT_EQ(send_times.load(), 3);
        //read(3M,1M)
        ent0->Read(buf, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/6);
        EXPECT_EQ(recv_times.load(), 1);
        //check read and write
        bool bRet = CheckReadWithTemplate(buf, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/6);
        EXPECT_EQ(bRet, true);

        HwsFdManager::GetInstance().Close(0);
    }


    TEST_F(HwsFdCacheTest, HwsFdSeqWrite003) {
        gWritePageSize = WRITE_PAGE_SIZE;
        char buf[WRITE_PAGE_SIZE] = {0};

        ReadFromTemplate(buf, WRITE_PAGE_SIZE, 0, WRITE_PAGE_SIZE);

        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);
        //append 5M data in 2 times
        ent0->Write(buf, 0, WRITE_PAGE_SIZE/2);
        ent0->Write(buf + WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/3);
        EXPECT_EQ(ent0->GetWriteMode(), APPEND);
        // 4 seq random modify write
        ent0->Write(buf + WRITE_PAGE_SIZE/6, WRITE_PAGE_SIZE/6, WRITE_PAGE_SIZE/6);
        ent0->Write(buf + WRITE_PAGE_SIZE/3, WRITE_PAGE_SIZE/3, WRITE_PAGE_SIZE/6);
        ent0->Write(buf + WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/6);
        ent0->Write(buf + 2*WRITE_PAGE_SIZE/3, 2*WRITE_PAGE_SIZE/3, WRITE_PAGE_SIZE/6);
        EXPECT_EQ(send_times.load(), 4);
        EXPECT_EQ(ent0->GetWriteMode(), MODIFY);
        //?/read(0M,5M)
        std::this_thread::sleep_for(std::chrono::milliseconds(HWS_WRITE_PAGE_WAIT_TIME_MS+100));
        ent0->Read(buf, 0, 5*WRITE_PAGE_SIZE/6);
        EXPECT_EQ(recv_times.load(), 1);
        //check read and write
        bool bRet = CheckReadWithTemplate(buf, 0, 5*WRITE_PAGE_SIZE/6);
        EXPECT_EQ(bRet, true);

        HwsFdManager::GetInstance().Close(0);
    }

	TEST_F(HwsFdCacheTest, HwsFdSeqWrite004) {
	    gWritePageSize = WRITE_PAGE_SIZE;
	    char buf[WRITE_PAGE_SIZE] = {0};
	    char* bufModify = NULL;
	    bufModify = (char*)malloc(WRITE_PAGE_SIZE);
	    memset(bufModify,0,WRITE_PAGE_SIZE);

	    ReadFromTemplate(buf, WRITE_PAGE_SIZE, 0, WRITE_PAGE_SIZE);
	    std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
	    ent0 = HwsFdManager::GetInstance().Get(0);
	    //append 5M data in 2 times
	    ent0->Write(buf, 0, WRITE_PAGE_SIZE);
	    // 4 seq random modify write
	    ent0->Write(bufModify + WRITE_PAGE_SIZE/6, WRITE_PAGE_SIZE/6, WRITE_PAGE_SIZE/6);
	    ent0->Write(bufModify + WRITE_PAGE_SIZE/3, WRITE_PAGE_SIZE/3, WRITE_PAGE_SIZE/6);
	    ent0->Write(bufModify + WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/6);
	    ent0->Write(bufModify + 2*WRITE_PAGE_SIZE/3, 2*WRITE_PAGE_SIZE/3, WRITE_PAGE_SIZE/6);
	    EXPECT_EQ(send_times.load(), 4);
	    EXPECT_EQ(ent0->GetWriteMode(), MODIFY);
	    //read(0M,6M)
	    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
	    ent0->Read(buf, 0, WRITE_PAGE_SIZE);
	    EXPECT_EQ(recv_times.load(), 1);
	    //check read and write
	    bool bRet = CheckReadWithTemplate(buf, 0, WRITE_PAGE_SIZE);
	    free(bufModify);
	    EXPECT_EQ(bRet, false);

	    HwsFdManager::GetInstance().Close(0);
	}

	TEST_F(HwsFdCacheTest, HwsFdSeqWrite005) {
	    gWritePageSize = WRITE_PAGE_SIZE;
	    char buf[WRITE_PAGE_SIZE] = {0};
	    ReadFromTemplate(buf, WRITE_PAGE_SIZE, 0, WRITE_PAGE_SIZE);

	    std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
	    ent0 = HwsFdManager::GetInstance().Get(0);
	    //append 5M data in 2 times
	    ent0->Write(buf, 0, WRITE_PAGE_SIZE/2);
	    ent0->Write(buf + WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/3);
	    EXPECT_EQ(ent0->GetWriteMode(), APPEND);
	    // 4 seq random modify write
	    ent0->Write(buf + WRITE_PAGE_SIZE/6, WRITE_PAGE_SIZE/6, WRITE_PAGE_SIZE/6);
	    ent0->Write(buf + WRITE_PAGE_SIZE/3, WRITE_PAGE_SIZE/3, WRITE_PAGE_SIZE/6);
	    ent0->Write(buf + WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/6);
	    ent0->Write(buf + 2*WRITE_PAGE_SIZE/3, 2*WRITE_PAGE_SIZE/3, WRITE_PAGE_SIZE/6);
	    EXPECT_EQ(send_times.load(), 4);
	    EXPECT_EQ(ent0->GetWriteMode(), MODIFY);
	    //add 1M data to file tail
	    ent0->Write(buf + 5*WRITE_PAGE_SIZE/6, 5*WRITE_PAGE_SIZE/6, WRITE_PAGE_SIZE/6);
	    EXPECT_EQ(ent0->GetWriteMode(), APPEND);
	    EXPECT_EQ(send_times.load(), 4);
	    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
	    //read(0M,6M)
	    ent0->Read(buf, 0, WRITE_PAGE_SIZE);
	    EXPECT_EQ(recv_times.load(), 1);
	    //check read and write
	    bool bRet = CheckReadWithTemplate(buf, 0, WRITE_PAGE_SIZE);
	    EXPECT_EQ(bRet,true);
	    HwsFdManager::GetInstance().Close(0);
	}

	TEST_F(HwsFdCacheTest, HwsFdSeqWrite006) {
	    gWritePageSize = WRITE_PAGE_SIZE;
	    char buf[WRITE_PAGE_SIZE] = {0};
	    //write 5M data
	    ReadFromTemplate(buf, WRITE_PAGE_SIZE, 0, WRITE_PAGE_SIZE);

	    std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
	    ent0 = HwsFdManager::GetInstance().Get(0);
	    ent0->Write(buf, 0, 5*WRITE_PAGE_SIZE/6);
	    // 4 random modify write
	    ent0->Write(buf, 0, WRITE_PAGE_SIZE/6);
	    ent0->Write(buf + WRITE_PAGE_SIZE/6, WRITE_PAGE_SIZE/6, WRITE_PAGE_SIZE/6);
	    ent0->Write(buf + WRITE_PAGE_SIZE/3, WRITE_PAGE_SIZE/3, WRITE_PAGE_SIZE/6);
	    ent0->Write(buf + WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/6);
	    EXPECT_EQ(send_times.load(), 4);
	    EXPECT_EQ(ent0->GetWriteMode(), MODIFY);
	    //append 1M data
	    ent0->Write(buf + 5*WRITE_PAGE_SIZE/6, 5*WRITE_PAGE_SIZE/6, WRITE_PAGE_SIZE/6);
	    std::this_thread::sleep_for(std::chrono::milliseconds(100));
	    EXPECT_EQ(send_times.load(), 5);
	    EXPECT_EQ(ent0->GetWriteMode(), APPEND);
	    std::this_thread::sleep_for(std::chrono::milliseconds(HWS_WRITE_PAGE_WAIT_TIME_MS+100));
	    EXPECT_EQ(send_times.load(), 6);
	    //read(0M,6M)
	    ent0->Read(buf, 0, WRITE_PAGE_SIZE);
	    EXPECT_EQ(recv_times.load(), 1);
	    //check read and write
	    bool bRet = CheckReadWithTemplate(buf, 0, WRITE_PAGE_SIZE);
	    EXPECT_EQ(bRet, true);

	    HwsFdManager::GetInstance().Close(0);
	}

	TEST_F(HwsFdCacheTest, HwsFdSeqWrite007) {
	    gWritePageSize = WRITE_PAGE_SIZE;
	    char buf[WRITE_PAGE_SIZE] = {0};
	    char* bufRead = NULL;
	    std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
	    ent0 = HwsFdManager::GetInstance().Get(0);
	    //write 12M data in two times
	    ReadFromTemplate(buf, WRITE_PAGE_SIZE, 0, WRITE_PAGE_SIZE);
	    ent0->Write(buf, 0, WRITE_PAGE_SIZE);
	    std::this_thread::sleep_for(std::chrono::milliseconds(100));
	    // 4 random modify write
	    ent0->Write(buf + WRITE_PAGE_SIZE/6, WRITE_PAGE_SIZE/6, WRITE_PAGE_SIZE/6);
	    ent0->Write(buf + WRITE_PAGE_SIZE/3, WRITE_PAGE_SIZE/3, WRITE_PAGE_SIZE/6);
	    ent0->Write(buf + WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/6);
	    ent0->Write(buf + 2*WRITE_PAGE_SIZE/3, 2*WRITE_PAGE_SIZE/3, WRITE_PAGE_SIZE/6);
	    EXPECT_EQ(send_times.load(), 4);
	    cout <<"HwsFdSeqWrite006:1"<<"Mode"<< ent0->GetWriteMode() <<endl;
	    EXPECT_EQ(ent0->GetWriteMode(), MODIFY);
	    // 4 random modify write
	    ent0->Write(buf + WRITE_PAGE_SIZE/2, WRITE_PAGE_SIZE/2, 256*1024);
	    ent0->Write(buf + WRITE_PAGE_SIZE/2 + 256*1024, WRITE_PAGE_SIZE/2 + 256*1024, 256*1024);
	    ent0->Write(buf + WRITE_PAGE_SIZE/2 + 2*256*1024, WRITE_PAGE_SIZE/2 + 2*256*1024, 256*1024);
	    ent0->Write(buf + WRITE_PAGE_SIZE/2 + 3*256*1024, WRITE_PAGE_SIZE/2 + 3*256*1024, 256*1024);
	    cout <<"HwsFdSeqWrite006:2"<<"Mode"<< ent0->GetWriteMode() <<endl;
	    EXPECT_EQ(ent0->GetWriteMode(), MODIFY);
	    EXPECT_EQ(send_times.load(), 8);
	    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
	    //read(0M,6M)
	    bufRead = (char*)malloc(WRITE_PAGE_SIZE);
	    ent0->Read(bufRead, 0, WRITE_PAGE_SIZE);
	    EXPECT_EQ(recv_times.load(), 1);
	    //check read and write
	    bool bRet = CheckReadWithTemplate(bufRead, 0, WRITE_PAGE_SIZE);
	    free(bufRead);
	    EXPECT_EQ(bRet, true);

	    HwsFdManager::GetInstance().Close(0);
	}

TEST_F(HwsFdCacheTest, HwsFdReadCheckData001) {
    char buf[READ_PAGE_SIZE] = {0};
    off64_t offset = 0;
    int loop = 0;
    int readPageNum = 0;
    int readBytes = 0;
    bool bCheckResult = true;

    if (TEST_TEMPLATE_FILE_SIZE/READ_PAGE_SIZE > 2)
    {
        readPageNum = TEST_TEMPLATE_FILE_SIZE/READ_PAGE_SIZE -2;
    }
    else
    {
        EXPECT_EQ(true, false);
        return;
    }

    for(loop = 0; loop < (readPageNum+1); loop++, offset += READ_PAGE_SIZE)
    {
        readBytes = ReadFromTemplate(buf, READ_PAGE_SIZE, offset,READ_PAGE_SIZE);
        EXPECT_EQ(readBytes, READ_PAGE_SIZE);
        s3fs_write_hw_obs_proc_test(buf, READ_PAGE_SIZE, offset);
    }

    std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, READ_PAGE_SIZE*readPageNum);
    ent0 = HwsFdManager::GetInstance().Get(0);
    offset = 0;
    for(loop = 0; loop < readPageNum*READ_PAGE_SIZE/1024/128; loop++) {
        ent0->Read(buf, offset, 1024*128);
        bCheckResult = CheckReadWithTemplate(buf, offset, 1024*128);
        EXPECT_EQ(bCheckResult, true);
        ent0->Read(buf, offset+1024*64, 1024*128);
        bCheckResult = CheckReadWithTemplate(buf, offset+1024*64, 1024*128);
        EXPECT_EQ(bCheckResult, true);
        offset += 1024*128;
    }
	HwsFdManager::GetInstance().Close(0);
}

TEST_F(HwsFdCacheTest, HwsFdWholeCacheCheckData001) {
    char buf[READ_PAGE_SIZE] = {0};
    off64_t offset = 0;
    int loop = 0;
    int readPageNum = 0;
    int readBytes = 0;
    bool bCheckResult = true;
    off64_t fileSize = 0;

    if (TEST_TEMPLATE_FILE_SIZE/READ_PAGE_SIZE > 2)
    {
        readPageNum = TEST_TEMPLATE_FILE_SIZE/READ_PAGE_SIZE -2;
    }
    else
    {
        EXPECT_EQ(true, false);
        return;
    }

    for(loop = 0; loop < (readPageNum+1); loop++, offset += READ_PAGE_SIZE)
    {
        readBytes = ReadFromTemplate(buf, READ_PAGE_SIZE, offset,READ_PAGE_SIZE);
        EXPECT_EQ(readBytes, READ_PAGE_SIZE);
        s3fs_write_hw_obs_proc_test(buf, READ_PAGE_SIZE, offset);
        fileSize += READ_PAGE_SIZE;
    }

    g_wholeFileCacheSwitch = true;
    g_wholeFileCachePreReadInterval = 10;
    g_wholeFileCacheMaxRecycleTime = 20;
    g_wholeFileCacheMaxHitTimes = 1000;

    std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, READ_PAGE_SIZE*readPageNum);
    ent0 = HwsFdManager::GetInstance().Get(0);
    offset = 0;
    for(loop = 0; loop < 1000; loop++) {
        offset = rand() % (fileSize - 128 * 1024 + 1);
        ent0->Read(buf, offset, 1024*128);
        bCheckResult = CheckReadWithTemplate(buf, offset, 1024*128);
        EXPECT_EQ(bCheckResult, true);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    while (ent0->GetWholeCacheStartedFlag())
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
	HwsFdManager::GetInstance().Close(0);
	HwsFdManager::GetInstance().DestroyWholeFileCacheTask(true);
}

TEST_F(HwsFdCacheTest, TestFdDynamicPagenum)
{
    bool bCheckResult = true;
    off64_t oldMaxCacheSize = gMaxCacheMemSize;
    /*check free cache size*/
    off64_t freeCacheSize = HwsFdManager::GetInstance().GetFreeCacheMemSize();
    if (freeCacheSize != gMaxCacheMemSize)
    {
        cout << "page not free,freeCacheSize=" << freeCacheSize <<
            ",maxCacheSize=" << gMaxCacheMemSize << std::endl;
        EXPECT_EQ(true, false);
    }
    gWritePageSize = WRITE_PAGE_SIZE;
    unsigned int writePageNum = 0;

    if (TEST_TEMPLATE_FILE_SIZE/WRITE_PAGE_SIZE > 2)
    {
        writePageNum = TEST_TEMPLATE_FILE_SIZE/READ_PAGE_SIZE -2;
    }

    char buf[WRITE_PAGE_SIZE] = {0};
    unsigned int i;
    off64_t offset = 0;
    size_t size = 1024*128;
    int readBytes = 0;
    struct timespec now_ts;

    gPrintCacheStatisMs = 0;
    HwsCacheStatis::GetInstance().PrintStatisAndClear();
    gPrintCacheStatisMs = 300000;

    std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
    ent0 = HwsFdManager::GetInstance().Get(0);
    for(i = 0; i < (writePageNum+1)*WRITE_PAGE_SIZE/size; i++, offset+=size)
    {
        /*change gMaxCacheMemSize,so change max page num*/
        clock_gettime(CLOCK_MONOTONIC, &now_ts);
        gMaxCacheMemSize = max((off64_t)WRITE_PAGE_SIZE,WRITE_PAGE_SIZE *
            (off64_t)(now_ts.tv_nsec % (gWritePageNum + 2)));
        readBytes = ReadFromTemplate(buf,WRITE_PAGE_SIZE,offset,size);
        EXPECT_EQ(readBytes, size);
        if (readBytes != (int)size)
        {
            break;
        }
        ent0->Write(buf, offset, size);
    }

    offset = 0;
    for(i = 0; i < writePageNum*READ_PAGE_SIZE/1024/128; i++)
    {
        /*change gMaxCacheMemSize,so change max page num*/
        clock_gettime(CLOCK_MONOTONIC, &now_ts);
        gMaxCacheMemSize = max((off64_t)WRITE_PAGE_SIZE,WRITE_PAGE_SIZE *
            (off64_t)(now_ts.tv_nsec % (gWritePageNum + 2)));

        ent0->Read(buf, offset, 1024*128);
        bCheckResult = CheckReadWithTemplate(buf, offset, 1024*128);
        EXPECT_EQ(bCheckResult, true);
        ent0->Read(buf, offset+1024*64, 1024*128);
        bCheckResult = CheckReadWithTemplate(buf, offset+1024*64, 1024*128);
        EXPECT_EQ(bCheckResult, true);
        offset += 1024*128;
    }

    HwsFdManager::GetInstance().Close(0);
    gMaxCacheMemSize = oldMaxCacheSize;
    gPrintCacheStatisMs = 0;
    HwsCacheStatis::GetInstance().PrintStatisAndClear();
    gPrintCacheStatisMs = 300000;
    
}
TEST_F(HwsFdCacheTest, Test_ReadLastWritePageAllocFail) {
    gWritePageSize = WRITE_PAGE_SIZE;
    char buf[WRITE_PAGE_SIZE] = {0};
    std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/ReadLastWritePageAllocFail", 0, 0);
    ent0 = HwsFdManager::GetInstance().Get(0);
    off64_t offset = 0;
    unsigned int i;
    bool bCheckResult = true;
    size_t size = 1024*128;
    int readBytes = 0;

    for(i = 0; i < (WRITE_PAGE_SIZE+HWS_MB_SIZE)/1024/128; i++, offset+=size)
    {
        readBytes = ReadFromTemplate(buf,WRITE_PAGE_SIZE,offset,size);
        EXPECT_EQ(readBytes, size);
        if (readBytes != (int)size)
        {
            break;
        }
        ent0->Write(buf, offset, size);
    }

    g_test_simulate_alloc_fail = 3;  /*simulate new write page fail*/
    offset = WRITE_PAGE_SIZE;
    ent0->Read(buf, offset, 1024*128);
    bCheckResult = CheckReadWithTemplate(buf, offset, 1024*128);
    EXPECT_EQ(bCheckResult, true);
	EXPECT_EQ(0,g_test_simulate_alloc_fail);
    HwsFdManager::GetInstance().Close(0);
}

TEST_F(HwsFdCacheTest, TestAllocWritePageFail) {
	gWritePageSize = WRITE_PAGE_SIZE;
	char buf[WRITE_PAGE_SIZE] = {0};
    unsigned int writePageNum = 10;
    off64_t offset = 0;
    size_t size = 1024*128;
    int readBytes = 0;
    bool bCheckResult = true;
    unsigned int i;

    g_test_simulate_alloc_fail = 3;  /*simulate new write page fail*/
	std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/TestAllocWritePageFail", 0, 0);
	ent0 = HwsFdManager::GetInstance().Get(0);
    for(i = 0; i < (writePageNum+1)*WRITE_PAGE_SIZE/size; i++, offset+=size)
    {
        readBytes = ReadFromTemplate(buf,WRITE_PAGE_SIZE,offset,size);
        EXPECT_EQ(readBytes, size);
        if (readBytes != (int)size)
        {
            break;
        }
        ent0->Write(buf, offset, size);
    }
	EXPECT_EQ(0,g_test_simulate_alloc_fail);

    offset = 0;
    for(i = 0; i < writePageNum*READ_PAGE_SIZE/1024/128; i++)
    {
        ent0->Read(buf, offset, 1024*128);
        bCheckResult = CheckReadWithTemplate(buf, offset, 1024*128);
        EXPECT_EQ(bCheckResult, true);
        ent0->Read(buf, offset+1024*64, 1024*128);
        bCheckResult = CheckReadWithTemplate(buf, offset+1024*64, 1024*128);
        EXPECT_EQ(bCheckResult, true);
        offset += 1024*128;
    }
	HwsFdManager::GetInstance().Close(0);

}
TEST_F(HwsFdCacheTest, TestAllocWritePageTimeout) {
	gWritePageSize = WRITE_PAGE_SIZE;
	char buf[WRITE_PAGE_SIZE] = {0};
    unsigned int writePageNum = 1;
    off64_t offset = 0;
    size_t size = 1024*128;
    int readBytes = 0;
    bool bCheckResult = true;
    unsigned int i;

    /*simulate new write page fail timeout*/
    g_test_simulate_alloc_fail = 20 + HWS_MAX_WRITE_RETRY_MS/HWS_ALLOC_MEM_RETRY_MS;
    g_AllocWritePageTimeoutNum = 0;
	std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/TestAllocWritePageTimeout", 0, 0);
	ent0 = HwsFdManager::GetInstance().Get(0);
    for(i = 0; i < (writePageNum+1)*WRITE_PAGE_SIZE/size; i++, offset+=size)
    {
        readBytes = ReadFromTemplate(buf,WRITE_PAGE_SIZE,offset,size);
        EXPECT_EQ(readBytes, size);
        if (readBytes != (int)size)
        {
            break;
        }
        ent0->Write(buf, offset, size);
    }
	EXPECT_EQ(1,g_AllocWritePageTimeoutNum);
    g_test_simulate_alloc_fail = 0;

    offset = 0;
    for(i = 0; i < writePageNum*READ_PAGE_SIZE/1024/128; i++)
    {
        ent0->Read(buf, offset, 1024*128);
        bCheckResult = CheckReadWithTemplate(buf, offset, 1024*128);
        EXPECT_EQ(bCheckResult, true);
        ent0->Read(buf, offset+1024*64, 1024*128);
        bCheckResult = CheckReadWithTemplate(buf, offset+1024*64, 1024*128);
        EXPECT_EQ(bCheckResult, true);
        offset += 1024*128;
    }
	HwsFdManager::GetInstance().Close(0);

    /*ent0 exists, just earse from map, not delete */
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    off64_t freeCacheSize = HwsFdManager::GetInstance().GetFreeCacheMemSize();
    if (freeCacheSize == gMaxCacheMemSize)
    {
        cout << "page free,freeCacheSize=" << freeCacheSize <<
            ",maxCacheSize=" << gMaxCacheMemSize << std::endl;
        EXPECT_EQ(true, false);
    }

    ent0 = nullptr;
    /*check free cache size,delay 1500 ms for page timeout */
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    freeCacheSize = HwsFdManager::GetInstance().GetFreeCacheMemSize();
    if (freeCacheSize != gMaxCacheMemSize)
    {
        cout << "page not free,freeCacheSize=" << freeCacheSize <<
            ",maxCacheSize=" << gMaxCacheMemSize << std::endl;
        EXPECT_EQ(true, false);
    }

}

TEST_F(HwsFdCacheTest, HwsFdReadAheadBeyondFileSize) {
    char buf[READ_PAGE_SIZE] = {0};
    off64_t offset = 0;
    int loop = 0;
    int readPageNum = 0;
    int readBytes = 0;
    bool bCheckResult = true;

    if (TEST_TEMPLATE_FILE_SIZE/READ_PAGE_SIZE > 2)
    {
        readPageNum = 2;
    }
    else
    {
        EXPECT_EQ(true, false);                                
        return;
    }

    for(loop = 0; loop < (readPageNum+1); loop++, offset += READ_PAGE_SIZE)
    {
        readBytes = ReadFromTemplate(buf, READ_PAGE_SIZE, offset,READ_PAGE_SIZE);
        EXPECT_EQ(readBytes, READ_PAGE_SIZE);                        
        s3fs_write_hw_obs_proc_test(buf, READ_PAGE_SIZE, offset);
    }
    gPrintCacheStatisMs = 0;
    HwsCacheStatis::GetInstance().PrintStatisAndClear();
    gPrintCacheStatisMs = 300000;
    
    std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, READ_PAGE_SIZE*readPageNum);
    ent0 = HwsFdManager::GetInstance().Get(0);
    offset = 0;
    for(loop = 0; loop < readPageNum*READ_PAGE_SIZE/1024/128; loop++) {
        ent0->Read(buf, offset, 1024*128);
        bCheckResult = CheckReadWithTemplate(buf, offset, 1024*128);
        EXPECT_EQ(bCheckResult, true);       
        offset += 1024*128;
    }
	HwsFdManager::GetInstance().Close(0);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    unsigned long long statisReadPageNum = 
        HwsCacheStatis::GetInstance().GetStatisNum(READ_AHEAD_PAGE);
    cout << "statisReadPageNum=" << statisReadPageNum  << std::endl;
    if (statisReadPageNum > (unsigned long long)readPageNum)
    {
        EXPECT_EQ(true, false);                                        
    }
}

// consider this situation:
// 1. read page begins to read but has not finished yet.
// 2. write page begins to write data with the same range, read page(1) is been disabled.
//    then, write finished
// 3. call clean(), read page(1) should not be deleted, because it has not finished.
// 4. read page finished.
// 5. call clean(), read page(1) should be deleted.
void read_for_invalid_page(std::shared_ptr<HwsFdEntity>& pEntity, char* buf, off64_t offset, size_t size)
{
    pEntity->Read(buf, offset, size);
}

void write_for_invalid_page(std::shared_ptr<HwsFdEntity>& pEntity, char* buf, off64_t offset, size_t size)
{
    pEntity->Write(buf, offset, size);
}

void mock_invalid_read_page(struct HwsFdReadPage* page, int read_size)
{
    if (g_test_read_page_disable_step.load() > 0)
    {
        std::cout << "do mock_invalid_read_page, path: " << page->path_.c_str()
        << " ,off: " << page->offset_ << " ,read_size: " << read_size << std::endl;
        // wait to be invalid by write
        g_test_read_page_disable_step.store(2);
        int rounds = 100; // in case of testcase error
        while (g_test_read_page_disable_step.load() != 0 && rounds > 0)
        {
            if (page->state_ != HWS_FD_READ_STATE_DISABLED)
            {
                std::cout << "read page wait to be disabled, path: " << page->path_.c_str()
                << " ,off: " << page->offset_ << " ,read_size: " << read_size << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            else
            {
                if (g_test_read_page_disable_step.load() == 0)
                {
                    break;
                }
                std::cout << "read page already disabled, wait clean, path: " << page->path_.c_str()
                << " ,off: " << page->offset_ << " ,read_size: " << read_size << std::endl;
                g_test_read_page_disable_step.store(3);
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            rounds--;
        }
    }
}

TEST_F(HwsFdCacheTest, HwsInvalidReadPage) {
    {
        char write_buf[READ_PAGE_SIZE] = {0};
        ReadFromTemplate(write_buf, WRITE_PAGE_SIZE, 0, WRITE_PAGE_SIZE);

        std::shared_ptr<HwsFdEntity> ent0 = HwsFdManager::GetInstance().Open("/home/test", 0, 0);
        ent0 = HwsFdManager::GetInstance().Get(0);

        ent0->Write(write_buf,  0, READ_PAGE_SIZE);
        ent0->Write(write_buf,  READ_PAGE_SIZE, READ_PAGE_SIZE);

        size_t read_size = 128 * 1024;
        off64_t offset = 0;

        //char read_buf[READ_PAGE_SIZE] = {0};
        char* read_buf = (char *)malloc(READ_PAGE_SIZE);
        memset(read_buf, 0, READ_PAGE_SIZE);

        // meet read ahead requirement
        for(int loop = 0; loop < gReadStatSize; loop++) {
            std::cout << offset << std::endl;
            ent0->Read(read_buf, offset, read_size);
            offset += read_size;
        }

        g_test_read_page_disable_step.store(1);
        // read, and this round will invoke read ahead
        std::thread read_thread(read_for_invalid_page, std::ref(ent0), read_buf, offset, 1024 * 1024);

        int rounds = 10;
        while(g_test_read_page_disable_step.load() != 2 && rounds > 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            rounds--;
        }
        if (g_test_read_page_disable_step.load() != 2)
        {
            cout << "read thread error" << std::endl;
            EXPECT_EQ(true, false);
        }

        // write, invalid last read
        std::thread write_thread(write_for_invalid_page, std::ref(ent0), write_buf, offset, 1024 * 1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        rounds = 10;
        while(g_test_read_page_disable_step.load() != 3 && rounds > 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            rounds--;
        }

        if (g_test_read_page_disable_step.load() != 3)
        {
            cout << "write thread error, not invalid read page" << std::endl;
            EXPECT_EQ(true, false);
        }
        write_thread.join();


        // call clean()
        ent0->CleanAndGetPageUsedMem();
        off64_t freeCacheSize = HwsFdManager::GetInstance().GetFreeCacheMemSize();
        if (freeCacheSize == gMaxCacheMemSize)
        {
            cout << "page free err 1,freeCacheSize=" << freeCacheSize <<
                ",maxCacheSize=" << gMaxCacheMemSize << std::endl;
            EXPECT_EQ(true, false);
        }

        // let read finish
        g_test_read_page_disable_step.store(0);
        read_thread.join();
        HwsFdManager::GetInstance().Close(0);

    }

    off64_t freeCacheSize = HwsFdManager::GetInstance().GetFreeCacheMemSize();
    if (freeCacheSize != gMaxCacheMemSize)
    {
        cout << "page free err 2,freeCacheSize=" << freeCacheSize <<
            ",maxCacheSize=" << gMaxCacheMemSize << std::endl;
        EXPECT_EQ(true, false);
    }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  GenerateTemplateFile();
  return RUN_ALL_TESTS();
}

