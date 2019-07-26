#ifndef _HWS_CONFIGURE_H_
#define _HWS_CONFIGURE_H_
#include <thread>
#include <algorithm>

#define HWS_CONFIG_VALUE_STR_LEN  128
#define HWS_CONFIG_INVALID_VALUE -1
#define READ_LOG_CONFIGURE_INTERVAL 10
#define OBSFS_LOG_MODE_SET_DELAY_MS  5000 /*obs fs log mode set 5000 ms after start*/

extern void hws_configure_task();

/*Int param enum*/
typedef enum
{
    //1,debug level and log flow control cfg para
    HWS_CFG_DEBUG_LOG_MODE = 0,  /*for global debug_log_mode*/
    HWS_CFG_STAT_PRINT_SECONDS,
    HWS_CFG_STAT_PRINT_COUNT,
    HWS_CFG_FUSE_INTF_LOG_LEVEL,
    HWS_CFG_DATA_CACHE_LOG_LEVEL,
    HWS_CFG_CACHE_ASSERT,
    HWS_CFG_COULDNT_RESOLVE_HOST,
    //print log num every 30 second
    HWS_CFG_MAX_PRINT_LOG_NUM_PERIOD,
    //statis_operate_long_ms
    HWS_CFG_STATIS_OPER_LONG_MS,

    //2,cache cfg para
    //cache check crc switch 
    HWS_CFG_CACHE_CHECK_CRC_OPEN, 
    //cache get attr switch 
    HWS_CFG_CACHE_ATTR_SWITCH_OPEN, 
    //cache get attr stat valid time
    HWS_CFG_CACHE_ATTR_VALID_MS, 
    //read page clean after 3 second
    HWS_CFG_READ_PAGE_CLEAN_MS, 
    //gMaxCacheMemSize(MB)
    HWS_CFG_MAX_CACHE_MEM_SIZE_MB, 
    //gReadStatDiffLongMs(ms)
    HWS_READ_AHEAD_STAT_DIFF_LONG, 
    //gReadStatDiffShortMs(ms)
    HWS_READ_AHEAD_STAT_DIFF_SHORT, 
    //gFreeCacheThresholdPercent,if lower,use gReadStatDiffShortMs
    HWS_FREE_CACHE_THRESHOLD_PERCENT, 
    //gReadStatVecSize
    HWS_READ_STAT_VEC_SIZE, 
    //gReadStatSequentialSize
    HWS_READ_STAT_SEQUENTIAL_SIZE, 
    //gReadStatSizeThreshold,default 4MB
    HWS_READ_STAT_SIZE_THRESHOLD, 
    //g_bIntersectWriteMerge,if merge intersect write to write cache
    HWS_INTERSECT_WRITE_MERGE,
    //gReadPageNum, default 12
    HWS_READ_PAGE_NUM,
    //gWritePageNum, default 12
    HWS_WRITE_PAGE_NUM, 
    
    HWS_CFG_INT_END
} HwsConfigIntEnum;

/*string param enum*/
typedef enum 
{
    HWS_CFG_DEBUG_LEVEL = 0,  /*for global debug_level*/
    HWS_CFG_STR_END
}HwsConfigStrEnum;

typedef struct
{    
    HwsConfigIntEnum  paramEnum;
    const char          *paramName;    
    int                  intValue;
} HwsConfigIntItem_s;

typedef struct
{
    HwsConfigStrEnum  paramEnum;
    const char          *paramName;    
    char                strValue[HWS_CONFIG_VALUE_STR_LEN];    
} HwsConfigStrItem_s;


class HwsConfigure
{
  private:
    std::thread thread_;

  private:
    void getIntByParamName(std::string& line,HwsConfigIntItem_s* pConfigItem);
    void hwsAnalyseConfigLine_Int(std::string& line);
    void getStrByParamName(std::string& line,HwsConfigStrItem_s* pConfigItem);
    void hwsAnalyseConfigLine_Str(std::string& line);
  public:
    HwsConfigure()
    {
    }
    ~HwsConfigure()
    {
        if (thread_.joinable())
        {
            thread_.join();
        }
    }

    static HwsConfigure& GetInstance(){
        static HwsConfigure instance;
        return instance;
    }
    void startThread()
    {
        thread_= std::thread(hws_configure_task);        
    }
    void hwsApplyConfigParam();
    void hwsAnalyseConfigFile(bool analyseIntParam);
    int HwsGetIntConfigInClass(
        HwsConfigIntEnum  paramEnum);
    void setObsFsLogLevel(const char* strlevel);
    void setFuseIntfLogLevel();
    void set_data_cache_log_level();    
};

int HwsGetIntConfigValue(
        HwsConfigIntEnum  paramEnum);

#endif // _HWS_CONFIGURE_H_

