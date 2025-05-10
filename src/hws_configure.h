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

#ifndef _HWS_CONFIGURE_H_
#define _HWS_CONFIGURE_H_
#include <thread>
#include <algorithm>

#define HWS_CONFIG_VALUE_STR_LEN  128
#define HWS_CONFIG_INVALID_VALUE -1
#define READ_LOG_CONFIGURE_INTERVAL 5     //every 5 seconds read config file
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
    //cache get attr stat valid time for list(ms)
    HWS_CFG_CACHE_ATTR_VALID_4_LIST_MS,
    //meta cache capacity
    HWS_CFG_META_CACHE_CAPACITY,
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

    //g_wholeFileCacheSwitch, default false
    HWS_WHOLE_CACHE_SWITCH,
    //g_wholeFileCacheMaxMemSize, default 10GB
    HWS_WHOLE_CACHE_MAX_MEM_SIZE,
    //g_wholeFileCachePreReadStatisPeriod, default 5 seconds
    HWS_WHOLE_CACHE_STATIS_PERIOD,
    //g_wholeFileCacheNotHitTimesPerPeriod, default 300 times
    HWS_WHOLE_CACHE_READ_TIMES,
    //g_wholeFileCacheNotHitSizePerPeriod, default 5 MB
    HWS_WHOLE_CACHE_READ_SIZE,
    //g_wholeFileCachePreReadInterval, default 50 ms
    HWS_WHOLE_CACHE_READ_INTERVAL_MS,
    //g_wholeFileCacheMaxRecycleTime, default 10800 seconds
    HWS_WHOLE_CACHE_MAX_RECYCLE_TIME,
    //g_wholeFileCacheMaxHitTimes, default 500000 times
    HWS_WHOLE_CACHE_MAX_HIT_TIMES,
    //g_MinReadWritePageByCacheSize
    HWS_MIN_READ_WRITE_PAGE_BY_CACHE_SIZE,

    //3. other paras
    //head req with inodeno,default true
    HWS_REQUEST_WITH_INODENO,
    //period check ak sk change
    HWS_PERIOD_CHECK_AK_SK_CHANGE,
    //g_listMaxKey, default 110
    HWS_LIST_MAX_KEY,

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
	int                  minValue;
	int                  maxValue;
} HwsConfigIntItem_s;

typedef struct
{
    HwsConfigStrEnum  paramEnum;
    const char          *paramName;    
    char                strValue[HWS_CONFIG_VALUE_STR_LEN];    
    unsigned int        minLen;
    unsigned int        maxLen;
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

