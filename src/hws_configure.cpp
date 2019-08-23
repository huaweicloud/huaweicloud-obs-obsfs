#include <string.h>
#include <assert.h>
#include <time.h>
#include <syslog.h>

#include <cstdint>
#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <map>

#include "common.h"
#include "hws_configure.h"

using namespace std;

//extern variable
extern bool use_obsfs_log;
extern bool cache_assert;
extern s3fs_log_level data_cache_log_level;
extern struct timespec hws_s3fs_start_ts;
//extern function
extern long diff_in_ms(struct timespec *start, struct timespec *end);
extern s3fs_log_level set_s3fs_log_level(s3fs_log_level level);

//global varialbe
const std::string g_logConfigurePath = "/etc/obsfsconfig";
s3fs_log_level fuse_intf_log_level;

HwsConfigIntItem_s  g_hwsConfigIntTable[] =
{
    //1,debug level and log flow control cfg para
    {
        HWS_CFG_DEBUG_LOG_MODE,
        "dbglogmode",
        HWS_CONFIG_INVALID_VALUE
    },
    {
        HWS_CFG_STAT_PRINT_SECONDS,
        "statisprintseconds",
        180
    },
    {
        HWS_CFG_STAT_PRINT_COUNT,
        "statisprintcount",
        5000
    },
    {
        HWS_CFG_FUSE_INTF_LOG_LEVEL,
        "fuse_intf_log_level",
        7
    },
    {
        HWS_CFG_DATA_CACHE_LOG_LEVEL,
        "obsfs_data_cache_log_level",
        7
    },
    {
        HWS_CFG_CACHE_ASSERT,
        "cacheassert",
        0
    },
    {
        HWS_CFG_COULDNT_RESOLVE_HOST,
        "can_not_resolve_host_retrycnt",
        10
    },
    {
        HWS_CFG_MAX_PRINT_LOG_NUM_PERIOD,
        "max_print_log_num_period",
        3
    },    
    {
        HWS_CFG_STATIS_OPER_LONG_MS,
        "statis_operate_long_ms",
        1000
    },    

    //2,cache cfg para    
    {
        HWS_CFG_CACHE_CHECK_CRC_OPEN,
        "cache_check_crc_open",
        1
    },   
    //default cache attr switch close
    {
        HWS_CFG_CACHE_ATTR_SWITCH_OPEN,
        "cache_attr_switch_open",
        0
    },
    //default cache attr valid is 2 hour
    {
        HWS_CFG_CACHE_ATTR_VALID_MS,
        "cache_attr_valid_ms",
        7200000
    },
    //read page clean after 3 second
    {
        HWS_CFG_READ_PAGE_CLEAN_MS,
        "read_page_clean_ms",
        3000
    },
    //gMaxCacheMemSize(MB)
    {
        HWS_CFG_MAX_CACHE_MEM_SIZE_MB,
        "max_cache_mem_size_mb",
        1024
    },
    //gReadStatDiffLongMs(ms)
    {
        HWS_READ_AHEAD_STAT_DIFF_LONG,
        "read_ahead_stat_diff_long_ms",
        50000
    },
    //gReadStatDiffShortMs(ms),one read 0.2s,24 read need 4.8s,must larger than 4.8s
    {
        HWS_READ_AHEAD_STAT_DIFF_SHORT,
        "read_ahead_stat_diff_short_ms",
        10000
    },   
    //gFreeCacheThresholdPercent,if lower,use gReadStatDiffShortMs
    {
        HWS_FREE_CACHE_THRESHOLD_PERCENT,
        "free_cache_threshold_percent",
        20
    },   
    //gReadStatVecSize
    {
        HWS_READ_STAT_VEC_SIZE,
        "read_stat_vec_size",
        32
    },
    //gReadStatSequentialSize
    {
        HWS_READ_STAT_SEQUENTIAL_SIZE,
        "read_stat_sequential_size",
        24
    },    
    //gReadStatSizeThreshold,default 4MB
    {
        HWS_READ_STAT_SIZE_THRESHOLD,
        "read_stat_size_threshold",
        4194304
    },    
    //g_bIntersectWriteMerge,if merge intersect write to write cache
    {
        HWS_INTERSECT_WRITE_MERGE,
        "intersect_write_merge",
        0
    },
    //gReadPageNum
    {
        HWS_READ_PAGE_NUM,
        "read_page_num",
        12
    },
    //gWritePageNum
    {
        HWS_WRITE_PAGE_NUM,
        "write_page_num",
        12
    }    
};

HwsConfigStrItem_s  g_hwsConfigStrTable[] =
{
    {
        HWS_CFG_DEBUG_LEVEL,
        "dbglevel",
        ""                /*default empty str*/
    }
    
};

void hws_configure_task()
{
    pthread_setname_np(pthread_self(), "config_d");    
    while (g_s3fs_start_flag)
    {
        std::this_thread::sleep_for(std::chrono::seconds(READ_LOG_CONFIGURE_INTERVAL));
        HwsConfigure::GetInstance().hwsAnalyseConfigFile(true);   /*analyse int param*/
        HwsConfigure::GetInstance().hwsAnalyseConfigFile(false);  /*analyse str param*/
        HwsConfigure::GetInstance().hwsApplyConfigParam();
     }
}
int HwsGetIntConfigValue(
        HwsConfigIntEnum  paramEnum)
{
    return HwsConfigure::GetInstance().HwsGetIntConfigInClass(paramEnum);
}

void HwsConfigure::getIntByParamName(std::string& line,HwsConfigIntItem_s* pConfigItem)
{
    if (NULL == pConfigItem)
    {
        S3FS_PRN_ERR("invalid param");        
        return;
    }
    std::string strParamValue;
    int intValue = HWS_CONFIG_INVALID_VALUE;  /*-1 is invalid*/
    
    if (std::string::npos != line.find_last_of("="))
    {
        strParamValue = line.substr(line.find_last_of("=") + 1);
        intValue = atoi(strParamValue.c_str());
        S3FS_PRN_INFO("line: %s, strParamValue=%s,intValue=%d", 
            line.c_str(), strParamValue.c_str(),intValue); 
    }
    else
    {
        S3FS_PRN_ERR("error find =,line=%s",line.c_str());                
    }
    if (HWS_CONFIG_INVALID_VALUE != intValue)
    {
        pConfigItem->intValue = intValue;
    }
}
void HwsConfigure::getStrByParamName(std::string& line,HwsConfigStrItem_s* pConfigItem)
{
    if (NULL == pConfigItem)
    {
        S3FS_PRN_ERR("invalid param");        
        return;
    }
    if (std::string::npos != line.find_last_of("="))
    {
        std::string valString = line.substr(line.find_last_of("=") + 1);
        strncpy(pConfigItem->strValue,valString.c_str(),HWS_CONFIG_VALUE_STR_LEN-1);
        
        S3FS_PRN_INFO("line: %s, strParamValue=%s", 
            line.c_str(), pConfigItem->strValue); 
    }
    else
    {
        S3FS_PRN_ERR("error find =,line=%s",line.c_str());                
    }
}
void HwsConfigure::hwsApplyConfigParam()
{
    //config log mode default is -1
    if (g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].intValue >= 0
        && g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].intValue <= (int)LOG_MODE_SYSLOG)
    {
        s3fs_log_mode new_log_mode = 
            (s3fs_log_mode)g_hwsConfigIntTable[HWS_CFG_DEBUG_LOG_MODE].intValue;
        if (new_log_mode != debug_log_mode)
        {
            S3FS_PRN_WARN("debug_log_mode change from %d to %d",
                debug_log_mode,new_log_mode);                            
        }
        if (new_log_mode != LOG_MODE_FOREGROUND) 
        {
            struct timespec now_ts;
            clock_gettime(CLOCK_MONOTONIC_COARSE, &now_ts);
            /*set obsfs log mod must delay 5 second after start*/
            if (diff_in_ms(&hws_s3fs_start_ts, &now_ts) < OBSFS_LOG_MODE_SET_DELAY_MS)
            {
                debug_log_mode = LOG_MODE_SYSLOG;
            }
            else
            {
                debug_log_mode = new_log_mode;
            }
        }
    }
    setObsFsLogLevel(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue);  
    setFuseIntfLogLevel();
    set_data_cache_log_level();
    //set cache assert
    bool new_cache_assert = 
        (bool)g_hwsConfigIntTable[HWS_CFG_CACHE_ASSERT].intValue;
    if (cache_assert != new_cache_assert)
    {
        S3FS_PRN_WARN("cache_assert change from %d to %d",
            cache_assert,new_cache_assert); 
        cache_assert = new_cache_assert;
    }
}
void HwsConfigure::hwsAnalyseConfigLine_Str(std::string& line)
{    
    unsigned int i;
    
    for (i = 0; i < HWS_CFG_STR_END; i++)
    {
        if (i > sizeof(g_hwsConfigStrTable)/sizeof(HwsConfigStrItem_s))
        {
            S3FS_PRN_ERR("error,HWS_CFG_STR_END large than config table");                            
            return;
        }
        if (string::npos != line.find(g_hwsConfigStrTable[i].paramName))
        {
            getStrByParamName(line,&(g_hwsConfigStrTable[i]));
        }
    }
}

void HwsConfigure::hwsAnalyseConfigLine_Int(std::string& line)
{    
    unsigned int i;
    
    for (i = 0; i < HWS_CFG_INT_END; i++)
    {
        if (i > sizeof(g_hwsConfigIntTable)/sizeof(HwsConfigIntItem_s))
        {
            S3FS_PRN_ERR("error,HWS_CFG_INT_END large than config table");                            
            return;
        }
        if (string::npos != line.find(g_hwsConfigIntTable[i].paramName))
        {
            getIntByParamName(line,&(g_hwsConfigIntTable[i]));
        }
    }
}

void HwsConfigure::hwsAnalyseConfigFile(bool analyseIntParam)
{   
    std::fstream fp(g_logConfigurePath);
    if (false == fp.is_open())
    {
        S3FS_PRN_DBG("open obsfsconfig failed");
        return;
    }
    std::string line;
    while (getline(fp, line))
    {
        if (analyseIntParam)
        {
            hwsAnalyseConfigLine_Int(line);
        }
        else
        {
            hwsAnalyseConfigLine_Str(line);
        }
    }
    fp.close();
}
int HwsConfigure::HwsGetIntConfigInClass(
        HwsConfigIntEnum  paramEnum)
{
    if (paramEnum >= HWS_CFG_INT_END)
    {
        S3FS_PRN_ERR("invalid param,paraEnum=%d",paramEnum);                
        return HWS_CONFIG_INVALID_VALUE;
    }

    return g_hwsConfigIntTable[paramEnum].intValue;
}
void HwsConfigure::setObsFsLogLevel(const char* strlevel)
{
    if (NULL == strlevel)
    {
        return;
    }
    /*return when equal default empty str in g_hwsConfigStrTable*/
    if (0 == strcasecmp(strlevel, ""))
    {
        return;
    }
    if(0 == strcasecmp(strlevel, "silent") || 0 == strcasecmp(strlevel, "critical") || 0 == strcasecmp(strlevel, "crit")){
      set_s3fs_log_level(S3FS_LOG_CRIT);
    }else if(0 == strcasecmp(strlevel, "error") || 0 == strcasecmp(strlevel, "err")){
      set_s3fs_log_level(S3FS_LOG_ERR);
    }else if(0 == strcasecmp(strlevel, "wan") || 0 == strcasecmp(strlevel, "warn") || 0 == strcasecmp(strlevel, "warning")){
      set_s3fs_log_level(S3FS_LOG_WARN);
    }else if(0 == strcasecmp(strlevel, "inf") || 0 == strcasecmp(strlevel, "info") || 0 == strcasecmp(strlevel, "information")){
      set_s3fs_log_level(S3FS_LOG_INFO);
    }else if(0 == strcasecmp(strlevel, "dbg") || 0 == strcasecmp(strlevel, "debug")){
      set_s3fs_log_level(S3FS_LOG_DBG);
    }else{
      S3FS_PRN_WARN("option dbglevel has unknown parameter(%s).", strlevel);
    }
}
void HwsConfigure::setFuseIntfLogLevel()
{
    fuse_intf_log_level = (s3fs_log_level)HwsGetIntConfigValue(HWS_CFG_FUSE_INTF_LOG_LEVEL);
}
void HwsConfigure::set_data_cache_log_level()
{
    data_cache_log_level = (s3fs_log_level)HwsGetIntConfigValue(HWS_CFG_DATA_CACHE_LOG_LEVEL);
}

