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

extern bool use_obsfs_log;
extern bool cache_assert;
extern struct timespec hws_s3fs_start_ts;
//extern function
extern long diff_in_ms(struct timespec *start, struct timespec *end);
extern s3fs_log_level set_s3fs_log_level(s3fs_log_level level);

#define READ_LOG_CONFIGURE_INTERVAL 10
const std::string g_logConfigurePath = "/etc/obsfsconfig";
HwsConfigIntItem_s  g_hwsConfigIntTable[] =
{
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
        HWS_CFG_CACHE_ASSERT,
        "cacheassert",
        0
    },
    {
        HWS_CFG_COULDNT_RESOLVE_HOST,
        "can_not_resolve_host_retrycnt",
        10
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
        bool new_cache_assert = 
            (bool)g_hwsConfigIntTable[HWS_CFG_CACHE_ASSERT].intValue;
        if (cache_assert != new_cache_assert)
        {
            S3FS_PRN_WARN("cache_assert change from %d to %d",
                cache_assert,new_cache_assert); 
            cache_assert = new_cache_assert;
        }
    }
    setObsFsLogLevel(g_hwsConfigStrTable[HWS_CFG_DEBUG_LEVEL].strValue);       
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
    if (NULL == fp)
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


