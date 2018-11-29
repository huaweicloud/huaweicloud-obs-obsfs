#ifndef _HWS_CONFIGURE_H_
#define _HWS_CONFIGURE_H_
#include <thread>
#include <algorithm>

#define HWS_CONFIG_VALUE_STR_LEN  128
#define HWS_CONFIG_INVALID_VALUE -1

extern void hws_configure_task();

/*Int param enum*/
typedef enum
{
    HWS_CFG_DEBUG_LOG_MODE = 0,  /*for global debug_log_mode*/
    HWS_CFG_STAT_PRINT_SECONDS,
    HWS_CFG_STAT_PRINT_COUNT,
    HWS_CFG_CACHE_ASSERT,
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
};

int HwsGetIntConfigValue(
        HwsConfigIntEnum  paramEnum);

#endif // _HWS_CONFIGURE_H_

