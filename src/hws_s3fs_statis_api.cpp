#include <stdio.h>
#include <stdlib.h>

#include <string.h>
#include <string>
#include <map>
#include <sys/time.h>

#include "common.h"
#include "hws_s3fs_statis_api.h"
#include "hws_configure.h"


std::string  statis_step[MAX_STEP_STATIS] = {"min_step", 
                                             "prehead_curhandl",
                                             "prehead_makecurl",
                                             "prehead_auth",
                                             "prehead_total",
                                             
                                             "head_total",
                                             "head_request_miss",
                                             "head_request_match",
                                             "head_request_response",

                                             "getattr_object_total",
                                             "getattr_lib_curl",
                                             "getattr_memset",
                                             "getattr_getIndex",
                                             "getattr_putIndex",
                                             "getattr_response",
                                             "getattr_object_access_total",
                                             "getattr_fuse_get_ctx",
                                             "getattr_object_access",
                                             "getattr_destroy_cur_handle",
                                             "getattr_total",

                                             "create_file_total",
                                             "create_file_lib_curl",
                                             "create_check_accss",
                                             "create_curhandl",
                                             "create_makecurl",
                                             "create_auth",
                                             "create_zerofile_total", 

                                             "write_file_total",
                                             "write_file_lib_curl",
                                             "write_curhandl",
                                             "write_makecurl",
                                             "write_auth",
                                             "write_bytes",
                                             "write_response",

                                             "read_file_total",
                                             "read_file_lib_curl",

                                             "list_bucket",

                                             "open_file_total",
                                             
                                             "lib_curl_send",
                                             "readlink_total",
                                             "makenode_total",
                                             "makedir_total",
                                             "unlink_total",
                                             "rmdir_total",
                                             "delete_libcurl",
                                             "symlink_total",
                                             "rename_total",
                                             "rename_libcurl",
                                             "chmod_total",
                                             "chown_total",
                                             "utimens_total",
                                             "truncate_total",
                                             "flush_total",
                                             "fsync_total",
                                             "release_total",
                                             "opendir_total",
                                             "access_total",
                                             "setXattr_total",
                                             "getXattr_total",
                                             "listXattr_total",
                                             "removeXattr_total",
                                             "cache_get_attr_stat_valid",
                                             "read_beyond_stat_file_size"
                                            };


tag_process_statis_t  process_statis[MAX_STEP_STATIS];


void InitStatis(){
    for (int i = 0; i < MAX_STEP_STATIS; i++){
        memset((void*)(process_statis+i),0,sizeof(tag_process_statis_t));
        pthread_spin_init(&(process_statis[i].process_step_lock), PTHREAD_PROCESS_PRIVATE);
    }
}

void clearStatis(){
    for (int i = 0; i < MAX_STEP_STATIS; i++){
        pthread_spin_destroy(&(process_statis[i].process_step_lock));
        process_statis[i].costTimes = 0;
        process_statis[i].maxTime   = 0;
        process_statis[i].numSum    = 0;
    }
}

void s3fsShowStatis(){
    for (int statisId = 0; statisId < MAX_STEP_STATIS; statisId ++){
        if (0 == process_statis[statisId].numSum){
            continue;
        }
        pthread_spin_lock(&(process_statis[statisId].process_step_lock));
       
        unsigned long long average = process_statis[statisId].costTimes / process_statis[statisId].numSum;
        S3FS_PRN_CRIT("Id[%d], Step(%s), AVG[%llu us], MAX[%llu us], total[%llu us], numSum[%llu us]", statisId, statis_step[statisId].c_str(), average, 
                                                                                     process_statis[statisId].maxTime,
                                                                                     process_statis[statisId].costTimes,
                                                                                     process_statis[statisId].numSum);
        pthread_spin_unlock(&(process_statis[statisId].process_step_lock));
    }
}
unsigned long long getDiffUs(timeval* pBeginTime,timeval* pEndTime)
{
    unsigned long long diff = 1000000 * (pEndTime->tv_sec - pBeginTime->tv_sec) + 
        pEndTime->tv_usec - pBeginTime->tv_usec;
    return diff;
}

void s3fsStatis(statis_type_t statisId, unsigned long long costTime,
          timeval* pEndTime)
{  
    //pthread_spin_lock(&(process_statis[statisId].process_step_lock));
    process_statis[statisId].numSum    ++;
    process_statis[statisId].costTimes += costTime;
    process_statis[statisId].maxTime    = (process_statis[statisId].maxTime < costTime) ? costTime : process_statis[statisId].maxTime;
    //pthread_spin_unlock(&(process_statis[statisId].process_step_lock));
    
    unsigned long long diffUs = getDiffUs(&(process_statis[statisId].lastPrintTime),pEndTime);
    /*print statis every 180 second or 5000 num*/
    unsigned long long printPeriodUs = 
        HwsGetIntConfigValue(HWS_CFG_STAT_PRINT_SECONDS)*1000000;
    if ( diffUs > printPeriodUs || 
        process_statis[statisId].numSum > (unsigned long long)HwsGetIntConfigValue(HWS_CFG_STAT_PRINT_COUNT))
    {
       unsigned long long average = 
            ((double)process_statis[statisId].costTimes) / process_statis[statisId].numSum;
       S3FS_PRN_CRIT("Id[%d], Step(%s), AVG[%llu us], MAX[%llu us], total[%llu us], numSum[%llu]", 
            statisId, statis_step[statisId].c_str(), average, 
            process_statis[statisId].maxTime,
            process_statis[statisId].costTimes,
            process_statis[statisId].numSum);
       /*clear stattis after print*/
       process_statis[statisId].costTimes = 0;
       process_statis[statisId].maxTime   = 0;
       process_statis[statisId].numSum    = 0;     
       memcpy(&(process_statis[statisId].lastPrintTime),pEndTime,sizeof(timeval));
   }
}

void s3fsStatisStart(statis_type_t statisId, timeval *begin_tv){
    if (NULL == begin_tv){
        S3FS_PRN_ERR("begin_tv is %p", begin_tv);
        return;
    }
    if (0 > statisId || statisId >=  MAX_STEP_STATIS){
        S3FS_PRN_ERR("could not record this stepId[%d], maxStepId is[%d]", statisId, MAX_STEP_STATIS);
        return;
    }
    gettimeofday(begin_tv, NULL);
}

void s3fsStatisEnd(statis_type_t statisId, timeval *pBeginTime,
        const char* urlOrPathStr)
{
    if (NULL == pBeginTime){
        S3FS_PRN_ERR("pBeginTime is null");
        return;
    }
    if (0 > statisId|| statisId>=  MAX_STEP_STATIS){
        S3FS_PRN_ERR("could not record this stepId[%d], maxStepId is[%d]", statisId, MAX_STEP_STATIS);
        return;
    }
    timeval      end_tv;
    gettimeofday(&end_tv, NULL);

    unsigned long long diff = getDiffUs(pBeginTime,&end_tv);
    unsigned long long statis_long_us = HwsGetIntConfigValue(HWS_CFG_STATIS_OPER_LONG_MS)*1000;
    if (NULL != urlOrPathStr && diff > statis_long_us)
    {
        S3FS_PRN_WARN("statisTime long,Step(%s),urlOrPathStr=%s,diff(%llu)",
            statis_step[statisId].c_str(),urlOrPathStr,diff);
    }
    s3fsStatis(statisId, diff,&end_tv);
}

