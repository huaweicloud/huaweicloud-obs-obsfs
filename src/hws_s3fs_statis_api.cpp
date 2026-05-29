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
                                             "list_bucket_lib_curl",

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
        // Initialize spinlock for backward compatibility
        pthread_spin_init(&(process_statis[i].process_step_lock), PTHREAD_PROCESS_PRIVATE);

        // Initialize atomic variables to 0
        process_statis[i].costTimes.store(0, std::memory_order_relaxed);
        process_statis[i].maxTime.store(0, std::memory_order_relaxed);
        process_statis[i].numSum.store(0, std::memory_order_relaxed);

        // Initialize non-atomic fields
        memset(&(process_statis[i].lastPrintTime), 0, sizeof(timeval));
    }
}

void clearStatis(){
    for (int i = 0; i < MAX_STEP_STATIS; i++){
        pthread_spin_destroy(&(process_statis[i].process_step_lock));
        // Reset atomic variables to 0
        process_statis[i].costTimes.store(0, std::memory_order_relaxed);
        process_statis[i].maxTime.store(0, std::memory_order_relaxed);
        process_statis[i].numSum.store(0, std::memory_order_relaxed);
    }
}

void s3fsShowStatis(){
    for (int statisId = 0; statisId < MAX_STEP_STATIS; statisId ++){
        // Load atomic values
        unsigned long long numSum = process_statis[statisId].numSum.load(std::memory_order_relaxed);
        if (0 == numSum){
            continue;
        }

        unsigned long long costTimes = process_statis[statisId].costTimes.load(std::memory_order_relaxed);
        unsigned long long maxTime = process_statis[statisId].maxTime.load(std::memory_order_relaxed);

        unsigned long long average = costTimes / numSum;
        S3FS_PRN_CRIT("Id[%d], Step(%s), AVG[%llu us], MAX[%llu us], total[%llu us], numSum[%llu us]", statisId, statis_step[statisId].c_str(), average,
                                                                                     maxTime, costTimes, numSum);
    }
}
unsigned long long getDiffUs(timeval* pBeginTime,timeval* pEndTime)
{
    unsigned long long diff = (unsigned long long)((1000000 * (pEndTime->tv_sec - pBeginTime->tv_sec) +
        pEndTime->tv_usec - pBeginTime->tv_usec));
    return diff;
}

void s3fsStatis(statis_type_t statisId, unsigned long long costTime,
          timeval* pEndTime)
{
    // ------------------------------------------------------------------------
    // PHASE 1: Lock-free atomic updates (high-frequency, low-contention)
    // ------------------------------------------------------------------------
    // These operations are fully lock-free and thread-safe
    // Using memory_order_relaxed for performance as we don't need synchronization guarantees

    // Increment operation count atomically
    process_statis[statisId].numSum.fetch_add(1, std::memory_order_relaxed);

    // Add to total cost time atomically
    process_statis[statisId].costTimes.fetch_add(costTime, std::memory_order_relaxed);

    // Update maxTime using compare-and-swap loop
    unsigned long long old_max = process_statis[statisId].maxTime.load(std::memory_order_relaxed);
    while (costTime > old_max) {
        // CAS: if maxTime is still old_max, replace it with costTime
        if (process_statis[statisId].maxTime.compare_exchange_weak(old_max, costTime,
                std::memory_order_release, std::memory_order_relaxed)) {
            break;  // CAS succeeded, maxTime updated
        }
        // CAS failed: another thread updated maxTime, reload and retry
    }

    // ------------------------------------------------------------------------
    // PHASE 2: Check if statistics should be printed (low-frequency)
    // ------------------------------------------------------------------------
    unsigned long long diffUs = getDiffUs(&(process_statis[statisId].lastPrintTime), pEndTime);

    // Get current numSum atomically
    unsigned long long current_numSum = process_statis[statisId].numSum.load(std::memory_order_relaxed);

    unsigned long long printPeriodUs =
        (unsigned long long)(unsigned int)HwsGetIntConfigValue(HWS_CFG_STAT_PRINT_SECONDS) * 1000000;
    unsigned long long printCountThreshold =
        (unsigned long long)(unsigned int)HwsGetIntConfigValue(HWS_CFG_STAT_PRINT_COUNT);

    if (diffUs > printPeriodUs || current_numSum > printCountThreshold) {
        // --------------------------------------------------------------------
        // PHASE 3: Print and reset statistics (very low-frequency)
        // --------------------------------------------------------------------
        // Use atomic exchange to atomically read and reset values
        // This ensures no updates are lost during the reset

        unsigned long long numSum = process_statis[statisId].numSum.exchange(0, std::memory_order_relaxed);
        unsigned long long totalCost = process_statis[statisId].costTimes.exchange(0, std::memory_order_relaxed);
        unsigned long long max = process_statis[statisId].maxTime.exchange(0, std::memory_order_relaxed);

        if (numSum > 0) {
            unsigned long long average = totalCost / numSum;
            S3FS_PRN_CRIT("Id[%d], Step(%s), AVG[%llu us], MAX[%llu us], total[%llu us], numSum[%llu]",
                statisId, statis_step[statisId].c_str(), average, max, totalCost, numSum);
        }

        // Update last print time (non-atomic, only accessed here)
        memcpy(&(process_statis[statisId].lastPrintTime), pEndTime, sizeof(timeval));
    }
}

void s3fsStatisStart(statis_type_t statisId, timeval *begin_tv){
    if (NULL == begin_tv){
        S3FS_PRN_ERR("begin_tv is NULL");
        return;
    }
    if (statisId >=  MAX_STEP_STATIS){
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
    if (statisId>=  MAX_STEP_STATIS){
        S3FS_PRN_ERR("could not record this stepId[%d], maxStepId is[%d]", statisId, MAX_STEP_STATIS);
        return;
    }
    timeval      end_tv;
    gettimeofday(&end_tv, NULL);

    unsigned long long diff = getDiffUs(pBeginTime,&end_tv);
    unsigned long long statis_long_us = (unsigned long long)(unsigned int)HwsGetIntConfigValue(HWS_CFG_STATIS_OPER_LONG_MS)*1000;
    if (NULL != urlOrPathStr && diff > statis_long_us)
    {
        S3FS_PRN_WARN("statisTime long,Step(%s),urlOrPathStr=%s,diff(%llu)",
            statis_step[statisId].c_str(),urlOrPathStr,diff);
    }
    s3fsStatis(statisId, diff,&end_tv);
}

