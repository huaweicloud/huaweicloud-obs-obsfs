#if 1
#ifndef _S3_STATIS_H_
#define _S3_STATIS_H_

#include <pthread.h>
//#include <linux/spinlock.h>
//#include <asm/system.h>


enum statis_type_t {
  MIN_STEP_STATIS = 0,      
  PRE_HEAD_CUR_HANDLE_DELAY,
  PRE_HEAD_MAKE_CURL_DELAY,
  PRE_HEAD_AUTH_DELAY,
  PRE_HEAD_DELAY,
  HEAD_DELAY,
  HEAD_REQUEST_MISS_DELAY,
  HEAD_REQUEST_MATCH_DELAY,
  HEAD_REQUEST_RESPONSE,
  
  GETATTR_OBJECT_TOTAL,
  GETATTR_LIB_CURL,
  GETATTR_MEMSET,
  GETATTR_GETINDEX_DELAY,
  GETATTR_PUTINDEX_DELAY,
  GETATTR_RESPONSE,
  GETATTR_OBJECT_ACCESS_TOTAL,
  GETATTR_FUSE_GET_CTX,
  GETATTR_OBJECT_ACCESS,
  GETATTR_DESTROY_CUHANDLE, 
  GETATTR_DELAY ,

  CREATE_FILE_TOTAL,
  CREATE_FILE_LIBCURL,
  CREATE_FILE_CHECK_ACCESS_DELAY ,
  CREATE_CUR_HANDLE_DELAY,
  CREATE_MAKE_CURL_DELAY,
  CREATE_AUTH_DELAY,
  CREATE_ZERO_FILE_DELAY,

  WRITE_FILE_TOTAL,
  WRITE_FILE_LIBCURL,
  WRITE_CUR_HANDLE_DELAY ,
  WRITE_MAKE_CURL_DELAY,
  WRITE_AUTH_DELAY,
  WRITE_FILE_DELAY,
  WRITE_RESPONSE_DELAY,

  READ_FILE_TOTAL,
  READ_FILE_LIBCURL,

  LIST_BUCKET,

  OPEN_FILE_TOTAL,
  
  LIB_CUR_SEND,
  READLINK_TOTAL,  
  MAKENODE_TOTAL,  
  MAKEDIR_TOTAL,  
  UNLINK_TOTAL,  
  RMDIR_TOTAL,  
  SYMLINK_TOTAL,  
  RENAME_TOTAL,  
  CHMOD_TOTAL,  
  CHOWN_TOTAL,  
  UTIMENS_TOTAL,  
  TRUNCATE_TOTAL,  
  FLUSH_TOTAL,  
  FSYNC_TOTAL,  
  RELEASE_TOTAL,  
  OPENDIR_TOTAL,  
  ACCESS_TOTAL,  
  SET_XATTR_TOTAL,  
  GET_XATTR_TOTAL,  
  LIST_XATTR_TOTAL,  
  REMOVE_XATTR_TOTAL,  
  CACHE_GET_ATTR_STAT_VALID,  
  READ_BEYOND_STAT_FILE_SIZE,  
  MAX_STEP_STATIS              
};

typedef struct _tag_process_statis {
    unsigned long long costTimes;
    unsigned long long maxTime;
    unsigned long long numSum;
    timeval            lastPrintTime;
    pthread_spinlock_t           process_step_lock;
    //pthread_mutex_t    process_step_lock;
}tag_process_statis_t;

void InitStatis();
void clearStatis();
void s3fsStatisStart(statis_type_t statisId, timeval *begin_tv);
void s3fsStatisEnd(statis_type_t statisId, timeval *begin_tv,
    const char* urlOrPathStr = NULL);
void s3fsShowStatis();

#endif
#endif
