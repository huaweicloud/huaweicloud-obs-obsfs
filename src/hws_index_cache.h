/*
 * s3fs - FUSE-based file system backed by Amazon S3
 *
 * Copyright(C) 2007 Randy Rizun <rrizun@gmail.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HWS_INDEX_CACHE_H_
#define HWS_INDEX_CACHE_H_
#include "common.h"
#include "snas_list.h"
#include <assert.h>
#include <time.h>

using namespace std;

struct Node{
    SNAS_ListHead           list_ptr;
    tag_index_cache_entry_t data;

    int   openCnt;
};

#define container_of(ptr, type, member) ({				\
    void *__mptr = (void *)(ptr);                   \
    ((type *)(__mptr - offsetof(type, member))); })

enum tag_open_cnt_use_type {
    NOCHANGE_OPEN_CNT = 0,
    ADD_OPEN_CNT      = 1,
    REDUCE_OPEN_CNT   = 2
};

class IndexCache{
private:
    IndexCache(){
        SNAS_InitListHead(&lru_head);

        SNAS_InitListHead(&openflag_head);

        size = 20000;  //init value
        entries_ = new Node[size];
        for(size_t i=0; i<size; ++i)
        {
            SNAS_ListAdd(&(entries_+i)->list_ptr, &lru_head);
        }

        pthread_spin_init(&index_cache_lock, PTHREAD_PROCESS_PRIVATE);
    }

    ~IndexCache()
    {
        delete[] entries_;
        pthread_spin_destroy(&(index_cache_lock));
    }

public:
    static IndexCache* getIndexCache(void);

    void DeleteIndex(string key);
    bool GetIndex(string key, tag_index_cache_entry_t* data);
    int  PutIndexNotchangeOpenCnt(string key, tag_index_cache_entry_t *data);
    int  PutIndexAddOpenCnt(string key, tag_index_cache_entry_t *data);
    int  PutIndexReduceOpenCnt(string key);
    int setFirstWriteFlag(string key);
    void ReplaceIndex(string srcKey, string destKey, tag_index_cache_entry_t* p_index_cache_entry);
    int setFilesizeOrClearGetAttrStat(string path,
            off64_t cacheFileSize,bool clearGetAttrStat);
    void AddEntryOpenCnt(string path);
    void resizeMetaCacheCapacity(size_t capacity);

private:

    void operateOpenCnt(Node *node, tag_open_cnt_use_type openCntType);
    void putNodeToList(Node *node, string key);
    int putIndexInternal(string key, tag_index_cache_entry_t *data, tag_open_cnt_use_type openCntType);
    Node* getFreeNodeAndEraseMap(const char* pathStr);

private:
    std::map<string, Node* > hashmap_;
    size_t  size;
    Node   *entries_;

    SNAS_ListHead    lru_head;
    SNAS_ListHead    openflag_head;

    pthread_spinlock_t index_cache_lock;
    static IndexCache  singleton;
};

#endif // HWS_INDEX_CACHE_H_

