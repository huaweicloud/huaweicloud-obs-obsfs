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

#ifndef HWS_INDEX_CACHE_H_
#define HWS_INDEX_CACHE_H_
#include "common.h"
#include "snas_list.h"
#include <assert.h>
#include <time.h>
#include <mutex>
#include <vector>  // for extend_arrays_ in IndexCache

using namespace std;

struct Node{
    SNAS_ListHead           list_ptr;
    tag_index_cache_entry_t data;

    int   openCnt = 0;
};

enum tag_open_cnt_use_type {
    NOCHANGE_OPEN_CNT = 0,
    ADD_OPEN_CNT      = 1,
    REDUCE_OPEN_CNT   = 2
};

class IndexCache{
private:
    IndexCache(const IndexCache&) = delete;             // non-copyable
    IndexCache& operator=(const IndexCache&) = delete;  // non-copyable

    IndexCache() : size(20000), entries_(new Node[size]) {
        SNAS_InitListHead(&lru_head);

        SNAS_InitListHead(&openflag_head);
        for(size_t i=0; i<size; ++i)
        {
            SNAS_ListAdd(&(entries_+i)->list_ptr, &lru_head);
        }
    }

    ~IndexCache()
    {
        delete[] entries_;
        // fix: release all extended arrays allocated by resizeMetaCacheCapacity
        for(size_t i = 0; i < extend_arrays_.size(); i++){
            delete[] extend_arrays_[i];
        }
    }

public:
    static IndexCache* getIndexCache(void);

    void DeleteIndex(string key);
    // [新增] 通配符删除方法 - 删除所有以 pattern 开头的索引缓存条目
    // Used for directory rename to invalidate all child path index caches
    // Time complexity: O(n) where n is the number of cache entries
    void DeleteIndexWildcard(const std::string& pattern);
    bool GetIndex(string key, tag_index_cache_entry_t* data);
    int  PutIndexNotchangeOpenCnt(string key, tag_index_cache_entry_t *data);
    int  PutIndexAddOpenCnt(string key, tag_index_cache_entry_t *data);
    int  PutIndexReduceOpenCnt(string key);
    int setFirstWriteFlag(string key);
    void ReplaceIndex(string srcKey, string destKey, tag_index_cache_entry_t* p_index_cache_entry);
    int setFilesizeOrClearGetAttrStat(string path,
            off64_t cacheFileSize,bool clearGetAttrStat, CACHE_STAT_TYPE statType = STAT_TYPE_BUTT);
    int setFilesizeAndClearIndexCacheTime(string path, off64_t cacheFileSize);
    void AddEntryOpenCnt(string path);
    void resizeMetaCacheCapacity(size_t capacity);

private:
    Node* getnodeInlock(string &key);
    void operateOpenCnt(Node *node, tag_open_cnt_use_type openCntType);
    void putNodeToList(Node *node, string key);
    int putIndexInternal(string key, tag_index_cache_entry_t *data, tag_open_cnt_use_type openCntType);
    Node* getFreeNodeAndEraseMap(const char* pathStr);

private:
    std::map<string, Node* > hashmap_;
    size_t  size;
    Node   *entries_;
    std::vector<Node*> extend_arrays_;  // saves extended array pointers for cleanup in destructor

    SNAS_ListHead    lru_head;
    SNAS_ListHead    openflag_head;

    std::mutex index_cache_mutex;
    static IndexCache  singleton;
};

#endif // HWS_INDEX_CACHE_H_

