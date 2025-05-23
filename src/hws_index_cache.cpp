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

#include "hws_index_cache.h"


extern bool cache_assert;
extern int gMetaCacheSize;


IndexCache IndexCache::singleton;

IndexCache* IndexCache::getIndexCache(void)
{
    return &singleton;
}

void IndexCache::operateOpenCnt(Node* node, tag_open_cnt_use_type openCntType)
{
    switch (openCntType)
    {
        case NOCHANGE_OPEN_CNT:
        {
            S3FS_PRN_DBG("openCnt[%d] not change!", node->openCnt);
            break;
        }
        case ADD_OPEN_CNT:
        {
            node->openCnt ++;
            S3FS_PRN_DBG("openCnt[%d], after added", node->openCnt);
            break;
        }
        case REDUCE_OPEN_CNT:
        {
            node->openCnt --;
            S3FS_PRN_DBG("openCnt[%d], after reduce", node->openCnt);
            break;
        }
        default :
        {
            S3FS_PRN_ERR("openCnt use typ[%d] error", openCntType);
            return;
        }
    }
    return;
}

Node* IndexCache::getnodeInlock(string &key)
{
    auto it = hashmap_.find(key);
    if (it != hashmap_.end()) {
        return it->second;
    }

    return NULL;
}

void IndexCache::putNodeToList(Node* node, string key)
{
    if ( 0 < node->openCnt)
    {
        SNAS_ListAdd(&node->list_ptr, &openflag_head);
        S3FS_PRN_DBG("[path=%s][dentryname=%s][inodeNo=%lld][firstWritFlag=%d][fsversionId=%s][plogHeadversion=%s][openCnt=%d],"\
            "in openflag list", key.c_str(), node->data.dentryname.c_str(), node->data.inodeNo, node->data.firstWritFlag,
            node->data.fsVersionId.c_str(), node->data.plogheadVersion.c_str(), node->openCnt);
    }
    else if (0 == node->openCnt)
    {
        SNAS_ListAdd(&node->list_ptr, &lru_head);
        S3FS_PRN_DBG("[path=%s][dentryname=%s][inodeNo=%lld][firstWritFlag=%d][fsversionId=%s][plogHeadversion=%s][openCnt=%d],"\
            "in lru list", key.c_str(), node->data.dentryname.c_str(), node->data.inodeNo, node->data.firstWritFlag,
            node->data.fsVersionId.c_str(), node->data.plogheadVersion.c_str(), node->openCnt);
    }
    else
    {

        hashmap_.erase(key);
        SNAS_ListAdd(&node->list_ptr, &lru_head);
        S3FS_PRN_ERR("[path=%s][dentryname=%s][inodeNo=%lld][firstWritFlag=%d][fsversionId=%s][plogHeadversion=%s],"\
            "openCnt[%d], inner error", key.c_str(), node->data.dentryname.c_str(), node->data.inodeNo, node->data.firstWritFlag,
            node->data.fsVersionId.c_str(), node->data.plogheadVersion.c_str(), node->openCnt);
    }
}

int IndexCache::setFirstWriteFlag(string key)
{
    std::lock_guard<std::mutex> lock(index_cache_mutex);
    Node *node = getnodeInlock(key);

    if(node)
    {
        /*firstWritFlag must set be false*/
        node->data.firstWritFlag= false;
    }

    return 0;
}

int IndexCache::setFilesizeOrClearGetAttrStat(string path,
        off64_t cacheFileSize, bool clearGetAttrStat, CACHE_STAT_TYPE statType)
{
    index_cache_mutex.lock();
    Node *node = getnodeInlock(path);
    off64_t tempFileSize = -1;

    if(node)
    {
        if (cacheFileSize > node->data.stGetAttrStat.st_size)
        {
            node->data.stGetAttrStat.st_size = cacheFileSize;
            tempFileSize = node->data.stGetAttrStat.st_size;
        }
        node->data.stGetAttrStat.st_mtime = time((time_t *)NULL);
        if (statType < STAT_TYPE_BUTT){
            node->data.statType = statType;
            clock_gettime(CLOCK_MONOTONIC_COARSE, &(node->data.getAttrCacheSetTs));
        }
        //clear getAttrCacheSetTs so stGetAttrStat is invalid
        if (clearGetAttrStat)
        {
            memset(&(node->data.stGetAttrStat), 0, sizeof(struct stat));
            memset(&(node->data.getAttrCacheSetTs), 0, sizeof(struct timespec));
        }
    }
    index_cache_mutex.unlock();

    S3FS_PRN_INFO("set statCache,tempFileSize=%ld,cacheFileSize=%ld,path=%s,clearGetAttrMs=%d", 
        tempFileSize,cacheFileSize,path.c_str(),clearGetAttrStat);

    return 0;
}

int IndexCache::setFilesizeAndClearIndexCacheTime(string path, off64_t cacheFileSize)
{
    index_cache_mutex.lock();
    Node *node = getnodeInlock(path);
    off64_t tempFileSize = -1;

    if(node)
    {
        node->data.stGetAttrStat.st_size = cacheFileSize;
        tempFileSize = node->data.stGetAttrStat.st_size;
        node->data.stGetAttrStat.st_mtime = time((time_t *) NULL);
        //clear getAttrCacheSetTs so stGetAttrStat is invalid
        memset(&(node->data.getAttrCacheSetTs), 0, sizeof(struct timespec));
    }
    index_cache_mutex.unlock();

    S3FS_PRN_INFO("set statCache,tempFileSize=%ld,cacheFileSize=%ld,path=%s",
                  tempFileSize, cacheFileSize, path.c_str());

    return 0;
}

void IndexCache::AddEntryOpenCnt(string path)
{
    std::lock_guard<std::mutex> lock(index_cache_mutex);
    Node *node = getnodeInlock(path);

    if(node)
    {
        operateOpenCnt(node, ADD_OPEN_CNT);
    }
}

Node* IndexCache::getFreeNodeAndEraseMap(const char* pathStr)
{
    SNAS_ListHead* pListHead = NULL;
    if(SNAS_ListEmpty(&lru_head))
    {
        //no free node in lru list,so get node from opencnt list
        if(SNAS_ListEmpty(&openflag_head))
        {
            S3FS_PRN_ERR("both free and opencnt list is empty,path=%s",pathStr);
            return NULL;
        }
        else
        {
            S3FS_PRN_WARN("free list empty,get from opencnt list,path=%s",pathStr);
            pListHead = &openflag_head;
        }
    }
    else
    {
        pListHead = &lru_head;
    }

    //get a node from lru_list tail
    SNAS_ListHead  *taillistnode = pListHead->prev;
    SNAS_ListDel(taillistnode);
    Node* node = container_of(taillistnode, Node, list_ptr);

    Node* nodeInMap = getnodeInlock(node->data.key);

    if (nodeInMap && nodeInMap == node)
    {
    	S3FS_PRN_INFO("erase node,  key[%s], openCnt[%d]",
            node->data.key.c_str(), node->openCnt);
            hashmap_.erase(node->data.key);
    }
    S3FS_PRN_DBG("free node, key[%s], openCnt[%d]",
        node->data.key.c_str(), node->openCnt);
    node->openCnt = 0;

    return node;
}

int IndexCache::putIndexInternal(string key,tag_index_cache_entry_t * data,tag_open_cnt_use_type openCntType)
{
    index_cache_mutex.lock();
    Node *node = getnodeInlock(key);

    if(node)
    { // node exists
        SNAS_ListDel(&node->list_ptr);

        if (0 < data->inodeNo)
        {
            node->data.inodeNo = data->inodeNo;
        }
        if (!data->dentryname.empty())
        {
            node->data.dentryname      = data->dentryname;
        }
        if (!data->fsVersionId.empty())
        {
            node->data.fsVersionId     = data->fsVersionId;
        }
        if (!data->plogheadVersion.empty())
        {
            node->data.plogheadVersion = data->plogheadVersion;
        }
        if (!data->originName.empty())
        {
            node->data.originName = data->originName;
        }
        node->data.getAttrCacheSetTs = data->getAttrCacheSetTs;
        node->data.stGetAttrStat = data->stGetAttrStat;
        
        operateOpenCnt(node, openCntType);
        putNodeToList(node, key);
        index_cache_mutex.unlock();
        S3FS_PRN_DBG("put index cache and exist, inode[%lld], dentryName[%s], key[%s], datakey[%s], openCnt[%d]",
        node->data.inodeNo, node->data.dentryname.c_str(), key.c_str(), node->data.key.c_str(), node->openCnt);
    }
    else
    {
        S3FS_PRN_DBG("put index cache and not exist");
        node = getFreeNodeAndEraseMap(key.c_str());
        if(NULL == node)
        {
            index_cache_mutex.unlock();
            S3FS_PRN_WARN("put index cache and not exist and lru_list is empty, then failed, inode[%lld], dentryName[%s], key[%s]",
                data->inodeNo, data->dentryname.c_str(), key.c_str());
            return -1;
        }

        //get a node from lru_list tail
        data->key     = key;
        node->data    = *data;
        operateOpenCnt(node, openCntType);
        hashmap_[key] = node;
        putNodeToList(node, key);

        index_cache_mutex.unlock();
        S3FS_PRN_INFO("put index cache and not exist, insert and move to head, inode[%lld], dentryName[%s], key[%s], datakey[%s], openCnt[%d]",
            node->data.inodeNo, node->data.dentryname.c_str(), key.c_str(), node->data.key.c_str(), node->openCnt);
    }
    return 0;
}

/**
 * @Description: update indexcache, openCnt donnt change
 * @Param: [key, index_cache_enytry]
 * @return: 0:success, -1: fail
 * @Date: 2018/7/23
 */
int IndexCache::PutIndexNotchangeOpenCnt(string key, tag_index_cache_entry_t *data)
{
    return putIndexInternal(key, data, NOCHANGE_OPEN_CNT);
}

/**
 * @Description: update indexcache, openCnt add 1
 * @Param: [key, index_cache_enytry]
 * @return: 0:success, -1: fail
 * @Date: 2018/7/23
 */
int IndexCache::PutIndexAddOpenCnt(string key, tag_index_cache_entry_t *data)
{
    return putIndexInternal(key, data, ADD_OPEN_CNT);
}

/**
 * @Description: close file , openCnt reduce 1
 * @Param: [key]
 * @return: 0:success, -1: fail
 * @Date: 2018/7/23
 */
int IndexCache::PutIndexReduceOpenCnt(string key)
{
    S3FS_PRN_DBG("start release file[%s] handle in index cache", key.c_str());
    std::lock_guard<std::mutex> lock(index_cache_mutex);
    Node *node = getnodeInlock(key);

    if(node)
    { // node exists
        SNAS_ListDel(&node->list_ptr);
        S3FS_PRN_DBG("release the file in cache, inode[%lld], dentryName[%s], key[%s], openCnt[%d]",
                        node->data.inodeNo, node->data.dentryname.c_str(), key.c_str(), node->openCnt);
        operateOpenCnt(node, REDUCE_OPEN_CNT);
        putNodeToList(node, key);
        return 0;
    }
    else
    {
        S3FS_PRN_ERR("file[%s] not exist ", key.c_str());
        return -1;
    }
}
//find cache entry by key and copy entry to output data
bool IndexCache::GetIndex(string key, tag_index_cache_entry_t* data)
{
    index_cache_mutex.lock();
    Node *node = getnodeInlock(key);
    if(node)
    {
        // check name in dentryname is same as name in key
        std::string dentryname = node->data.dentryname;
        std::string name_in_node = dentryname.substr(dentryname.find_last_of("/") + 1);
        std::string name_in_key = key.substr(key.find_last_of("/") + 1);
        if(name_in_node.compare(name_in_key) != 0 || key.compare(node->data.key) != 0)
        {
            S3FS_PRN_ERR("cache inconsistent: dentryName(%s), inode(%lld), key(%s), keyInNode(%s)",
                         dentryname.c_str(), node->data.inodeNo, key.c_str(), node->data.key.c_str());
            index_cache_mutex.unlock();

            if (cache_assert)
            {
                assert(true == false);
            }
            return false;
        }

        /* update to head of list for lru cache */
        SNAS_ListDel(&node->list_ptr);
        putNodeToList(node, key);
        *data = node->data;

        index_cache_mutex.unlock();

        /* check fuse path and shardkey from indexcache */
        const char* key_c_str = key.c_str();
        key_c_str++;

        S3FS_PRN_DBG("get index cache dentryname(%s) with key(%s), openCnt[%d]",
            data->dentryname.c_str(), key_c_str, node->openCnt);
        return true;
    }
    else
    {// not exist
        index_cache_mutex.unlock();
        S3FS_PRN_DBG("get index cache and not exist, then return false");
        return false;
    }
}

void IndexCache::DeleteIndex(string key)
{
    S3FS_PRN_DBG("start delete index cache path = %s",key.c_str());

    index_cache_mutex.lock();

    Node *node = getnodeInlock(key);
    if(node)
    {
        // node exists

        SNAS_ListDel(&node->list_ptr);
        SNAS_ListAddTail(&node->list_ptr, &lru_head);

        hashmap_.erase(key);
        index_cache_mutex.unlock();
	    S3FS_PRN_INFO("delete index cache and exist, inode[%lld], dentryName[%s], key[%s], openCnt[%d]",
            node->data.inodeNo, node->data.dentryname.c_str(), key.c_str(), node->openCnt);

    }
    else
    {
        //not existed
        index_cache_mutex.unlock();
        S3FS_PRN_DBG("delete index cache but not exist, key[%s]", key.c_str());
    }
}

/* only for rename now */
void IndexCache::ReplaceIndex(string srcKey, string destKey, tag_index_cache_entry_t* p_index_cache_entry)
{
    index_cache_mutex.lock();

    Node *srcNode = getnodeInlock(srcKey);
    Node *destNode = getnodeInlock(destKey);
    if(srcNode)
    {
        // srcNode exists
        int openCnt = srcNode->openCnt;
        if (destNode)
        {
            if (destNode->data.inodeNo == srcNode->data.inodeNo)
            {
                /* update destNode openCnt and location in list */
                destNode->openCnt += openCnt;
            }
            else
            {
                S3FS_PRN_WARN("oldDestKey [path=%s, inode=%lld, dentryName=%s], change to [inode=%lld, dentryName=%s].",
                    destKey.c_str(), destNode->data.inodeNo, destNode->data.dentryname.c_str(),
                    srcNode->data.inodeNo, srcNode->data.dentryname.c_str());
                std::string dentryName = destNode->data.dentryname;
                destNode->data = srcNode->data;
                destNode->data.key = destKey;
                destNode->data.dentryname = dentryName;
                destNode->openCnt = openCnt;
            }

            SNAS_ListDel(&destNode->list_ptr);
            putNodeToList(destNode, destKey);
        }
        else if (0 != openCnt)
        {
            destNode = getFreeNodeAndEraseMap(destKey.c_str());
            if (NULL == destNode)
            {
                S3FS_PRN_ERR("no available node, replace index cache from path = %s to %s failed, openCnt(%d).",srcKey.c_str(), destKey.c_str(), openCnt);
            }
            else if (NULL == p_index_cache_entry || p_index_cache_entry->dentryname.empty())
            {
                S3FS_PRN_ERR("no dentryName, replace index cache from path = %s to %s failed, openCnt(%d).",srcKey.c_str(), destKey.c_str(), openCnt);
            }
            else
            {
                destNode->data = srcNode->data;
                destNode->data.key = destKey;
                destNode->data.dentryname = p_index_cache_entry->dentryname;
                destNode->openCnt = openCnt;

                putNodeToList(destNode, destKey);
                hashmap_[destKey] = destNode;
            }
        }

        SNAS_ListDel(&srcNode->list_ptr);
        SNAS_ListAddTail(&srcNode->list_ptr, &lru_head);

        hashmap_.erase(srcKey);

        index_cache_mutex.unlock();
        if (destNode) {
            S3FS_PRN_INFO("replace index cache from path = %s to %s, openCnt[%d]",
                          srcKey.c_str(), destKey.c_str(), destNode->openCnt);
        }else{
            S3FS_PRN_INFO("replace index cache from path = %s to %s fail", srcKey.c_str(), destKey.c_str());
        }

    }
    else
    {
        //not existed
        index_cache_mutex.unlock();
        S3FS_PRN_DBG("replace index cache path %s, but not exist", srcKey.c_str());
    }
}
void IndexCache::resizeMetaCacheCapacity(size_t capacity)
{
    int diffNum = capacity - gMetaCacheSize;
    if (diffNum < 0)
    {
        return;
    }

    size_t extendNum = diffNum;

    Node* extendEntries = new Node[extendNum];

    index_cache_mutex.lock();
    for(size_t i=0; i<extendNum; ++i)
    {
        SNAS_ListAdd(&(extendEntries+i)->list_ptr, &lru_head);
    }
    gMetaCacheSize = capacity;
    index_cache_mutex.unlock();
}

