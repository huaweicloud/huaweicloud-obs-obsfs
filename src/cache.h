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

#ifndef S3FS_CACHE_H_
#define S3FS_CACHE_H_

#include <mutex>
#include "common.h"

//
// Struct
//
struct stat_cache_entry {
  struct stat       stbuf;
  unsigned long     hit_count;
  struct timespec   cache_date;
  headers_t         meta;
  bool              isforce;
  bool              noobjcache;  // Flag: cache is no object for no listing.
  unsigned long     notruncate;  // 0<:   not remove automatically at checking truncate

  stat_cache_entry() : hit_count(0), isforce(false), noobjcache(false), notruncate(0L) {
    memset(&stbuf, 0, sizeof(struct stat));
    cache_date.tv_sec  = 0;
    cache_date.tv_nsec = 0;
    meta.clear();
  }
};

typedef std::map<std::string, stat_cache_entry*> stat_cache_t; // key=path

//
// Class
//
class StatCache
{
  private:
    static StatCache       singleton;
    static std::mutex      stat_cache_lock;
    stat_cache_t  stat_cache;
    bool          IsExpireTime;
    bool          IsExpireIntervalType;         // if this flag is true, cache data is updated at last access time.
    time_t        ExpireTime;
    unsigned long CacheSize;
    bool          IsCacheNoObject;

  private:
    StatCache();
    ~StatCache();

    void Clear(void);
    bool GetStat(std::string& key, struct stat* pst, headers_t* meta, bool overcheck, const char* petag, bool* pisforce);
    // HasLock variants — called with stat_cache_lock already held
    bool AddStatHasLock(std::string& key, headers_t& meta, bool forcedir, bool no_truncate);
    bool DelStatHasLock(const char* key);
    bool TruncateCacheHasLock(void);

  public:
    // Reference singleton
    static StatCache* getStatCacheData(void) {
      return &singleton;
    }

    // Attribute
    unsigned long GetCacheSize(void) const;
    unsigned long SetCacheSize(unsigned long size);
    time_t GetExpireTime(void) const;
    time_t SetExpireTime(time_t expire, bool is_interval = false);
    time_t UnsetExpireTime(void);
    bool SetCacheNoObject(bool flag);
    bool EnableCacheNoObject(void) {
      return SetCacheNoObject(true);
    }
    bool DisableCacheNoObject(void) {
      return SetCacheNoObject(false);
    }
    bool GetCacheNoObject(void) const {
      return IsCacheNoObject;
    }

    // Get stat cache
    bool GetStat(std::string& key, struct stat* pst, headers_t* meta, bool overcheck = true, bool* pisforce = NULL) {
      return GetStat(key, pst, meta, overcheck, NULL, pisforce);
    }
    bool GetStat(std::string& key, struct stat* pst, bool overcheck = true) {
      return GetStat(key, pst, NULL, overcheck, NULL, NULL);
    }
    bool GetStat(std::string& key, headers_t* meta, bool overcheck = true) {
      return GetStat(key, NULL, meta, overcheck, NULL, NULL);
    }
    bool HasStat(std::string& key, bool overcheck = true) {
      return GetStat(key, NULL, NULL, overcheck, NULL, NULL);
    }
    bool HasStat(std::string& key, const char* etag, bool overcheck = true) {
      return GetStat(key, NULL, NULL, overcheck, etag, NULL);
    }

    // Cache For no object
    bool IsNoObjectCache(std::string& key, bool overcheck = true);
    bool AddNoObjectCache(std::string& key);

    // Add stat cache
    bool AddStat(std::string& key, headers_t& meta, bool forcedir = false, bool no_truncate = false);

    // Change no truncate flag
    void ChangeNoTruncateFlag(std::string key, bool no_truncate);

    // Delete stat cache
    bool DelStat(const char* key);
    bool DelStat(std::string& key) {
      return DelStat(key.c_str());
    }

    // ISSUE2026051100001 iter-7: delete only if entry exists and notruncate==0.
    // Caller is readdir's stale-entry cleanup. notruncate>0 entries belong to
    // in-progress mkdir/create/open operations that may not yet be visible in
    // OBS ListBucket (eventual consistency window) — they must not be evicted
    // by an "external delete" signal derived from ListBucket result.
    //
    // ISSUE2026051200001 review fix: also refuse deletion for entries younger
    // than 1 second (cleanup-race grace) — see implementation comment.
    // `now_monotonic_sec` is CLOCK_MONOTONIC_COARSE seconds (same clock as
    // stat_cache_entry::cache_date). Pass 0 to read it internally; pass a
    // snapshot to share a consistent grace boundary across one cleanup pass
    // (or to fast-forward in tests).
    // Returns true if an entry was actually removed; false if missing, pinned,
    // or within the grace window.
    bool DelStatIfTruncatable(const std::string& key, time_t now_monotonic_sec = 0);

    // Test-only: subtract `seconds` from an existing entry's cache_date so
    // tests can simulate aged entries without sleeping. Returns false if the
    // key is absent. Production code must not call this.
    bool TestBackdateEntry(const std::string& key, long seconds);

    // [新增] 通配符删除方法 - 删除所有以 pattern 开头的条目
    // Used for directory rename to invalidate all child path caches
    // Time complexity: O(n) where n is the number of cache entries
    bool DelStatWildcard(const std::string& pattern);

    // [新增] Get all child entries for a directory (Object bucket mode only)
    // Traverses stat_cache to find direct children of the specified directory
    // Returns: map of basename -> objtype_t for all direct children
    bool GetChildStatMap(const std::string& dir, s3obj_type_map_t& objmap);
};

//
// Functions
//
bool convert_header_to_stat(const char* path, headers_t& meta, struct stat* pst, bool forcedir = false);

#endif // S3FS_CACHE_H_

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: noet sw=4 ts=4 fdm=marker
* vim<600: noet sw=4 ts=4
*/
