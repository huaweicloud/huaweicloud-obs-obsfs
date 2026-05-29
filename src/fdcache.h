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
#ifndef FD_CACHE_H_
#define FD_CACHE_H_

#include <sys/statvfs.h>
#include <mutex>
#include "curl.h"
#include "fdcache_untreated.h"

//------------------------------------------------
// CacheFileStat
//------------------------------------------------
class CacheFileStat
{
  private:
    CacheFileStat(const CacheFileStat&) = delete;             // non-copyable
    CacheFileStat& operator=(const CacheFileStat&) = delete;  // non-copyable
    std::string path;
    int         fd;

  private:
    static bool MakeCacheFileStatPath(const char* path, std::string& sfile_path, bool is_create_dir = true);

  public:
    static bool DeleteCacheFileStat(const char* path);
    static bool RenameCacheFileStat(const char* oldpath, const char* newpath);
    static bool CheckCacheFileStatTopDir(void);
    static bool DeleteCacheFileStatDirectory(void);

    explicit CacheFileStat(const char* tpath = NULL);
    ~CacheFileStat();

    bool Open(void);
    bool Release(void);
    bool OverWriteFile(const std::string& strall) const;
    bool SetPath(const char* tpath, bool is_open = true);
    int GetFd(void) const { return fd; }
};

//------------------------------------------------
// fdpage & PageList
//------------------------------------------------
// page block information
struct fdpage
{
  off_t  offset;
  size_t bytes;
  bool   loaded;
  bool   modified;  // true = this region was modified by Write()

  fdpage(off_t start = 0, size_t size = 0, bool is_loaded = false, bool is_modified = false)
           : offset(start), bytes(size), loaded(is_loaded), modified(is_modified) {}

  off_t next(void) const { return (offset + bytes); }
  off_t end(void) const { return (0 < bytes ? offset + bytes - 1 : 0); }
};
typedef std::list<struct fdpage*> fdpage_list_t;

class FdEntity;

//
// Management of loading area/modifying
//
class PageList
{
  friend class FdEntity;    // only one method access directly pages.

  private:
    fdpage_list_t pages;

  private:
    void Clear(void);
    bool Compress(void);
    bool Parse(off_t new_pos);

  public:
    static void FreeList(fdpage_list_t& list);

    explicit PageList(size_t size = 0, bool is_loaded = false);
    ~PageList();

    bool Init(size_t size, bool is_loaded);
    size_t Size(void) const;
    bool Resize(size_t size, bool is_loaded);

    bool IsPageLoaded(off_t start = 0, size_t size = 0) const;                  // size=0 is checking to end of list
    bool SetPageLoadedStatus(off_t start, size_t size, bool is_loaded = true, bool is_compress = true);
    bool FindUnloadedPage(off_t start, off_t& resstart, size_t& ressize) const;
    size_t GetTotalUnloadedPageSize(off_t start = 0, size_t size = 0) const;    // size=0 is checking to end of list
    int GetUnloadedPages(fdpage_list_t& unloaded_list, off_t start = 0, size_t size = 0) const;  // size=0 is checking to end of list

    size_t GetTotalLoadedPageSize(off_t start = 0, size_t size = 0) const;    // size=0 is checking to end of list

    bool SetPageModifiedStatus(off_t start, size_t size, bool is_modified, bool is_compress = true);
    void GetPageListsForMultipartUpload(fdpage_list_t& dlpages, fdpage_list_t& mixuppages, off_t max_partsize) const;
    void ClearAllModified(void);

    bool Serialize(CacheFileStat& file, bool is_output, ino_t inode = 0);
    void Dump(void);
};

//------------------------------------------------
// class FdEntity
//------------------------------------------------
class FdEntity
{
  private:
    FdEntity(const FdEntity&) = delete;             // non-copyable
    FdEntity& operator=(const FdEntity&) = delete;  // non-copyable

    pthread_mutex_t fdent_lock;
    bool            is_lock_init;
    PageList        pagelist;
    int             refcnt;         // reference count
    std::string     path;           // object path
    std::string     cachepath;      // local cache file path
                                    // (if this is empty, does not load/save pagelist.)
    std::string     mirrorpath;     // mirror file path to local cache file path
    int             fd;             // file descriptor(tmp file or cache file)
    FILE*           pfile;          // file pointer(tmp file or cache file)
    bool            is_modify;      // if file is changed, this flag is true
    headers_t       orgmeta;        // original headers at opening
    size_t          size_orgmeta;   // original file size in original headers
    ino_t           inode;          // [s3fs-fuse compat] cache file inode at open time

    std::string     upload_id;      // for no cached multipart uploading when no disk space
    etaglist_t      etaglist;       // for no cached multipart uploading when no disk space
    UntreatedParts  untreated_list; // tracks dirty ranges for no-cache multipart upload

    // Async prefetch state (streamread mode)
    pthread_t       prefetch_thread;
    bool            prefetch_active;
    pthread_mutex_t prefetch_mutex;
    off_t           prefetch_start;
    size_t          prefetch_size;

    // Streamread v2: range-based prefetch state
    off_t           sr_range_start;      // current consumption range start (-1 = uninitialized)
    int             sr_range_reads;      // Read count within current range
    bool            sr_next_launched;    // whether next range prefetch has been triggered

    // CRC32C end-to-end integrity verification (object bucket only)
    // Records per-segment CRC from user write buffer ("truth value"),
    // verified against tmpfile pread before upload to detect silent corruption.
    static const off_t CRC_SEGMENT_SIZE = 4 * 1024 * 1024;  // 4MB CRC segment, independent of multipart_size
    struct SegmentCRC {
        uint32_t crc32c;
        bool     valid;
        SegmentCRC() : crc32c(0), valid(false) {}
    };
    std::vector<SegmentCRC>  segment_crcs_;          // index = offset / CRC_SEGMENT_SIZE
    std::vector<off_t>       segment_written_end_;   // per-segment written end offset (append vs overwrite)
    bool                     nocache_fallback_;       // true after NoCacheLoadAndPost, skip CRC verify
    size_t                   last_tracked_loaded_bytes_;  // disk usage tracking for max_tmpdir_disk_usage

  private:
    static int FillFile(int fd, unsigned char byte, size_t size, off_t start);

    void Clear(void);
    void UpdateDiskUsedTracking();
    void SaveDiskTrackingOnClose();
    int OpenMirrorFile(void);
    bool SetAllStatus(bool is_loaded);                          // [NOTE] not locking
    //bool SetAllStatusLoaded(void) { return SetAllStatus(true); }
    bool SetAllStatusUnloaded(void) { return SetAllStatus(false); }

    // Async prefetch (streamread mode)
    struct PrefetchArg {
      FdEntity* entity;
      off_t     start;
      size_t    size;
    };
    static void* PrefetchWorkerEntry(void* arg);
    void PrefetchWorker(off_t start, size_t size);
    void WaitPrefetch(void);
    void LaunchRangePrefetch(off_t start, size_t size);
    void PunchRange(off_t range_start, size_t range_size);
    size_t GetStreamReadWindow(void);

    // CRC32C integrity helpers
    uint32_t CrcFromTmpfile(size_t seg_idx, off_t file_size);
    void     EnsureSegmentCrcsSize(size_t needed_segs);
    void     UpdateCrcOnWrite(const char* bytes, ssize_t wsize, off_t start, off_t file_size);
    void     UpdateBoundarySegmentCrc(off_t old_size, off_t new_size);
    int      VerifyCrcBeforeUpload(const fdpage_list_t& upload_parts, off_t file_size);

  public:
    static ino_t GetInode(int file_fd);   // fstat(file_fd) → inode
    ino_t GetInode(void) const;      // stat(cachepath) → inode

    explicit FdEntity(const char* tpath = NULL, const char* cpath = NULL);
    ~FdEntity();

    void Close(void);
    void CloseRef(void);
    bool CloseRefAndCheck(void);  // returns true if refcnt reached 0
    void CloseCleanup(void);
    void CancelPrefetch(void);
    bool IsOpen(void) const { return (-1 != fd); }
    bool IsPrefetchActive(void) const { return prefetch_active; }
    int Open(headers_t* pmeta = NULL, ssize_t size = -1, time_t time = -1, bool no_fd_lock_wait = false);
    bool OpenAndLoadAll(headers_t* pmeta = NULL, size_t* size = NULL, bool force_load = false);
    int Dup(bool no_fd_lock_wait = false);

    const char* GetPath(void) const { return path.c_str(); }
    void SetPath(const std::string &newpath) { path = newpath; }
    bool RenamePath(const std::string& newpath, std::string& fentmapkey);
    int GetFd(void) const { return fd; }

    bool GetStats(struct stat& st);
    // ISSUE2026051200001 M.3: snapshot the original PUT headers (uid/gid/mode etc.)
    // saved at open time. release() uses this to reconstruct full meta when
    // updating StatCache post-flush, avoiding the previous stripped-meta bug
    // that wiped uid/gid/mode on every close.
    bool GetOriginalHeaders(headers_t& meta_out) const;
    int SetMtime(time_t time);
    bool UpdateMtime(void);
    bool GetSize(size_t& size);
    bool SetMode(mode_t mode);
    bool SetUId(uid_t uid);
    bool SetGId(gid_t gid);
    bool SetContentType(const char* path);

    int Load(off_t start = 0, size_t size = 0, bool is_modified_flag = false); // size=0 means loading to end
    int NoCacheLoadAndPost(off_t start = 0, size_t size = 0);   // size=0 means loading to end
    int NoCachePreMultipartPost(void);
    int NoCacheMultipartPost(int tgfd, off_t start, size_t size);
    int NoCacheCompleteMultipartPost(void);

    bool IsModified() const { return is_modify; }
    bool HasUntreatedParts() const { return !untreated_list.empty(); }
    int RowFlush(const char* tpath, bool force_sync = false);
    int Flush(bool force_sync = false) { return RowFlush(NULL, force_sync); }

    bool ReserveDiskSpace(size_t size);  // entity-level Reserve with retry + cleanup

    ssize_t Read(char* bytes, off_t start, size_t size, bool force_load = false);
    ssize_t Write(const char* bytes, off_t start, size_t size);

    bool PunchHole(off_t start = 0, size_t size = 0);
    bool TransitionToMultipart(void);

    void CleanupCache();
};
typedef std::map<std::string, class FdEntity*> fdent_map_t;   // key=path, value=FdEntity*

//------------------------------------------------
// class FdManager
//------------------------------------------------
class FdManager
{
  friend class FdEntity;   // FdEntity::UpdateDiskUsedTracking() reads max_tmpdir_disk_usage

  private:
    static FdManager       singleton;
    static pthread_mutex_t fd_manager_lock;
    static pthread_mutex_t cache_cleanup_lock;
    static std::mutex      disk_reservation_lock;
    static bool            is_lock_init;
    static std::string     cache_dir;
    static bool            check_cache_dir_exist;
    static size_t          free_disk_space; // limit free disk space
    static size_t          disk_reserved_bytes; // total bytes reserved by all threads
    static size_t          max_tmpdir_disk_usage; // 0=unlimited, >0=cap in bytes
    static size_t          obsfs_disk_used_bytes_; // internal disk usage counter (replaces base_free_disk_space)
    static std::map<std::string, size_t> cache_file_tracked_bytes_; // per-file tracking for use_cache mode
    static std::string     tmp_dir;          // tmpdir= mount option, default "/tmp"
    static off_t           max_dirty_data;   // flush threshold, default 5GB (-1=disabled)

    fdent_map_t            fent;

  private:
    static uint64_t GetFreeDiskSpaceHasLock(const char* path);
    static bool     IsSafeDiskSpaceHasLock(const char* path, size_t size);
    static bool IsDir(const std::string& dir);
    void CleanupCacheDirInternal(const std::string &path = "");

  public:
    FdManager();
    ~FdManager();

    // Reference singleton
    static FdManager* get(void) { return &singleton; }

    static bool DeleteCacheDirectory(void);
    static int DeleteCacheFile(const char* path);
    static bool SetCacheDir(const char* dir);
    static bool IsCacheDir(void) { return (0 < FdManager::cache_dir.size()); }
    static const char* GetCacheDir(void) { return FdManager::cache_dir.c_str(); }
    static bool MakeCachePath(const char* path, std::string& cache_path, bool is_create_dir = true, bool is_mirror_path = false);
    static bool CheckCacheTopDir(void);
    static bool MakeRandomTempPath(const char* path, std::string& tmppath);
    static bool SetCheckCacheDirExist(bool is_check);
    static bool CheckCacheDirExist(void);

    static uint64_t GetFreeDiskSpace(const char* path);
    static size_t GetEnsureFreeDiskSpace(void);
    static size_t SetEnsureFreeDiskSpace(size_t size);
    static size_t InitEnsureFreeDiskSpace(void) { return SetEnsureFreeDiskSpace(0); }
    static bool IsSafeDiskSpace(const char* path, size_t size, bool withmsg = false);
    static bool ReserveDiskSpace(size_t size);
    static void ReleaseDiskSpace(size_t size);
    static size_t GetReservedDiskSpace(void);
    static void   SetMaxTmpdirDiskUsage(size_t bytes);
    static size_t GetMaxTmpdirDiskUsage(void);
    static void   AdjustDiskUsed(ssize_t delta);
    static size_t GetDiskUsed(void) { return obsfs_disk_used_bytes_; }
    static void   SetCacheFileTrackedBytes(const std::string& path, size_t bytes);
    static size_t GetCacheFileTrackedBytes(const std::string& path);
    static void   RemoveCacheFileTrackedBytes(const std::string& path);

    static bool SetTmpDir(const char* dir);
    static const char* GetTmpDir(void) { return FdManager::tmp_dir.c_str(); }
    static bool CheckTmpDirExist(void);
    static bool CheckTmpDirFsType(void);
    static bool IsSupportedFsType(long fs_type);
    static bool IsSupportedFsName(const char* fs_name);
    static FILE* MakeTempFile(void);

    static void SetMaxDirtyData(off_t size) { FdManager::max_dirty_data = size; }
    static off_t GetMaxDirtyData(void) { return FdManager::max_dirty_data; }

    // WARNING: Returns raw pointer WITHOUT incrementing refcnt.
    // Safe ONLY when the caller holds a valid FUSE file handle (fi->fh)
    // whose refcnt contribution is guaranteed by FUSE kernel serialization
    // (flush/fsync complete before release for the same fh).
    // For operations WITHOUT fi->fh (e.g., getattr), use GetOpenEntitySize()
    // or GetFdEntityWithDup() instead. See ISSUE2026031500003.
    FdEntity* GetFdEntity(const char* path, int existfd = -1);
    FdEntity* GetFdEntityWithDup(const char* path, int existfd = -1);
    bool GetOpenEntitySize(const char* path, off_t* size);
    FdEntity* Open(const char* path, headers_t* pmeta = NULL, ssize_t size = -1, time_t time = -1, bool force_tmpfile = false, bool is_create = true, bool no_fd_lock_wait = false);
    FdEntity* ExistOpen(const char* path, int existfd = -1, bool ignore_existfd = false);
    void Rename(const std::string &from, const std::string &to);
    bool Close(FdEntity* ent);
    bool CloseRef(FdEntity* ent);
    bool ChangeEntityToTempPath(FdEntity* ent, const char* path);
    void CleanupCacheDir();
};

#endif // FD_CACHE_H_

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: noet sw=4 ts=4 fdm=marker
* vim<600: noet sw=4 ts=4
*/
