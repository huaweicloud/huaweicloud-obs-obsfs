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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>
#include <fcntl.h>
#include <curl/curl.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <map>
#include <string>
#include <list>
#include <set>

#include "s3fs_operations.h"
#include "s3fs_util.h"
#include "string_util.h"
#include "common.h"
#include "curl.h"
#include "cache.h"
#include "hws_index_cache.h"
#include "hws_fd_cache.h"
#include "fdcache.h"

//
// directory_empty()/s3fs_readdir_s3() no longer hold any process-local lock:
// all local variables and StatCache has its own internal lock.
// Holding a lock during S3 network I/O was a deadlock/hang risk.
//
// pending_dir_entries (a process-local "newly-created" name index) was removed
// in ISSUE2026051100001 cleanup: every add_pending_dir_entry() call was paired
// with an AddStat(no_truncate=true) on the same key, so StatCache children
// already cover everything readdir/directory_empty needs. See the issue doc
// for the full subset proof and Recommendation.
//

//
// Retry mechanism for s3fs_write_s3
//
#define WRITE_RETRY_MAX_ATTEMPTS 8
#define WRITE_RETRY_DELAY_MS 4000

//
// Multipart upload configuration
//
#define MULTIPART_THRESHOLD (25 * 1024 * 1024)  // 25MB - use multipart for files larger than this
#define MULTIPART_PART_SIZE (10 * 1024 * 1024)   // 10MB - size of each part in multipart upload

//
// OBS object key total length limit (including mount_prefix, all directory levels and slashes)
//
#define MAX_OBJECT_KEY_LENGTH 1024

static int validate_object_key_length(const char* path)
{
    // Check total object key length (mount_prefix + path ≤ 1024)
    std::string fullPath = mount_prefix + path;
    size_t full_path_size = fullPath.size() - 1;
    if(full_path_size > MAX_OBJECT_KEY_LENGTH){
        S3FS_PRN_ERR("object key length exceeds maximum, [path=%s][size=%zu][max=%d]",
                     fullPath.c_str(), full_path_size, MAX_OBJECT_KEY_LENGTH);
        return -ENAMETOOLONG;
    }

    // Check last path component length (≤ NAME_MAX=255)
    // FUSE VFS does not enforce f_namemax, so we must check explicitly
    const char* basename = strrchr(path, '/');
    basename = basename ? basename + 1 : path;
    if(strlen(basename) > NAME_MAX){
        S3FS_PRN_ERR("filename exceeds NAME_MAX, [path=%s][name=%s][len=%zu]",
                     path, basename, strlen(basename));
        return -ENAMETOOLONG;
    }
    return 0;
}

// [ISSUE2026040900001 C1/C2] Removed unused helper functions:
//   - is_retryable_error()
//   - map_obs_error_to_errno()
// They were added in 4fb1d769 (2026-01-22) as part of a retry framework whose
// wrapper functions were later removed in df303d97 (2026-02-28) due to a
// truncate data-loss bug. The two helpers were left behind but had zero
// production callers; the only consumers were 2 unit-test files (helper_test
// + 3 mock_test cases), which have also been removed by this issue.

// Forward declarations for static helper functions used in rename_directory_s3
static bool is_truncated_s3ops(xmlDocPtr doc);
static std::string get_next_marker_s3ops(xmlDocPtr doc);

//
// Helper functions for object semantic mode
//

// Forward declarations
static bool GetXmlNsUrl(xmlDocPtr doc, std::string& nsurl);

// Helper function to get current time as string (for ctime updates)
static std::string s3fs_str_realtime()
{
    struct timespec ts;
    if(-1 == clock_gettime(CLOCK_REALTIME, &ts)){
        S3FS_PRN_WARN("failed to clock_gettime by errno(%d)", errno);
        ts.tv_sec = time(NULL);
        ts.tv_nsec = 0;
    }
    char buf[64];
    snprintf(buf, sizeof(buf), "%jd", (intmax_t)ts.tv_sec);
    return std::string(buf);
}

// Check parent directory access for rename operations
// This function checks if we have permission to write/execute in parent directories
static int check_parent_object_access_s3(const char* path, int mask)
{
    std::string parent;

    S3FS_PRN_DBG("[path=%s][mask=%d]", path, mask);

    if(0 == strcmp(path, "/") || 0 == strcmp(path, ".")){
        // path is mount point.
        return 0;
    }

    // Check execute permission on all parent directories
    if(X_OK == (mask & X_OK)){
        for(parent = mydirname(path); !parent.empty(); parent = mydirname(parent)){
            if(parent == "."){
                parent = "/";
            }
            // Check if parent directory exists and we have execute permission
            struct stat stbuf;
            if(0 != s3fs_getattr_s3(parent.c_str(), &stbuf)){
                S3FS_PRN_DBG("Parent directory not found: %s", parent.c_str());
                return -ENOENT;
            }
            if(parent == "/" || parent == "."){
                break;
            }
        }
    }

    // Check remaining permissions on immediate parent
    mask = (mask & ~X_OK);
    if(0 != mask){
        parent = mydirname(path);
        if(parent == "."){
            parent = "/";
        }
        // For S3 object semantic mode, we don't have full permission checking
        // Just verify the parent directory exists
        struct stat stbuf;
        if(0 != s3fs_getattr_s3(parent.c_str(), &stbuf)){
            S3FS_PRN_DBG("Parent directory not found: %s", parent.c_str());
            return -ENOENT;
        }
    }
    return 0;
}

// Get xattr value from object metadata
static bool get_meta_xattr_value_s3(const char* path, std::string& rawvalue)
{
    if(!path || '\0' == path[0]){
        S3FS_PRN_ERR("path is empty.");
        return false;
    }
    S3FS_PRN_DBG("[path=%s]", path);

    rawvalue.clear();

    headers_t meta;
    S3fsCurl s3fscurl;
    int result = s3fscurl.HeadRequest(path, meta);
    if(0 != result){
        S3FS_PRN_ERR("Failed to get object(%s) headers", path);
        return false;
    }

    headers_t::const_iterator iter;
    if(meta.cend() == (iter = meta.find("x-amz-meta-xattr"))){
        return false;
    }
    rawvalue = iter->second;
    return true;
}

// Rename a single object using Copy API
static int rename_object_s3(const char* from, const char* to, bool update_ctime)
{
    int         result;
    headers_t   meta;
    struct stat stbuf;

    S3FS_PRN_INFO1("[from=%s][to=%s][update_ctime=%d]", from, to, update_ctime);

    // [S4] Flush removed — s3fs_rename_s3 L1670-1682 already flushes before calling us.
    // s3fs-fuse rename_object (s3fs.cpp:1525) also does not flush.
    // Flush(true) had force_sync=true risk (ISSUE2026022500003).

    // Check permissions
    if(0 != (result = check_parent_object_access_s3(to, W_OK | X_OK))){
        S3FS_PRN_ERR("No permission for target parent directory: %s", to);
        return result;
    }
    if(0 != (result = check_parent_object_access_s3(from, W_OK | X_OK))){
        S3FS_PRN_ERR("No permission for source parent directory: %s", from);
        return result;
    }

    // [P1] Single HEAD request for both stat and meta (was two separate HEADs).
    // Reference: s3fs-fuse get_object_attribute(from, &stbuf, &meta) at s3fs.cpp:1541
    S3fsCurl s3fscurl;
    result = s3fscurl.HeadRequest(from, meta);
    if(0 != result){
        S3FS_PRN_ERR("Source object not found or metadata error: %s (result=%d)", from, result);
        return result;
    }
    if(!convert_header_to_stat(from, meta, &stbuf)){
        S3FS_PRN_ERR("Failed to convert headers to stat: %s", from);
        return -EIO;
    }

    // Build copy source path
    std::string strSourcePath = from;
    std::string copy_source = "/" + bucket + get_realpath(strSourcePath.c_str());

    // Update metadata for the copy operation
    if(update_ctime){
        meta["x-amz-meta-ctime"] = s3fs_str_realtime();
    }
    meta["x-amz-copy-source"] = urlEncode(copy_source);
    meta["Content-Type"] = S3fsCurl::LookupMimeType(to);
    meta["x-amz-metadata-directive"] = "REPLACE";

    // Preserve xattr if present
    std::string xattrvalue;
    if(get_meta_xattr_value_s3(from, xattrvalue)){
        S3FS_PRN_DBG("Set xattrs = %s", urlDecode(xattrvalue).c_str());
        meta["x-amz-meta-xattr"] = xattrvalue;
    }

    // Check if file is large (>=5GB) and use multipart upload copy if needed
    // AWS S3 limit: single copy operation max 5GB
    const off_t LARGE_FILE_THRESHOLD = 5368709120; // 5GB in bytes
    bool is_large_file = (stbuf.st_size >= LARGE_FILE_THRESHOLD);

    if(is_large_file){
        S3FS_PRN_INFO("Large file detected (size=%ld >= 5GB), using multipart upload copy", (long)stbuf.st_size);
        result = s3fscurl.MultipartRenameRequest(from, to, meta, stbuf.st_size);
    }else{
        // Perform the copy operation using standard copy API
        result = s3fscurl.PutHeadRequest(to, meta, true, "");
    }

    if(0 != result){
        S3FS_PRN_ERR("Copy failed: %s -> %s (result=%d)", from, to, result);
        return result;
    }

    // Rename FdEntity in fd manager: renames cache file, stat file, and path atomically.
    // RenamePath handles disk cache rename; rename() syscall overwrites destination.
    // No explicit DeleteCacheFile needed (s3fs-fuse rename_object also doesn't call it).
    FdManager::get()->Rename(from, to);

    // [B1+B2+B3+P2] Delete source and propagate error.
    // Reference: s3fs-fuse s3fs.cpp:1610-1627
    result = s3fscurl.DeleteRequest(from);
    if(0 != result){
        S3FS_PRN_ERR("Copy succeeded but delete of source failed: %s (result=%d). "
                     "Source object may still exist on S3.", from, result);
    }

    // Only remove source from StatCache if delete succeeded (s3fs-fuse L1310)
    std::string from_key = from;
    std::string to_key = to;
    if(0 == result){
        StatCache::getStatCacheData()->DelStat(from_key);
    }

    // ALWAYS populate destination StatCache — copy succeeded (s3fs-fuse L1616-1621)
    // [P2] Reuse existing meta from HEAD(from), no extra HEAD(to) needed.
    // ISSUE2026051200001 M.3: notruncate=false. obsfs PUT is synchronous —
    // by the time we get here the object is on OBS; no "phantom existence"
    // window. Aligns with s3fs-fuse mkdir behavior (s3fs.cpp:1280 default
    // notruncate=false).
    StatCache::getStatCacheData()->AddStat(to_key, meta, false, false);

    return result;
}

// Rename a directory by cloning it
static int clone_directory_object_s3(const char* from, const char* to, bool update_ctime, const char* pxattrvalue)
{
    int result;
    struct stat stbuf;

    S3FS_PRN_INFO1("[from=%s][to=%s][update_ctime=%d]", from, to, update_ctime);

    // Get source directory attributes
    if(0 != (result = s3fs_getattr_s3(from, &stbuf))){
        S3FS_PRN_ERR("Source directory not found: %s", from);
        return result;
    }

    if(!S_ISDIR(stbuf.st_mode)){
        S3FS_PRN_ERR("Source is not a directory: %s", from);
        return -ENOTDIR;
    }

    // Ensure destination path ends with /
    std::string dest_path = to;
    if(dest_path.length() > 1 && dest_path.back() != '/'){
        dest_path += "/";
    }

    // Ensure source path ends with / for directory marker lookup
    std::string from_path = from;
    if(from_path.length() > 1 && from_path.back() != '/'){
        from_path += "/";
    }

    // Get source metadata
    headers_t meta;
    S3fsCurl s3fscurl;
    result = s3fscurl.HeadRequest(from_path.c_str(), meta);
    if(0 != result){
        S3FS_PRN_ERR("Failed to get source metadata: %s", from_path.c_str());
        return result;
    }

    // Update ctime if requested
    if(update_ctime){
        meta["x-amz-meta-ctime"] = s3fs_str_realtime();
    }

    // Set xattr if provided
    if(pxattrvalue){
        meta["x-amz-meta-xattr"] = pxattrvalue;
    }

    // Set directory content type
    meta["Content-Type"] = "application/x-directory";

    // Create the directory marker object
    result = s3fscurl.PutRequest(dest_path.c_str(), meta, -1);
    if(0 != result){
        S3FS_PRN_ERR("Failed to create directory marker: %s (result=%d)", dest_path.c_str(), result);
        return result;
    }

    // Add to StatCache
    // ISSUE2026051200001 M.3: notruncate=false (synchronous PUT, no phantom window)
    StatCache::getStatCacheData()->AddStat(dest_path, meta, true, false);

    S3FS_PRN_INFO("Directory cloned: %s -> %s", from, to);
    return 0;
}

// Rename a directory (recursive)
static int rename_directory_s3(const char* from, const char* to)
{
    S3FS_PRN_INFO1("[from=%s][to=%s]", from, to);

    std::string basepath = from;
    if(basepath != "/"){
        basepath += "/";
    }

    // [M3] Capture StatCache children BEFORE invalidation.
    // May include files created but not yet flushed to S3.
    // Reference: s3fs-fuse s3fs.cpp:1823 GetChildStatList(basepath, headlist)
    s3obj_type_map_t statcache_children;
    StatCache::getStatCacheData()->GetChildStatMap(basepath, statcache_children);

    // [新增] 删除所有子路径缓存 (在 S3 操作之前)
    // This ensures cache consistency before we start S3 operations
    S3FS_PRN_INFO3("invalidating stat cache for renamed directory[path=%s]", basepath.c_str());
    StatCache::getStatCacheData()->DelStatWildcard(basepath);
    StatCache::getStatCacheData()->DelStat(from);

    // IndexCache invalidation removed for object bucket mode
    // StatCache::DelStatWildcard() already handles this

    // ===== NEW: Flush all open files under this directory (Object bucket) =====
    // CRITICAL: Prevent data loss when renaming directory with open files
    // Race condition scenario:
    //   1. User writes data to /dir/file.txt (data in cache, not uploaded)
    //   2. User renames directory: mv /dir /dir2
    //   3. Without flush, file.txt data is lost or uploaded to wrong path
    //
    // Solution: Flush all open files under /dir before S3 operations
    // Reference: s3fs-fuse doesn't have this issue because it flushes on release
    // Note: s3fs-fuse's rename_directory does not flush open files under the directory.
    // The flush-before-rename for individual files is handled at the top-level rename_file_s3.

    // List all objects under the source directory (recursive, no delimiter)
    // Reference: s3fs-fuse uses flat listing with pagination to get ALL descendants
    std::string s3prefix = basepath.substr(1);  // strip leading '/' for S3 key prefix
    std::string base_query = "prefix=";
    if(basepath != "/"){
        base_query += urlEncode(s3prefix) + "&";
    }
    base_query += "max-keys=1000&encoding-type=url";

    // Structure to hold all objects to rename
    struct mvnode {
        std::string old_path;
        std::string new_path;
        bool is_dir;
    };
    std::vector<mvnode> mvnodes;

    // Add the base directory itself
    std::string strfrom = from;
    std::string strto = to;
    if(strto.length() > 1 && strto.back() != '/'){
        strto += "/";
    }
    mvnodes.push_back({strfrom, strto, true});

    // S3 prefix length (without leading '/') for correct path extraction
    size_t prefix_len = s3prefix.length();

    // Pagination loop: keep listing until IsTruncated is false
    bool truncated = true;
    std::string next_marker;

    while(truncated){
        std::string query = base_query;
        if(!next_marker.empty()){
            query += "&marker=" + urlEncode(next_marker);
        }

        S3fsCurl s3fscurl;
        int result = s3fscurl.ListBucketRequest(from, query.c_str());
        if(0 != result){
            S3FS_PRN_ERR("ListBucketRequest failed for directory rename: %s (result=%d)", from, result);
            return result;
        }

        const BodyData* body = s3fscurl.GetBodyData();
        if(!body || body->size() == 0){
            S3FS_PRN_ERR("Empty response body for directory listing: %s", from);
            return -EIO;
        }

        // Parse XML to get objects from this page
        xmlDocPtr doc = xmlReadMemory(body->str(), static_cast<int>(body->size()), "", NULL, 0);
        if(!doc){
            S3FS_PRN_ERR("xmlReadMemory failed for directory listing: %s", from);
            return -EIO;
        }

        xmlXPathContextPtr ctx = xmlXPathNewContext(doc);
        if(!ctx){
            xmlFreeDoc(doc);
            return -EIO;
        }

        // Register namespace if present
        std::string xmlnsurl;
        if(GetXmlNsUrl(doc, xmlnsurl)){
            xmlXPathRegisterNs(ctx, (xmlChar*)"s3", (xmlChar*)xmlnsurl.c_str());
        }

        // Track last key seen for fallback marker
        std::string last_key;

        // Process all objects (Contents) - flat listing includes all descendants
        // Directory markers are detected by key suffix '/'
        std::string contents_xpath = "//";
        if(!xmlnsurl.empty()){
            contents_xpath += "s3:";
        }
        contents_xpath += "Contents";

        xmlXPathObjectPtr contents_xp = xmlXPathEvalExpression((xmlChar*)contents_xpath.c_str(), ctx);
        if(contents_xp && !xmlXPathNodeSetIsEmpty(contents_xp->nodesetval)){
            xmlNodeSetPtr nodes = contents_xp->nodesetval;
            for(int i = 0; i < nodes->nodeNr; i++){
                ctx->node = nodes->nodeTab[i];

                std::string key_xpath = ".//";
                if(!xmlnsurl.empty()){
                    key_xpath += "s3:";
                }
                key_xpath += "Key";

                xmlXPathObjectPtr key_xp = xmlXPathEvalExpression((xmlChar*)key_xpath.c_str(), ctx);
                if(key_xp && !xmlXPathNodeSetIsEmpty(key_xp->nodesetval)){
                    xmlChar* key = xmlNodeListGetString(doc, key_xp->nodesetval->nodeTab[0]->xmlChildrenNode, 1);
                    if(key){
                        std::string full_key = (char*)key;
                        xmlFree(key);

                        full_key = urlDecodeSpecial(full_key);
                        last_key = full_key;

                        // Skip the base directory marker itself
                        if(full_key == s3prefix || full_key == basepath){
                            if(key_xp) xmlXPathFreeObject(key_xp);
                            continue;
                        }

                        // Detect directory markers by trailing '/'
                        bool is_dir = (!full_key.empty() && full_key.back() == '/');

                        std::string from_name = "/" + full_key;
                        std::string to_name = strto + full_key.substr(prefix_len);

                        mvnodes.push_back({from_name, to_name, is_dir});
                    }
                }
                if(key_xp){
                    xmlXPathFreeObject(key_xp);
                }
            }
        }
        if(contents_xp){
            xmlXPathFreeObject(contents_xp);
        }

        // Check if there are more pages
        truncated = is_truncated_s3ops(doc);
        if(truncated){
            // Try NextMarker first, fall back to last key seen
            next_marker = get_next_marker_s3ops(doc);
            if(next_marker.empty()){
                // Flat listing (no delimiter) may not return NextMarker;
                // use the last key from this page as marker (same as s3fs-fuse)
                if(!last_key.empty()){
                    next_marker = last_key;
                } else {
                    S3FS_PRN_WARN("IsTruncated=true but no NextMarker and no last key; breaking pagination loop");
                    truncated = false;
                }
            }
            S3FS_PRN_INFO("Directory rename pagination: listed %zu objects so far, next_marker=%s",
                         mvnodes.size(), next_marker.c_str());
        }

        xmlXPathFreeContext(ctx);
        xmlFreeDoc(doc);
    }

    S3FS_PRN_INFO("Directory rename: total %zu objects listed for %s", mvnodes.size(), from);

    // [M3] Merge StatCache-only children into rename list.
    // Prevents data loss: create /dir/file.txt (unflushed) + mv /dir /dir2
    for(const auto& entry : statcache_children){
        std::string child_from = basepath + entry.first;
        std::string child_to = strto + entry.first;
        bool child_is_dir = IS_DIR_OBJ(entry.second);
        bool already_listed = false;
        for(const auto& mn : mvnodes){
            if(mn.old_path == child_from){
                already_listed = true;
                break;
            }
        }
        if(!already_listed){
            S3FS_PRN_INFO("Merging StatCache-only child into rename list: %s",
                         child_from.c_str());
            mvnodes.push_back({child_from, child_to, child_is_dir});
        }
    }

    // Sort by path (files before directories, shallow before deep)
    std::sort(mvnodes.begin(), mvnodes.end(), [](const mvnode& a, const mvnode& b){
        return a.old_path < b.old_path;
    });

    int result;

    // Clone directory objects first
    for(const auto& mn : mvnodes){
        if(mn.is_dir){
            std::string xattrvalue;
            const char* pxattrvalue = nullptr;
            if(get_meta_xattr_value_s3(mn.old_path.c_str(), xattrvalue)){
                pxattrvalue = xattrvalue.c_str();
            }

            // Update ctime only for top-level directory
            bool update_ctime = (mn.old_path == strfrom);
            if(0 != (result = clone_directory_object_s3(mn.old_path.c_str(), mn.new_path.c_str(), update_ctime, pxattrvalue))){
                S3FS_PRN_ERR("clone_directory_object failed: %s -> %s (result=%d)",
                             mn.old_path.c_str(), mn.new_path.c_str(), result);
                return result;
            }
        }
    }

    // Rename files
    for(const auto& mn : mvnodes){
        if(!mn.is_dir){
            // Keep ctime for files
            if(0 != (result = rename_object_s3(mn.old_path.c_str(), mn.new_path.c_str(), false))){
                S3FS_PRN_ERR("rename_object failed: %s -> %s (result=%d)",
                             mn.old_path.c_str(), mn.new_path.c_str(), result);
                return result;
            }
        }
    }

    // IndexCache path mapping updates removed for object bucket mode
    // StatCache.DelStat() at lines 708-710 already invalidated old paths
    // StatCache will be repopulated on next getattr/lookup calls

    // Remove old directory markers (bottom-up) via direct S3 DELETE.
    // We bypass s3fs_rmdir_s3() / directory_empty() because we've already moved
    // every child — the old dirs are guaranteed empty by construction, no need
    // to spend a ListBucket round-trip per directory to re-prove that.
    // Reference: s3fs-fuse also uses direct delete for old directory markers.
    for(auto it = mvnodes.rbegin(); it != mvnodes.rend(); ++it){
        if(it->is_dir && it->old_path != strfrom){
            std::string old_dir = it->old_path;
            if(!old_dir.empty() && old_dir.back() != '/'){
                old_dir += "/";
            }
            // [B4] Propagate delete error for subdirectory markers.
            S3fsCurl del_curl;
            if(0 != (result = del_curl.DeleteRequest(old_dir.c_str()))){
                S3FS_PRN_ERR("Failed to delete old directory marker: %s (result=%d)", old_dir.c_str(), result);
                return result;
            }
            StatCache::getStatCacheData()->DelStat(it->old_path);
        }
    }

    // Finally remove the source base directory marker
    {
        std::string base_dir = strfrom;
        if(!base_dir.empty() && base_dir.back() != '/'){
            base_dir += "/";
        }
        // [B5] Propagate delete error for base directory marker.
        S3fsCurl del_curl;
        if(0 != (result = del_curl.DeleteRequest(base_dir.c_str()))){
            S3FS_PRN_ERR("Failed to delete source directory marker: %s (result=%d)", base_dir.c_str(), result);
            return result;
        }
        StatCache::getStatCacheData()->DelStat(strfrom);
    }

    // [B6] Only reachable when all deletes succeeded.
    S3FS_PRN_INFO("Directory rename completed: %s -> %s", from, to);
    return 0;
}

// Helper function to get XML namespace URL from document
static bool GetXmlNsUrl(xmlDocPtr doc, std::string& nsurl)
{
    nsurl = "";
    if(!doc){
        return false;
    }

    xmlNodePtr root = xmlDocGetRootElement(doc);
    if(!root){
        return false;
    }

    // Check for ANY default namespace on root element (prefix == NULL)
    // This supports both standard S3 namespace and OBS-specific namespace
    xmlNsPtr ns = root->nsDef;
    while(ns != NULL){
        if(ns->prefix == NULL){
            // Found a default namespace (xmlns="..."), use it for XPath queries
            nsurl = (char*)ns->href;
            S3FS_PRN_DBG("Found default namespace: %s", nsurl.c_str());
            return true;
        }
        ns = ns->next;
    }

    return false;
}

// XPath text extraction helper (local to s3fs_operations.cpp)
// Mirrors get_base_exp in hws_s3fs.cpp but uses our local GetXmlNsUrl
static xmlChar* get_base_exp_s3ops(xmlDocPtr doc, const char* exp)
{
    if(!doc){
        return NULL;
    }
    xmlXPathContextPtr ctx = xmlXPathNewContext(doc);
    if(!ctx){
        return NULL;
    }

    std::string xmlnsurl;
    std::string exp_string = "//";
    if(GetXmlNsUrl(doc, xmlnsurl)){
        xmlXPathRegisterNs(ctx, (xmlChar*)"s3", (xmlChar*)xmlnsurl.c_str());
        exp_string += "s3:";
    }
    exp_string += exp;

    xmlXPathObjectPtr marker_xp = xmlXPathEvalExpression((xmlChar*)exp_string.c_str(), ctx);
    if(!marker_xp){
        xmlXPathFreeContext(ctx);
        return NULL;
    }
    if(xmlXPathNodeSetIsEmpty(marker_xp->nodesetval)){
        xmlXPathFreeObject(marker_xp);
        xmlXPathFreeContext(ctx);
        return NULL;
    }
    xmlNodeSetPtr nodes = marker_xp->nodesetval;
    xmlChar* result = xmlNodeListGetString(doc, nodes->nodeTab[0]->xmlChildrenNode, 1);

    xmlXPathFreeObject(marker_xp);
    xmlXPathFreeContext(ctx);
    return result;
}

// Check IsTruncated element in ListBucket response
static bool is_truncated_s3ops(xmlDocPtr doc)
{
    bool result = false;
    xmlChar* strTruncate = get_base_exp_s3ops(doc, "IsTruncated");
    if(!strTruncate){
        return result;
    }
    if(0 == strcasecmp((const char*)strTruncate, "true")){
        result = true;
    }
    xmlFree(strTruncate);
    return result;
}

// Extract NextMarker element from ListBucket response.
// ListBucket requests use encoding-type=url, so the server returns URL-encoded
// NextMarker values. We decode here so callers can urlEncode() once for the
// query string, avoiding double encoding (ISSUE2026022600012).
static std::string get_next_marker_s3ops(xmlDocPtr doc)
{
    xmlChar* marker = get_base_exp_s3ops(doc, "NextMarker");
    if(!marker){
        return std::string();
    }
    std::string result = urlDecodeSpecial((const char*)marker);
    xmlFree(marker);
    return result;
}

// Simple string to off_t conversion (replaces static str_to_off_t)
off_t str_to_off_t(const char* str)
{
    if(!str || '\0' == *str){
        return 0;
    }
    char* endptr;
    off_t result = strtol(str, &endptr, 10);
    return result;
}

// Build standard metadata headers for S3 objects
// This helper function ensures consistent metadata across all operations
// If preserve_mode is true, existing mode in meta will be preserved
void build_standard_metadata(headers_t& meta, mode_t mode, const std::string& content_type, bool preserve_mode)
{
    // Preserve mode if requested
    std::string saved_mode;
    if(preserve_mode){
        headers_t::iterator it = meta.find(hws_obs_meta_mode);
        if(it != meta.end()){
            saved_mode = it->second;
        }
    }

    meta.clear();

    // Set content type
    if(!content_type.empty()){
        meta["Content-Type"] = content_type;
    }

    // Add standard metadata using hws_obs_meta constants
    char buf[64];
    snprintf(buf, sizeof(buf), "%d", getuid());
    meta[hws_obs_meta_uid] = buf;

    snprintf(buf, sizeof(buf), "%d", getgid());
    meta[hws_obs_meta_gid] = buf;

    if(preserve_mode && !saved_mode.empty()){
        meta[hws_obs_meta_mode] = saved_mode;
    } else {
        // Store full mode_t as decimal (matches s3fs-fuse behavior)
        // get_mode() in s3fs_util.cpp uses s3fs_strtoofft() which parses decimal
        snprintf(buf, sizeof(buf), "%d", (int)mode);
        meta[hws_obs_meta_mode] = buf;
    }

    time_t now = time(NULL);
    snprintf(buf, sizeof(buf), "%ld", (long)now);
    meta[hws_obs_meta_mtime] = buf;
}

// Helper functions generate_virtual_inode() and get_basename()
// are now defined in s3fs_util.cpp for sharing with IndexCache

// Stub implementations for object semantic (standard S3) mode
// These will be implemented in subsequent phases

// Basic file/directory operations
int s3fs_getattr_s3(const char* path, struct stat* stbuf)
{
    int result;
    headers_t meta;

    S3FS_PRN_INFO("[path=%s]", path);

    // Handle root directory specially
    if(0 == strcmp(path, "/")){
        memset(stbuf, 0, sizeof(struct stat));
        stbuf->st_nlink = 2;  // Root directory: . and ..
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_uid = getuid();
        stbuf->st_gid = getgid();
        stbuf->st_size = 4096;
        stbuf->st_atime = stbuf->st_mtime = stbuf->st_ctime = time(NULL);
        stbuf->st_ino = generate_virtual_inode(path);
        return 0;
    }

    ino_t virtual_ino = generate_virtual_inode(path);

    // Check StatCache before querying OBS
    // This handles OBS eventual consistency for recently written files
    std::string cache_key = path;
    S3FS_PRN_DBG("[path=%s] Looking up in StatCache with key=%s", path, cache_key.c_str());

    // For object bucket mode, use overcheck=true to handle directory markers (with trailing slash)
    // For file bucket mode, use overcheck=false to maintain original behavior
    bool overcheck = (g_bucket_type == BUCKET_TYPE_OBJECT_SEMANTIC);
    if(StatCache::getStatCacheData()->GetStat(cache_key, stbuf, &meta, overcheck)){
        S3FS_PRN_DBG("[path=%s] Found in stat cache!", path);
        stbuf->st_ino = virtual_ino;
        // If file is open, override st_size from FdEntity (s3fs-fuse Issue 241).
        // Uses GetOpenEntitySize: atomic lookup + fstat under fd_manager_lock,
        // no raw pointer exposed. See ISSUE2026031500003.
        {
            off_t open_size;
            if(FdManager::get()->GetOpenEntitySize(path, &open_size)){
                stbuf->st_size = open_size;
            }
        }
        return 0;
    }
    S3FS_PRN_DBG("[path=%s] NOT found in stat cache, querying S3", path);

    // ISSUE2026051200001 review M-1: short-circuit on negative cache.
    // If a prior getattr already proved the object doesn't exist (and the
    // user opted into noobj cache), avoid the HEAD/HEAD/ListBucket triplet.
    // No-op when IsCacheNoObject is false (default).
    if(g_bucket_type == BUCKET_TYPE_OBJECT_SEMANTIC &&
       StatCache::getStatCacheData()->IsNoObjectCache(cache_key)){
        S3FS_PRN_DBG("[path=%s] negative cache hit", path);
        return -ENOENT;
    }

    // First attempt: query without trailing slash
    S3fsCurl s3fscurl;
    result = s3fscurl.HeadRequest(path, meta);

    // If failed, try with trailing slash (directory marker)
    if(result != 0 && (result == -ENOENT || result == -2)){
        std::string dir_path = path;

        // Add trailing slash if not present
        if(!dir_path.empty() && dir_path.back() != '/'){
            dir_path += '/';
        }

        S3FS_PRN_DBG("[path=%s] Trying with directory marker: %s", path, dir_path.c_str());

        headers_t meta_tmp;
        int result2 = s3fscurl.HeadRequest(dir_path.c_str(), meta_tmp);

        if(result2 == 0){
            // Success - this is a directory marker object
            S3FS_PRN_INFO("[path=%s] Found directory marker", path);
            meta = meta_tmp;
            result = 0;
        }
    }

    // ISSUE2026051200001 C.1: implicit directory probe.
    // Before declaring ENOENT, check whether `path` is an "implicit directory"
    // — a prefix that has no marker object but does have children. This
    // happens when external SDK / OBS Console writes deep paths
    // (e.g. `dir/sub/leaf.txt`) without creating dir/ or dir/sub/ markers.
    //
    // Reuses S3fsCurl::CheckObjectBucketPrefixExists (added in ISSUE2026042900001
    // for the mount-prefix check): same ListBucket(prefix=path/, delimiter=/)
    // shape, same XML probe (any non-empty <Contents><Key> or
    // <CommonPrefixes><Prefix> => exists), same prefix normalization
    // (leading-'/' strip + trailing-'/' append). One shared helper means
    // mount-prefix validation and getattr fallback can't drift.
    //
    // Object bucket only. File bucket has its own path semantics and
    // ListBucket-style probing isn't appropriate there.
    if(result != 0 && g_bucket_type == BUCKET_TYPE_OBJECT_SEMANTIC){
        S3fsCurl probe_curl;
        if(0 == probe_curl.CheckObjectBucketPrefixExists(path)){
            S3FS_PRN_INFO("[path=%s] detected as implicit directory (ListBucket probe non-empty)", path);
            // Synthesize complete meta so the function-tail code produces
            // a sane stbuf for the kernel and the AddStat-in-cache path
            // (M.2-equivalent) can be exercised by the next readdir.
            meta.clear();
            meta["Content-Type"]    = "application/x-directory";
            meta["Content-Length"]  = "0";
            meta[hws_obs_meta_uid]  = std::to_string(geteuid());
            meta[hws_obs_meta_gid]  = std::to_string(getegid());
            // M-3 review: align with s3fs-fuse 0750 (was 0755).
            meta[hws_obs_meta_mode] = std::to_string(S_IFDIR | 0750);
            // No mtime — ListBucket prefix probe doesn't return
            // LastModified for CommonPrefixes. Consistent with
            // s3fs-fuse DIR_NOT_EXIST_OBJECT path.
            result = 0;
        }
    }

    // Object not found - return ENOENT directly like s3fs-fuse
    // No retry loop here to avoid multi-threading issues with fprintf
    if(result != 0){
        S3FS_PRN_DBG("object not found: %s (result=%d)", path, result);
        // ISSUE2026051200001 review M-1: write a negative-cache entry so
        // repeated stat() of the same non-existent path does NOT re-issue
        // HEAD + HEAD + ListBucket triplet on every call. AddNoObjectCache
        // is a no-op when IsCacheNoObject is false (default), so this is
        // safe to call unconditionally; users who enable
        // `-o enable_noobj_cache` opt into the speedup.
        if(g_bucket_type == BUCKET_TYPE_OBJECT_SEMANTIC){
            std::string ne_key(path);
            StatCache::getStatCacheData()->AddNoObjectCache(ne_key);
        }
        return -ENOENT;
    }

    // Initialize stat structure
    memset(stbuf, 0, sizeof(struct stat));

    // Parse metadata from headers
    stbuf->st_ino = generate_virtual_inode(path);

    // Get file size from Content-Length header
    headers_t::iterator it = meta.find("Content-Length");
    if(it != meta.end()){
        stbuf->st_size = str_to_off_t(it->second.c_str());
    }

    // Get modification time: prefer x-amz-meta-mtime, fallback to Last-Modified
    // Reference: s3fs-fuse metaheader.cpp get_mtime() + convert_header_to_stat()
    stbuf->st_mtime = get_mtime(meta, true);

    // Set current time for atime and ctime (we don't have accurate values from S3)
    stbuf->st_atime = stbuf->st_ctime = time(NULL);

    // Determine if it's a symlink (check for symlink target metadata)
    it = meta.find("x-amz-meta-symlink-target");
    bool is_symlink = (it != meta.end());

    // Determine if it's a directory
    it = meta.find("Content-Type");
    bool is_directory = false;
    if(it != meta.end()){
        is_directory = (it->second.find("application/x-directory") != std::string::npos ||
                       it->second.find("text/directory") != std::string::npos);
    }

    // Also check if path ends with / for directory marker objects
    size_t path_len = strlen(path);
    if(path_len > 1 && path[path_len - 1] == '/'){
        is_directory = true;
    }

    // Set mode and type using get_mode() which handles both decimal storage
    // (s3fs-fuse compatible) and file type inference from Content-Type
    // This uses the same code path as StatCache (cache.cpp convert_header_to_stat)
    stbuf->st_mode = get_mode(meta, path, true, is_directory);

    if(is_symlink){
        // Override file type for symlinks
        stbuf->st_mode = S_IFLNK | (stbuf->st_mode & 07777);
        stbuf->st_nlink = 1;
        // st_size for symlinks should be the length of the target path
        it = meta.find("x-amz-meta-symlink-target");
        if(it != meta.end()){
            stbuf->st_size = it->second.length();
        }
    } else if(S_ISDIR(stbuf->st_mode)){
        stbuf->st_nlink = 2;  // Directories have . and ..
    } else {
        stbuf->st_nlink = 1;
    }

    // Get UID from metadata or default to current user
    it = meta.find("x-amz-meta-uid");
    if(it != meta.end()){
        stbuf->st_uid = (uid_t)str_to_off_t(it->second.c_str());
    } else {
        stbuf->st_uid = getuid();
    }

    // Get GID from metadata or default to current group
    it = meta.find("x-amz-meta-gid");
    if(it != meta.end()){
        stbuf->st_gid = (gid_t)str_to_off_t(it->second.c_str());
    } else {
        stbuf->st_gid = getgid();
    }

    // If file is open, override st_size from FdEntity (s3fs-fuse Issue 241).
    // Uses GetOpenEntitySize: atomic lookup + fstat under fd_manager_lock,
    // no raw pointer exposed. See ISSUE2026031500003.
    {
        off_t open_size;
        if(FdManager::get()->GetOpenEntitySize(path, &open_size)){
            stbuf->st_size = open_size;
        }
    }

    return 0;
}

int s3fs_readlink_s3(const char* path, char* buf, size_t size) {
    int result;
    headers_t meta;

    S3FS_PRN_INFO("[path=%s][size=%zu]", path, size);

    // Get object metadata to check for symlink target
    S3fsCurl s3fscurl;
    result = s3fscurl.HeadRequest(path, meta);

    if(result != 0){
        S3FS_PRN_ERR("head request failed: %s (result=%d)", path, result);
        return result;
    }

    // Check for symlink target in metadata
    headers_t::iterator it = meta.find("x-amz-meta-symlink-target");
    if(it == meta.end()){
        S3FS_PRN_ERR("not a symlink: %s", path);
        return -EINVAL;
    }

    // Copy target to buffer, ensuring null termination
    size_t target_len = it->second.length();
    if(target_len >= size){
        S3FS_PRN_ERR("buffer too small for symlink target: %s (need %zu, have %zu)",
                      path, target_len + 1, size);
        return -ENAMETOOLONG;
    }

    strncpy(buf, it->second.c_str(), size);
    buf[target_len] = '\0';

    S3FS_PRN_INFO("readlink: %s -> %s", path, buf);
    return 0;
}

int s3fs_mknod_s3(const char* path, mode_t mode, dev_t rdev) {
    S3FS_PRN_DBG("=== s3fs_mknod_s3 CALLED: path=%s mode=%04o rdev=%d ===", path, mode, (int)rdev);

    int result;
    headers_t meta;

    if(0 != (result = validate_object_key_length(path))){
        return result;
    }

    S3FS_PRN_INFO("[path=%s][mode=%04o][rdev=%d]", path, mode, (int)rdev);

    // S3 only supports regular files
    if(!S_ISREG(mode)){
        S3FS_PRN_ERR("S3 only supports regular files: %s (mode=%04o)", path, mode);
        return -ENOTSUP;
    }

    // Build standard file metadata
    build_standard_metadata(meta, mode, S3fsCurl::LookupMimeType(path));

    // Create zero-byte object (fd=-1)
    S3fsCurl s3fscurl;
    S3FS_PRN_DBG("[mknod] PUT_REQUEST_START: path=%s", path);
    result = s3fscurl.PutRequest(path, meta, -1);
    S3FS_PRN_DBG("[mknod] PUT_REQUEST_END: result=%d for path=%s", result, path);

    if(0 != result){
        S3FS_PRN_ERR("failed to create file: %s (result=%d)", path, result);
        return result;
    }

    S3FS_PRN_INFO("file created: %s", path);

    // For object semantic mode, add to StatCache so it can be found by readdir
    if(g_bucket_type == BUCKET_TYPE_OBJECT_SEMANTIC){
        std::string cache_key = path;
        // ISSUE2026051200001 M.3: notruncate=false (synchronous PUT, no phantom window)
        StatCache::getStatCacheData()->AddStat(cache_key, meta, false, false);
        S3FS_PRN_DBG("[path=%s] Added to StatCache", path);
    }

    return 0;
}

int s3fs_mkdir_s3(const char* path, mode_t mode)
{
    int result;
    std::string strpath;
    headers_t meta;

    if(0 != (result = validate_object_key_length(path))){
        return result;
    }

    S3FS_PRN_INFO("[path=%s][mode=%04o]", path, mode);

    // The mount point itself always exists
    if(0 == strcmp(path, "/")){
        return 0;
    }

    // Normalize path - ensure it ends with / for directory marker
    strpath = path;
    if('/' != *strpath.rbegin()){
        strpath += "/";
    }

    // Build standard directory metadata
    build_standard_metadata(meta, mode, std::string("application/x-directory"));

    // Create the directory marker object (zero-byte object with fd=-1)
    S3fsCurl s3fscurl;
    S3FS_PRN_DBG("PUT_REQUEST_START: Creating directory marker at path=%s (normalized=%s)", path, strpath.c_str());
    result = s3fscurl.PutRequest(strpath.c_str(), meta, -1);
    S3FS_PRN_DBG("PUT_REQUEST_END: result=%d for path=%s", result, path);

    if(0 != result){
        S3FS_PRN_ERR("failed to create directory: %s (result=%d)", path, result);
        return result;
    }

    S3FS_PRN_DBG("directory created successfully: %s", path);

    // For object semantic mode, add to IndexCache and StatCache for immediate access
    if(g_bucket_type == BUCKET_TYPE_OBJECT_SEMANTIC){
        // Build metadata for StatCache
        headers_t dir_meta;
        build_standard_metadata(dir_meta, mode, std::string("application/x-directory"));
        dir_meta["Content-Length"] = "0";  // Directory marker is 0-byte

        // Add to StatCache using the original path (without trailing slash)
        // so that FUSE getattr calls will find it
        std::string cache_key = path;
        S3FS_PRN_DBG("[mkdir] Adding to StatCache: key=%s (original path=%s)", cache_key.c_str(), path);
        // ISSUE2026051200001 M.3: notruncate=false (synchronous PUT, no phantom window)
        bool cache_added = StatCache::getStatCacheData()->AddStat(cache_key, dir_meta, true, false);
        S3FS_PRN_DBG("[mkdir] AddStat returned: %s for key=%s", cache_added ? "SUCCESS" : "FAILED", cache_key.c_str());

        // IndexCache entry creation removed for object bucket mode
        // StatCache.AddStat() above already handles directory metadata caching
    }

    return 0;
}

int s3fs_unlink_s3(const char* path)
{
    int result;
    S3fsCurl s3fscurl;

    S3FS_PRN_INFO("[path=%s]", path);

    // Check parent directory access
    if(0 != (result = check_parent_object_access_s3(path, W_OK | X_OK))){
        return result;
    }

    // Delete the object
    result = s3fscurl.DeleteRequest(path);

    if(0 != result){
        S3FS_PRN_ERR("failed to delete object: %s (result=%d)", path, result);
        return result;
    }

    // Remove from StatCache so subsequent getattr will get correct result
    std::string cache_key = path;
    StatCache::getStatCacheData()->DelStat(cache_key);

    // Remove FdEntity cache file (no-op when cache_dir is empty, i.e. no-cache mode)
    FdManager::DeleteCacheFile(path);

    // IndexCache deletion removed for object bucket mode
    // StatCache.DelStat() above already handles cache invalidation

    S3FS_PRN_INFO("object deleted: %s", path);
    return 0;
}

// Helper function to check if a directory is empty
// Returns 0 if empty, -ENOTEMPTY if not empty, negative error on failure
// Note: No mutex needed — all locals are stack-allocated and StatCache has its
// own internal lock; S3 network calls must not be held under a lock.
static int directory_empty(const char* path)
{
    S3FS_PRN_INFO("[path=%s]", path);
    if(!path) {
        S3FS_PRN_ERR("directory_empty: NULL path");
        return -EINVAL;
    }

    // Fast path: if StatCache witnesses any direct child of this dir (and the
    // child entry is still within TTL), skip the OBS round-trip. Saves one
    // ListBucket on every rmdir/rename right after a local create — and is a
    // pure subset of what list_bucket would authoritatively return, so no
    // risk of false ENOTEMPTY (per-child GetStat below evicts stale ghosts
    // left by external deletes, both forms — with and without trailing slash
    // — via overcheck=true).
    {
        s3obj_type_map_t children;
        StatCache::getStatCacheData()->GetChildStatMap(path, children);
        for(const auto& kv : children) {
            std::string child_path = (strcmp(path, "/") == 0)
                ? "/" + kv.first
                : std::string(path) + "/" + kv.first;
            struct stat stbuf;
            if(StatCache::getStatCacheData()->GetStat(child_path, &stbuf, nullptr, true /*overcheck*/)) {
                S3FS_PRN_INFO("directory not empty: StatCache witness %s in %s",
                             kv.first.c_str(), path);
                return -ENOTEMPTY;
            }
        }
    }

    // Build the prefix for listing
    std::string prefix;
    if(strcmp(path, "/") != 0) {
        prefix = path;
        if(!prefix.empty() && prefix[0] == '/') {
            prefix = prefix.substr(1);
        }
        if(!prefix.empty() && prefix[prefix.length() - 1] != '/') {
            prefix += "/";
        }
    }

    // Build query string - max-keys=2: directory marker + at most 1 child
    std::string query;
    if(!prefix.empty()) {
        query += "prefix=" + urlEncode(prefix) + "&";
    }
    query += "delimiter=/&max-keys=2&encoding-type=url";

    // Make the list request (no lock held during network I/O)
    S3fsCurl s3fscurl;
    int result = s3fscurl.ListBucketRequest(path, query.c_str());
    if(0 != result) {
        S3FS_PRN_ERR("ListBucketRequest failed for %s (result=%d)", path, result);
        return result;
    }

    const BodyData* body = s3fscurl.GetBodyData();
    if(!body || body->size() == 0) {
        return 0;  // empty response = empty directory
    }

    // Parse XML response
    xmlDocPtr doc = xmlReadMemory(body->str(), static_cast<int>(body->size()), "", NULL, 0);
    if(!doc) {
        S3FS_PRN_WARN("xmlReadMemory failed for directory_empty: %s", path);
        return -EIO;
    }

    xmlXPathContextPtr ctx = xmlXPathNewContext(doc);
    if(!ctx) {
        S3FS_PRN_ERR("xmlXPathNewContext failed");
        xmlFreeDoc(doc);
        return -EIO;
    }

    // Register namespace if present
    std::string xmlnsurl;
    if(GetXmlNsUrl(doc, xmlnsurl)) {
        xmlXPathRegisterNs(ctx, (xmlChar*)"s3", (xmlChar*)xmlnsurl.c_str());
    }

    // Check for Contents (files)
    std::string contents_xpath = "//";
    if(!xmlnsurl.empty()) {
        contents_xpath += "s3:";
    }
    contents_xpath += "Contents";

    bool is_empty = true;

    xmlXPathObjectPtr contents_xp = xmlXPathEvalExpression((xmlChar*)contents_xpath.c_str(), ctx);
    if(contents_xp && !xmlXPathNodeSetIsEmpty(contents_xp->nodesetval)) {
        xmlNodeSetPtr nodes = contents_xp->nodesetval;

        for(int i = 0; i < nodes->nodeNr && is_empty; i++) {
            ctx->node = nodes->nodeTab[i];

            std::string key_xpath = ".//";
            if(!xmlnsurl.empty()) {
                key_xpath += "s3:";
            }
            key_xpath += "Key";

            xmlXPathObjectPtr key_xp = xmlXPathEvalExpression((xmlChar*)key_xpath.c_str(), ctx);
            if(key_xp && !xmlXPathNodeSetIsEmpty(key_xp->nodesetval)) {
                xmlChar* key = xmlNodeListGetString(doc, key_xp->nodesetval->nodeTab[0]->xmlChildrenNode, 1);
                if(key) {
                    std::string full_key = urlDecodeSpecial((char*)key);
                    xmlFree(key);

                    // Skip the directory marker itself (e.g., "dir/")
                    if(full_key != prefix &&
                       full_key.length() > prefix.length() &&
                       full_key.find(prefix) == 0) {
                        is_empty = false;
                        S3FS_PRN_DBG("directory not empty: child object %s", full_key.c_str());
                    }
                }
            }
            if(key_xp) {
                xmlXPathFreeObject(key_xp);
            }
        }
    }
    if(contents_xp) {
        xmlXPathFreeObject(contents_xp);
    }

    // Also check CommonPrefixes (subdirectories)
    if(is_empty) {
        std::string cp_xpath = "//";
        if(!xmlnsurl.empty()) {
            cp_xpath += "s3:";
        }
        cp_xpath += "CommonPrefixes";

        xmlXPathObjectPtr cp_xp = xmlXPathEvalExpression((xmlChar*)cp_xpath.c_str(), ctx);
        if(cp_xp && !xmlXPathNodeSetIsEmpty(cp_xp->nodesetval)) {
            is_empty = false;
            S3FS_PRN_DBG("directory not empty: has %d subdirectories", cp_xp->nodesetval->nodeNr);
        }
        if(cp_xp) {
            xmlXPathFreeObject(cp_xp);
        }
    }

    xmlXPathFreeContext(ctx);
    xmlFreeDoc(doc);

    S3FS_PRN_DBG("[path=%s] empty=%s", path, is_empty ? "true" : "false");
    return is_empty ? 0 : -ENOTEMPTY;
}

int s3fs_rmdir_s3(const char* path) {
    int result;
    std::string strpath;
    S3fsCurl s3fscurl;

    S3FS_PRN_INFO("[path=%s]", path);

    // Don't allow removing root
    if(0 == strcmp(path, "/")){
        return -EINVAL;
    }

    // Check parent directory access
    if(0 != (result = check_parent_object_access_s3(path, W_OK | X_OK))){
        return result;
    }

    // For object semantic mode, check if directory is empty first
    if(g_bucket_type == BUCKET_TYPE_OBJECT_SEMANTIC){
        S3FS_PRN_INFO("checking if directory is empty: %s", path);
        int empty_result = directory_empty(path);
        S3FS_PRN_INFO("directory_empty returned: %d", empty_result);
        if(empty_result != 0) {
            if(empty_result == -ENOTEMPTY) {
                S3FS_PRN_INFO("directory not empty: %s", path);
            }
            return empty_result;
        }
    }

    // Normalize path - ensure it ends with /
    strpath = path;
    if('/' != *strpath.rbegin()){
        strpath += "/";
    }

    // Delete the directory marker object
    result = s3fscurl.DeleteRequest(strpath.c_str());

    if(0 != result){
        S3FS_PRN_ERR("failed to remove directory: %s (result=%d)", path, result);
        return result;
    }

    // Clean up caches
    if(g_bucket_type == BUCKET_TYPE_OBJECT_SEMANTIC){
        // Also remove from caches using the original path (without trailing slash)
        std::string orig_path = path;
        StatCache::getStatCacheData()->DelStat(orig_path);
        S3FS_PRN_DBG("[path=%s] Also removed from caches using original path", orig_path.c_str());
    }

    S3FS_PRN_INFO("directory removed: %s", path);
    return 0;
}

int s3fs_symlink_s3(const char* from, const char* to) {
    int result;
    headers_t meta;

    if(0 != (result = validate_object_key_length(to))){
        return result;
    }

    S3FS_PRN_INFO("[from=%s][to=%s]", from, to);

    // Build metadata for symlink
    build_standard_metadata(meta, S_IFLNK | 0777, std::string("application/symlink"));

    // Add symlink target to metadata
    meta["x-amz-meta-symlink-target"] = from;

    // Create symlink object (zero-byte object with fd=-1)
    S3fsCurl s3fscurl;
    result = s3fscurl.PutRequest(to, meta, -1);

    if(0 != result){
        S3FS_PRN_ERR("failed to create symlink: %s -> %s (result=%d)", to, from, result);
        return result;
    }

    S3FS_PRN_INFO("symlink created: %s -> %s", to, from);
    return 0;
}

int s3fs_rename_s3(const char* from, const char* to)
{
    int result;
    struct stat stbuf;

    S3FS_PRN_INFO("[from=%s][to=%s]", from, to);

    if(!from || !to){
        return -EINVAL;
    }

    if(0 != (result = validate_object_key_length(to))){
        return result;
    }

    // Cannot rename root
    if(0 == strcmp(from, "/")){
        S3FS_PRN_ERR("cannot rename root directory");
        return -EINVAL;
    }

    // Check permissions on both source and target parent directories
    if(0 != (result = check_parent_object_access_s3(to, W_OK | X_OK))){
        S3FS_PRN_ERR("No permission for target parent directory: %s", to);
        return result;
    }
    if(0 != (result = check_parent_object_access_s3(from, W_OK | X_OK))){
        S3FS_PRN_ERR("No permission for source parent directory: %s", from);
        return result;
    }

    // Check if source exists and get its attributes
    if(0 != (result = s3fs_getattr_s3(from, &stbuf))){
        S3FS_PRN_ERR("source object not found: %s (result=%d)", from, result);
        return -ENOENT;
    }

    // Check if target is empty (if it's an existing directory)
    // This follows s3fs-fuse behavior: always call directory_empty(to)
    // directory_empty handles the case where to doesn't exist, is a file, or is a directory
    if(0 != (result = directory_empty(to))){
        S3FS_PRN_INFO("target directory not empty or error: %s (result=%d)", to, result);
        return result;
    }

    // Flush pending writes if file is open (following s3fs-fuse behavior)
    {
        FdEntity* ent = FdManager::get()->ExistOpen(from, -1, true);
        if(ent){
            int flush_result = ent->Flush(true);
            if(0 != flush_result){
                S3FS_PRN_ERR("could not flush file(%s): result=%d", from, flush_result);
                FdManager::get()->Close(ent);
                return flush_result;
            }
            StatCache::getStatCacheData()->DelStat(from);
            FdManager::get()->Close(ent);
        }
    }

    // Determine if source is a directory
    if(S_ISDIR(stbuf.st_mode)){
        // Rename directory
        result = rename_directory_s3(from, to);
    } else {
        // Rename single file
        result = rename_object_s3(from, to, true);  // update ctime
    }

    if(result == 0){
        S3FS_PRN_INFO("rename completed: %s -> %s", from, to);
    } else {
        S3FS_PRN_ERR("rename failed: %s -> %s (result=%d)", from, to, result);
    }

    return result;
}

int s3fs_link_s3(const char* from, const char* to) {
    // S3 doesn't support hard links natively
    // Each object has a unique key, so hard links cannot be implemented
    (void)from;
    (void)to;
    S3FS_PRN_DBG("hard links not supported by S3: %s -> %s", from, to);
    return -ENOTSUP;
}

// Attribute modification operations
// Note: For S3, changing attributes requires re-uploading the object
// with modified metadata using the copy API with REPLACE directive.
int s3fs_chmod_s3(const char* path, mode_t mode) {
    int result;
    headers_t meta;

    S3FS_PRN_INFO("[path=%s][mode=%04o]", path, mode);

    // Root directory: chmod is a no-op
    if(0 == strcmp(path, "/")){
        S3FS_PRN_DBG("chmod on root directory is a no-op");
        return 0;
    }

    // Get current metadata
    S3fsCurl s3fscurl;
    result = s3fscurl.HeadRequest(path, meta);

    if(result != 0){
        S3FS_PRN_ERR("head request failed: %s (result=%d)", path, result);
        return result;
    }

    // Update mode in metadata (full mode_t as decimal, matching s3fs-fuse)
    char buf[64];
    snprintf(buf, sizeof(buf), "%d", (int)mode);
    meta["x-amz-meta-mode"] = buf;

    // Prepare copy request to update metadata
    std::string copy_source = "/" + bucket + get_realpath(path);
    meta["x-amz-copy-source"] = urlEncode(copy_source);
    meta["x-amz-metadata-directive"] = "REPLACE";

    // Use PutHeadRequest with is_copy=true to update metadata
    result = s3fscurl.PutHeadRequest(path, meta, true, "");

    if(0 != result){
        S3FS_PRN_ERR("put head request failed: %s (result=%d)", path, result);
        return -EIO;
    }

    // Update StatCache so subsequent stat calls will see the new mode
    std::string cache_key = path;
    StatCache::getStatCacheData()->DelStat(cache_key);

    S3FS_PRN_INFO("mode updated: %s", path);
    return 0;
}

int s3fs_chmod_nocopy_s3(const char* path, mode_t mode) {
    // In nocopy mode, chmod is not supported as it requires copy API
    // to update metadata. Return success to avoid breaking applications.
    (void)path;
    (void)mode;
    S3FS_PRN_DBG("[path=%s][mode=%04o] - nocopy mode, chmod not supported, returning success", path, mode);
    return 0;
}

int s3fs_chown_s3(const char* path, uid_t uid, gid_t gid) {
    int result;
    headers_t meta;

    S3FS_PRN_INFO("[path=%s][uid=%u][gid=%u]", path, (unsigned int)uid, (unsigned int)gid);

    // Root directory: chown is a no-op
    if(0 == strcmp(path, "/")){
        S3FS_PRN_DBG("chown on root directory is a no-op");
        return 0;
    }

    // Get current metadata
    S3fsCurl s3fscurl;
    result = s3fscurl.HeadRequest(path, meta);

    if(result != 0){
        S3FS_PRN_ERR("head request failed: %s (result=%d)", path, result);
        return result;
    }

    // Update uid and gid in metadata
    // Note: If uid is -1, don't change uid. If gid is -1, don't change gid.
    char buf[64];
    if(uid != (uid_t)-1){
        snprintf(buf, sizeof(buf), "%d", uid);
        meta["x-amz-meta-uid"] = buf;
    }
    if(gid != (gid_t)-1){
        snprintf(buf, sizeof(buf), "%d", gid);
        meta["x-amz-meta-gid"] = buf;
    }

    // Prepare copy request to update metadata
    std::string copy_source = "/" + bucket + get_realpath(path);
    meta["x-amz-copy-source"] = urlEncode(copy_source);
    meta["x-amz-metadata-directive"] = "REPLACE";

    // Use PutHeadRequest with is_copy=true to update metadata
    result = s3fscurl.PutHeadRequest(path, meta, true, "");

    if(0 != result){
        S3FS_PRN_ERR("put head request failed: %s (result=%d)", path, result);
        return -EIO;
    }

    S3FS_PRN_INFO("ownership updated: %s", path);
    return 0;
}

int s3fs_chown_nocopy_s3(const char* path, uid_t uid, gid_t gid) {
    // In nocopy mode, chown is not supported as it requires copy API
    // to update metadata. Return success to avoid breaking applications.
    (void)path;
    (void)uid;
    (void)gid;
    S3FS_PRN_DBG("[path=%s][uid=%u][gid=%u] - nocopy mode, chown not supported, returning success", path, (unsigned int)uid, (unsigned int)gid);
    return 0;
}

int s3fs_utimens_s3(const char* path, const struct timespec ts[2]) {
    int result;
    headers_t meta;

    S3FS_PRN_INFO("[path=%s][mtime=%jd]", path, (intmax_t)(ts[1].tv_sec));

    // Root directory: changing mtime is a no-op
    if(0 == strcmp(path, "/")){
        S3FS_PRN_DBG("Could not change mtime for mount point.");
        return 0;
    }

    // Get current metadata
    S3fsCurl s3fscurl;
    result = s3fscurl.HeadRequest(path, meta);

    if(result != 0){
        S3FS_PRN_ERR("head request failed: %s (result=%d)", path, result);
        return result;
    }

    // Update mtime in metadata
    meta["x-amz-meta-mtime"] = str(ts[1].tv_sec);

    // Prepare copy request to update metadata
    std::string copy_source = "/" + bucket + get_realpath(path);
    meta["x-amz-copy-source"] = urlEncode(copy_source);
    meta["x-amz-metadata-directive"] = "REPLACE";

    // Use PutHeadRequest with is_copy=true to update metadata
    result = s3fscurl.PutHeadRequest(path, meta, true, "");

    if(0 != result){
        S3FS_PRN_ERR("put head request failed: %s (result=%d)", path, result);
        return -EIO;
    }

    // Invalidate stat cache so next getattr picks up the new mtime
    StatCache::getStatCacheData()->DelStat(path);

    S3FS_PRN_INFO("mtime updated: %s", path);
    return 0;
}

int s3fs_utimens_nocopy_s3(const char* path, const struct timespec ts[2]) {
    // In nocopy mode, utimens is not supported as it requires copy API
    // to update metadata. Return success to avoid breaking applications.
    (void)path;
    (void)ts;
    S3FS_PRN_DBG("[path=%s] - nocopy mode, utimens not supported, returning success", path);
    return 0;
}

// File data operations
int s3fs_truncate_s3(const char* path, off_t size)
{
    int result;
    headers_t meta;

    S3FS_PRN_INFO("[path=%s][size=%jd]", path, (intmax_t)size);

    // Validate size parameter
    if(size < 0){
        size = 0;
    }

    // Get current file info
    S3fsCurl s3fscurl;
    result = s3fscurl.HeadRequest(path, meta);

    bool file_exists = (result == 0);
    off_t current_size = 0;

    if(file_exists){
        // Get current size from Content-Length
        headers_t::iterator it = meta.find("Content-Length");
        if(it != meta.end()){
            current_size = str_to_off_t(it->second.c_str());
        }

        // Check if it's a directory
        it = meta.find("Content-Type");
        if(it != meta.end()){
            if(it->second.find("application/x-directory") != std::string::npos ||
               it->second.find("text/directory") != std::string::npos){
                S3FS_PRN_WARN("truncate on directory is no-op: %s", path);
                return 0;
            }
        }

        // If truncating to same size, nothing to do
        if(size == current_size){
            return 0;
        }
    } else if(result != -ENOENT){
        S3FS_PRN_ERR("failed to get file info: %s (result=%d)", path, result);
        return result;
    }

    // Build metadata for new file if it doesn't exist
    if(!file_exists){
        build_standard_metadata(meta, 0, S3fsCurl::LookupMimeType(path), true);
    }

    // [FIX] Use FdEntity for truncate (following s3fs-fuse approach)
    //
    // Previous implementation bypassed FdEntity and directly uploaded to S3,
    // leaving the local cache file out of sync. This caused truncate+append
    // bugs in cache mode: the cache file retained the old size, so subsequent
    // O_APPEND writes used a stale offset.
    //
    // IMPORTANT: When truncating an existing file with data (current_size > 0,
    // target size > 0), we must first open at CURRENT size (so size_orgmeta is
    // set correctly), then Load() the data we want to preserve from S3, and
    // only then resize to target size via Write(). Opening directly at the
    // target size would create a sparse-zero tmpfile, and RowFlush(force_sync)
    // would upload zeros, corrupting the S3 object. (ISSUE2026022500003)
    //
    // Reference: s3fs-fuse defers the truncate flush to close (s3fs.cpp:2926-2927),
    // relying on the file already being open. obsfs truncate is a standalone
    // FUSE op without a preceding open, so we must flush here.
    //
    S3FS_PRN_INFO("truncate via FdEntity: %s (from=%jd, to=%jd, exists=%d)",
                  path, (intmax_t)current_size, (intmax_t)size, file_exists);

    FdEntity* ent;

    if(file_exists && size > 0 && current_size > 0){
        // Existing file with data to preserve:
        // 1. Open at current_size → sets size_orgmeta correctly for Load()
        // 2. OpenAndLoadAll → downloads S3 content into tmpfile
        // 3. Write a single padding byte at (size-1) to resize pagelist
        //    and mark is_modify=true (the byte value doesn't matter since
        //    Load already filled it, but this updates the pagelist boundary)
        // 4. If shrinking, ftruncate tmpfile and pagelist.Resize()
        // 5. RowFlush uploads the correctly-sized data to S3
        ent = FdManager::get()->Open(path, &meta, static_cast<ssize_t>(current_size), -1, false, true, false);
        if(!ent){
            S3FS_PRN_ERR("could not open FdEntity for truncate: %s", path);
            return -EIO;
        }

        // Load all existing content from S3 into tmpfile
        if(!ent->OpenAndLoadAll(&meta, NULL, true)){
            S3FS_PRN_ERR("failed to load data for truncate: %s", path);
            FdManager::get()->Close(ent);
            return -EIO;
        }

        // Resize the tmpfile to target size
        if(-1 == ftruncate(ent->GetFd(), size)){
            S3FS_PRN_ERR("ftruncate failed: %s (errno=%d)", path, errno);
            FdManager::get()->Close(ent);
            return -errno;
        }

        // Use Write to update pagelist size and set is_modify=true.
        // Read the last byte (already loaded) and write it back to trigger
        // the is_modify flag without changing data content.
        char last_byte = 0;
        if(size > 0){
            ssize_t r = ent->Read(&last_byte, size - 1, 1);
            if(r != 1){
                // File may have been extended beyond original content
                last_byte = 0;
            }
            ent->Write(&last_byte, size - 1, 1);
        }
    } else {
        // Truncate to zero or new file: no data to preserve
        ent = FdManager::get()->Open(path, &meta, static_cast<ssize_t>(size), -1, false, true, false);
        if(!ent){
            S3FS_PRN_ERR("could not open FdEntity for truncate: %s", path);
            return -EIO;
        }

        if(size == 0){
            // For truncate-to-zero, write a zero-length marker to set is_modify
            // An empty write at offset 0 won't change anything but sets the flag
            // Actually use force_sync since the content is correctly empty
        }
    }

    // Flush to upload truncated content to S3
    // For the existing-file path, is_modify was set by Write() above.
    // For the truncate-to-zero path, use force_sync=true since the empty
    // tmpfile correctly represents the desired content.
    bool need_force = (!file_exists || size == 0 || current_size == 0);
    result = ent->RowFlush(path, need_force);
    if(result != 0){
        S3FS_PRN_ERR("failed to flush truncated file: %s (result=%d)", path, result);
    }

    // Close FdEntity
    FdManager::get()->Close(ent);

    if(result != 0){
        return result;
    }

    // Update stat cache with correct size to avoid OBS eventual consistency issues
    // (HeadRequest immediately after PutRequest may return stale Content-Length)
    StatCache::getStatCacheData()->DelStat(path);
    {
        headers_t size_meta = meta;
        size_meta["Content-Length"] = std::to_string(size);
        std::string cache_key = path;
        // ISSUE2026051200001 M.3: notruncate=false (synchronous PUT)
        StatCache::getStatCacheData()->AddStat(cache_key, size_meta, false, false);
    }

    S3FS_PRN_INFO("truncate completed: %s (size=%jd)", path, (intmax_t)size);
    return 0;
}

int s3fs_create_s3(const char* path, mode_t mode, struct fuse_file_info* fi)
{
    S3FS_PRN_DBG("=== s3fs_create_s3 CALLED: path=%s mode=%04o flags=%d ===", path, mode, fi->flags);

    int result;
    headers_t meta;

    if(0 != (result = validate_object_key_length(path))){
        return result;
    }

    S3FS_PRN_INFO("[path=%s][mode=%04o][flags=%d]", path, mode, fi->flags);

    // Check if file already exists
    S3fsCurl s3fscurl;
    result = s3fscurl.HeadRequest(path, meta);

    if(0 == result){
        // File exists - open it instead
        return s3fs_open_s3(path, fi);
    }

    // Build standard file metadata with appropriate content type
    build_standard_metadata(meta, mode, S3fsCurl::LookupMimeType(path));

    // Upload zero-byte object (fd=-1)
    S3FS_PRN_DBG("[create] PUT_REQUEST_START: path=%s", path);
    result = s3fscurl.PutRequest(path, meta, -1);
    S3FS_PRN_DBG("[create] PUT_REQUEST_END: result=%d for path=%s", result, path);

    if(0 != result){
        S3FS_PRN_ERR("failed to create file: %s (result=%d)", path, result);
        return result;
    }

    S3FS_PRN_DBG("file created successfully: %s", path);

    // For object semantic mode, add to StatCache so it can be found by readdir
    if(g_bucket_type == BUCKET_TYPE_OBJECT_SEMANTIC){
        std::string cache_key = path;
        // ISSUE2026051200001 M.3: notruncate=false (synchronous PUT)
        StatCache::getStatCacheData()->AddStat(cache_key, meta, false, false);
        S3FS_PRN_DBG("[path=%s] Added to StatCache", path);
    }

    // ========================================================================
    // IMPORTANT: Object bucket uses FdManager for file tracking
    // This provides local cache support for proper write/read operations
    // ========================================================================

    // Create file entity with FdManager (new file, size=0)
    FdEntity* ent = FdManager::get()->Open(path, NULL, 0, -1, false, true, false);
    if(!ent){
        S3FS_PRN_ERR("[object-bucket] Failed to create FdEntity for path=%s", path);
        return -EIO;
    }

    // Store the fd (file descriptor) as file handle
    // Note: FdEntity uses fd-based lookup, not inode-based
    fi->fh = static_cast<uint64_t>(ent->GetFd());

    S3FS_PRN_INFO("[object-bucket] File created and opened: %s (fh=%lu)", path, (unsigned long)fi->fh);
    return 0;
}

int s3fs_open_s3(const char* path, struct fuse_file_info* fi)
{
    int result;
    headers_t meta;
    bool file_exists = false;
    off_t file_size = 0;

    S3FS_PRN_INFO("[path=%s][flags=%d]", path, fi->flags);

    // Check if file exists
    S3fsCurl s3fscurl;
    result = s3fscurl.HeadRequest(path, meta);

    if(result == 0){
        file_exists = true;
        // Get file size from Content-Length
        std::string file_size_str = meta["Content-Length"];
        file_size = file_size_str.empty() ? 0 : atoll(file_size_str.c_str());
    }

    // For new files being created (O_CREAT), we allow open to succeed
    // The actual object will be created on first write
    if(!file_exists){
        // Check if this is a write operation that would create the file
        if((fi->flags & O_ACCMODE) != O_RDONLY){
            S3FS_PRN_INFO("file does not exist, opening for creation: %s (flags=0x%x)", path, fi->flags);
            file_size = 0;  // New file starts at 0 bytes
        } else {
            S3FS_PRN_ERR("file not found for read: %s (result=%d)", path, result);
            return -ENOENT;
        }
    }

    // Handle O_TRUNC flag: truncate file to 0 bytes
    if(file_exists && (fi->flags & O_TRUNC)){
        if(file_size > 0){
            S3FS_PRN_INFO("O_TRUNC set, truncating file: %s (size=%lld)", path, (long long)file_size);

            // Build standard metadata, preserving existing mode
            build_standard_metadata(meta, 0, S3fsCurl::LookupMimeType(path), true);

            // Upload 0-byte object to truncate
            result = s3fscurl.PutRequest(path, meta, -1);
            if(result != 0){
                S3FS_PRN_ERR("failed to truncate file: %s (result=%d)", path, result);
                return result;
            }
            S3FS_PRN_INFO("file truncated: %s", path);

            // Invalidate stat cache so getattr will fetch the new size
            StatCache::getStatCacheData()->DelStat(path);
            S3FS_PRN_INFO("stat cache invalidated after truncate: %s", path);

            file_size = 0;
        } else {
            S3FS_PRN_DBG("O_TRUNC set but file is already 0-byte, skipping truncate");
        }
    }

    // ========================================================================
    // IMPORTANT: Object bucket uses FdManager for file tracking
    // This provides local cache support for proper write/read operations
    // ========================================================================

    // Open file entity with FdManager
    // CRITICAL: Pass &meta so FdEntity can update size_orgmeta on reopen (for append)
    headers_t* pmeta = file_exists ? &meta : NULL;
    FdEntity* ent = FdManager::get()->Open(path, pmeta, static_cast<ssize_t>(file_size), -1, false, true, false);
    if(!ent){
        S3FS_PRN_ERR("[object-bucket] Failed to open FdEntity for path=%s", path);
        return -EIO;
    }

    // Store the fd (file descriptor) as file handle
    // Note: FdEntity uses fd-based lookup, not inode-based
    fi->fh = static_cast<uint64_t>(ent->GetFd());

    S3FS_PRN_INFO("[object-bucket] File opened: %s (fh=%lu, size=%lld, exists=%d)",
                  path, (unsigned long)fi->fh, (long long)file_size, file_exists);
    return 0;
}

int s3fs_read_s3(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* fi)
{
    S3FS_PRN_DBG("[object-bucket] [path=%s][size=%zu][offset=%lld][fh=%llu]",
                 path, size, (long long)offset, (unsigned long long)fi->fh);

    // ========================================================================
    // IMPORTANT: Object bucket uses FdEntity for read (with local cache)
    // ========================================================================

    // Get FdEntity using path, with refcnt increment to prevent use-after-free.
    // Without Dup, a concurrent FUSE RELEASE can delete the entity while Read() runs.
    FdEntity* ent = FdManager::get()->GetFdEntityWithDup(path, static_cast<int>(fi->fh));
    if(!ent) {
        S3FS_PRN_ERR("[object-bucket] Failed to get FdEntity for read path=%s", path);
        return -EBADF;
    }

    // Read from FdEntity (will load from OBS if not cached)
    ssize_t bytes_read = ent->Read(buf, offset, size, false);

    // Release the extra refcnt from GetFdEntityWithDup (lightweight: no CancelPrefetch)
    FdManager::get()->CloseRef(ent);
    ent = NULL;

    if(bytes_read < 0) {
        S3FS_PRN_ERR("[object-bucket] FdEntity::Read failed: path=%s result=%zd", path, bytes_read);
        return static_cast<int>(bytes_read);
    }

    S3FS_PRN_DBG("[object-bucket] Read completed: path=%s bytes=%zd", path, bytes_read);
    return static_cast<int>(bytes_read);
}

int s3fs_write_s3(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* fi)
{
    S3FS_PRN_INFO("[object-bucket] [path=%s][size=%zu][offset=%lld][fh=%llu]", path, size, (long long)offset, (unsigned long long)fi->fh);

    // Handle zero-size writes early (POSIX compliant)
    // POSIX: A write of 0 bytes shall return 0 with no other effect
    if(size == 0) {
        S3FS_PRN_INFO("zero-size write request, returning success");
        return 0;
    }

    // Validate buffer pointer for non-zero writes
    if(!buf) {
        S3FS_PRN_ERR("null buffer pointer with non-zero size %zu", size);
        return -EINVAL;
    }

    // Validate offset for non-zero writes
    if(offset < 0) {
        S3FS_PRN_ERR("negative offset: %lld", (long long)offset);
        return -EINVAL;
    }

    // ========================================================================
    // IMPORTANT: Object bucket uses FdManager/FdEntity (local cache based)
    // This provides proper append write and multi-segment write support
    // ========================================================================

    S3FS_PRN_DBG("[object-bucket] Using FdEntity for write operation");

    // Get FdEntity with refcnt increment to prevent use-after-free.
    FdEntity* ent = FdManager::get()->GetFdEntityWithDup(path, static_cast<int>(fi->fh));
    if(!ent) {
        S3FS_PRN_ERR("[object-bucket] Failed to get FdEntity for path=%s (fh=%llu)", path, (unsigned long long)fi->fh);
        return -EBADF;
    }

    // Write to the FdEntity
    // NOTE: For O_APPEND, FUSE kernel (non-direct_io) already adjusts offset
    // to file size in VFS vfs_write(). No user-space handling needed.
    // Matches s3fs-fuse (no O_APPEND code in s3fs_write).
    ssize_t wresult = ent->Write(buf, offset, size);

    // max_dirty_data: transition to multipart upload when file exceeds threshold
    // Reference: s3fs-fuse checks max_dirty_data in the FUSE write handler (s3fs.cpp:3149)
    // s3fs-fuse uses RowFlush+PunchHole because its mixmultipart mode uses S3 Copy API.
    // obsfs lacks mixmultipart, so we transition to multipart upload mode instead —
    // each byte is uploaded exactly once via NoCacheLoadAndPost + NoCacheMultipartPost.
    if(wresult > 0){
        off_t max_dirty = FdManager::GetMaxDirtyData();
        if(max_dirty > 0){
            size_t file_size;
            if(ent->GetSize(file_size) && static_cast<off_t>(file_size) >= max_dirty){
                if(!ent->TransitionToMultipart()){
                    S3FS_PRN_WARN("[max_dirty_data] multipart transition failed for path=%s", path);
                }
            }
        }
    }

    // Release the extra refcnt from GetFdEntityWithDup (lightweight: no CancelPrefetch)
    FdManager::get()->CloseRef(ent);
    ent = NULL;

    if(wresult < 0) {
        S3FS_PRN_ERR("[object-bucket] FdEntity::Write failed: path=%s result=%zd", path, wresult);
        return static_cast<int>(wresult);
    }

    S3FS_PRN_INFO("[object-bucket] Write completed: path=%s bytes=%zd", path, wresult);
    return static_cast<int>(wresult);
}

int s3fs_flush_s3(const char* path, struct fuse_file_info* fi)
{
    (void)fi;
    S3FS_PRN_DBG("[object-bucket] [path=%s]", path);

    // ========================================================================
    // Object bucket flush using FdEntity
    // ========================================================================

    // Safe without Dup: caller's fi->fh holds a refcnt contribution.
    // FUSE kernel guarantees flush completes before release for the same fh.
    // See ISSUE2026031500003 for full analysis.
    FdEntity* ent = FdManager::get()->GetFdEntity(path, static_cast<int>(fi->fh));

    if(!ent) {
        // No open file handle, nothing to flush
        S3FS_PRN_DBG("[object-bucket] No FdEntity found for flush: %s", path);
        return 0;
    }

    // Flush: upload pending data
    S3FS_PRN_INFO("[object-bucket] Flushing file: %s", path);
    bool was_modified = ent->IsModified();
    int result = ent->Flush();
    if(result != 0) {
        S3FS_PRN_ERR("[object-bucket] Flush failed for %s: %d", path, result);
        return result;
    }

    // Update StatCache after flush ONLY if the file was actually dirty and uploaded
    // If the file wasn't modified (e.g., opened by touch -t for utimens only),
    // we must NOT overwrite the stat cache — it may contain correct mtime from utimens
    if(was_modified) {
        std::string cache_key = path;
        headers_t meta;

        // Populate meta headers with file information
        size_t file_size = 0;
        if(ent->GetSize(file_size)) {
            meta["Content-Length"] = str(file_size);
        }

        // Use current time as mtime (the upload just happened)
        // This matches what S3 will set as Last-Modified for the object
        meta[hws_obs_meta_mtime] = str(time(NULL));

        // Add to StatCache (forcedir=false because this is a file, not a directory)
        StatCache::getStatCacheData()->AddStat(cache_key, meta, false, false);
        S3FS_PRN_DBG("[object-bucket] Updated StatCache after flush: %s (size=%zu)", path, file_size);
    }

    return 0;
}

int s3fs_fsync_s3(const char* path, int datasync, struct fuse_file_info* fi)
{
    (void)fi;
    S3FS_PRN_DBG("[object-bucket] [path=%s][datasync=%d]", path, datasync);

    // ========================================================================
    // Object bucket fsync using FdEntity
    // ========================================================================

    // Safe without Dup: caller's fi->fh holds a refcnt contribution.
    // FUSE kernel guarantees fsync completes before release for the same fh.
    // See ISSUE2026031500003 for full analysis.
    FdEntity* ent = FdManager::get()->GetFdEntity(path, static_cast<int>(fi->fh));

    if(!ent) {
        // No open file handle
        S3FS_PRN_DBG("[object-bucket] No FdEntity found for fsync: %s", path);
        return 0;
    }

    // Fsync: sync data to storage
    //
    // IMPORTANT: force_sync vs is_modify distinction (ISSUE2026022500003)
    //
    //   Flush(force_sync=false) — only uploads if is_modify is true (data was
    //   written). This is the correct behavior for fsync: if no write occurred
    //   on this FdEntity, there is nothing to sync.
    //
    //   Flush(force_sync=true) — bypasses the is_modify check and always
    //   uploads. This is WRONG for fsync because: when a file is opened O_RDWR
    //   without any write, the tmpfile contains sparse zeros (from ftruncate in
    //   Open), and uploading it would overwrite the real S3 content with zeros.
    //
    //   Reference: s3fs-fuse also uses force_sync=false in its fsync handler
    //   (s3fs.cpp around line 3272).
    //
    // Note: datasync parameter from FUSE:
    //   datasync=0: sync data + metadata
    //   datasync!=0: sync data only (fdatasync)
    // Both cases use the same Flush() call; datasync only affects metadata.
    //
    S3FS_PRN_INFO("[object-bucket] Syncing file: %s (datasync=%d)", path, datasync);
    int result = ent->Flush();
    if(result != 0) {
        S3FS_PRN_ERR("[object-bucket] Fsync failed for %s: %d", path, result);
        return result;
    }

    // Invalidate stat cache (file size may have changed after flush)
    StatCache::getStatCacheData()->DelStat(path);

    return 0;
}

int s3fs_release_s3(const char* path, struct fuse_file_info* fi)
{
    S3FS_PRN_DBG("[object-bucket] [path=%s][fh=%llu]", path, (unsigned long long)fi->fh);

    // Safe without Dup: this IS the release call — it decrements refcnt.
    // No other release for the same fh can race (FUSE sends one RELEASE per fh).
    // See ISSUE2026031500003 for full analysis.
    FdEntity* ent = FdManager::get()->GetFdEntity(path, static_cast<int>(fi->fh));

    if(!ent) {
        S3FS_PRN_ERR("[object-bucket] No FdEntity found for release: %s (fh=%llu)",
                      path, (unsigned long long)fi->fh);
        return -EIO;
    }

    // Flush data to S3 (s3fs-fuse: "There are cases when s3fs_flush is not called
    // and s3fs_release is called. Therefore, Flush() is called here.")
    bool was_modified = ent->IsModified();
    int result = ent->Flush();
    if(result != 0) {
        S3FS_PRN_ERR("[object-bucket] Failed to flush file on release: %s (result=%d)",
                     path, result);
    }

    // Update StatCache ONLY if the file was actually dirty and needed flushing
    // Do not update stat cache when file was opened but not written (e.g., touch -t for utimens)
    // because that would overwrite correct mtime values set by utimens
    //
    // ISSUE2026051200001 M.3 — two bugs in this path were fixed:
    //   (a) the previous code built a "stripped" meta containing only
    //       Content-Length + mtime, then AddStat'd. AddStat overwrites,
    //       so any existing uid/gid/mode in cache was wiped — causing the
    //       "leaf.txt shows uid=0 right after echo > leaf.txt" symptom.
    //   (b) notruncate=true was used, but the file is already on OBS at
    //       this point (Flush returned 0). Accumulating notruncate>0
    //       entries forever caused unbounded cache growth and OOM risk.
    // Fix:
    //   - Use FdEntity::GetOriginalHeaders() to retrieve full meta saved
    //     at open time (uid/gid/mode/Content-Type/etc.). Refresh
    //     Content-Length and mtime to reflect post-flush state.
    //     This both populates the cache AND preserves user metadata.
    //   - notruncate=false so the entry follows normal LRU/TTL lifecycle,
    //     aligning with s3fs-fuse s3fs.cpp:3336 ClearNoTruncateFlag in
    //     release.
    //   - Fall back to existing StatCache entry if FdEntity has no
    //     original headers (defensive); finally DelStat so next getattr
    //     re-HEADs.
    if(result == 0 && was_modified && g_bucket_type == BUCKET_TYPE_OBJECT_SEMANTIC) {
        std::string path_key(path);
        size_t file_size = 0;
        ent->GetSize(file_size);

        headers_t updated_meta;
        bool have_meta = ent->GetOriginalHeaders(updated_meta);

        if(!have_meta){
            // Fallback: try existing StatCache entry (in case open() didn't
            // populate orgmeta via a HEAD).
            struct stat existing_stbuf;
            have_meta = StatCache::getStatCacheData()->GetStat(
                path_key, &existing_stbuf, &updated_meta);
        }

        if(have_meta){
            updated_meta["Content-Length"] = std::to_string(file_size);
            updated_meta[hws_obs_meta_mtime] = str(time(NULL));
            StatCache::getStatCacheData()->AddStat(path_key, updated_meta, false, false);
        }else{
            // No source of meta available — drop cache, force next getattr
            // to re-HEAD from OBS.
            StatCache::getStatCacheData()->DelStat(path_key);
        }
    }

    // Close the FdEntity
    FdManager::get()->Close(ent);

    S3FS_PRN_DBG("[object-bucket] Release completed: %s", path);
    return 0;
}

// Directory operations
int s3fs_opendir_s3(const char* path, struct fuse_file_info* fi)
{
    (void)path;
    (void)fi;

    // For object mode, opendir is a no-op
    // The actual directory listing happens in readdir
    S3FS_PRN_DBG("[path=%s]", path);
    return 0;
}

int s3fs_readdir_s3(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* fi)
{
    (void)offset;
    (void)fi;

    S3FS_PRN_INFO("[path=%s]", path);

    // Always add . and .. entries
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    // Build the prefix for listing
    std::string prefix = "";
    if(strcmp(path, "/") != 0) {
        prefix = path;
        // Remove leading slash and ensure trailing slash for directory listing
        if(!prefix.empty() && prefix[0] == '/') {
            prefix = prefix.substr(1);
        }
        if(!prefix.empty() && prefix[prefix.length() - 1] != '/') {
            prefix += "/";
        }
    }

    // Track all entries we've added from ListBucket to avoid duplicates
    std::set<std::string> listed_entries;

    // Build base query string for ListBucketRequest (pagination adds marker per page)
    std::string base_query = "";
    if(!prefix.empty()) {
        base_query += "prefix=" + urlEncode(prefix) + "&";
    }
    base_query += "delimiter=/&max-keys=1000&encoding-type=url";

    // Pagination loop: keep listing until IsTruncated is false
    bool truncated = true;
    std::string next_marker;

    while(truncated){
        std::string query = base_query;
        if(!next_marker.empty()){
            query += "&marker=" + urlEncode(next_marker);
        }

        // Make the list request (new S3fsCurl per page, same as rename_directory_s3)
        S3fsCurl s3fscurl;
        int result = s3fscurl.ListBucketRequest(path, query.c_str());
        if(0 != result) {
            S3FS_PRN_ERR("ListBucketRequest failed for path %s (result=%d)", path, result);
            return result;
        }

        // Get the response body
        const BodyData* body = s3fscurl.GetBodyData();
        if(!body || body->size() == 0) {
            S3FS_PRN_ERR("Empty response body for path %s", path);
            return -EIO;
        }

        // Debug: Log first part of response body
        S3FS_PRN_INFO("ListBucket response body (first 500 chars): %.500s", body->str());

        // Parse XML response
        xmlDocPtr doc = xmlReadMemory(body->str(), static_cast<int>(body->size()), "", NULL, 0);
        if(!doc) {
            S3FS_PRN_ERR("xmlReadMemory failed for path %s", path);
            return -EIO;
        }

        xmlXPathContextPtr ctx = xmlXPathNewContext(doc);
        if(!ctx) {
            S3FS_PRN_ERR("xmlXPathNewContext failed for path %s", path);
            xmlFreeDoc(doc);
            return -EIO;
        }

        // Register namespace if present
        std::string xmlnsurl;
        if(GetXmlNsUrl(doc, xmlnsurl)) {
            xmlXPathRegisterNs(ctx, (xmlChar*)"s3", (xmlChar*)xmlnsurl.c_str());
        }

        // Track last key seen for fallback marker (reset per page)
        std::string last_key;

        // Extract and list files (Contents)
        std::string contents_xpath = "//";
        if(!xmlnsurl.empty()) {
            contents_xpath += "s3:";
        }
        contents_xpath += "Contents";

        xmlXPathObjectPtr contents_xp = xmlXPathEvalExpression((xmlChar*)contents_xpath.c_str(), ctx);
        if(contents_xp && !xmlXPathNodeSetIsEmpty(contents_xp->nodesetval)) {
            xmlNodeSetPtr nodes = contents_xp->nodesetval;
            for(int i = 0; i < nodes->nodeNr; i++) {
                ctx->node = nodes->nodeTab[i];

                // Get the Key element
                std::string key_xpath = ".//";
                if(!xmlnsurl.empty()) {
                    key_xpath += "s3:";
                }
                key_xpath += "Key";

                xmlXPathObjectPtr key_xp = xmlXPathEvalExpression((xmlChar*)key_xpath.c_str(), ctx);
                if(key_xp && !xmlXPathNodeSetIsEmpty(key_xp->nodesetval)) {
                    xmlChar* key = xmlNodeListGetString(doc, key_xp->nodesetval->nodeTab[0]->xmlChildrenNode, 1);
                    if(key) {
                        std::string full_key = (char*)key;
                        xmlFree(key);

                        // Decode URL-encoded key
                        full_key = urlDecodeSpecial(full_key);
                        last_key = full_key;

                        // Extract just the basename relative to the prefix
                        std::string name = full_key;
                        if(!prefix.empty() && full_key.find(prefix) == 0) {
                            name = full_key.substr(prefix.length());
                        }

                        // S3 protocol nuance: with delimiter=/ set, "dir/" marker
                        // objects should be rolled into CommonPrefixes by spec. AWS S3
                        // does this; some implementations (notably s3proxy used in
                        // integration tests) instead return the marker in <Contents>.
                        // Handle this case explicitly: synthesize a directory entry
                        // for the marker (filler + listed_entries) so:
                        //   1. the dir name appears in readdir output,
                        //   2. the iter-7 stale-entry cleanup doesn't mistake the
                        //      mkdir-cached entry for an externally-deleted one.
                        if(!name.empty() && name[name.length() - 1] == '/'){
                            std::string dir_name = name.substr(0, name.length() - 1);
                            if(!dir_name.empty()){
                                struct stat st;
                                memset(&st, 0, sizeof(st));
                                st.st_ino   = generate_virtual_inode(full_key.c_str());
                                // M-3 review: 0750 aligns with s3fs-fuse fallback
                                st.st_mode  = S_IFDIR | 0750;
                                st.st_nlink = 2;
                                st.st_uid   = geteuid();
                                st.st_gid   = getegid();
                                st.st_atime = st.st_mtime = st.st_ctime = time(NULL);
                                filler(buf, dir_name.c_str(), &st, 0);
                                listed_entries.insert(dir_name);
                                S3FS_PRN_DBG("readdir added directory marker from Contents: %s",
                                             dir_name.c_str());
                            }
                        }

                        // Skip empty names and directory markers
                        if(!name.empty() && name[name.length() - 1] != '/') {
                            // Build stat for the file
                            struct stat st;
                            memset(&st, 0, sizeof(st));
                            st.st_ino = generate_virtual_inode(full_key.c_str());
                            // M-3 review: 0640 aligns with s3fs-fuse fallback
                            st.st_mode = S_IFREG | 0640;
                            st.st_nlink = 1;
                            st.st_size = 0;
                            st.st_blocks = 1;
                            st.st_uid = geteuid();
                            st.st_gid = getegid();
                            st.st_atime = st.st_mtime = st.st_ctime = time(NULL);

                            // ISSUE2026051200001 M.1: extract Size + LastModified + ETag
                            // from the ListBucket response itself. Size and LastModified
                            // are authoritative (no per-entry HEAD needed). ETag is also
                            // included so subsequent StatCache::GetStat with petag-check
                            // doesn't trigger a forced refresh.
                            off_t parsed_size = 0;
                            time_t parsed_mtime = 0;
                            std::string parsed_etag;

                            std::string size_xpath = ".//";
                            if(!xmlnsurl.empty()) {
                                size_xpath += "s3:";
                            }
                            size_xpath += "Size";
                            xmlXPathObjectPtr size_xp = xmlXPathEvalExpression((xmlChar*)size_xpath.c_str(), ctx);
                            if(size_xp && !xmlXPathNodeSetIsEmpty(size_xp->nodesetval)) {
                                xmlChar* size_str = xmlNodeListGetString(doc, size_xp->nodesetval->nodeTab[0]->xmlChildrenNode, 1);
                                if(size_str) {
                                    parsed_size = str_to_off_t((char*)size_str);
                                    st.st_size = parsed_size;
                                    xmlFree(size_str);
                                }
                            }
                            if(size_xp) {
                                xmlXPathFreeObject(size_xp);
                            }

                            std::string lm_xpath = ".//";
                            if(!xmlnsurl.empty()) {
                                lm_xpath += "s3:";
                            }
                            lm_xpath += "LastModified";
                            xmlXPathObjectPtr lm_xp = xmlXPathEvalExpression((xmlChar*)lm_xpath.c_str(), ctx);
                            if(lm_xp && !xmlXPathNodeSetIsEmpty(lm_xp->nodesetval)) {
                                xmlChar* lm_str = xmlNodeListGetString(doc, lm_xp->nodesetval->nodeTab[0]->xmlChildrenNode, 1);
                                if(lm_str) {
                                    // ListBucket <LastModified> is ISO 8601 (e.g.
                                    // "2026-05-12T07:00:00.000Z"). cvtIAMExpireStringToTime
                                    // strptime %Y-%m-%dT%H:%M:%S + timegm — ignores the
                                    // fractional + 'Z' suffix and treats as GMT, which
                                    // matches the Z marker. Works on AWS S3 and OBS.
                                    parsed_mtime = cvtIAMExpireStringToTime((const char*)lm_str);
                                    if(parsed_mtime > 0){
                                        st.st_atime = st.st_mtime = st.st_ctime = parsed_mtime;
                                    }
                                    xmlFree(lm_str);
                                }
                            }
                            if(lm_xp) {
                                xmlXPathFreeObject(lm_xp);
                            }

                            std::string etag_xpath = ".//";
                            if(!xmlnsurl.empty()) {
                                etag_xpath += "s3:";
                            }
                            etag_xpath += "ETag";
                            xmlXPathObjectPtr etag_xp = xmlXPathEvalExpression((xmlChar*)etag_xpath.c_str(), ctx);
                            if(etag_xp && !xmlXPathNodeSetIsEmpty(etag_xp->nodesetval)) {
                                xmlChar* etag_str = xmlNodeListGetString(doc, etag_xp->nodesetval->nodeTab[0]->xmlChildrenNode, 1);
                                if(etag_str) {
                                    parsed_etag = (const char*)etag_str;
                                    // Strip surrounding quotes if present (S3 always quotes).
                                    if(parsed_etag.size() >= 2 &&
                                       parsed_etag.front() == '"' &&
                                       parsed_etag.back() == '"'){
                                        parsed_etag = parsed_etag.substr(1, parsed_etag.size() - 2);
                                    }
                                    xmlFree(etag_str);
                                }
                            }
                            if(etag_xp) {
                                xmlXPathFreeObject(etag_xp);
                            }

                            filler(buf, name.c_str(), &st, 0);
                            listed_entries.insert(name);
                            S3FS_PRN_DBG("readdir added file: %s", name.c_str());

                            // ISSUE2026051200001 M.1: synthesize complete meta and
                            // write to StatCache. Avoids the per-entry HEAD that
                            // s3fs-fuse readdir_multi_head needs for the common case
                            // (objects without custom x-amz-meta-uid). Trade-off
                            // documented in issue: objects WITH custom x-amz-meta-uid
                            // will display as mount user until cache TTL expires +
                            // explicit getattr triggers HEAD; covered by mount option
                            // `accurate_readdir` (future work) if precision matters.
                            headers_t file_meta;
                            file_meta["Content-Type"] = "application/octet-stream";
                            file_meta["Content-Length"] = std::to_string(parsed_size);
                            if(!parsed_etag.empty()){
                                file_meta["ETag"] = parsed_etag;
                            }
                            file_meta[hws_obs_meta_uid]  = std::to_string(geteuid());
                            file_meta[hws_obs_meta_gid]  = std::to_string(getegid());
                            // M-3 review: 0640 aligns with s3fs-fuse fallback
                            file_meta[hws_obs_meta_mode] = std::to_string(S_IFREG | 0640);
                            if(parsed_mtime > 0){
                                file_meta[hws_obs_meta_mtime] = std::to_string((long)parsed_mtime);
                            }

                            // Build full cache key and AddStat (notruncate=false: this
                            // is just an index-derived hint; subject to LRU/TTL).
                            // M-4 review: respect `accurate_readdir` mount option —
                            // when on, skip synthesized AddStat so the next getattr
                            // falls back to per-entry HEAD (recovers s3fs-fuse
                            // readdir_multi_head accuracy for custom uid/gid/mode).
                            if(!accurate_readdir){
                                std::string file_path;
                                if(strcmp(path, "/") == 0){
                                    file_path = "/" + name;
                                }else{
                                    file_path = path;
                                    if(!file_path.empty() && file_path.back() != '/'){
                                        file_path += "/";
                                    }
                                    file_path += name;
                                }
                                // M.2 invariant: AddStat replaces; we do NOT want to
                                // overwrite a fresh real-meta entry from a recent
                                // mkdir/create/getattr-HEAD. Check existing first.
                                struct stat probe_stat;
                                headers_t probe_meta;
                                std::string probe_key = file_path;
                                if(!StatCache::getStatCacheData()->GetStat(probe_key, &probe_stat, &probe_meta, true)){
                                    StatCache::getStatCacheData()->AddStat(file_path, file_meta, false, false);
                                }
                            }
                        }
                    }
                }
                if(key_xp) {
                    xmlXPathFreeObject(key_xp);
                }
            }
        }
        if(contents_xp) {
            xmlXPathFreeObject(contents_xp);
        }

        // Extract and list subdirectories (CommonPrefixes)
        std::string prefix_xpath = "//";
        if(!xmlnsurl.empty()) {
            prefix_xpath += "s3:";
        }
        prefix_xpath += "CommonPrefixes";

        xmlXPathObjectPtr prefix_xp = xmlXPathEvalExpression((xmlChar*)prefix_xpath.c_str(), ctx);
        if(prefix_xp && !xmlXPathNodeSetIsEmpty(prefix_xp->nodesetval)) {
            xmlNodeSetPtr nodes = prefix_xp->nodesetval;
            for(int i = 0; i < nodes->nodeNr; i++) {
                ctx->node = nodes->nodeTab[i];

                // Get the Prefix element
                std::string cp_xpath = ".//";
                if(!xmlnsurl.empty()) {
                    cp_xpath += "s3:";
                }
                cp_xpath += "Prefix";

                xmlXPathObjectPtr cp_key_xp = xmlXPathEvalExpression((xmlChar*)cp_xpath.c_str(), ctx);
                if(cp_key_xp && !xmlXPathNodeSetIsEmpty(cp_key_xp->nodesetval)) {
                    xmlChar* cp_key = xmlNodeListGetString(doc, cp_key_xp->nodesetval->nodeTab[0]->xmlChildrenNode, 1);
                    if(cp_key) {
                        std::string full_prefix = (char*)cp_key;
                        xmlFree(cp_key);

                        // Decode URL-encoded prefix
                        full_prefix = urlDecodeSpecial(full_prefix);

                        // Extract just the basename relative to the prefix
                        std::string name = full_prefix;
                        if(!prefix.empty() && full_prefix.find(prefix) == 0) {
                            name = full_prefix.substr(prefix.length());
                        }

                        // Remove trailing slash for directory name
                        if(!name.empty() && name[name.length() - 1] == '/') {
                            name = name.substr(0, name.length() - 1);
                        }

                        // Skip empty names
                        if(!name.empty()) {
                            // Build stat for the directory
                            struct stat st;
                            memset(&st, 0, sizeof(st));
                            st.st_ino = generate_virtual_inode(full_prefix.c_str());
                            // M-3 review: 0750 aligns with s3fs-fuse fallback
                            st.st_mode = S_IFDIR | 0750;
                            st.st_nlink = 2;
                            st.st_uid = geteuid();
                            st.st_gid = getegid();
                            st.st_atime = st.st_mtime = st.st_ctime = time(NULL);

                            filler(buf, name.c_str(), &st, 0);
                            listed_entries.insert(name);
                            S3FS_PRN_DBG("readdir added directory: %s", name.c_str());

                            // Add directory to stat cache so getattr can find it.
                            // Needed both for implicit directories (CommonPrefixes
                            // without a marker object) AND for marker-backed dirs
                            // (so a subsequent getattr doesn't have to HEAD again).
                            std::string dir_path;
                            if(strcmp(path, "/") == 0) {
                                dir_path = "/" + name;
                            } else {
                                dir_path = path;
                                if(dir_path[dir_path.length() - 1] != '/') {
                                    dir_path += "/";
                                }
                                dir_path += name;
                            }

                            // ISSUE2026051200001 M.2: synthesize complete meta (uid/gid/
                            // mode/mtime) instead of just Content-Length+Content-Type.
                            // The previous "minimal" form left convert_header_to_stat
                            // with no user metadata → fell through to get_uid/get_gid/
                            // get_mode fallbacks that historically returned 0/0/040000,
                            // making all readdir-discovered dirs appear root-owned and
                            // unenterable (the original SMB-guest symptom).
                            //
                            // M.2 invariant: do NOT overwrite an existing cache entry.
                            // A prior mkdir / getattr-HEAD may have populated a real
                            // meta we shouldn't trample. AddStat in StatCache replaces
                            // unconditionally, so we use a GetStat probe first.
                            // Combined with the iter-7 stale cleanup which runs BEFORE
                            // this merge block in the readdir flow, the surviving
                            // cache state after readdir is:
                            //   - real meta entries (from mkdir or prior HEAD): kept
                            //     untouched (probe finds them)
                            //   - cleanup-deleted entries (externally removed):
                            //     re-populated with synthetic meta here (probe misses)
                            //   - fresh entries from this readdir: synthetic meta
                            //
                            // M-4 review: respect `accurate_readdir` mount option —
                            // when on, skip the synthesized AddStat so getattr falls
                            // back to per-entry HEAD.
                            struct stat probe_stat;
                            headers_t probe_meta;
                            std::string probe_key = dir_path;
                            if(accurate_readdir){
                                // skip — caller wants HEAD-on-demand accuracy
                            }else if(!StatCache::getStatCacheData()->GetStat(probe_key, &probe_stat, &probe_meta, true)){
                                headers_t dir_meta;
                                dir_meta["Content-Type"]     = "application/x-directory";
                                dir_meta["Content-Length"]   = "0";
                                dir_meta[hws_obs_meta_uid]   = std::to_string(geteuid());
                                dir_meta[hws_obs_meta_gid]   = std::to_string(getegid());
                                // M-3 review: 0750 aligns with s3fs-fuse fallback
                                dir_meta[hws_obs_meta_mode]  = std::to_string(S_IFDIR | 0750);
                                // mtime intentionally omitted: implicit dirs have no
                                // authoritative mtime from CommonPrefixes (no
                                // LastModified field). get_mtime fallback returns 0
                                // (epoch). Consistent with s3fs-fuse behavior for
                                // DIR_NOT_EXIST_OBJECT.
                                StatCache::getStatCacheData()->AddStat(dir_path, dir_meta, true, false);
                                S3FS_PRN_DBG("Added directory to stat cache: %s (synthetic full meta)", dir_path.c_str());
                            }else{
                                S3FS_PRN_DBG("Skipped synthetic AddStat for %s (real meta present)", dir_path.c_str());
                            }
                        }
                    }
                }
                if(cp_key_xp) {
                    xmlXPathFreeObject(cp_key_xp);
                }
            }
        }
        if(prefix_xp) {
            xmlXPathFreeObject(prefix_xp);
        }

        // Check if there are more pages
        truncated = is_truncated_s3ops(doc);
        if(truncated){
            next_marker = get_next_marker_s3ops(doc);
            if(next_marker.empty()){
                if(!last_key.empty()){
                    next_marker = last_key;
                } else {
                    S3FS_PRN_WARN("IsTruncated but no NextMarker/last key; stop pagination");
                    truncated = false;
                }
            }
            S3FS_PRN_INFO("readdir pagination: next_marker=%s", next_marker.c_str());
        }

        xmlXPathFreeContext(ctx);
        xmlFreeDoc(doc);
    }  // end while(truncated)

    // For object semantic mode, merge StatCache entries with ListBucket results
    // This ensures newly created files/directories are immediately visible
    if(g_bucket_type == BUCKET_TYPE_OBJECT_SEMANTIC) {
        S3FS_PRN_DBG("Merging StatCache entries for directory: %s", path);

        // ISSUE2026051100001 iter-7: stale-entry cleanup.
        // Reconcile StatCache children with the authoritative ListBucket result.
        // Any child still in StatCache but NOT in listed_entries (which at this
        // point contains exactly the names returned by ListBucket pagination)
        // was likely deleted externally — drop it so subsequent
        // getattr/rmdir/rename see the current state.
        //
        // CRITICAL ORDERING: this MUST run BEFORE the GetChildStatMap merge
        // below. The merge "rescues" entries that ListBucket missed by inserting
        // them into listed_entries — that's needed for notruncate>0 entries
        // (in-progress mkdir/create whose objects haven't propagated yet) but
        // not for notruncate=0 entries (which are subject to ListBucket truth).
        // DelStatIfTruncatable enforces the distinction by skipping
        // notruncate>0 entries. After cleanup, the merge below sees only
        // protected (notruncate>0) entries left for rescue.
        //
        // This block is gated by `truncated==false` (we reached past the
        // pagination loop without early return on error), avoiding false
        // deletions on partial listings.
        {
            s3obj_type_map_t cached_for_purge;
            StatCache::getStatCacheData()->GetChildStatMap(path, cached_for_purge);
            // Snapshot `now` once so every DelStatIfTruncatable in this pass
            // sees the same grace boundary (avoids edge effects across the
            // second tick mid-loop). Must be CLOCK_MONOTONIC_COARSE seconds
            // (same clock as stat_cache_entry::cache_date).
            struct timespec cleanup_ts;
            clock_gettime(CLOCK_MONOTONIC_COARSE, &cleanup_ts);
            time_t cleanup_now = cleanup_ts.tv_sec;
            for(const auto& kv : cached_for_purge){
                const std::string& entry_name = kv.first;
                if(listed_entries.find(entry_name) != listed_entries.end()){
                    continue;
                }
                std::string child_path;
                if(strcmp(path, "/") == 0){
                    child_path = "/" + entry_name;
                }else{
                    child_path = path;
                    if(!child_path.empty() && child_path.back() != '/'){
                        child_path += "/";
                    }
                    child_path += entry_name;
                }
                if(StatCache::getStatCacheData()->DelStatIfTruncatable(child_path, cleanup_now)){
                    S3FS_PRN_INFO("readdir stale cleanup: removed %s (not in current ListBucket)",
                                 child_path.c_str());
                }
            }
        }

        // Get all child entries from StatCache (after stale cleanup above).
        // Surviving entries are: (a) notruncate>0 (in-progress, protected),
        // or (b) newly-AddStat'd by current readdir loop. Both deserve to
        // appear in readdir output if missed by ListBucket.
        s3obj_type_map_t cached_entries;
        if(StatCache::getStatCacheData()->GetChildStatMap(path, cached_entries)) {
            S3FS_PRN_DBG("StatCache returned %zu entries for directory: %s",
                        cached_entries.size(), path);

            for(const auto& entry : cached_entries) {
                const std::string& entry_name = entry.first;
                objtype_t entry_type = entry.second;

                // Skip if already listed from ListBucket
                if(listed_entries.find(entry_name) != listed_entries.end()) {
                    S3FS_PRN_DBG("Entry %s already in ListBucket results, skipping",
                               entry_name.c_str());
                    continue;
                }

                // Build full path for this entry
                std::string full_path;
                if(strcmp(path, "/") == 0) {
                    full_path = "/" + entry_name;
                } else {
                    full_path = path;
                    if(full_path[full_path.length() - 1] != '/') {
                        full_path += "/";
                    }
                    full_path += entry_name;
                }

                // Get detailed stat from StatCache
                std::string cache_key = full_path;
                struct stat stbuf;
                headers_t meta;

                if(StatCache::getStatCacheData()->GetStat(cache_key, &stbuf, &meta, false)) {
                    // Add to directory listing
                    filler(buf, entry_name.c_str(), &stbuf, 0);
                    listed_entries.insert(entry_name);
                    S3FS_PRN_INFO("readdir added StatCache entry: %s (type=%d)",
                                 entry_name.c_str(), entry_type);
                } else {
                    // Entry in child map but not in cache (shouldn't happen)
                    S3FS_PRN_WARN("Entry %s in child map but not in StatCache, skipping",
                                 entry_name.c_str());
                }
            }
        }

        S3FS_PRN_DBG("StatCache merge completed for directory: %s", path);

        // Note: pending_dir_entries merge was removed in ISSUE2026051100001
        // cleanup. Every add_pending_dir_entry() call site paired an
        // AddStat(no_truncate=true), so anything pending could see was
        // already in StatCache → already returned by GetChildStatMap above.
    }

    S3FS_PRN_INFO("readdir completed: %s", path);
    return 0;
}

// Other operations
int s3fs_statfs_s3(const char* path, struct statvfs* stbuf)
{
    (void)path;

    // Return fake filesystem stats for S3
    // S3 doesn't have a real concept of filesystem size/limits
    memset(stbuf, 0, sizeof(struct statvfs));

    // Set some reasonable defaults
    stbuf->f_bsize = 4096;              // Block size
    stbuf->f_frsize = 4096;             // Fragment size
    stbuf->f_blocks = 0xFFFFFFFF;       // Total blocks (very large)
    stbuf->f_bfree = 0xFFFFFFFF;        // Free blocks (very large)
    stbuf->f_bavail = 0xFFFFFFFF;       // Available blocks (very large)
    stbuf->f_files = 0xFFFFFFFF;        // Total inodes (very large)
    stbuf->f_ffree = 0xFFFFFFFF;        // Free inodes (very large)
    stbuf->f_favail = 0xFFFFFFFF;       // Available inodes (very large)
    stbuf->f_namemax = NAME_MAX;         // Max single filename component (255)

    return 0;
}

int s3fs_access_s3(const char* path, int mask)
{
    S3FS_PRN_DBG("[path=%s][mask=%d]", path, mask);

    // Root directory is always accessible
    if(0 == strcmp(path, "/")){
        return 0;
    }

    // Use getattr to check existence — this properly handles:
    // 1. StatCache lookups (immediate visibility after mkdir/create)
    // 2. Directory markers stored with trailing slash in S3
    // 3. FdManager for files currently open
    struct stat stbuf;
    int result = s3fs_getattr_s3(path, &stbuf);

    if(result != 0){
        return result;
    }

    // For F_OK, existence is sufficient
    if(F_OK == mask){
        return 0;
    }

    // Running as root or mount-user grants all access
    struct fuse_context* pcxt = fuse_get_context();
    if(pcxt && (pcxt->uid == 0 || pcxt->uid == getuid())){
        return 0;
    }

    // Check permission bits against mask
    mode_t mode = stbuf.st_mode;
    if((mask & R_OK) && !(mode & (S_IRUSR | S_IRGRP | S_IROTH))){
        return -EACCES;
    }
    if((mask & W_OK) && !(mode & (S_IWUSR | S_IWGRP | S_IWOTH))){
        return -EACCES;
    }
    if((mask & X_OK) && !(mode & (S_IXUSR | S_IXGRP | S_IXOTH))){
        return -EACCES;
    }

    return 0;
}

// Extended attributes
// Prefix for storing extended attributes in S3 metadata
static const std::string XATTR_META_PREFIX = "x-amz-meta-xattr-";

int s3fs_setxattr_s3(const char* path, const char* name, const char* value, size_t size, int flags) {
    int result;
    headers_t meta;

    S3FS_PRN_INFO("[path=%s][name=%s][size=%zu][flags=%d]", path, name, size, flags);

    // Get current metadata
    S3fsCurl s3fscurl;
    result = s3fscurl.HeadRequest(path, meta);

    if(result != 0 && result != -ENOENT){
        S3FS_PRN_ERR("head request failed: %s (result=%d)", path, result);
        return result;
    }

    // Build metadata key for xattr
    std::string xattr_key = XATTR_META_PREFIX + name;

    // Check flags (consistent with s3fs-fuse behavior)
    bool xattr_exists = (meta.find(xattr_key) != meta.end());
#ifdef XATTR_REPLACE
    if((flags & XATTR_REPLACE) && !xattr_exists){
        S3FS_PRN_ERR("xattr not found but XATTR_REPLACE specified: %s", name);
        return -ENODATA;
    }
#endif
#ifdef XATTR_CREATE
    if((flags & XATTR_CREATE) && xattr_exists){
        S3FS_PRN_ERR("xattr exists but XATTR_CREATE specified: %s", name);
        return -EEXIST;
    }
#endif

    // Set xattr value in metadata (store as base64 to handle binary data)
    // For simplicity, store as string - a production implementation would use base64
    meta[xattr_key] = std::string(value, size);

    // Prepare copy request to update metadata
    std::string copy_source = "/" + bucket + get_realpath(path);
    meta["x-amz-copy-source"] = urlEncode(copy_source);
    meta["x-amz-metadata-directive"] = "REPLACE";

    // Use PutHeadRequest with is_copy=true to update metadata
    result = s3fscurl.PutHeadRequest(path, meta, true, "");

    if(0 != result){
        S3FS_PRN_ERR("put head request failed: %s (result=%d)", path, result);
        return -EIO;
    }

    // Invalidate stat cache so next getattr picks up the new metadata
    StatCache::getStatCacheData()->DelStat(path);

    S3FS_PRN_INFO("xattr set: %s = %s", name, value);
    return 0;
}

int s3fs_getxattr_s3(const char* path, const char* name, char* value, size_t size) {
    int result;
    headers_t meta;

    S3FS_PRN_INFO("[path=%s][name=%s][size=%zu]", path, name, size);

    // Get current metadata
    S3fsCurl s3fscurl;
    result = s3fscurl.HeadRequest(path, meta);

    if(result != 0){
        S3FS_PRN_ERR("head request failed: %s (result=%d)", path, result);
        return result;
    }

    // Build metadata key for xattr
    std::string xattr_key = XATTR_META_PREFIX + name;

    // Find xattr in metadata
    headers_t::iterator it = meta.find(xattr_key);
    if(it == meta.end()){
        S3FS_PRN_ERR("xattr not found: %s", name);
        return -ENODATA;
    }

    // Check buffer size
    if(size == 0){
        // Return size needed
        return it->second.length();
    }

    if(size < it->second.length()){
        S3FS_PRN_ERR("buffer too small for xattr: %s (need %zu, have %zu)",
                      name, it->second.length(), size);
        return -ERANGE;
    }

    // Copy value to buffer
    memcpy(value, it->second.c_str(), it->second.length());

    S3FS_PRN_DBG("xattr get: name=%s, length=%zu", name, it->second.length());
    return it->second.length();
}

int s3fs_listxattr_s3(const char* path, char* list, size_t size) {
    int result;
    headers_t meta;

    S3FS_PRN_INFO("[path=%s][size=%zu]", path, size);

    // Get current metadata
    S3fsCurl s3fscurl;
    result = s3fscurl.HeadRequest(path, meta);

    if(result != 0){
        S3FS_PRN_ERR("head request failed: %s (result=%d)", path, result);
        return result;
    }

    // Calculate size needed for list
    size_t list_size = 0;
    std::vector<std::string> xattr_names;

    for(headers_t::iterator it = meta.begin(); it != meta.end(); ++it){
        // Check if this metadata key is an xattr
        if(it->first.compare(0, XATTR_META_PREFIX.length(), XATTR_META_PREFIX) == 0){
            // Extract xattr name (remove prefix)
            std::string name = it->first.substr(XATTR_META_PREFIX.length());
            xattr_names.push_back(name);
            list_size += name.length() + 1;  // +1 for null terminator
        }
    }

    // If size is 0, return size needed
    if(size == 0){
        return list_size;
    }

    // Check if buffer is too small
    if(size < list_size){
        S3FS_PRN_ERR("buffer too small for xattr list (need %zu, have %zu)", list_size, size);
        return -ERANGE;
    }

    // Build list of null-terminated attribute names
    char* ptr = list;
    for(size_t i = 0; i < xattr_names.size(); i++){
        size_t len = xattr_names[i].length() + 1;
        memcpy(ptr, xattr_names[i].c_str(), len);
        ptr += len;
    }

    S3FS_PRN_DBG("xattr list: %zu attributes", xattr_names.size());
    return list_size;
}

int s3fs_removexattr_s3(const char* path, const char* name) {
    int result;
    headers_t meta;

    S3FS_PRN_INFO("[path=%s][name=%s]", path, name);

    // Get current metadata
    S3fsCurl s3fscurl;
    result = s3fscurl.HeadRequest(path, meta);

    if(result != 0){
        S3FS_PRN_ERR("head request failed: %s (result=%d)", path, result);
        return result;
    }

    // Build metadata key for xattr
    std::string xattr_key = XATTR_META_PREFIX + name;

    // Check if xattr exists
    if(meta.find(xattr_key) == meta.end()){
        S3FS_PRN_ERR("xattr not found: %s", name);
        return -ENODATA;
    }

    // Remove xattr from metadata
    meta.erase(xattr_key);

    // Prepare copy request to update metadata
    std::string copy_source = "/" + bucket + get_realpath(path);
    meta["x-amz-copy-source"] = urlEncode(copy_source);
    meta["x-amz-metadata-directive"] = "REPLACE";

    // Use PutHeadRequest with is_copy=true to update metadata
    result = s3fscurl.PutHeadRequest(path, meta, true, "");

    if(0 != result){
        S3FS_PRN_ERR("put head request failed: %s (result=%d)", path, result);
        return -EIO;
    }

    // Invalidate stat cache so next getattr picks up the updated metadata
    StatCache::getStatCacheData()->DelStat(path);

    S3FS_PRN_INFO("xattr removed: %s", name);
    return 0;
}
