#include "hws_fs_util.h"

/**
 * path: input file path
 * resolvedPath: resolved file path
 * shouldExist: whether the file always exists. if not, it may not exist
 */
bool verifyPath(const char *path, char *resolvedPath, bool shouldExist) {
    if (shouldExist) {
        if (realpath(path, resolvedPath) == NULL) {
            S3FS_PRN_ERR("resolve filepath failed(%s).", path);
            return false;
        }
    } else {
        // if the file exists
        if (access(path, F_OK) == 0) {
            if (realpath(path, resolvedPath) == NULL) {
                S3FS_PRN_ERR("resolve filepath failed(%s).", path);
                return false;
            }
        } else {
            // [ISSUE2026040700002 task E] Bounds-checked copy to prevent stack
            // buffer overflow. Caller declares resolvedPath as char[PATH_MAX];
            // reject paths whose strlen >= PATH_MAX so the copy (including the
            // terminating NUL) always fits.
            size_t len = strlen(path);
            if (len >= PATH_MAX) {
                S3FS_PRN_ERR("path too long (%zu bytes, max %d).", len, PATH_MAX - 1);
                return false;
            }
            memcpy(resolvedPath, path, len + 1);
        }
    }
    return true;
}
