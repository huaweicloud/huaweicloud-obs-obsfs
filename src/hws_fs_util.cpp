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
            strcpy(resolvedPath, path);
        }
    }
    return true;
}
