#ifndef S3FS_OPERATIONS_H_
#define S3FS_OPERATIONS_H_

#include "common.h"
#include <fuse.h>
#include <string>

// FUSE operation implementations for object semantic (standard S3) mode
// These functions provide S3-compatible behavior without requiring
// OBS-specific headers like x-hws-fs-inodeno and x-hws-fs-inode-dentryname

// Helper functions
off_t str_to_off_t(const char* str);
void build_standard_metadata(headers_t& meta, mode_t mode, const std::string& content_type = "", bool preserve_mode = false);

// Basic file/directory operations
int s3fs_getattr_s3(const char* path, struct stat* stbuf);
int s3fs_readlink_s3(const char* path, char* buf, size_t size);
int s3fs_mknod_s3(const char* path, mode_t mode, dev_t rdev);
int s3fs_mkdir_s3(const char* path, mode_t mode);
int s3fs_unlink_s3(const char* path);
int s3fs_rmdir_s3(const char* path);
int s3fs_symlink_s3(const char* from, const char* to);
int s3fs_rename_s3(const char* from, const char* to);
int s3fs_link_s3(const char* from, const char* to);

// Attribute modification operations
int s3fs_chmod_s3(const char* path, mode_t mode);
int s3fs_chmod_nocopy_s3(const char* path, mode_t mode);
int s3fs_chown_s3(const char* path, uid_t uid, gid_t gid);
int s3fs_chown_nocopy_s3(const char* path, uid_t uid, gid_t gid);
int s3fs_utimens_s3(const char* path, const struct timespec ts[2]);
int s3fs_utimens_nocopy_s3(const char* path, const struct timespec ts[2]);

// File data operations
int s3fs_truncate_s3(const char* path, off_t size);
int s3fs_create_s3(const char* path, mode_t mode, struct fuse_file_info* fi);
int s3fs_open_s3(const char* path, struct fuse_file_info* fi);
int s3fs_read_s3(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* fi);
int s3fs_write_s3(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* fi);
int s3fs_flush_s3(const char* path, struct fuse_file_info* fi);
int s3fs_fsync_s3(const char* path, int datasync, struct fuse_file_info* fi);
int s3fs_release_s3(const char* path, struct fuse_file_info* fi);

// Directory operations
int s3fs_opendir_s3(const char* path, struct fuse_file_info* fi);
int s3fs_readdir_s3(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* fi);

// Other operations
int s3fs_statfs_s3(const char* path, struct statvfs* stbuf);
int s3fs_access_s3(const char* path, int mask);

// Extended attributes
int s3fs_setxattr_s3(const char* path, const char* name, const char* value, size_t size, int flags);
int s3fs_getxattr_s3(const char* path, const char* name, char* value, size_t size);
int s3fs_listxattr_s3(const char* path, char* list, size_t size);
int s3fs_removexattr_s3(const char* path, const char* name);

#endif // S3FS_OPERATIONS_H_
