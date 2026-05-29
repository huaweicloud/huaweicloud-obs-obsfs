// fs_mock.cpp
// Implementation of file system mock using GNU ld --wrap
//
// IMPORTANT: This is NEW infrastructure - no modification to fdcache.cpp required.

#include "fs_mock.h"
#include <errno.h>
#include <cstring>
#include <cstdarg>

FsMockController& FsMockController::Instance() {
    static FsMockController instance;
    return instance;
}

void FsMockController::Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::queue<int> empty_int;
    std::queue<ssize_t> empty_ssize;
    std::queue<std::pair<int, struct stat>> empty_stat;
    std::queue<int> empty_err;

    open_results_.swap(empty_int);
    open_errors_.swap(empty_err);
    unlink_results_.swap(empty_int);
    unlink_errors_.swap(empty_err);
    link_results_.swap(empty_int);
    link_errors_.swap(empty_err);
    ftruncate_results_.swap(empty_int);
    ftruncate_errors_.swap(empty_err);
    fstat_results_.swap(empty_stat);
    fstat_errors_.swap(empty_err);
    pwrite_results_.swap(empty_ssize);
    pwrite_errors_.swap(empty_err);
    stat_results_.swap(empty_stat);
    stat_errors_.swap(empty_err);
    pread_results_.swap(empty_ssize);
    pread_errors_.swap(empty_err);
    close_results_.swap(empty_int);
    close_errors_.swap(empty_err);

    mock_enabled_ = false;
}

void FsMockController::SetOpenResult(int fd, int err) {
    std::lock_guard<std::mutex> lock(mutex_);
    open_results_.push(fd);
    open_errors_.push(err);
}

int FsMockController::GetNextOpenResult() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (open_results_.empty()) return -1;  // Default: fail
    int fd = open_results_.front();
    open_results_.pop();
    if (!open_errors_.empty()) {
        errno = open_errors_.front();
        open_errors_.pop();
    }
    return fd;
}

void FsMockController::QueueOpenResults(std::initializer_list<int> fds) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (int fd : fds) {
        open_results_.push(fd);
        open_errors_.push(0);
    }
}

void FsMockController::SetUnlinkResult(int result, int err) {
    std::lock_guard<std::mutex> lock(mutex_);
    unlink_results_.push(result);
    unlink_errors_.push(err);
}

int FsMockController::GetNextUnlinkResult() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (unlink_results_.empty()) return 0;  // Default: success
    int result = unlink_results_.front();
    unlink_results_.pop();
    if (!unlink_errors_.empty()) {
        errno = unlink_errors_.front();
        unlink_errors_.pop();
    }
    return result;
}

void FsMockController::SetLinkResult(int result, int err) {
    std::lock_guard<std::mutex> lock(mutex_);
    link_results_.push(result);
    link_errors_.push(err);
}

int FsMockController::GetNextLinkResult() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (link_results_.empty()) return 0;
    int result = link_results_.front();
    link_results_.pop();
    if (!link_errors_.empty()) {
        errno = link_errors_.front();
        link_errors_.pop();
    }
    return result;
}

void FsMockController::SetFtruncateResult(int result, int err) {
    std::lock_guard<std::mutex> lock(mutex_);
    ftruncate_results_.push(result);
    ftruncate_errors_.push(err);
}

int FsMockController::GetNextFtruncateResult() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (ftruncate_results_.empty()) return 0;
    int result = ftruncate_results_.front();
    ftruncate_results_.pop();
    if (!ftruncate_errors_.empty()) {
        errno = ftruncate_errors_.front();
        ftruncate_errors_.pop();
    }
    return result;
}

void FsMockController::SetFstatResult(int result, const struct stat& st, int err) {
    std::lock_guard<std::mutex> lock(mutex_);
    fstat_results_.push({result, st});
    fstat_errors_.push(err);
}

int FsMockController::GetNextFstatResult(struct stat* st) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fstat_results_.empty()) {
        memset(st, 0, sizeof(*st));
        return 0;
    }
    auto& pair = fstat_results_.front();
    int result = pair.first;
    *st = pair.second;
    fstat_results_.pop();
    if (!fstat_errors_.empty()) {
        errno = fstat_errors_.front();
        fstat_errors_.pop();
    }
    return result;
}

void FsMockController::SetPwriteResult(ssize_t result, int err) {
    std::lock_guard<std::mutex> lock(mutex_);
    pwrite_results_.push(result);
    pwrite_errors_.push(err);
}

ssize_t FsMockController::GetNextPwriteResult() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pwrite_results_.empty()) return -1;
    ssize_t result = pwrite_results_.front();
    pwrite_results_.pop();
    if (!pwrite_errors_.empty()) {
        errno = pwrite_errors_.front();
        pwrite_errors_.pop();
    }
    return result;
}

void FsMockController::SetStatResult(int result, const struct stat& st, int err) {
    std::lock_guard<std::mutex> lock(mutex_);
    stat_results_.push({result, st});
    stat_errors_.push(err);
}

int FsMockController::GetNextStatResult(struct stat* st) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stat_results_.empty()) {
        memset(st, 0, sizeof(*st));
        return -1;  // Default: fail with ENOENT
    }
    auto& pair = stat_results_.front();
    int result = pair.first;
    *st = pair.second;
    stat_results_.pop();
    if (!stat_errors_.empty()) {
        errno = stat_errors_.front();
        stat_errors_.pop();
    }
    return result;
}

void FsMockController::SetPreadResult(ssize_t result, int err) {
    std::lock_guard<std::mutex> lock(mutex_);
    pread_results_.push(result);
    pread_errors_.push(err);
}

ssize_t FsMockController::GetNextPreadResult() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pread_results_.empty()) return -1;
    ssize_t result = pread_results_.front();
    pread_results_.pop();
    if (!pread_errors_.empty()) {
        errno = pread_errors_.front();
        pread_errors_.pop();
    }
    return result;
}

void FsMockController::SetCloseResult(int result, int err) {
    std::lock_guard<std::mutex> lock(mutex_);
    close_results_.push(result);
    close_errors_.push(err);
}

int FsMockController::GetNextCloseResult() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (close_results_.empty()) return 0;
    int result = close_results_.front();
    close_results_.pop();
    if (!close_errors_.empty()) {
        errno = close_errors_.front();
        close_errors_.pop();
    }
    return result;
}

// Wrapped functions - these will be called via --wrap=funcname
extern "C" {

int __wrap_open(const char* pathname, int flags, ...) {
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list args;
        va_start(args, flags);
        mode = va_arg(args, mode_t);
        va_end(args);
    }
    if (FsMockController::Instance().IsMockEnabled()) {
        return FsMockController::Instance().GetNextOpenResult();
    }
    // Fallback to real function
    extern int __real_open(const char*, int, mode_t);
    return __real_open(pathname, flags, mode);
}

int __wrap_open64(const char* pathname, int flags, ...) {
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list args;
        va_start(args, flags);
        mode = va_arg(args, mode_t);
        va_end(args);
    }
    if (FsMockController::Instance().IsMockEnabled()) {
        return FsMockController::Instance().GetNextOpenResult();
    }
    extern int __real_open64(const char*, int, mode_t);
    return __real_open64(pathname, flags, mode);
}

int __wrap_unlink(const char* pathname) {
    if (FsMockController::Instance().IsMockEnabled()) {
        return FsMockController::Instance().GetNextUnlinkResult();
    }
    extern int __real_unlink(const char*);
    return __real_unlink(pathname);
}

int __wrap_link(const char* oldpath, const char* newpath) {
    if (FsMockController::Instance().IsMockEnabled()) {
        return FsMockController::Instance().GetNextLinkResult();
    }
    extern int __real_link(const char*, const char*);
    return __real_link(oldpath, newpath);
}

int __wrap_ftruncate(int fd, off_t length) {
    if (FsMockController::Instance().IsMockEnabled()) {
        return FsMockController::Instance().GetNextFtruncateResult();
    }
    extern int __real_ftruncate(int, off_t);
    return __real_ftruncate(fd, length);
}

// With _FILE_OFFSET_BITS=64, ftruncate() maps to ftruncate64()
int __wrap_ftruncate64(int fd, off_t length) {
    if (FsMockController::Instance().IsMockEnabled()) {
        return FsMockController::Instance().GetNextFtruncateResult();
    }
    extern int __real_ftruncate64(int, off_t);
    return __real_ftruncate64(fd, length);
}

int __wrap_fstat(int fd, struct stat* statbuf) {
    if (FsMockController::Instance().IsMockEnabled()) {
        return FsMockController::Instance().GetNextFstatResult(statbuf);
    }
    extern int __real_fstat(int, struct stat*);
    return __real_fstat(fd, statbuf);
}

ssize_t __wrap_pwrite(int fd, const void* buf, size_t count, off_t offset) {
    if (FsMockController::Instance().IsMockEnabled()) {
        return FsMockController::Instance().GetNextPwriteResult();
    }
    extern ssize_t __real_pwrite(int, const void*, size_t, off_t);
    return __real_pwrite(fd, buf, count, offset);
}

ssize_t __wrap_pread(int fd, void* buf, size_t count, off_t offset) {
    if (FsMockController::Instance().IsMockEnabled()) {
        return FsMockController::Instance().GetNextPreadResult();
    }
    extern ssize_t __real_pread(int, void*, size_t, off_t);
    return __real_pread(fd, buf, count, offset);
}

int __wrap_stat(const char* pathname, struct stat* statbuf) {
    if (FsMockController::Instance().IsMockEnabled()) {
        return FsMockController::Instance().GetNextStatResult(statbuf);
    }
    extern int __real_stat(const char*, struct stat*);
    return __real_stat(pathname, statbuf);
}

int __wrap___xstat(int ver, const char* pathname, struct stat* statbuf) {
    if (FsMockController::Instance().IsMockEnabled()) {
        return FsMockController::Instance().GetNextStatResult(statbuf);
    }
    extern int __real___xstat(int, const char*, struct stat*);
    return __real___xstat(ver, pathname, statbuf);
}

int __wrap___lxstat(int ver, const char* pathname, struct stat* statbuf) {
    if (FsMockController::Instance().IsMockEnabled()) {
        return FsMockController::Instance().GetNextStatResult(statbuf);
    }
    extern int __real___lxstat(int, const char*, struct stat*);
    return __real___lxstat(ver, pathname, statbuf);
}

int __wrap_close(int fd) {
    if (FsMockController::Instance().IsMockEnabled()) {
        return FsMockController::Instance().GetNextCloseResult();
    }
    extern int __real_close(int);
    return __real_close(fd);
}

}  // extern "C"
