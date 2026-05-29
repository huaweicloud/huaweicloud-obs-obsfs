// fs_mock.h
// Mock infrastructure for file system APIs using GNU ld --wrap.
// Provides controllable error injection for testing error paths.
//
// IMPORTANT: This is NEW infrastructure - no modification to fdcache.cpp required.
// Tests link against existing fdcache.o and use --wrap flags for interception.

#ifndef FS_MOCK_H_
#define FS_MOCK_H_

#include <functional>
#include <queue>
#include <string>
#include <map>
#include <mutex>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>

class FsMockController {
public:
    static FsMockController& Instance();
    void Reset();

    // ---- open mock control ----
    void SetOpenResult(int fd, int err = 0);  // fd=-1 with err for failure
    int GetNextOpenResult();
    void QueueOpenResults(std::initializer_list<int> fds);

    // ---- unlink mock control ----
    void SetUnlinkResult(int result, int err = 0);
    int GetNextUnlinkResult();

    // ---- link mock control ----
    void SetLinkResult(int result, int err = 0);
    int GetNextLinkResult();

    // ---- ftruncate mock control ----
    // Note: With _FILE_OFFSET_BITS=64, ftruncate maps to ftruncate64.
    // Both are wrapped to cover all cases.
    void SetFtruncateResult(int result, int err = 0);
    int GetNextFtruncateResult();

    // ---- fstat mock control ----
    void SetFstatResult(int result, const struct stat& st, int err = 0);
    int GetNextFstatResult(struct stat* st);

    // ---- pwrite mock control ----
    void SetPwriteResult(ssize_t result, int err = 0);
    ssize_t GetNextPwriteResult();

    // ---- stat mock control ----
    void SetStatResult(int result, const struct stat& st, int err = 0);
    int GetNextStatResult(struct stat* st);

    // ---- pread mock control ----
    void SetPreadResult(ssize_t result, int err = 0);
    ssize_t GetNextPreadResult();

    // ---- close mock control ----
    void SetCloseResult(int result, int err = 0);
    int GetNextCloseResult();

    // Enable/disable mock mode
    void SetMockEnabled(bool enabled) { mock_enabled_ = enabled; }
    bool IsMockEnabled() const { return mock_enabled_; }

private:
    FsMockController() = default;
    ~FsMockController() = default;
    FsMockController(const FsMockController&) = delete;
    FsMockController& operator=(const FsMockController&) = delete;

    bool mock_enabled_ = false;

    std::queue<int> open_results_;
    std::queue<int> open_errors_;
    std::queue<int> unlink_results_;
    std::queue<int> unlink_errors_;
    std::queue<int> link_results_;
    std::queue<int> link_errors_;
    std::queue<int> ftruncate_results_;
    std::queue<int> ftruncate_errors_;
    std::queue<std::pair<int, struct stat>> fstat_results_;
    std::queue<int> fstat_errors_;
    std::queue<ssize_t> pwrite_results_;
    std::queue<int> pwrite_errors_;
    std::queue<std::pair<int, struct stat>> stat_results_;
    std::queue<int> stat_errors_;
    std::queue<ssize_t> pread_results_;
    std::queue<int> pread_errors_;
    std::queue<int> close_results_;
    std::queue<int> close_errors_;

    std::mutex mutex_;
};

class ScopedFsMock {
public:
    ScopedFsMock() { FsMockController::Instance().Reset(); FsMockController::Instance().SetMockEnabled(true); }
    ~ScopedFsMock() { FsMockController::Instance().Reset(); FsMockController::Instance().SetMockEnabled(false); }
};

#endif // FS_MOCK_H_
