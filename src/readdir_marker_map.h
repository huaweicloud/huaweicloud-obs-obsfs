#ifndef __HWS_READDIR_MARKER_MAP_H__
#define __HWS_READDIR_MARKER_MAP_H__

#include <stdio.h>
#include <time.h>
#include <string>
#include <sstream>
#include <mutex>
#include <map>
#include <list>
#include <vector>
#include <assert.h>
#include <atomic>


// get UTC nanoseconds
typedef int64_t nsec_type;
#define SEC_TO_NSEC     1000000000LL
static inline nsec_type get_utc_nanoseconds()
{
    nsec_type cur_time = 0;
    struct timespec spec;
    clock_gettime(CLOCK_MONOTONIC, &spec);
    cur_time = (((nsec_type)spec.tv_sec) * SEC_TO_NSEC) + spec.tv_nsec;
    return cur_time;
}

#define NSEC_TO_SECONDS(nsec)       ((nsec) / SEC_TO_NSEC)
#define NSEC_TO_HOURS(nsec)         (NSEC_TO_SECONDS(nsec) / 3600)
#define NSEC_TO_DAYS(nsec)          (NSEC_TO_HOURS(nsec) / 24)

struct readdir_marker
{
    nsec_type   nsec_;
    long long   ino_;
    std::string path_;
    std::string marker_;

    readdir_marker(
                nsec_type nsec,
                long long ino,
                const std::string& path,
                const std::string& marker)
            : nsec_(nsec),
              ino_(ino),
              path_(path),
              marker_(marker) {}
    readdir_marker() : readdir_marker(0, 0, "", "") {}
    std::string to_string(off_t pos) const {
        std::ostringstream oss;  //创建一个格式化输出流
        oss << "[ino=" << ino_
            << ",path=" << path_
            << ",marker[" << pos << "]=" << marker_
            << ",nsec=" << (nsec_/SEC_TO_NSEC) << "." << (nsec_%SEC_TO_NSEC) << "]";
        return oss.str();
    }
};

#define MARKER_MAX_DEAD_TIME    120 // unit : second
#define MARKER_MAX_DEAD_NSEC    (((nsec_type)MARKER_MAX_DEAD_TIME) * SEC_TO_NSEC)

#define MARKER_RECYCLE_NSEC     (10*SEC_TO_NSEC)
#define MARKER_RECYCLE_NSEC_MAX (3600*24*SEC_TO_NSEC)

class readdir_marker_map {
public:
    // types
    using map_type = std::map<off_t, readdir_marker>;
    using iterator = typename map_type::iterator;
    using size_type = typename map_type::size_type;
    // constants
    const static off_t npos = -1;

private:
    map_type map_;                  // the map of busy markers in which each one is identified by unique position. (A position likes a context id).
    size_type limited_size_;        // limited size of this map. The value range of posistions is [spos, spos+limited_size_) .
    off_t last_pos;
    std::mutex mtx_;
    nsec_type last_recycle_time_;

    bool _is_full() { return ((limited_size_ <= map_.size()) ? true : false); }

    // recycle pos that has been dead for a long time from the busy map.
    bool _need_recycle();
    void _recycle();

    // allocate one positive context id.
    off_t _alloc();

    // find marker by pos and path
    iterator _find(off_t pos, const readdir_marker& marker);

    // find and output marker
    off_t _search(off_t pos, readdir_marker& marker);

    // insert marker
    off_t _insert(const readdir_marker& marker);

    // update marker
    off_t _update(off_t pos, const readdir_marker& marker);

    // remove marker
    off_t _remove(off_t pos, const readdir_marker& marker);

public:
    // allocation/deallocation
    readdir_marker_map(size_type limited_size)
        : limited_size_(limited_size), last_pos(0), last_recycle_time_(0) {}

    // search or insert marker
    off_t search_or_insert(off_t pos, readdir_marker& marker);

    // update marker
    off_t update(off_t pos, const readdir_marker& marker);

    // remove marker
    off_t remove(off_t pos, const readdir_marker& marker);

    // check pos
    bool check_pos(off_t pos) const {
        return (pos > 0);
    }

    // check size
    bool check_size(size_type size) const {
        auto map_size = map_.size();
        return ((map_size <= limited_size_) && (map_size == size));
    }

    size_type size() const { return map_.size(); }

    size_type limited_size() const { return limited_size_; }

    // check map
    void clear () {
        map_.clear();
        last_pos = 0;
    }

};

typedef readdir_marker_map::size_type size_type;

extern readdir_marker_map g_readdir_marker_map;

extern int marker_search_or_insert(long long ino, const std::string& path, off_t& marker_pos, std::string& marker);
extern off_t marker_update(long long ino, const std::string& path, off_t marker_pos, const std::string& marker);
extern off_t marker_remove(long long ino, const std::string& path, off_t marker_pos);

#endif /* __HWS_READDIR_MARKER_MAP_H__ */
