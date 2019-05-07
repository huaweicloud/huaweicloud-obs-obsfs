#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <stdint.h>
#include <string>
#include <map>

#include "common.h"
#include "string_util.h"
#include "obsfs_log.h"
#include "readdir_marker_map.h"


//
// class readdir_marker_map:
//

// allocate one positive context id.
off_t readdir_marker_map::_alloc() {
    // try to recycle
    _recycle();
    if (_is_full()) {
        return npos;
    }
    // allocate a pos for a new marker
    last_pos++;
    if (last_pos <= 0) {
        last_pos = 1;
    }
    // check that the new pos is idle
    auto it = map_.find(last_pos);
    if (it != map_.end()) {
        return (-1);
    }
    return last_pos;
}

// check if need to recycle
bool readdir_marker_map::_need_recycle() {
    auto now = get_utc_nanoseconds();
    if (now < last_recycle_time_) {
        S3FS_PRN_ERR("Force recycle due to backwards time. size(%ld,%ld), time(%ld,%ld)",
            limited_size_, map_.size(), last_recycle_time_, now);
    } else if ((now - last_recycle_time_) >= MARKER_RECYCLE_NSEC_MAX) {
        S3FS_PRN_ERR("Force recycle one time every %lld hours. size(%ld,%ld), time(%ld,%ld)",
            NSEC_TO_HOURS(MARKER_RECYCLE_NSEC_MAX),
            limited_size_, map_.size(), last_recycle_time_, now);
    } else if ((map_.size() >= limited_size_)
                && ((now - last_recycle_time_) >= MARKER_RECYCLE_NSEC)) {
        S3FS_PRN_ERR("Force recycle every %lld seconds. size(%ld,%ld), time(%ld,%ld)",
            NSEC_TO_SECONDS(MARKER_RECYCLE_NSEC),
            limited_size_, map_.size(), last_recycle_time_, now);
    } else {
        // no need to recycle
        return false;
    }
    // need to recycle
    return true;
}

// recycle marker that has been dead for a long time from the busy map.
void readdir_marker_map::_recycle() {
    // check if need to recycle
    if (!_need_recycle()) {
        return;
    }

    // recycle
    auto now = get_utc_nanoseconds();
    size_type recycle_count = 0;
    auto it = map_.begin();
    while (it != map_.end()) {
        auto nsec = it->second.nsec_;
        if (now < nsec) {
            S3FS_PRN_ERR("%s marker is forwards in time. now(%ld) < nsec(%ld)",
                it->second.to_string(it->first).c_str(), now, nsec);
            break;
        }
        if ((now - nsec) > MARKER_MAX_DEAD_NSEC) {
            it = map_.erase(it);
            recycle_count++;
        } else {
            it++;
        }
    }
    S3FS_PRN_ERR("recycle %ld mark positions", recycle_count);
    last_recycle_time_ = get_utc_nanoseconds();
    return;
}

// find marker by pos and path
readdir_marker_map::iterator readdir_marker_map::_find(off_t pos, const readdir_marker& marker) {
    // find marker
    auto it = map_.find(pos);
    if (it == map_.end()) {
        return map_.end();
    }
    // check path
    if ((it->second.ino_ != marker.ino_) || (it->second.path_ != marker.path_)) {
        S3FS_PRN_ERR("%s conflicts with %s",
            it->second.to_string(it->first).c_str(), marker.to_string(pos).c_str());
        return map_.end();
    }
    return it;
}

// find and output marker
off_t readdir_marker_map::_search(off_t pos, readdir_marker& marker) {
    // find marker
    auto it = _find(pos, marker);
    if (it == map_.end()) {
        return npos;
    }
    // output maker
    marker.nsec_ = it->second.nsec_;
    marker.marker_ = it->second.marker_;
    return pos;
}

// insert marker
off_t readdir_marker_map::_insert(const readdir_marker& marker) {
    off_t pos = _alloc();
    if (pos > 0) {
        map_.insert(std::pair<off_t, readdir_marker>(pos, marker));
    }
    return pos;
}

// update marker
off_t readdir_marker_map::_update(off_t pos, const readdir_marker& marker) {
    // find marker
    auto it = _find(pos, marker);
    if (it == map_.end()) {
        return npos;
    }
    // update marker
    it->second.nsec_ = marker.nsec_;
    it->second.marker_ = marker.marker_;
    return pos;
}

// remove marker
off_t readdir_marker_map::_remove(off_t pos, const readdir_marker& marker) {
    // find marker
    auto it = _find(pos, marker);
    if (it == map_.end()) {
        return npos;
    }
    // delete marker
    map_.erase(it);
    return pos;
}

// search or insert marker
off_t readdir_marker_map::search_or_insert(off_t pos, readdir_marker& marker) {
    std::lock_guard<std::mutex> mtx(mtx_);
    if (0 == pos) {
        return _insert(marker);
    } else {
        return _search(pos, marker);
    }
}

// update marker
off_t readdir_marker_map::update(off_t pos, const readdir_marker& marker) {
    std::lock_guard<std::mutex> mtx(mtx_);
    return _update(pos, marker);
}

// remove marker
off_t readdir_marker_map::remove(off_t pos, const readdir_marker& marker) {
    std::lock_guard<std::mutex> mtx(mtx_);
    return _remove(pos, marker);
}



//
// g_readdir_marker_map:
//

readdir_marker_map g_readdir_marker_map(1000000);

int marker_search_or_insert(long long ino, const std::string& path, off_t& marker_pos, std::string& marker)
{
    readdir_marker readdir_marker(get_utc_nanoseconds(), ino, path, marker);
    auto pos = g_readdir_marker_map.search_or_insert(marker_pos, readdir_marker);
    if (pos != readdir_marker_map::npos) {
        //found or created successfully
        marker_pos = pos;
        marker = readdir_marker.marker_;
        return 0;
    }
    if (0 == marker_pos) {
        // create failed.
        S3FS_PRN_ERR("[ino=%lld,path=%s,off=%ld] marker map is overflow.",
            ino, path.c_str(), marker_pos);
        marker_pos = readdir_marker_map::npos;
        return -EIO;
    } else {
        // not found.
        S3FS_PRN_ERR("[ino=%lld,path=%s,off=%ld] not existed.",
            ino, path.c_str(), marker_pos);
        marker_pos = readdir_marker_map::npos;
        return -EINVAL;
    }
}

off_t marker_update(long long ino, const std::string& path, off_t marker_pos, const std::string& marker)
{
    readdir_marker readdir_marker(get_utc_nanoseconds(), ino, path, marker);
    return g_readdir_marker_map.update(marker_pos, readdir_marker);
}

off_t marker_remove(long long ino, const std::string& path, off_t marker_pos)
{
    readdir_marker readdir_marker(0, ino, path, "");
    return g_readdir_marker_map.remove(marker_pos, readdir_marker);
}

