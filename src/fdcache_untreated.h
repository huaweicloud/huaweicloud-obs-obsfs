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

#ifndef FDCACHE_UNTREATED_H_
#define FDCACHE_UNTREATED_H_

#include <sys/types.h>
#include <list>

//------------------------------------------------
// Structure: untreatedpart
//------------------------------------------------
// Represents a contiguous range [start, start+size) of data that
// has been written but not yet uploaded via multipart.
//
struct untreatedpart
{
    off_t start;
    off_t size;
    long  untreated_tag;    // monotonically increasing tag to identify most recent part

    explicit untreatedpart(off_t start = 0, off_t size = 0, long tag = 0)
        : start(start), size(size), untreated_tag(tag) {}

    void clear(void)
    {
        start         = 0;
        size          = 0;
        untreated_tag = 0;
    }

    // Check if this part overlaps or is adjacent to [start2, start2+size2)
    bool check_overlap(off_t start2, off_t size2) const
    {
        if(start2 < (start + size) && start < (start2 + size2)){
            return true;
        }
        return false;
    }

    // Check if this part is adjacent to [start2, start2+size2)
    bool check_adjacent(off_t start2, off_t size2) const
    {
        if((start + size) == start2 || (start2 + size2) == start){
            return true;
        }
        return false;
    }

    // Stretch this part to include [start2, start2+size2)
    bool stretch(off_t start2, off_t size2, long tag2)
    {
        if(!check_overlap(start2, size2) && !check_adjacent(start2, size2)){
            return false;
        }
        off_t new_start = std::min(start, start2);
        off_t new_end   = std::max(start + size, start2 + size2);
        start           = new_start;
        size            = new_end - new_start;
        untreated_tag   = tag2;
        return true;
    }
};

typedef std::list<untreatedpart> untreated_list_t;

//------------------------------------------------
// Class: UntreatedParts
//------------------------------------------------
// Manages a list of untreated (written but not yet uploaded) ranges.
// Supports merging overlapping/adjacent ranges and splitting on clear.
//
// NOTE: This class does NOT have internal locking. Callers must hold
// the appropriate lock (e.g., FdEntity::fdent_lock) before calling
// any method.
//
class UntreatedParts
{
  private:
    untreated_list_t untreated_list;
    long             last_tag;

    bool RowGetPart(off_t& start, off_t& size, off_t max_size, off_t min_size) const;

  public:
    UntreatedParts() : last_tag(0) {}

    bool   empty() const { return untreated_list.empty(); }
    size_t size() const { return untreated_list.size(); }

    bool AddPart(off_t start, off_t size);
    bool GetLastUpdatedPart(off_t& start, off_t& size, off_t max_size, off_t min_size) const;
    bool ClearParts(off_t start, off_t size);
    bool ClearAll();
    bool GetLastUpdatePart(off_t& start, off_t& size) const;
    bool ReplaceLastUpdatePart(off_t start, off_t size);
    bool RemoveLastUpdatePart();
    bool Duplicate(untreated_list_t& list) const;
    void Dump() const;
};

#endif // FDCACHE_UNTREATED_H_

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: noet sw=4 ts=4 fdm=marker
* vim<600: noet sw=4 ts=4
*/
