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

#include <cstdio>
#include <algorithm>

#include "common.h"
#include "fdcache_untreated.h"

//------------------------------------------------
// UntreatedParts methods
//------------------------------------------------

// Add a part [start, start+size) to the list. Merges with overlapping
// or adjacent existing parts.
//
bool UntreatedParts::AddPart(off_t start, off_t size)
{
    if(start < 0 || size <= 0){
        return false;
    }

    ++last_tag;

    // Find the first part that could overlap or be adjacent
    for(untreated_list_t::iterator iter = untreated_list.begin(); iter != untreated_list.end(); ){
        if(iter->stretch(start, size, last_tag)){
            // Merged with existing part. Now check if subsequent parts
            // should also be merged into this one.
            untreated_list_t::iterator merged = iter;
            ++iter;
            while(iter != untreated_list.end()){
                if(merged->stretch(iter->start, iter->size, last_tag)){
                    iter = untreated_list.erase(iter);
                }else{
                    break;
                }
            }
            return true;
        }
        if(start < iter->start){
            // Insert before this part (no overlap)
            untreated_list.insert(iter, untreatedpart(start, size, last_tag));
            return true;
        }
        ++iter;
    }

    // Append at end
    untreated_list.push_back(untreatedpart(start, size, last_tag));
    return true;
}

// Internal helper: find the part with the highest tag.
// Returns part constrained by [min_size, max_size].
//
bool UntreatedParts::RowGetPart(off_t& start, off_t& size, off_t max_size, off_t min_size) const
{
    if(untreated_list.empty()){
        return false;
    }

    // Find the part with the highest tag
    // [ISSUE2026040700003] The post-loop `if(latest == end()) return false;`
    // check was removed: it is mathematically unreachable. The empty()
    // guard above guarantees the loop body runs at least once, and the ||
    // short-circuit on the first iteration unconditionally assigns latest
    // to a valid iterator. See issue for the full proof.
    untreated_list_t::const_iterator latest = untreated_list.end();
    for(untreated_list_t::const_iterator iter = untreated_list.begin(); iter != untreated_list.end(); ++iter){
        if(latest == untreated_list.end() || iter->untreated_tag > latest->untreated_tag){
            latest = iter;
        }
    }

    if(0 < min_size && latest->size < min_size){
        return false;
    }

    start = latest->start;
    if(0 < max_size && latest->size > max_size){
        size = max_size;
    }else{
        size = latest->size;
    }
    return true;
}

// Get the most recently updated part that meets size constraints.
// max_size: if part is larger, truncate to max_size (0 = no limit)
// min_size: if part is smaller, return false (0 = no minimum)
//
bool UntreatedParts::GetLastUpdatedPart(off_t& start, off_t& size, off_t max_size, off_t min_size) const
{
    return RowGetPart(start, size, max_size, min_size);
}

// Clear (remove or trim) the range [start, start+size) from the list.
// If size==0, clear from start to infinity.
// Handles 4 overlap cases: exact match, left trim, right trim, split.
//
bool UntreatedParts::ClearParts(off_t start, off_t size)
{
    if(start < 0){
        return false;
    }

    // size==0 means clear from start to end
    off_t clear_end;
    if(0 == size){
        // Use a large value to represent infinity
        clear_end = static_cast<off_t>(0x7FFFFFFFFFFFFFFFLL);
    }else if(size < 0){
        return false;
    }else{
        clear_end = start + size;
    }

    for(untreated_list_t::iterator iter = untreated_list.begin(); iter != untreated_list.end(); ){
        off_t part_start = iter->start;
        off_t part_end   = iter->start + iter->size;

        if(clear_end <= part_start){
            // No more overlaps possible (list is sorted by start)
            break;
        }
        if(start >= part_end){
            // No overlap with this part
            ++iter;
            continue;
        }

        // There is overlap. Determine the case:
        if(start <= part_start && clear_end >= part_end){
            // Case 1: Complete removal
            iter = untreated_list.erase(iter);
        }else if(start <= part_start){
            // Case 2: Left trim
            iter->size  = part_end - clear_end;
            iter->start = clear_end;
            ++iter;
        }else if(clear_end >= part_end){
            // Case 3: Right trim
            iter->size = start - part_start;
            ++iter;
        }else{
            // Case 4: Split — clear punches a hole in the middle
            long tag = iter->untreated_tag;
            // Adjust current part to be the left piece
            iter->size = start - part_start;
            // Insert right piece after current
            ++iter;
            untreated_list.insert(iter, untreatedpart(clear_end, part_end - clear_end, tag));
            // iter now points past the inserted element, no need to advance
        }
    }
    return true;
}

// Clear all parts.
//
bool UntreatedParts::ClearAll()
{
    untreated_list.clear();
    last_tag = 0;
    return true;
}

// Get the part with the most recent tag (no size constraints).
//
bool UntreatedParts::GetLastUpdatePart(off_t& start, off_t& size) const
{
    if(untreated_list.empty()){
        return false;
    }

    // [ISSUE2026040700003] post-loop unreachable check removed (see RowGetPart).
    untreated_list_t::const_iterator latest = untreated_list.end();
    for(untreated_list_t::const_iterator iter = untreated_list.begin(); iter != untreated_list.end(); ++iter){
        if(latest == untreated_list.end() || iter->untreated_tag > latest->untreated_tag){
            latest = iter;
        }
    }

    start = latest->start;
    size  = latest->size;
    return true;
}

// Replace the most recently tagged part with new start/size.
// If size <= 0, removes the part instead.
//
bool UntreatedParts::ReplaceLastUpdatePart(off_t start, off_t size)
{
    if(untreated_list.empty()){
        return false;
    }

    // [ISSUE2026040700003] post-loop unreachable check removed (see RowGetPart).
    untreated_list_t::iterator latest = untreated_list.end();
    for(untreated_list_t::iterator iter = untreated_list.begin(); iter != untreated_list.end(); ++iter){
        if(latest == untreated_list.end() || iter->untreated_tag > latest->untreated_tag){
            latest = iter;
        }
    }

    if(size <= 0){
        untreated_list.erase(latest);
    }else{
        latest->start = start;
        latest->size  = size;
    }
    return true;
}

// Remove the most recently tagged part.
//
bool UntreatedParts::RemoveLastUpdatePart()
{
    if(untreated_list.empty()){
        return false;
    }

    // [ISSUE2026040700003] post-loop unreachable check removed (see RowGetPart).
    untreated_list_t::iterator latest = untreated_list.end();
    for(untreated_list_t::iterator iter = untreated_list.begin(); iter != untreated_list.end(); ++iter){
        if(latest == untreated_list.end() || iter->untreated_tag > latest->untreated_tag){
            latest = iter;
        }
    }

    untreated_list.erase(latest);
    return true;
}

// Copy the list contents to the caller's list.
//
bool UntreatedParts::Duplicate(untreated_list_t& list) const
{
    list = untreated_list;
    return true;
}

// Debug dump of all parts.
//
void UntreatedParts::Dump() const
{
    S3FS_PRN_DBG("UntreatedParts: count=%zu", untreated_list.size());
    int idx = 0;
    for(untreated_list_t::const_iterator iter = untreated_list.begin(); iter != untreated_list.end(); ++iter, ++idx){
        S3FS_PRN_DBG("  [%d] start=%jd, size=%jd, tag=%ld",
                     idx, (intmax_t)iter->start, (intmax_t)iter->size, iter->untreated_tag);
    }
}

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: noet sw=4 ts=4 fdm=marker
* vim<600: noet sw=4 ts=4
*/
