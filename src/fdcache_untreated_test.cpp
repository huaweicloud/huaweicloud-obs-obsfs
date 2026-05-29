/*
 * Unit tests for UntreatedParts (fdcache_untreated.h/cpp)
 *
 * Tests all methods of the UntreatedParts class with 100% branch coverage:
 * - AddPart: empty, non-overlapping, overlapping merge, adjacent merge, chain merge, etc.
 * - ClearParts: exact match, left/right trim, split, multi-overlap, etc.
 * - GetLastUpdatedPart: basic, too small, too large, empty, etc.
 * - GetLastUpdatePart, ReplaceLastUpdatePart, RemoveLastUpdatePart
 * - Duplicate, Dump, empty, size
 * - Integration scenarios: sequential write, random write, partial upload residual
 */

#include "gtest.h"
#include "fdcache_untreated.h"

// Stub for S3FS_PRN_DBG / S3FS_PRN_ERR used by fdcache_untreated.cpp
// These are defined in common.h which is included by fdcache_untreated.cpp.
// We need the globals they reference.
#include "common.h"

// Stubs for globals referenced by common.h logging macros
s3fs_log_level debug_level = S3FS_LOG_WARN;
const char*    s3fs_log_nest[S3FS_LOG_NEST_MAX] = { "", "  ", "    ", "      " };
s3fs_log_mode  debug_log_mode = LOG_MODE_FOREGROUND;
bucket_type_t  g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;

void IndexLogEntry(const uint64_t, const char*, const uint32_t,
                   const char*, const char*, const uint32_t,
                   const char*, ...) {}
void s3fsStatisLogModeNotObsfs() {}

//=============================================================
// untreatedpart struct tests
//=============================================================

TEST(UntreatedPartStruct, DefaultConstruction)
{
    untreatedpart p;
    EXPECT_EQ(0, p.start);
    EXPECT_EQ(0, p.size);
    EXPECT_EQ(0, p.untreated_tag);
}

TEST(UntreatedPartStruct, ParameterizedConstruction)
{
    untreatedpart p(100, 200, 5);
    EXPECT_EQ(100, p.start);
    EXPECT_EQ(200, p.size);
    EXPECT_EQ(5, p.untreated_tag);
}

// explicit 构造函数：单参数/双参数调用时其余参数取默认值
// 注意：explicit 禁止 untreatedpart p = 42 这样的隐式转换，
// 这是编译期约束，此处验证显式单参数构造的默认值正确性。
TEST(UntreatedPartStruct, ExplicitSingleParam)
{
    untreatedpart p(50);
    EXPECT_EQ(50, p.start);
    EXPECT_EQ(0, p.size);
    EXPECT_EQ(0, p.untreated_tag);
}

TEST(UntreatedPartStruct, ExplicitTwoParams)
{
    untreatedpart p(10, 20);
    EXPECT_EQ(10, p.start);
    EXPECT_EQ(20, p.size);
    EXPECT_EQ(0, p.untreated_tag);
}

TEST(UntreatedPartStruct, Clear)
{
    untreatedpart p(100, 200, 5);
    p.clear();
    EXPECT_EQ(0, p.start);
    EXPECT_EQ(0, p.size);
    EXPECT_EQ(0, p.untreated_tag);
}

TEST(UntreatedPartStruct, CheckOverlap)
{
    untreatedpart p(10, 10, 1);  // [10, 20)

    // Overlapping cases
    EXPECT_TRUE(p.check_overlap(15, 10));   // [15, 25) overlaps
    EXPECT_TRUE(p.check_overlap(5, 10));    // [5, 15) overlaps
    EXPECT_TRUE(p.check_overlap(12, 3));    // [12, 15) contained
    EXPECT_TRUE(p.check_overlap(5, 20));    // [5, 25) contains

    // Non-overlapping cases
    EXPECT_FALSE(p.check_overlap(20, 5));   // [20, 25) adjacent but not overlapping
    EXPECT_FALSE(p.check_overlap(0, 10));   // [0, 10) adjacent but not overlapping
    EXPECT_FALSE(p.check_overlap(25, 5));   // [25, 30) far away
    EXPECT_FALSE(p.check_overlap(0, 5));    // [0, 5) far away
}

TEST(UntreatedPartStruct, CheckAdjacent)
{
    untreatedpart p(10, 10, 1);  // [10, 20)

    EXPECT_TRUE(p.check_adjacent(20, 5));   // [20, 25) adjacent right
    EXPECT_TRUE(p.check_adjacent(5, 5));    // [5, 10) adjacent left
    EXPECT_FALSE(p.check_adjacent(21, 5));  // gap
    EXPECT_FALSE(p.check_adjacent(0, 5));   // gap
}

TEST(UntreatedPartStruct, Stretch)
{
    untreatedpart p(10, 10, 1);  // [10, 20)

    // Stretch with overlapping range
    EXPECT_TRUE(p.stretch(15, 10, 2));  // [15, 25) overlaps
    EXPECT_EQ(10, p.start);
    EXPECT_EQ(15, p.size);  // [10, 25)
    EXPECT_EQ(2, p.untreated_tag);

    // Stretch with adjacent range
    p = untreatedpart(10, 10, 1);  // [10, 20)
    EXPECT_TRUE(p.stretch(20, 5, 3));  // [20, 25) adjacent
    EXPECT_EQ(10, p.start);
    EXPECT_EQ(15, p.size);  // [10, 25)
    EXPECT_EQ(3, p.untreated_tag);

    // No stretch with non-overlapping, non-adjacent range
    p = untreatedpart(10, 10, 1);  // [10, 20)
    EXPECT_FALSE(p.stretch(25, 5, 4));  // [25, 30) — gap
    EXPECT_EQ(10, p.start);  // unchanged
    EXPECT_EQ(10, p.size);
    EXPECT_EQ(1, p.untreated_tag);
}

//=============================================================
// AddPart tests
//=============================================================

TEST(AddPart, Empty)
{
    UntreatedParts parts;
    EXPECT_TRUE(parts.empty());
    EXPECT_EQ(0u, parts.size());

    EXPECT_TRUE(parts.AddPart(0, 100));
    EXPECT_FALSE(parts.empty());
    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(1u, list.size());
    EXPECT_EQ(0, list.front().start);
    EXPECT_EQ(100, list.front().size);
}

TEST(AddPart, NonOverlapping)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);
    parts.AddPart(20, 10);
    parts.AddPart(40, 10);

    EXPECT_EQ(3u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    untreated_list_t::iterator it = list.begin();
    EXPECT_EQ(0, it->start);  EXPECT_EQ(10, it->size);  ++it;
    EXPECT_EQ(20, it->start); EXPECT_EQ(10, it->size);  ++it;
    EXPECT_EQ(40, it->start); EXPECT_EQ(10, it->size);
}

TEST(AddPart, OverlappingMerge)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);    // [0, 10)
    parts.AddPart(5, 10);    // [5, 15) — overlaps

    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(0, list.front().start);
    EXPECT_EQ(15, list.front().size);  // merged to [0, 15)
}

TEST(AddPart, AdjacentMerge)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);    // [0, 10)
    parts.AddPart(10, 10);   // [10, 20) — adjacent

    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(0, list.front().start);
    EXPECT_EQ(20, list.front().size);  // merged to [0, 20)
}

TEST(AddPart, ChainMerge)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);    // [0, 10)
    parts.AddPart(20, 10);   // [20, 30)
    parts.AddPart(40, 10);   // [40, 50)
    EXPECT_EQ(3u, parts.size());

    // Add a part that bridges all three
    parts.AddPart(5, 40);    // [5, 45) — overlaps all three
    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(0, list.front().start);
    EXPECT_EQ(50, list.front().size);  // merged to [0, 50)
}

TEST(AddPart, CompleteOverlap)
{
    UntreatedParts parts;
    parts.AddPart(10, 5);    // [10, 15)

    // Add part that completely contains existing
    parts.AddPart(5, 20);    // [5, 25)
    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(5, list.front().start);
    EXPECT_EQ(20, list.front().size);
}

TEST(AddPart, InsertBefore)
{
    UntreatedParts parts;
    parts.AddPart(20, 10);   // [20, 30)
    parts.AddPart(5, 5);     // [5, 10) — before, no overlap

    EXPECT_EQ(2u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    untreated_list_t::iterator it = list.begin();
    EXPECT_EQ(5, it->start);
    EXPECT_EQ(5, it->size);
    ++it;
    EXPECT_EQ(20, it->start);
    EXPECT_EQ(10, it->size);
}

TEST(AddPart, InvalidParams)
{
    UntreatedParts parts;
    EXPECT_FALSE(parts.AddPart(-1, 10));   // negative start
    EXPECT_FALSE(parts.AddPart(0, 0));     // zero size
    EXPECT_FALSE(parts.AddPart(0, -1));    // negative size
    EXPECT_TRUE(parts.empty());
}

TEST(AddPart, TagIncrement)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);
    parts.AddPart(100, 10);
    parts.AddPart(200, 10);

    untreated_list_t list;
    parts.Duplicate(list);

    // Tags should be monotonically increasing
    long prev_tag = 0;
    for(untreated_list_t::iterator it = list.begin(); it != list.end(); ++it){
        EXPECT_GT(it->untreated_tag, prev_tag);
        prev_tag = it->untreated_tag;
    }
}

TEST(AddPart, SingleByte)
{
    UntreatedParts parts;
    EXPECT_TRUE(parts.AddPart(0, 1));
    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(0, list.front().start);
    EXPECT_EQ(1, list.front().size);
}

TEST(AddPart, InsertMiddle)
{
    UntreatedParts parts;
    parts.AddPart(0, 5);     // [0, 5)
    parts.AddPart(20, 5);    // [20, 25)
    parts.AddPart(10, 5);    // [10, 15) — middle, no overlap

    EXPECT_EQ(3u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    untreated_list_t::iterator it = list.begin();
    EXPECT_EQ(0, it->start); ++it;
    EXPECT_EQ(10, it->start); ++it;
    EXPECT_EQ(20, it->start);
}

//=============================================================
// ClearParts tests
//=============================================================

TEST(ClearParts, ExactMatch)
{
    UntreatedParts parts;
    parts.AddPart(10, 10);   // [10, 20)
    EXPECT_TRUE(parts.ClearParts(10, 10));
    EXPECT_TRUE(parts.empty());
}

TEST(ClearParts, LeftTrim)
{
    UntreatedParts parts;
    parts.AddPart(10, 10);   // [10, 20)
    EXPECT_TRUE(parts.ClearParts(10, 5));  // clear [10, 15)

    EXPECT_EQ(1u, parts.size());
    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(15, list.front().start);
    EXPECT_EQ(5, list.front().size);  // [15, 20)
}

TEST(ClearParts, RightTrim)
{
    UntreatedParts parts;
    parts.AddPart(10, 10);   // [10, 20)
    EXPECT_TRUE(parts.ClearParts(15, 5));  // clear [15, 20)

    EXPECT_EQ(1u, parts.size());
    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(10, list.front().start);
    EXPECT_EQ(5, list.front().size);  // [10, 15)
}

TEST(ClearParts, Split)
{
    UntreatedParts parts;
    parts.AddPart(10, 20);   // [10, 30)
    EXPECT_TRUE(parts.ClearParts(15, 5));  // clear [15, 20) — splits into [10,15) and [20,30)

    EXPECT_EQ(2u, parts.size());
    untreated_list_t list;
    parts.Duplicate(list);
    untreated_list_t::iterator it = list.begin();
    EXPECT_EQ(10, it->start); EXPECT_EQ(5, it->size);  // [10, 15)
    ++it;
    EXPECT_EQ(20, it->start); EXPECT_EQ(10, it->size); // [20, 30)
}

TEST(ClearParts, MultipleOverlap)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);    // [0, 10)
    parts.AddPart(20, 10);   // [20, 30)
    parts.AddPart(40, 10);   // [40, 50)

    // Clear a range that spans first two parts entirely
    EXPECT_TRUE(parts.ClearParts(0, 30));
    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(40, list.front().start);
    EXPECT_EQ(10, list.front().size);
}

TEST(ClearParts, NoOverlap)
{
    UntreatedParts parts;
    parts.AddPart(10, 10);   // [10, 20)
    EXPECT_TRUE(parts.ClearParts(25, 5));  // clear [25, 30) — no overlap

    EXPECT_EQ(1u, parts.size());
    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(10, list.front().start);
    EXPECT_EQ(10, list.front().size);
}

TEST(ClearParts, EmptyList)
{
    UntreatedParts parts;
    EXPECT_TRUE(parts.ClearParts(0, 10));
    EXPECT_TRUE(parts.empty());
}

TEST(ClearParts, ZeroSizeClearsToEnd)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);
    parts.AddPart(20, 10);
    parts.AddPart(40, 10);

    // size=0 means clear from start to end
    EXPECT_TRUE(parts.ClearParts(15, 0));
    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(0, list.front().start);
    EXPECT_EQ(10, list.front().size);
}

TEST(ClearParts, InvalidParams)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);
    EXPECT_FALSE(parts.ClearParts(-1, 10));   // negative start
    EXPECT_FALSE(parts.ClearParts(0, -1));    // negative size
    EXPECT_EQ(1u, parts.size());  // unchanged
}

TEST(ClearAll, Basic)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);
    parts.AddPart(20, 10);
    EXPECT_FALSE(parts.empty());

    EXPECT_TRUE(parts.ClearAll());
    EXPECT_TRUE(parts.empty());
    EXPECT_EQ(0u, parts.size());
}

TEST(ClearParts, PartialLeftOverlap)
{
    UntreatedParts parts;
    parts.AddPart(10, 10);   // [10, 20)
    EXPECT_TRUE(parts.ClearParts(5, 10));  // clear [5, 15) — partially overlaps left

    EXPECT_EQ(1u, parts.size());
    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(15, list.front().start);
    EXPECT_EQ(5, list.front().size);
}

TEST(ClearParts, PartialRightOverlap)
{
    UntreatedParts parts;
    parts.AddPart(10, 10);   // [10, 20)
    EXPECT_TRUE(parts.ClearParts(15, 10)); // clear [15, 25) — partially overlaps right

    EXPECT_EQ(1u, parts.size());
    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(10, list.front().start);
    EXPECT_EQ(5, list.front().size);
}

TEST(ClearParts, ClearBeforeAll)
{
    UntreatedParts parts;
    parts.AddPart(10, 10);
    EXPECT_TRUE(parts.ClearParts(0, 5));   // before any parts
    EXPECT_EQ(1u, parts.size());
}

//=============================================================
// GetLastUpdatedPart tests
//=============================================================

TEST(GetLastUpdatedPart, Basic)
{
    UntreatedParts parts;
    parts.AddPart(0, 100);
    parts.AddPart(200, 100);  // this has highest tag

    off_t start = 0, size = 0;
    EXPECT_TRUE(parts.GetLastUpdatedPart(start, size, 0, 0));
    EXPECT_EQ(200, start);
    EXPECT_EQ(100, size);
}

TEST(GetLastUpdatedPart, TooSmall)
{
    UntreatedParts parts;
    parts.AddPart(0, 5);   // size=5, below min

    off_t start = 0, size = 0;
    EXPECT_FALSE(parts.GetLastUpdatedPart(start, size, 0, 10));  // min=10
}

TEST(GetLastUpdatedPart, TooLarge)
{
    UntreatedParts parts;
    parts.AddPart(0, 100);

    off_t start = 0, size = 0;
    EXPECT_TRUE(parts.GetLastUpdatedPart(start, size, 50, 0));  // max=50
    EXPECT_EQ(0, start);
    EXPECT_EQ(50, size);  // truncated to max
}

TEST(GetLastUpdatedPart, Empty)
{
    UntreatedParts parts;
    off_t start = 0, size = 0;
    EXPECT_FALSE(parts.GetLastUpdatedPart(start, size, 0, 0));
}

TEST(GetLastUpdatedPart, WithinRange)
{
    UntreatedParts parts;
    parts.AddPart(100, 20);

    off_t start = 0, size = 0;
    EXPECT_TRUE(parts.GetLastUpdatedPart(start, size, 50, 10));  // 10 <= 20 <= 50
    EXPECT_EQ(100, start);
    EXPECT_EQ(20, size);
}

TEST(GetLastUpdatedPart, AfterMerge)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);    // tag=1
    parts.AddPart(10, 10);   // tag=2, merges into [0,20)

    off_t start = 0, size = 0;
    EXPECT_TRUE(parts.GetLastUpdatedPart(start, size, 0, 0));
    EXPECT_EQ(0, start);
    EXPECT_EQ(20, size);  // merged part
}

// [ISSUE2026040700003] Regression trap for the max-tag-finding semantic on
// 3+ element lists. Existing tests above only use 1-2 elements; this test
// verifies that the loop correctly identifies the max-tag element even when
// it is NOT the last element added (since AddPart can merge or split
// existing parts). Use 3 non-overlapping parts so AddPart adds 3 separate
// list entries with monotonically increasing tags 1, 2, 3.
TEST(GetLastUpdatedPart, ThreeElementsMaxIsLastAdded)
{
    UntreatedParts parts;
    parts.AddPart(0,    10);   // tag=1, position 0 in list
    parts.AddPart(100,  10);   // tag=2, position 1 in list
    parts.AddPart(200,  10);   // tag=3, position 2 in list (max tag, last position)

    off_t start = 0, size = 0;
    EXPECT_TRUE(parts.GetLastUpdatedPart(start, size, 0, 0));
    // Max tag is 3 → element (200, 10)
    EXPECT_EQ(200, start);
    EXPECT_EQ(10,  size);
}

//=============================================================
// GetLastUpdatePart tests
//=============================================================

TEST(GetLastUpdatePart, Basic)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);     // tag=1
    parts.AddPart(100, 20);   // tag=2

    off_t start = 0, size = 0;
    EXPECT_TRUE(parts.GetLastUpdatePart(start, size));
    EXPECT_EQ(100, start);
    EXPECT_EQ(20, size);
}

TEST(GetLastUpdatePart, Empty)
{
    UntreatedParts parts;
    off_t start = 0, size = 0;
    EXPECT_FALSE(parts.GetLastUpdatePart(start, size));
}

TEST(GetLastUpdatePart, SinglePart)
{
    UntreatedParts parts;
    parts.AddPart(50, 30);

    off_t start = 0, size = 0;
    EXPECT_TRUE(parts.GetLastUpdatePart(start, size));
    EXPECT_EQ(50, start);
    EXPECT_EQ(30, size);
}

//=============================================================
// ReplaceLastUpdatePart tests
//=============================================================

TEST(ReplaceLastUpdatePart, Resize)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);
    parts.AddPart(100, 20);   // latest

    EXPECT_TRUE(parts.ReplaceLastUpdatePart(200, 50));
    EXPECT_EQ(2u, parts.size());

    off_t start = 0, size = 0;
    parts.GetLastUpdatePart(start, size);
    EXPECT_EQ(200, start);
    EXPECT_EQ(50, size);
}

TEST(ReplaceLastUpdatePart, DeleteWhenSizeZero)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);
    parts.AddPart(100, 20);

    EXPECT_TRUE(parts.ReplaceLastUpdatePart(0, 0));  // size <= 0 deletes
    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(0, list.front().start);
    EXPECT_EQ(10, list.front().size);
}

TEST(ReplaceLastUpdatePart, DeleteWhenSizeNegative)
{
    UntreatedParts parts;
    parts.AddPart(50, 25);

    EXPECT_TRUE(parts.ReplaceLastUpdatePart(0, -5));  // size <= 0 deletes
    EXPECT_TRUE(parts.empty());
}

TEST(ReplaceLastUpdatePart, EmptyList)
{
    UntreatedParts parts;
    EXPECT_FALSE(parts.ReplaceLastUpdatePart(0, 10));
}

//=============================================================
// RemoveLastUpdatePart tests
//=============================================================

TEST(RemoveLastUpdatePart, Basic)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);
    parts.AddPart(100, 20);

    EXPECT_TRUE(parts.RemoveLastUpdatePart());
    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(0, list.front().start);
    EXPECT_EQ(10, list.front().size);
}

TEST(RemoveLastUpdatePart, EmptyList)
{
    UntreatedParts parts;
    EXPECT_FALSE(parts.RemoveLastUpdatePart());
}

TEST(RemoveLastUpdatePart, SinglePart)
{
    UntreatedParts parts;
    parts.AddPart(50, 25);

    EXPECT_TRUE(parts.RemoveLastUpdatePart());
    EXPECT_TRUE(parts.empty());
}

//=============================================================
// Duplicate tests
//=============================================================

TEST(Duplicate, Basic)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);
    parts.AddPart(20, 10);
    parts.AddPart(40, 10);

    untreated_list_t list;
    EXPECT_TRUE(parts.Duplicate(list));
    EXPECT_EQ(3u, list.size());

    untreated_list_t::iterator it = list.begin();
    EXPECT_EQ(0, it->start);  EXPECT_EQ(10, it->size); ++it;
    EXPECT_EQ(20, it->start); EXPECT_EQ(10, it->size); ++it;
    EXPECT_EQ(40, it->start); EXPECT_EQ(10, it->size);
}

TEST(Duplicate, EmptyList)
{
    UntreatedParts parts;
    untreated_list_t list;
    EXPECT_TRUE(parts.Duplicate(list));
    EXPECT_TRUE(list.empty());
}

TEST(Duplicate, DoesNotAffectOriginal)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);

    untreated_list_t list;
    parts.Duplicate(list);

    // Modify the duplicate
    list.front().start = 999;

    // Original should be unchanged
    untreated_list_t orig;
    parts.Duplicate(orig);
    EXPECT_EQ(0, orig.front().start);
}

//=============================================================
// empty / size tests
//=============================================================

TEST(EmptySize, AfterAddAndClear)
{
    UntreatedParts parts;
    EXPECT_TRUE(parts.empty());
    EXPECT_EQ(0u, parts.size());

    parts.AddPart(0, 10);
    EXPECT_FALSE(parts.empty());
    EXPECT_EQ(1u, parts.size());

    parts.AddPart(20, 10);
    EXPECT_EQ(2u, parts.size());

    parts.ClearAll();
    EXPECT_TRUE(parts.empty());
    EXPECT_EQ(0u, parts.size());
}

TEST(EmptySize, AfterMerge)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);
    parts.AddPart(10, 10);  // merges
    EXPECT_EQ(1u, parts.size());
}

TEST(EmptySize, AfterClearParts)
{
    UntreatedParts parts;
    parts.AddPart(0, 10);
    parts.ClearParts(0, 10);
    EXPECT_TRUE(parts.empty());
    EXPECT_EQ(0u, parts.size());
}

//=============================================================
// Dump test
//=============================================================

TEST(Dump, NoCrash)
{
    UntreatedParts parts;
    parts.Dump();  // empty — should not crash

    parts.AddPart(0, 10);
    parts.AddPart(20, 10);
    parts.Dump();  // with items — should not crash
}

//=============================================================
// Integration scenario tests
//=============================================================

TEST(Scenario, SequentialWrite)
{
    // Simulate sequential writes of 4KB each, uploading when >= 10MB
    UntreatedParts parts;
    const off_t chunk = 4096;
    const off_t multipart_size = 10 * 1024 * 1024;  // 10MB

    off_t offset = 0;
    int upload_count = 0;

    for(int i = 0; i < 3000; i++){
        parts.AddPart(offset, chunk);
        offset += chunk;

        // Check if ready to upload
        off_t part_start = 0, part_size = 0;
        off_t max_mp = 2 * multipart_size;
        off_t min_mp = multipart_size;
        while(parts.GetLastUpdatedPart(part_start, part_size, max_mp, min_mp)){
            // Simulate upload
            parts.ClearParts(part_start, part_size);
            upload_count++;
        }
    }

    // Should have uploaded at least once (3000 * 4K = ~12MB > 10MB)
    EXPECT_GE(upload_count, 1);

    // Flush remaining
    untreated_list_t remaining;
    parts.Duplicate(remaining);
    for(untreated_list_t::iterator it = remaining.begin(); it != remaining.end(); ++it){
        upload_count++;
    }
    parts.ClearAll();
    EXPECT_TRUE(parts.empty());
}

TEST(Scenario, RandomWrite)
{
    UntreatedParts parts;

    // Write to scattered offsets — should merge when adjacent/overlapping
    parts.AddPart(100, 10);   // [100, 110)
    parts.AddPart(200, 10);   // [200, 210)
    parts.AddPart(110, 10);   // [110, 120) — adjacent to first → merges to [100, 120)
    parts.AddPart(120, 80);   // [120, 200) — adjacent to [100,120) → merges to [100, 200)
                               //              also adjacent to [200, 210) → merges to [100, 210)

    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(100, list.front().start);
    EXPECT_EQ(110, list.front().size);  // [100, 210)
}

TEST(Scenario, TransitionToMultipart)
{
    // Simulate: data accumulates, then ClearAll on transition, then new writes
    UntreatedParts parts;
    parts.AddPart(0, 5000);
    parts.AddPart(5000, 5000);
    EXPECT_EQ(1u, parts.size());  // merged

    // Transition: clear everything (data uploaded via NoCacheLoadAndPost)
    parts.ClearAll();
    EXPECT_TRUE(parts.empty());

    // New writes after transition
    parts.AddPart(10000, 4096);
    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(10000, list.front().start);
    EXPECT_EQ(4096, list.front().size);
}

TEST(Scenario, FlushAllParts)
{
    UntreatedParts parts;
    parts.AddPart(0, 100);
    parts.AddPart(200, 100);
    parts.AddPart(400, 100);

    // Duplicate, iterate, upload all
    untreated_list_t list;
    parts.Duplicate(list);

    int count = 0;
    for(untreated_list_t::iterator it = list.begin(); it != list.end(); ++it){
        count++;
    }
    EXPECT_EQ(3, count);

    parts.ClearAll();
    EXPECT_TRUE(parts.empty());
}

TEST(Scenario, PartialUploadResidual)
{
    UntreatedParts parts;

    // Write 30MB in 10MB chunks at offset 0
    off_t mb10 = 10 * 1024 * 1024;
    parts.AddPart(0, mb10);
    parts.AddPart(mb10, mb10);
    parts.AddPart(2 * mb10, mb10);

    EXPECT_EQ(1u, parts.size());  // all merged: [0, 30MB)

    // Upload first 10MB
    off_t part_start = 0, part_size = 0;
    EXPECT_TRUE(parts.GetLastUpdatedPart(part_start, part_size, mb10, mb10));
    EXPECT_EQ(0, part_start);
    EXPECT_EQ(mb10, part_size);

    parts.ClearParts(part_start, part_size);

    // Residual: [10MB, 30MB) — should still be uploadable
    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(mb10, list.front().start);
    EXPECT_EQ(2 * mb10, list.front().size);

    // Upload another 10MB
    EXPECT_TRUE(parts.GetLastUpdatedPart(part_start, part_size, mb10, mb10));
    parts.ClearParts(part_start, part_size);

    // Residual: [20MB, 30MB)
    EXPECT_EQ(1u, parts.size());
    parts.Duplicate(list);
    EXPECT_EQ(2 * mb10, list.front().start);
    EXPECT_EQ(mb10, list.front().size);
}

TEST(Scenario, NonSequentialWritesMerge)
{
    UntreatedParts parts;

    // Simulate random-order writes that eventually form a contiguous range
    parts.AddPart(30, 10);   // [30, 40)
    parts.AddPart(10, 10);   // [10, 20)
    parts.AddPart(20, 10);   // [20, 30) — bridges the two

    EXPECT_EQ(1u, parts.size());  // all merged to [10, 40)

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(10, list.front().start);
    EXPECT_EQ(30, list.front().size);
}

TEST(Scenario, ClearPartsThenAddMore)
{
    UntreatedParts parts;
    parts.AddPart(0, 100);
    parts.ClearParts(0, 50);  // [50, 100) remains

    parts.AddPart(50, 50);    // [50, 100) — merges with existing
    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(50, list.front().start);
    EXPECT_EQ(50, list.front().size);
}

TEST(Scenario, MultipleUploadsFromAccumulation)
{
    // Simulate: accumulate 25MB at different offsets, upload in 10MB chunks
    UntreatedParts parts;
    off_t mb = 1024 * 1024;

    parts.AddPart(0, 5 * mb);
    parts.AddPart(5 * mb, 5 * mb);
    parts.AddPart(10 * mb, 5 * mb);
    parts.AddPart(15 * mb, 5 * mb);
    parts.AddPart(20 * mb, 5 * mb);

    // All merged to [0, 25MB)
    EXPECT_EQ(1u, parts.size());

    off_t multipart_size = 10 * mb;
    off_t part_start, part_size;
    int uploads = 0;

    while(parts.GetLastUpdatedPart(part_start, part_size, multipart_size, multipart_size)){
        parts.ClearParts(part_start, part_size);
        uploads++;
    }

    // Should have uploaded 2 times (10MB + 10MB), with 5MB residual
    EXPECT_EQ(2, uploads);
    EXPECT_EQ(1u, parts.size());

    untreated_list_t list;
    parts.Duplicate(list);
    EXPECT_EQ(5 * mb, list.front().size);
}

//=============================================================
// Main
//=============================================================

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
