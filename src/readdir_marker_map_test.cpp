#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <stdint.h>
#include <string>
#include <map>
#include <thread>

#include "gtest.h"

#include "common.h"
#include "string_util.h"
#include "obsfs_log.h"
#include "readdir_marker_map.h"

// stub variables:
s3fs_log_level debug_level = S3FS_LOG_WARN;
s3fs_log_mode debug_log_mode = LOG_MODE_SYSLOG;
const char* s3fs_log_nest[S3FS_LOG_NEST_MAX] = {"", "  ", "    ", "      "};

template<typename T> std::string toString(const T& t){
    std::ostringstream oss;
    oss<<t;
    return oss.str();
}

class spin_mutex {
private:
    std::atomic<bool> flag = ATOMIC_VAR_INIT(false);
public:
    spin_mutex() = default;
    spin_mutex(const spin_mutex&) = delete;
    spin_mutex& operator= (const spin_mutex&) = delete;
    void lock() {
        bool expected = false;
        while(!flag.compare_exchange_strong(expected, true)) {
            expected = false;
        }
    }
    void unlock() { flag.store(false); }
};

class random_sleep {
private:
    const static unsigned int sleep_msec_min  = 1;
    const static unsigned int sleep_msec_max  = 11;
    static spin_mutex spin_lock;
public:
    static unsigned int seed;
    random_sleep() = default;
    random_sleep(const random_sleep&) = delete;
    static void sleep() {
        unsigned int rand = 0;
        assert(sleep_msec_min <= sleep_msec_max);
        if (sleep_msec_min == sleep_msec_max) {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_msec_min));
            return;
        }
        spin_lock.lock();
        rand = rand_r(&seed);
        spin_lock.unlock();
        rand = (rand % (sleep_msec_max - sleep_msec_min)) + sleep_msec_min;
        std::this_thread::sleep_for(std::chrono::milliseconds(rand));
    }
};
unsigned int random_sleep::seed = static_cast<unsigned int>(time(NULL));
spin_mutex random_sleep::spin_lock;

#define random_marker "djkdli754fg9fkejule"
off_t create_marker_normal(std::string path_prefix, off_t marker_int) {
    std::string new_marker = toString(marker_int);

    // create marker
    std::string path = path_prefix + new_marker;
    std::string marker = random_marker;
    off_t pos = 0;
    int result = marker_search_or_insert(marker_int, path, pos, marker);
    EXPECT_EQ(result, 0);
    EXPECT_TRUE(g_readdir_marker_map.check_pos(pos));
    // search failed and marker is not changed.
    EXPECT_EQ(marker, random_marker);

    // update marker
    marker = new_marker;
    auto pos1 = marker_update(marker_int, path, pos , marker);
    EXPECT_EQ(pos, pos1);

    // search marker
    marker = random_marker;
    pos1 = pos;
    result = marker_search_or_insert(marker_int, path, pos1, marker);
    EXPECT_EQ(result, 0);
    EXPECT_EQ(pos, pos1);
    EXPECT_EQ(marker, new_marker);

    return pos;
}

bool create_marker(std::string path_prefix, size_type marker_int) {
    std::string new_marker = toString(marker_int);

    // create marker
    std::string path = path_prefix + new_marker;
    std::string marker = random_marker;
    off_t pos = 0;
    int result = marker_search_or_insert(marker_int, path, pos, marker);
    if (result != 0) {
        return false;
    }
    if (!g_readdir_marker_map.check_pos(pos)) {
        return false;
    }
    if (marker != random_marker) {
        return false;
    }

    // update marker
    marker = new_marker;
    auto pos1 = marker_update(marker_int, path, pos , marker);
    if (pos != pos1) {
        return false;
    }

    // search marker
    marker = random_marker;
    pos1 = pos;
    result = marker_search_or_insert(marker_int, path, pos1, marker);
    if (result != 0) {
        return false;
    }
    if (pos != pos1) {
        return false;
    }
    if (marker != new_marker) {
        return false;
    }

    return true;
}

namespace {
    // The fixture for testing class Foo.
    class ReaddirMarkerMapTest : public ::testing::Test {
     protected:
      // You can remove any or all of the following functions if its body
      // is empty.

      ReaddirMarkerMapTest() {
         // You can do set-up work for each test here.
      }

      ~ReaddirMarkerMapTest() override {
         // You can do clean-up work that doesn't throw exceptions here.
      }

      // If the constructor and destructor are not enough for setting up
      // and cleaning up each test, you can define the following methods:

      void SetUp() override {
         // Code here will be called immediately after the constructor (right
         // before each test).
      }

      void TearDown() override {
         // Code here will be called immediately after each test (right
         // before the destructor).
      }

      // Objects declared here can be used by all tests in the test case for Foo.
    };

    #define MARKER_UPSERT_NUM   1000
    static void *test_normal(void *arg) {
        int result = 0;
        off_t pos = 0;
        off_t pos1 = 0;
        const std::string path = "/home/test";
        std::string prev_marker = "";
        std::string next_marker = "";
        int next_marker_int = 0;
        bool first = true;
        long long ino = 123456;

        for (int i = 0; i < MARKER_UPSERT_NUM; i++) {
            next_marker = prev_marker;
            pos1 = pos;
            result = marker_search_or_insert(ino, path, pos1, next_marker);
            EXPECT_EQ(result, 0);
            EXPECT_TRUE(g_readdir_marker_map.check_pos(pos1));
            EXPECT_TRUE(first || (pos1 == pos));
            EXPECT_TRUE((first && next_marker.empty()) || ((!first) && (next_marker == prev_marker)));

            first = false;

            random_sleep::sleep();

            next_marker_int++;
            next_marker = toString(next_marker_int);
            pos = marker_update(ino, path, pos1, next_marker);
            EXPECT_EQ(pos, pos1);

            prev_marker = next_marker;

            random_sleep::sleep();
        }

        pos1 = marker_remove(ino, path, pos);

        return nullptr;
    }

    #define TEST_THREAD_NUM     100
    TEST_F(ReaddirMarkerMapTest, MultiThreadsNormal) {
        int tnum, num_threads = TEST_THREAD_NUM;
        pthread_t *thread_id;

        thread_id = (pthread_t *)calloc(num_threads, sizeof(*thread_id));
        for (tnum = 0; tnum < num_threads; tnum++) {
            pthread_create(&thread_id[tnum], NULL,
                           &test_normal, NULL);
        }

        for (tnum = 0; tnum < num_threads; tnum++) {
            pthread_join(thread_id[tnum], NULL);
        }

        free(thread_id);

        // check result
        EXPECT_EQ(g_readdir_marker_map.size(), 0);
    }

    TEST_F(ReaddirMarkerMapTest, Normal) {
        // init map
        g_readdir_marker_map.clear();
        // create marker
        std::string path_prefix = "/home/test";
        create_marker_normal(path_prefix, 1);
        // check result
        EXPECT_EQ(g_readdir_marker_map.size(), 1);
    }

    TEST_F(ReaddirMarkerMapTest, FailedDueToWrongPath) {
        int result = 0;
        off_t pos = 0;
        off_t pos1 = 0;
        off_t npos = readdir_marker_map::npos;

        // init map
        g_readdir_marker_map.clear();

        // create marker
        std::string path_prefix = "/home/test";
        pos = create_marker_normal(path_prefix, 1);
        // check result
        EXPECT_EQ(g_readdir_marker_map.size(), 1);
        std::string path1 = path_prefix + toString(1);

        // upsert failed due to wrong ino.
        std::string path2 = path_prefix + toString(2);
        std::string marker = "2";
        pos1 = marker_update(2, path1, pos, marker);
        EXPECT_EQ(pos1, npos);
        // upsert failed due to wrong path.
        marker = "2";
        pos1 = marker_update(1, path2, pos, marker);
        EXPECT_EQ(pos1, npos);
        // check map is not changed after failed upsert.
        pos1 = pos;
        result = marker_search_or_insert(1, path1, pos1, marker);
        EXPECT_EQ(result, 0);
        EXPECT_EQ(pos1, pos);
        EXPECT_EQ(marker, "1");

        // search failed due to wrong ino.
        marker = random_marker;
        pos1 = pos;
        result = marker_search_or_insert(2, path1, pos1, marker);
        EXPECT_EQ(result, -EINVAL);
        EXPECT_EQ(pos1, npos);
        EXPECT_EQ(marker, random_marker);
        // search failed due to wrong path.
        marker = random_marker;
        pos1 = pos;
        result = marker_search_or_insert(1, path2, pos1, marker);
        EXPECT_EQ(result, -EINVAL);
        EXPECT_EQ(pos1, npos);
        EXPECT_EQ(marker, random_marker);
        // check map is not changed after failed search.
        pos1 = pos;
        result = marker_search_or_insert(1, path1, pos1, marker);
        EXPECT_EQ(result, 0);
        EXPECT_EQ(pos1, pos);
        EXPECT_EQ(marker, "1");
    }

    TEST_F(ReaddirMarkerMapTest, FailedDueToOverflow) {
        g_readdir_marker_map.clear();

        size_type limited_size = g_readdir_marker_map.limited_size();
        std::string path_prefix = "/home/test";

        // create markers
        for (size_type i = 0; i < limited_size; i++) {
            create_marker_normal(path_prefix, i+1);
        }
        // check result
        EXPECT_EQ(g_readdir_marker_map.size(), limited_size);

        // create one marker failed after the map is overflow.
        bool success = create_marker(path_prefix, limited_size+1);
        EXPECT_FALSE(success);
    }

    TEST_F(ReaddirMarkerMapTest, Recycle) {
        g_readdir_marker_map.clear();

        size_type limited_size = g_readdir_marker_map.limited_size();
        std::string path_prefix = "/home/test";

        // create markers
        for (size_type i = 0; i < limited_size; i++) {
            create_marker_normal(path_prefix, i+1);
        }
        // check result
        EXPECT_EQ(g_readdir_marker_map.size(), limited_size);

        // sleep enough time to simulate all marker has been dead for a long time.
        sleep(MARKER_MAX_DEAD_TIME+1);

        // create one marker successfully after the map is overflow but recycled successfully.
        bool success = create_marker(path_prefix, limited_size+1);
        EXPECT_TRUE(success);
        EXPECT_TRUE(g_readdir_marker_map.check_size(1));
    }

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

//only for compile
void s3fsStatisLogModeNotObsfs()        
{
}

