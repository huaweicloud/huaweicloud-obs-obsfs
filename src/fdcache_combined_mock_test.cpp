// fdcache_combined_mock_test.cpp
// Tests requiring BOTH curl mock (network) AND fs mock (file system) interception.
// Specifically tests WriteMultipart ftruncate/ClearParts ordering (ISSUE2026031000007).
//
// Links with: curl_mock.cpp + fs_mock.cpp
// Wrap flags: curl_easy_perform, curl_easy_getinfo, curl_easy_setopt, sleep,
//             open, open64, unlink, link, ftruncate, fstat, pwrite, pread, stat, close

#include "gtest.h"
#include "curl_mock.h"
#include "fs_mock.h"
#include "common.h"
#include "fdcache.h"
#include "curl.h"
#include <string>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <cstring>
#include <vector>

using namespace std;

// External stubs (provided by test_stubs.o)
extern "C" {
void IndexLogEntry(const uint64_t, const char*, const uint32_t,
                   const char*, const char*, const uint32_t,
                   const char*, ...);
}
void s3fsStatisLogModeNotObsfs();

// External global variables from curl.cpp
extern string bucket;
extern string host;
extern string endpoint;
extern string service_path;
extern bool pathrequeststyle;
extern bucket_type_t g_bucket_type;

// ============================================================
// Test Fixture
// ============================================================

class FdEntityCombinedMockTest : public ::testing::Test {
protected:
    // Saved global state
    string saved_bucket;
    string saved_host;
    string saved_endpoint;
    string saved_service_path;
    bool saved_pathrequeststyle;
    bucket_type_t saved_bucket_type;

    // multipart_size for triggering WriteMultipart path
    off_t mp_size;

    void SetUp() override {
        CurlMockController::Instance().Reset();
        FsMockController::Instance().Reset();

        // Save global state
        saved_bucket = bucket;
        saved_host = host;
        saved_endpoint = endpoint;
        saved_service_path = service_path;
        saved_pathrequeststyle = pathrequeststyle;
        saved_bucket_type = g_bucket_type;

        // Set test state
        bucket = "test-bucket";
        host = "127.0.0.1";
        endpoint = "http://127.0.0.1:8080";
        service_path = "/";
        pathrequeststyle = false;
        g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
        FdManager::SetCacheDir("");

        // Use minimum multipart size (5MB) — SetMultipartSize takes MB, multiplies by 1024*1024
        S3fsCurl::SetMultipartSize(5);
        mp_size = S3fsCurl::GetMultipartSize();  // 5 * 1024 * 1024
        S3fsCurl::SetContentMd5(false);
    }

    void TearDown() override {
        CurlMockController::Instance().Reset();
        FsMockController::Instance().Reset();
        FsMockController::Instance().SetMockEnabled(false);

        // Restore global state
        bucket = saved_bucket;
        host = saved_host;
        endpoint = saved_endpoint;
        service_path = saved_service_path;
        pathrequeststyle = saved_pathrequeststyle;
        g_bucket_type = saved_bucket_type;
        S3fsCurl::SetMultipartSize(10);  // restore default 10MB
        S3fsCurl::SetContentMd5(true);
        FdManager::SetCacheDir("");
    }

    // Helper: set up NoCachePreMultipartPost to succeed (sets upload_id)
    void SetupMultipartInit(FdEntity& ent) {
        string xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                     "<InitiateMultipartUploadResult>"
                     "<UploadId>test-upload-id-007</UploadId>"
                     "</InitiateMultipartUploadResult>";
        CurlMockController::Instance().SetResponseCode(200);
        CurlMockController::Instance().SetResponseBody(xml);
        CurlMockController::Instance().QueuePerformResult(CURLE_OK);

        int result = ent.NoCachePreMultipartPost();
        ASSERT_EQ(result, 0) << "NoCachePreMultipartPost failed";
    }
};

// Global environment for S3fsCurl initialization
class CombinedMockEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        ASSERT_TRUE(S3fsCurl::InitS3fsCurl(NULL));
    }
    void TearDown() override {
        S3fsCurl::DestroyS3fsCurl();
    }
};

::testing::Environment* const combined_mock_env =
    ::testing::AddGlobalTestEnvironment(new CombinedMockEnvironment);

// ============================================================
// ISSUE2026031000007: WriteMultipart ftruncate/ClearParts ordering
// ============================================================

// Purpose: Verify that when ftruncate fails after NoCacheMultipartPost,
//          untreated_list is NOT cleared (data tracking preserved for retry)
// Expected: Write returns -EIO, HasUntreatedParts() returns true
// Why: ISSUE2026031000007 - ftruncate must happen before ClearParts
TEST_F(FdEntityCombinedMockTest, WriteMultipart_FtruncateFail_UntreatedListPreserved) {
    // Step 1: Open FdEntity with size large enough for multipart write (no mock)
    FdEntity ent("/test/ftrunc_fail");
    int open_result = ent.Open(NULL, mp_size * 2, -1);
    ASSERT_EQ(open_result, 0) << "Failed to open FdEntity";

    // Step 2: Initialize multipart upload (sets upload_id via curl mock)
    SetupMultipartInit(ent);

    // Step 3: Prepare mocks for Write()
    // Write internally does: pwrite → AddPart → GetLastUpdatedPart → NoCacheMultipartPost (curl) → ftruncate
    CurlMockController::Instance().Reset();
    CurlMockController::Instance().SetResponseCode(200);
    CurlMockController::Instance().QueuePerformResult(CURLE_OK);
    CurlMockController::Instance().AddMockResponseHeader("ETag", "\"abc123\"");

    // Enable fs mock: pwrite succeeds with mp_size bytes, ftruncate fails
    FsMockController::Instance().SetMockEnabled(true);
    FsMockController::Instance().SetPwriteResult(mp_size, 0);      // pwrite returns mp_size
    FsMockController::Instance().SetFtruncateResult(-1, EIO);       // ftruncate(fd,0) fails

    // Step 4: Write multipart_size bytes to trigger multipart upload
    vector<char> data(mp_size, 'A');
    ssize_t write_result = ent.Write(data.data(), 0, mp_size);

    // Step 5: Verify
    EXPECT_EQ(write_result, -EIO) << "Write should return -EIO on ftruncate failure";

    // KEY ASSERTION: After fix, ftruncate fails BEFORE ClearParts
    // → untreated_list should still have the uploaded part tracked
    EXPECT_TRUE(ent.HasUntreatedParts())
        << "untreated_list should be preserved when ftruncate fails (ISSUE2026031000007)";

    FsMockController::Instance().SetMockEnabled(false);
    ent.Close();
}

// Purpose: Verify that when ftruncate succeeds, untreated_list IS cleared normally
// Expected: Write returns mp_size (success), HasUntreatedParts() returns false
// Why: Positive test - normal path still works after fix
TEST_F(FdEntityCombinedMockTest, WriteMultipart_FtruncateSuccess_UntreatedListCleared) {
    // Step 1: Open FdEntity
    FdEntity ent("/test/ftrunc_ok");
    int open_result = ent.Open(NULL, mp_size * 2, -1);
    ASSERT_EQ(open_result, 0) << "Failed to open FdEntity";

    // Step 2: Initialize multipart upload
    SetupMultipartInit(ent);

    // Step 3: Prepare mocks
    CurlMockController::Instance().Reset();
    CurlMockController::Instance().SetResponseCode(200);
    CurlMockController::Instance().QueuePerformResult(CURLE_OK);
    CurlMockController::Instance().AddMockResponseHeader("ETag", "\"abc123\"");

    // Enable fs mock: pwrite succeeds, both ftruncates succeed
    FsMockController::Instance().SetMockEnabled(true);
    FsMockController::Instance().SetPwriteResult(mp_size, 0);      // pwrite returns mp_size
    FsMockController::Instance().SetFtruncateResult(0, 0);          // ftruncate(fd,0) succeeds
    FsMockController::Instance().SetFtruncateResult(0, 0);          // ftruncate(fd,size) succeeds

    // Step 4: Write multipart_size bytes
    vector<char> data(mp_size, 'A');
    ssize_t write_result = ent.Write(data.data(), 0, mp_size);

    // Step 5: Verify
    EXPECT_EQ(write_result, mp_size) << "Write should return bytes written on success";
    EXPECT_FALSE(ent.HasUntreatedParts())
        << "untreated_list should be cleared after successful ftruncate + ClearParts";

    FsMockController::Instance().SetMockEnabled(false);
    ent.Close();
}

// Purpose: Verify that first ftruncate(fd,0) succeeds but second ftruncate(fd,size)
//          fails → untreated_list is still preserved
// Expected: Write returns -EIO, HasUntreatedParts() returns true
// Why: Both ftruncate calls must succeed before ClearParts runs
TEST_F(FdEntityCombinedMockTest, WriteMultipart_SecondFtruncateFail_UntreatedListPreserved) {
    // Step 1: Open FdEntity
    FdEntity ent("/test/ftrunc_second_fail");
    int open_result = ent.Open(NULL, mp_size * 2, -1);
    ASSERT_EQ(open_result, 0) << "Failed to open FdEntity";

    // Step 2: Initialize multipart upload
    SetupMultipartInit(ent);

    // Step 3: Prepare mocks
    CurlMockController::Instance().Reset();
    CurlMockController::Instance().SetResponseCode(200);
    CurlMockController::Instance().QueuePerformResult(CURLE_OK);
    CurlMockController::Instance().AddMockResponseHeader("ETag", "\"abc123\"");

    // Enable fs mock: pwrite succeeds, first ftruncate succeeds, second fails
    FsMockController::Instance().SetMockEnabled(true);
    FsMockController::Instance().SetPwriteResult(mp_size, 0);      // pwrite returns mp_size
    FsMockController::Instance().SetFtruncateResult(0, 0);          // ftruncate(fd,0) succeeds
    FsMockController::Instance().SetFtruncateResult(-1, EIO);       // ftruncate(fd,size) fails

    // Step 4: Write multipart_size bytes
    vector<char> data(mp_size, 'A');
    ssize_t write_result = ent.Write(data.data(), 0, mp_size);

    // Step 5: Verify
    EXPECT_EQ(write_result, -EIO) << "Write should return -EIO on second ftruncate failure";
    EXPECT_TRUE(ent.HasUntreatedParts())
        << "untreated_list should be preserved when second ftruncate fails";

    FsMockController::Instance().SetMockEnabled(false);
    ent.Close();
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
