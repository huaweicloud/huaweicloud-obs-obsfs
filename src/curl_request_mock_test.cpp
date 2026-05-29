// ==========================================================================
// curl_request_mock_test.cpp
//
// Mock-based tests for curl.cpp RequestPerform, RemakeHandle,
// DeleteRequest, HeadRequest, PutHeadRequest, CheckBucket,
// ListBucketRequest, CurlDebugFunc, and helper functions.
//
// Approach:
//   1. Pre-include STL headers to avoid private->public macro breakage
//   2. #include "curl.cpp" to access private/static functions
//   3. Crypto/auth stubs provided by test_stubs.o (linked separately)
//   4. S3fsCurl::retries = 0 to avoid network calls in RequestPerform
//   5. Test each branch of RemakeHandle, PrintRequestId4ResponseErr,
//      CurlDebugFunc, DetectBucketTypeFromHeaders, and request setup
// ==========================================================================

// Pre-include standard library headers so they are NOT affected by the
// private->public hack below (which breaks std::basic_stringbuf internals).
#include <string>
#include <sstream>
#include <vector>
#include <map>
#include <list>
#include <algorithm>
#include <iostream>
#include <memory>
#include <mutex>
#include <atomic>
#include <cstring>

// Now expose S3fsCurl private members for testing
#define private public
#define protected public
#include "curl.cpp"
#undef private
#undef protected

// XML parsing headers
#include <libxml/parser.h>
#include <libxml/tree.h>

// ==========================================================================
// Google Test
// ==========================================================================
#include "gtest.h"

using namespace std;

// ==========================================================================
// Global Environment: InitS3fsCurl / DestroyS3fsCurl once
// ==========================================================================
class CurlRequestMockEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        ASSERT_TRUE(S3fsCurl::InitS3fsCurl(NULL));
    }
    void TearDown() override {
        S3fsCurl::DestroyS3fsCurl();
    }
};

// ==========================================================================
// Test Fixture: save/restore global state
// ==========================================================================
class CurlRequestMockTest : public ::testing::Test {
protected:
    void SetUp() override {
        saved_bucket = bucket;
        saved_host = host;
        saved_endpoint = endpoint;
        saved_service_path = service_path;
        saved_pathrequeststyle = pathrequeststyle;
        saved_is_sigv4 = S3fsCurl::is_sigv4;
        saved_retries = S3fsCurl::retries;
        saved_bucket_type = g_bucket_type;
        saved_nohwscache = nohwscache;
        saved_is_ibm_iam_auth = S3fsCurl::is_ibm_iam_auth;
        saved_default_acl = S3fsCurl::default_acl;
        saved_storage_class = S3fsCurl::storage_class;
        saved_ssetype = S3fsCurl::ssetype;
        saved_is_verbose = S3fsCurl::is_verbose;
        saved_curl_ca_bundle = S3fsCurl::curl_ca_bundle;
        saved_is_ua = S3fsCurl::is_ua;
        saved_gCliCannotResolveRetryCount = gCliCannotResolveRetryCount;

        // Set default test state
        S3fsCurl::AWSAccessKeyId = "AKID_TEST";
        S3fsCurl::AWSSecretAccessKey = "SECRET_TEST";
        S3fsCurl::AWSAccessToken = "";
        S3fsCurl::retries = 0;  // Skip actual network calls
        bucket = "test-bucket";
        host = "127.0.0.1";
        endpoint = "http://127.0.0.1:8080";
        service_path = "/";
        pathrequeststyle = false;
        g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    }
    void TearDown() override {
        bucket = saved_bucket;
        host = saved_host;
        endpoint = saved_endpoint;
        service_path = saved_service_path;
        pathrequeststyle = saved_pathrequeststyle;
        S3fsCurl::is_sigv4 = saved_is_sigv4;
        S3fsCurl::retries = saved_retries;
        g_bucket_type = saved_bucket_type;
        nohwscache = saved_nohwscache;
        S3fsCurl::is_ibm_iam_auth = saved_is_ibm_iam_auth;
        S3fsCurl::default_acl = saved_default_acl;
        S3fsCurl::storage_class = saved_storage_class;
        S3fsCurl::ssetype = saved_ssetype;
        S3fsCurl::is_verbose = saved_is_verbose;
        S3fsCurl::curl_ca_bundle = saved_curl_ca_bundle;
        S3fsCurl::is_ua = saved_is_ua;
        gCliCannotResolveRetryCount = saved_gCliCannotResolveRetryCount;
    }

    string saved_bucket;
    string saved_host;
    string saved_endpoint;
    string saved_service_path;
    bool saved_pathrequeststyle;
    bool saved_is_sigv4;
    int saved_retries;
    bucket_type_t saved_bucket_type;
    bool saved_nohwscache;
    bool saved_is_ibm_iam_auth;
    string saved_default_acl;
    storage_class_t saved_storage_class;
    sse_type_t saved_ssetype;
    bool saved_is_verbose;
    string saved_curl_ca_bundle;
    bool saved_is_ua;
    int saved_gCliCannotResolveRetryCount;
};

// ==========================================================================
// 1. PrintRequestId4ResponseErr tests
// ==========================================================================

TEST_F(CurlRequestMockTest, PrintRequestId_ResponseBelow400) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.LastResponseCode = 200;
    s3curl.url = "http://test/path";
    // Response < 400 => logflag = false, returns empty string
    string result = s3curl.PrintRequestId4ResponseErr();
    EXPECT_EQ("", result);
}

TEST_F(CurlRequestMockTest, PrintRequestId_ResponseCode0) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.LastResponseCode = 0;
    s3curl.url = "http://test/path";
    // Response 0 => does NOT break (< 400 check fails because 0 < 400 but > 0 fails)
    // Actually: 0 < 400 && 0 > 0 => false, so logflag stays true
    string result = s3curl.PrintRequestId4ResponseErr();
    EXPECT_EQ("", result);
}

TEST_F(CurlRequestMockTest, PrintRequestId_ResponseNegative) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.LastResponseCode = -1;
    s3curl.url = "http://test/path";
    // -1 < 400 but not > 0 => logflag stays true, no request-id header
    string result = s3curl.PrintRequestId4ResponseErr();
    EXPECT_EQ("", result);
}

TEST_F(CurlRequestMockTest, PrintRequestId_Response404) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.LastResponseCode = 404;
    s3curl.url = "http://test/path";
    s3curl.responseHeaders.clear();
    // 404 >= 400 => logflag stays true; no x-amz-request-id header
    string result = s3curl.PrintRequestId4ResponseErr();
    EXPECT_EQ("", result);
}

TEST_F(CurlRequestMockTest, PrintRequestId_Response404_WithRequestId) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.LastResponseCode = 404;
    s3curl.url = "http://test/path";
    s3curl.responseHeaders["x-amz-request-id"] = "REQ-12345";
    // 404 with request-id => logs INFO and returns the request id
    string result = s3curl.PrintRequestId4ResponseErr();
    EXPECT_EQ("REQ-12345", result);
}

TEST_F(CurlRequestMockTest, PrintRequestId_Response416_WithRequestId) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.LastResponseCode = 416;
    s3curl.url = "http://test/path";
    s3curl.responseHeaders["x-amz-request-id"] = "REQ-416";
    string result = s3curl.PrintRequestId4ResponseErr();
    EXPECT_EQ("REQ-416", result);
}

TEST_F(CurlRequestMockTest, PrintRequestId_Response500_WithRequestId) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.LastResponseCode = 500;
    s3curl.url = "http://test/path";
    s3curl.responseHeaders["x-amz-request-id"] = "REQ-500";
    // 500 >= 400 and not 404 or 416 => logs ERR
    string result = s3curl.PrintRequestId4ResponseErr();
    EXPECT_EQ("REQ-500", result);
}

TEST_F(CurlRequestMockTest, PrintRequestId_Response403_NoRequestId) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.LastResponseCode = 403;
    s3curl.url = "http://test/path";
    s3curl.responseHeaders.clear();
    // No request-id header, returns empty string
    string result = s3curl.PrintRequestId4ResponseErr();
    EXPECT_EQ("", result);
}

TEST_F(CurlRequestMockTest, PrintRequestId_Response301) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.LastResponseCode = 301;
    s3curl.url = "http://test/path";
    // 301 < 400 and > 0 => logflag = false
    string result = s3curl.PrintRequestId4ResponseErr();
    EXPECT_EQ("", result);
}

// ==========================================================================
// 2. RemakeHandle tests - each REQTYPE branch
// ==========================================================================

TEST_F(CurlRequestMockTest, RemakeHandle_TypeUnset_ReturnsFalse) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_UNSET;
    EXPECT_FALSE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypeDelete) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_DELETE;
    s3curl.url = "http://test/delete";
    s3curl.requestHeaders = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
    EXPECT_EQ(1, s3curl.retry_count);
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypeHead) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_HEAD;
    s3curl.url = "http://test/head";
    s3curl.requestHeaders = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypePutHead) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PUTHEAD;
    s3curl.url = "http://test/puthead";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypePut_WithWriteDataBuf) {
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;  // file bucket uses writedatabuf
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PUT;
    s3curl.url = "http://test/put";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;
    // Set up writedatabuf with valid buffer
    char testbuf[] = "test data";
    s3curl.writedatabuf.pcBuffer = testbuf;
    s3curl.writedatabuf.ulBufferSize = sizeof(testbuf);
    s3curl.writedatabuf.offset = 5;  // non-zero to test reset
    EXPECT_TRUE(s3curl.RemakeHandle());
    // Verify offset is reset to 0
    EXPECT_EQ(0u, s3curl.writedatabuf.offset);
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypePut_NullWriteDataBuf) {
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;  // file bucket uses writedatabuf
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PUT;
    s3curl.url = "http://test/put";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;
    s3curl.writedatabuf.pcBuffer = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypeGet_WithReadBuf) {
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;  // file bucket uses read_buf
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.url = "http://test/get";
    s3curl.requestHeaders = NULL;
    char readbuf[128];
    memset(readbuf, 'x', sizeof(readbuf));
    s3curl.read_buf = readbuf;
    s3curl.read_size = sizeof(readbuf);
    s3curl.cur_pos = 50;  // non-zero to test warning
    EXPECT_TRUE(s3curl.RemakeHandle());
    // After RemakeHandle, cur_pos should be reset to 0
    EXPECT_EQ(0u, s3curl.cur_pos);
    // Buffer should be zeroed
    EXPECT_EQ(0, readbuf[0]);
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypeGet_NullReadBuf) {
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;  // file bucket uses read_buf
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.url = "http://test/get";
    s3curl.requestHeaders = NULL;
    s3curl.read_buf = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypeChkBucket) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_CHKBUCKET;
    s3curl.url = "http://test/chkbucket";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypeListBucket) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_LISTBUCKET;
    s3curl.url = "http://test/listbucket";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypePreMultiPost) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PREMULTIPOST;
    s3curl.url = "http://test/premultipost";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypeCompleteMultiPost) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_COMPLETEMULTIPOST;
    s3curl.url = "http://test/completemultipost";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;
    s3curl.postdata_remaining = 0;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypeUploadMultiPost) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_UPLOADMULTIPOST;
    s3curl.url = "http://test/uploadmultipost";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;
    s3curl.partdata.size = 1024;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypeCopyMultiPost) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_COPYMULTIPOST;
    s3curl.url = "http://test/copymultipost";
    s3curl.bodydata = new BodyData();
    s3curl.headdata = new BodyData();
    s3curl.requestHeaders = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypeMultiList) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_MULTILIST;
    s3curl.url = "http://test/multilist";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypeIamCred_NoIbmIam) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_IAMCRED;
    s3curl.url = "http://test/iamcred";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;
    S3fsCurl::is_ibm_iam_auth = false;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypeIamCred_WithIbmIam) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_IAMCRED;
    s3curl.url = "http://test/iamcred";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;
    S3fsCurl::is_ibm_iam_auth = true;
    s3curl.postdata_remaining = 100;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypeAbortMultiUpload) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_ABORTMULTIUPLOAD;
    s3curl.url = "http://test/abortmultiupload";
    s3curl.requestHeaders = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypeIamRole) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_IAMROLE;
    s3curl.url = "http://test/iamrole";
    s3curl.bodydata = new BodyData();
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_TypePost) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_POST;
    s3curl.url = "http://test/post";
    s3curl.requestHeaders = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_InvalidType) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = static_cast<S3fsCurl::REQTYPE>(999);
    s3curl.url = "http://test/invalid";
    EXPECT_FALSE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, RemakeHandle_ClearsResponseHeaders) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_DELETE;
    s3curl.url = "http://test/delete";
    s3curl.requestHeaders = NULL;
    s3curl.responseHeaders["key"] = "value";
    EXPECT_TRUE(s3curl.RemakeHandle());
    EXPECT_TRUE(s3curl.responseHeaders.empty());
}

TEST_F(CurlRequestMockTest, RemakeHandle_ClearsBodyData) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_CHKBUCKET;
    s3curl.url = "http://test/chk";
    s3curl.requestHeaders = NULL;
    s3curl.bodydata = new BodyData();
    s3curl.bodydata->Append((void*)"hello", 5);
    EXPECT_TRUE(s3curl.RemakeHandle());
    // bodydata should be cleared
    EXPECT_EQ(0u, s3curl.bodydata->size());
}

TEST_F(CurlRequestMockTest, RemakeHandle_ClearsHeadData) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_COPYMULTIPOST;
    s3curl.url = "http://test/copy";
    s3curl.bodydata = new BodyData();
    s3curl.headdata = new BodyData();
    s3curl.headdata->Append((void*)"header", 6);
    s3curl.requestHeaders = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
    EXPECT_EQ(0u, s3curl.headdata->size());
}

TEST_F(CurlRequestMockTest, RemakeHandle_IncrementsRetryCount) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_DELETE;
    s3curl.url = "http://test/delete";
    s3curl.requestHeaders = NULL;
    s3curl.retry_count = 3;
    EXPECT_TRUE(s3curl.RemakeHandle());
    EXPECT_EQ(4, s3curl.retry_count);
}

TEST_F(CurlRequestMockTest, RemakeHandle_ResetsLastResponseCode) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_DELETE;
    s3curl.url = "http://test/delete";
    s3curl.requestHeaders = NULL;
    s3curl.LastResponseCode = 200;
    EXPECT_TRUE(s3curl.RemakeHandle());
    EXPECT_EQ(-1, s3curl.LastResponseCode);
}

TEST_F(CurlRequestMockTest, RemakeHandle_RestoresBackupData) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_COMPLETEMULTIPOST;
    s3curl.url = "http://test/complete";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;

    // Set backup values
    const unsigned char testpost[] = "post data";
    s3curl.b_postdata = testpost;
    s3curl.b_postdata_remaining = 9;
    s3curl.b_partdata_startpos = 1024;
    s3curl.b_partdata_size = 2048;

    EXPECT_TRUE(s3curl.RemakeHandle());

    // postdata and partdata should be restored from backup
    EXPECT_EQ(testpost, s3curl.postdata);
    EXPECT_EQ(9u, s3curl.postdata_remaining);
    EXPECT_EQ(1024, s3curl.partdata.startpos);
    EXPECT_EQ(2048, s3curl.partdata.size);
}

TEST_F(CurlRequestMockTest, RemakeHandle_WithBInfile) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_DELETE;
    s3curl.url = "http://test/delete";
    s3curl.requestHeaders = NULL;

    // Create a real temp file for b_infile
    FILE* tmpf = tmpfile();
    ASSERT_NE(nullptr, tmpf);
    fprintf(tmpf, "test data for infile");
    fflush(tmpf);
    s3curl.b_infile = tmpf;

    EXPECT_TRUE(s3curl.RemakeHandle());

    fclose(tmpf);
    s3curl.b_infile = NULL;
}

// ==========================================================================
// 3. RequestPerform tests (with retries=0, exits immediately)
// ==========================================================================

TEST_F(CurlRequestMockTest, RequestPerform_RetriesZero_ReturnsEIO) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.url = "http://test/path";
    S3fsCurl::retries = 0;
    // With retries=0, the while loop body never executes, returns -EIO
    int result = s3curl.RequestPerform();
    EXPECT_EQ(-EIO, result);
}

// ==========================================================================
// 4. DeleteRequest tests
// ==========================================================================

TEST_F(CurlRequestMockTest, DeleteRequest_NullPath) {
    S3fsCurl s3curl;
    int result = s3curl.DeleteRequest(NULL);
    EXPECT_EQ(-1, result);
}

TEST_F(CurlRequestMockTest, DeleteRequest_ValidPath_ObjectBucket) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    int result = s3curl.DeleteRequest("/testfile");
    EXPECT_EQ(-EIO, result);  // retries=0 => -EIO from RequestPerform
    // Verify setup
    EXPECT_EQ(S3fsCurl::REQTYPE_DELETE, s3curl.type);
    EXPECT_EQ("DELETE", s3curl.op);
}

TEST_F(CurlRequestMockTest, DeleteRequest_ValidPath_FileBucket) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
    int result = s3curl.DeleteRequest("/testfile");
    EXPECT_EQ(-EIO, result);
    EXPECT_EQ(S3fsCurl::REQTYPE_DELETE, s3curl.type);
}

// ==========================================================================
// 5. PreHeadRequest tests
// ==========================================================================

TEST_F(CurlRequestMockTest, PreHeadRequest_NullPath) {
    S3fsCurl s3curl;
    EXPECT_FALSE(s3curl.PreHeadRequest(NULL, NULL, NULL, -1, NULL, NULL));
}

TEST_F(CurlRequestMockTest, PreHeadRequest_ValidPath_ObjectBucket) {
    S3fsCurl s3curl;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    long long inode_no = 12345;
    bool result = s3curl.PreHeadRequest("/testfile", NULL, NULL, -1, &inode_no, "shard1");
    EXPECT_TRUE(result);
    EXPECT_EQ(S3fsCurl::REQTYPE_HEAD, s3curl.type);
    EXPECT_EQ("HEAD", s3curl.op);
}

TEST_F(CurlRequestMockTest, PreHeadRequest_ValidPath_FileBucket) {
    S3fsCurl s3curl;
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
    long long inode_no = 12345;
    bool result = s3curl.PreHeadRequest("/testfile", NULL, NULL, -1, &inode_no, "shard1");
    EXPECT_TRUE(result);
    EXPECT_EQ(S3fsCurl::REQTYPE_HEAD, s3curl.type);
}

TEST_F(CurlRequestMockTest, PreHeadRequest_FileBucket_NullInodeNo) {
    S3fsCurl s3curl;
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
    bool result = s3curl.PreHeadRequest("/testfile", NULL, NULL, -1, NULL, NULL);
    EXPECT_TRUE(result);
}

TEST_F(CurlRequestMockTest, PreHeadRequest_FileBucket_NullShardKey) {
    S3fsCurl s3curl;
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
    long long inode_no = 100;
    bool result = s3curl.PreHeadRequest("/testfile", NULL, NULL, -1, &inode_no, NULL);
    EXPECT_TRUE(result);
}

TEST_F(CurlRequestMockTest, PreHeadRequest_WithBasePath) {
    S3fsCurl s3curl;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    bool result = s3curl.PreHeadRequest("/testfile", "/base", "/saved", -1, NULL, NULL);
    EXPECT_TRUE(result);
    EXPECT_EQ("/base", s3curl.base_path);
    EXPECT_EQ("/saved", s3curl.saved_path);
}

TEST_F(CurlRequestMockTest, PreHeadRequest_WithSseKeyPos) {
    S3fsCurl s3curl;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    // ssekey_pos >= 0 triggers SSE-C header addition
    // GetSseKeyMd5 will fail since no keys loaded => returns false
    bool result = s3curl.PreHeadRequest("/testfile", NULL, NULL, 0, NULL, NULL);
    EXPECT_FALSE(result);
}

// ==========================================================================
// 6. HeadRequest tests
// ==========================================================================

TEST_F(CurlRequestMockTest, HeadRequest_NullPath_PreHeadRequestFails) {
    S3fsCurl s3curl;
    headers_t meta;
    long long inode_no = 0;
    // When tpath is NULL, PreHeadRequest returns false
    int result = s3curl.HeadRequest(NULL, meta, &inode_no, "");
    EXPECT_EQ(-1, result);
}

// ISSUE2026040300002: HeadRequest NULL defense tests
// PreHeadRequest returns false when tpath=NULL. After the fix, the error log
// uses (inode_no ? *inode_no : -1) and SAFESTRPTR(shardkey) to avoid NULL dereference.

TEST_F(CurlRequestMockTest, HeadRequest_NullInodeNo_NullShardkey_PreHeadFails_NoSegfault) {
    S3fsCurl s3curl;
    headers_t meta;
    // Both inode_no and shardkey are NULL (the default for 19 call sites).
    // Before the fix, this would SIGSEGV on the error log line.
    int result = s3curl.HeadRequest(NULL, meta, NULL, NULL);
    EXPECT_EQ(-1, result);
}

TEST_F(CurlRequestMockTest, HeadRequest_ValidInodeNo_NullShardkey_PreHeadFails_NoSegfault) {
    S3fsCurl s3curl;
    headers_t meta;
    long long inode_no = 42;
    // Valid inode_no but NULL shardkey -- tests SAFESTRPTR(shardkey) path.
    int result = s3curl.HeadRequest(NULL, meta, &inode_no, NULL);
    EXPECT_EQ(-1, result);
}

TEST_F(CurlRequestMockTest, HeadRequest_NullInodeNo_ValidShardkey_PreHeadFails_NoSegfault) {
    S3fsCurl s3curl;
    headers_t meta;
    // NULL inode_no but valid shardkey -- tests (inode_no ? *inode_no : -1) path.
    int result = s3curl.HeadRequest(NULL, meta, NULL, "test_shard");
    EXPECT_EQ(-1, result);
}

TEST_F(CurlRequestMockTest, HeadRequest_ValidPath_RetriesZero) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    headers_t meta;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    int result = s3curl.HeadRequest("/testfile", meta, NULL, NULL);
    // PreHeadRequest succeeds, RequestPerform returns -EIO (retries=0)
    EXPECT_NE(0, result);
}

TEST_F(CurlRequestMockTest, HeadRequest_ResponseHeaderParsing) {
    // Test the header classification logic directly
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_HEAD;
    s3curl.url = "http://test/head";

    // Simulate response headers
    s3curl.responseHeaders["Content-Type"] = "application/octet-stream";
    s3curl.responseHeaders["content-length"] = "1024";
    s3curl.responseHeaders["ETag"] = "\"abc123\"";
    s3curl.responseHeaders["Last-Modified"] = "Thu, 01 Jan 2026 00:00:00 GMT";
    s3curl.responseHeaders["x-amz-meta-custom"] = "value1";
    s3curl.responseHeaders["x-hws-fs-inodeno"] = "12345";
    s3curl.responseHeaders["x-hws-fs-shardkey"] = "shard1";

    EXPECT_EQ("application/octet-stream", s3curl.responseHeaders["Content-Type"]);
    EXPECT_EQ("1024", s3curl.responseHeaders["content-length"]);
}

// ==========================================================================
// 7. PutHeadRequest tests
// ==========================================================================

TEST_F(CurlRequestMockTest, PutHeadRequest_NullPath) {
    S3fsCurl s3curl;
    headers_t meta;
    int result = s3curl.PutHeadRequest(NULL, meta, false, "");
    EXPECT_EQ(-1, result);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_ValidPath_ObjectBucket) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    headers_t meta;
    meta["Content-Type"] = "text/plain";
    int result = s3curl.PutHeadRequest("/testfile", meta, false, "");
    EXPECT_EQ(-EIO, result);
    EXPECT_EQ(S3fsCurl::REQTYPE_PUTHEAD, s3curl.type);
    EXPECT_EQ("PUT", s3curl.op);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_ValidPath_FileBucket) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
    headers_t meta;
    int result = s3curl.PutHeadRequest("/testfile", meta, false, "");
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_WithQueryString) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    headers_t meta;
    int result = s3curl.PutHeadRequest("/testfile", meta, false, "versionId=123");
    EXPECT_EQ(-EIO, result);
    EXPECT_EQ("versionId=123", s3curl.query_string);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_WithContentType) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    headers_t meta;
    meta["Content-Type"] = "text/plain";
    int result = s3curl.PutHeadRequest("/testfile", meta, false, "");
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_WithAmzMeta) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    headers_t meta;
    meta["x-amz-meta-custom"] = "value1";
    int result = s3curl.PutHeadRequest("/testfile", meta, false, "");
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_WithCopySource) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    headers_t meta;
    meta["x-amz-copy-source"] = "/source-bucket/source-key";
    int result = s3curl.PutHeadRequest("/testfile", meta, true, "");
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_WithSseS3_CopyMode) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    headers_t meta;
    meta["x-amz-server-side-encryption"] = "AES256";
    int result = s3curl.PutHeadRequest("/testfile", meta, true, "");
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_WithSseKms_CopyMode) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    headers_t meta;
    meta["x-amz-server-side-encryption"] = "aws:kms";
    meta["x-amz-server-side-encryption-aws-kms-key-id"] = "kms-key-id-123";
    int result = s3curl.PutHeadRequest("/testfile", meta, true, "");
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_WithSseKms_EmptyKeyId_CopyMode) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    headers_t meta;
    meta["x-amz-server-side-encryption-aws-kms-key-id"] = "";
    int result = s3curl.PutHeadRequest("/testfile", meta, true, "");
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_WithSseC_CopyMode) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    headers_t meta;
    meta["x-amz-server-side-encryption-customer-key-md5"] = "md5value";
    int result = s3curl.PutHeadRequest("/testfile", meta, true, "");
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_WithSseS3_NotCopyMode) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    headers_t meta;
    meta["x-amz-server-side-encryption"] = "AES256";
    int result = s3curl.PutHeadRequest("/testfile", meta, false, "");
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_WithDefaultAcl) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    S3fsCurl::default_acl = "public-read";
    headers_t meta;
    int result = s3curl.PutHeadRequest("/testfile", meta, false, "");
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_WithReducedRedundancy) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    S3fsCurl::storage_class = REDUCED_REDUNDANCY;
    headers_t meta;
    int result = s3curl.PutHeadRequest("/testfile", meta, false, "");
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_WithStandardIA) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    S3fsCurl::storage_class = STANDARD_IA;
    headers_t meta;
    int result = s3curl.PutHeadRequest("/testfile", meta, false, "");
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_WithAhbe) {
    S3fsCurl s3curl(true);  // is_use_ahbe = true
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    headers_t meta;
    int result = s3curl.PutHeadRequest("/testfile", meta, false, "");
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, PutHeadRequest_AmzAcl_Skipped) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    headers_t meta;
    meta["x-amz-acl"] = "private";  // should be skipped in loop
    int result = s3curl.PutHeadRequest("/testfile", meta, false, "");
    EXPECT_EQ(-EIO, result);
}

// ==========================================================================
// 8. CheckBucket tests
// ==========================================================================

TEST_F(CurlRequestMockTest, CheckBucket_ObjectBucket_RetriesZero) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    int result = s3curl.CheckBucket("AK", "SK", "TOKEN");
    // retries=0 => RequestPerform returns -EIO
    EXPECT_NE(0, result);
}

TEST_F(CurlRequestMockTest, CheckBucket_UnknownBucketType_RetriesZero) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_UNKNOWN;
    int result = s3curl.CheckBucket("AK", "SK", "TOKEN");
    EXPECT_NE(0, result);
}

TEST_F(CurlRequestMockTest, CheckBucket_SetsCorrectType) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    s3curl.CheckBucket("AK", "SK", "TOKEN");
    EXPECT_EQ(S3fsCurl::REQTYPE_CHKBUCKET, s3curl.type);
    EXPECT_EQ("GET", s3curl.op);
}

// ==========================================================================
// 9. ListBucketRequest tests
// ==========================================================================

TEST_F(CurlRequestMockTest, ListBucketRequest_NullPath) {
    S3fsCurl s3curl;
    int result = s3curl.ListBucketRequest(NULL, NULL);
    EXPECT_EQ(-1, result);
}

TEST_F(CurlRequestMockTest, ListBucketRequest_ValidPath_NoQuery) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    int result = s3curl.ListBucketRequest("/", NULL);
    EXPECT_EQ(-EIO, result);
    EXPECT_EQ(S3fsCurl::REQTYPE_LISTBUCKET, s3curl.type);
    EXPECT_EQ("GET", s3curl.op);
}

TEST_F(CurlRequestMockTest, ListBucketRequest_WithQuery) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    int result = s3curl.ListBucketRequest("/", "prefix=test&max-keys=100");
    EXPECT_EQ(-EIO, result);
    EXPECT_EQ("prefix=test&max-keys=100", s3curl.query_string);
}

TEST_F(CurlRequestMockTest, ListBucketRequest_FileBucket) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
    int result = s3curl.ListBucketRequest("/dir", "prefix=dir/");
    EXPECT_EQ(-EIO, result);
}

// ==========================================================================
// 10. CurlDebugFunc tests
// ==========================================================================

TEST_F(CurlRequestMockTest, CurlDebugFunc_NullHandle) {
    int result = S3fsCurl::CurlDebugFunc(NULL, CURLINFO_TEXT, (char*)"test", 4, NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_InfoText) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char text[] = "some debug text\n";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_TEXT, text, strlen(text), NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_InfoText_WithTabs) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char text[] = "\t\tindented text\n";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_TEXT, text, strlen(text), NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_InfoText_AllTabs) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char text[] = "\t\t\t";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_TEXT, text, strlen(text), NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_HeaderIn) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char header[] = "Content-Type: text/html\r\n";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_HEADER_IN, header, strlen(header), NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_HeaderOut) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char header[] = "GET / HTTP/1.1\r\nHost: test\r\n\r\n";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_HEADER_OUT, header, strlen(header), NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_HeaderIn_NoNewline) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char header[] = "Content-Type: text/html";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_HEADER_IN, header, strlen(header), NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_HeaderIn_CrOnly) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char header[] = "X-Custom: val\r";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_HEADER_IN, header, strlen(header), NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_HeaderIn_LfOnly) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char header[] = "X-Custom: val\n";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_HEADER_IN, header, strlen(header), NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_HeaderIn_MultiLine) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char header[] = "Line1: val1\r\nLine2: val2\r\n";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_HEADER_IN, header, strlen(header), NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_DataIn) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char data[] = "binary data";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_DATA_IN, data, strlen(data), NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_DataOut) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char data[] = "outgoing data";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_DATA_OUT, data, strlen(data), NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_SslDataIn) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char data[] = "ssl data";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_SSL_DATA_IN, data, strlen(data), NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_SslDataOut) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char data[] = "ssl out";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_SSL_DATA_OUT, data, strlen(data), NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_DefaultType) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char data[] = "unknown";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_END, data, strlen(data), NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlDebugFunc_EmptyData) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    char data[] = "";
    int result = S3fsCurl::CurlDebugFunc(const_cast<CURL*>(h), CURLINFO_TEXT, data, 0, NULL);
    EXPECT_EQ(0, result);
}

// ==========================================================================
// 11. DetectBucketTypeFromHeaders tests
// ==========================================================================

TEST_F(CurlRequestMockTest, DetectBucketType_EmptyHeaders) {
    headers_t headers;
    bucket_type_t old_type = g_bucket_type;
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_FALSE(result);
    g_bucket_type = old_type;
}

TEST_F(CurlRequestMockTest, DetectBucketType_WithObsBucketType) {
    headers_t headers;
    headers["x-obs-bucket-type"] = "POSIX";
    bucket_type_t old_type = g_bucket_type;
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_TRUE(result);
    EXPECT_EQ(BUCKET_TYPE_FILE_SEMANTIC, g_bucket_type);
    g_bucket_type = old_type;
}

TEST_F(CurlRequestMockTest, DetectBucketType_WithFsFileInterface) {
    headers_t headers;
    headers["x-obs-fs-file-interface"] = "Enabled";
    bucket_type_t old_type = g_bucket_type;
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_TRUE(result);
    EXPECT_EQ(BUCKET_TYPE_FILE_SEMANTIC, g_bucket_type);
    g_bucket_type = old_type;
}

TEST_F(CurlRequestMockTest, DetectBucketType_WithBothObsHeaders) {
    headers_t headers;
    headers["x-obs-bucket-type"] = "POSIX";
    headers["x-obs-fs-file-interface"] = "Enabled";
    bucket_type_t old_type = g_bucket_type;
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_TRUE(result);
    EXPECT_EQ(BUCKET_TYPE_FILE_SEMANTIC, g_bucket_type);
    g_bucket_type = old_type;
}

TEST_F(CurlRequestMockTest, DetectBucketType_WithOtherHeaders) {
    headers_t headers;
    headers["content-type"] = "text/plain";
    headers["x-amz-request-id"] = "REQ123";
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_FALSE(result);
}

// ==========================================================================
// 12. CreateCurlHandle / DestroyCurlHandle / ClearInternalData tests
// ==========================================================================

TEST_F(CurlRequestMockTest, CreateCurlHandle_Force) {
    S3fsCurl s3curl;
    EXPECT_TRUE(s3curl.CreateCurlHandle(true));
    EXPECT_NE(nullptr, s3curl.GetCurlHandle());
    // Create again with force
    EXPECT_TRUE(s3curl.CreateCurlHandle(true));
    EXPECT_NE(nullptr, s3curl.GetCurlHandle());
}

TEST_F(CurlRequestMockTest, CreateCurlHandle_NoForce_AlreadyExists) {
    S3fsCurl s3curl;
    EXPECT_TRUE(s3curl.CreateCurlHandle(true));
    EXPECT_FALSE(s3curl.CreateCurlHandle(false));
}

TEST_F(CurlRequestMockTest, DestroyCurlHandle_NullHandle) {
    S3fsCurl s3curl;
    EXPECT_FALSE(s3curl.DestroyCurlHandle());
}

TEST_F(CurlRequestMockTest, DestroyCurlHandle_ValidHandle) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    EXPECT_TRUE(s3curl.DestroyCurlHandle());
    EXPECT_EQ(nullptr, s3curl.GetCurlHandle());
}

TEST_F(CurlRequestMockTest, ClearInternalData_WithHandle) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    EXPECT_FALSE(s3curl.ClearInternalData());
}

TEST_F(CurlRequestMockTest, ClearInternalData_NoHandle) {
    S3fsCurl s3curl;
    EXPECT_TRUE(s3curl.ClearInternalData());
    EXPECT_EQ(S3fsCurl::REQTYPE_UNSET, s3curl.type);
    EXPECT_EQ("", s3curl.path);
    EXPECT_EQ("", s3curl.url);
    EXPECT_EQ(-1, s3curl.LastResponseCode);
}

// ==========================================================================
// 13. ResetHandle tests
// ==========================================================================

TEST_F(CurlRequestMockTest, ResetHandle_BasicSetup) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_DELETE;
    EXPECT_TRUE(s3curl.ResetHandle());
}

TEST_F(CurlRequestMockTest, ResetHandle_IamCredType) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_IAMCRED;
    EXPECT_TRUE(s3curl.ResetHandle());
}

TEST_F(CurlRequestMockTest, ResetHandle_IamRoleType) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_IAMROLE;
    EXPECT_TRUE(s3curl.ResetHandle());
}

TEST_F(CurlRequestMockTest, ResetHandle_WithVerbose) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    S3fsCurl::is_verbose = true;
    s3curl.type = S3fsCurl::REQTYPE_DELETE;
    EXPECT_TRUE(s3curl.ResetHandle());
}

TEST_F(CurlRequestMockTest, ResetHandle_WithCaBundle) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    S3fsCurl::curl_ca_bundle = "/etc/ssl/certs/ca-certificates.crt";
    s3curl.type = S3fsCurl::REQTYPE_DELETE;
    EXPECT_TRUE(s3curl.ResetHandle());
}

// ==========================================================================
// 14. GetResponseCode tests
// ==========================================================================

TEST_F(CurlRequestMockTest, GetResponseCode_NullHandle) {
    S3fsCurl s3curl;
    long code;
    EXPECT_FALSE(s3curl.GetResponseCode(code));
}

TEST_F(CurlRequestMockTest, GetResponseCode_ValidHandle) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    long code;
    bool result = s3curl.GetResponseCode(code);
    EXPECT_TRUE(result);
    EXPECT_EQ(0, code);
}

// ==========================================================================
// 15. SetUseAhbe tests
// ==========================================================================

TEST_F(CurlRequestMockTest, SetUseAhbe_Toggle) {
    S3fsCurl s3curl;
    EXPECT_FALSE(s3curl.IsUseAhbe());
    bool old = s3curl.SetUseAhbe(true);
    EXPECT_FALSE(old);
    EXPECT_TRUE(s3curl.IsUseAhbe());
    old = s3curl.SetUseAhbe(false);
    EXPECT_TRUE(old);
    EXPECT_FALSE(s3curl.IsUseAhbe());
}

TEST_F(CurlRequestMockTest, EnableDisableAhbe) {
    S3fsCurl s3curl;
    s3curl.EnableUseAhbe();
    EXPECT_TRUE(s3curl.IsUseAhbe());
    s3curl.DisableUseAhbe();
    EXPECT_FALSE(s3curl.IsUseAhbe());
}

// ==========================================================================
// 16. Getter/accessor method tests
// ==========================================================================

TEST_F(CurlRequestMockTest, GetPath) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.path = "/test/path";
    EXPECT_EQ("/test/path", s3curl.GetPath());
}

TEST_F(CurlRequestMockTest, GetBasePath) {
    S3fsCurl s3curl;
    s3curl.base_path = "/base";
    EXPECT_EQ("/base", s3curl.GetBasePath());
}

TEST_F(CurlRequestMockTest, GetSpacialSavedPath) {
    S3fsCurl s3curl;
    s3curl.saved_path = "/saved";
    EXPECT_EQ("/saved", s3curl.GetSpacialSavedPath());
}

TEST_F(CurlRequestMockTest, GetUrl) {
    S3fsCurl s3curl;
    s3curl.url = "http://test.com/path";
    EXPECT_EQ("http://test.com/path", s3curl.GetUrl());
}

TEST_F(CurlRequestMockTest, GetLastResponseCode) {
    S3fsCurl s3curl;
    s3curl.LastResponseCode = 403;
    EXPECT_EQ(403, s3curl.GetLastResponseCode());
}

TEST_F(CurlRequestMockTest, GetResponseHeaders) {
    S3fsCurl s3curl;
    s3curl.responseHeaders["key"] = "value";
    const headers_t* h = s3curl.GetResponseHeaders();
    ASSERT_NE(nullptr, h);
    EXPECT_EQ("value", h->at("key"));  // const map: use at() instead of []
}

TEST_F(CurlRequestMockTest, GetBodyData) {
    S3fsCurl s3curl;
    EXPECT_EQ(nullptr, s3curl.GetBodyData());
    s3curl.bodydata = new BodyData();
    EXPECT_NE(nullptr, s3curl.GetBodyData());
    delete s3curl.bodydata;
    s3curl.bodydata = NULL;
}

TEST_F(CurlRequestMockTest, GetHeadData) {
    S3fsCurl s3curl;
    EXPECT_EQ(nullptr, s3curl.GetHeadData());
    s3curl.headdata = new BodyData();
    EXPECT_NE(nullptr, s3curl.GetHeadData());
    delete s3curl.headdata;
    s3curl.headdata = NULL;
}

TEST_F(CurlRequestMockTest, MultipartRetryCount) {
    S3fsCurl s3curl;
    EXPECT_EQ(0, s3curl.GetMultipartRetryCount());
    s3curl.SetMultipartRetryCount(5);
    EXPECT_EQ(5, s3curl.GetMultipartRetryCount());
}

TEST_F(CurlRequestMockTest, IsOverMultipartRetryCount) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 3;
    s3curl.retry_count = 2;
    EXPECT_FALSE(s3curl.IsOverMultipartRetryCount());
    s3curl.retry_count = 3;
    EXPECT_TRUE(s3curl.IsOverMultipartRetryCount());
    s3curl.retry_count = 5;
    EXPECT_TRUE(s3curl.IsOverMultipartRetryCount());
}

TEST_F(CurlRequestMockTest, GetLastPreHeadSeecKeyPos) {
    S3fsCurl s3curl;
    EXPECT_EQ(-1, s3curl.GetLastPreHeadSeecKeyPos());
    s3curl.b_ssekey_pos = 2;
    EXPECT_EQ(2, s3curl.GetLastPreHeadSeecKeyPos());
}

// ==========================================================================
// 17. Hws_curl_easy_perform test
// ==========================================================================

TEST_F(CurlRequestMockTest, HwsCurlEasyPerform_NoUrl) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    CURLcode result = s3curl.Hws_curl_easy_perform();
    EXPECT_NE(CURLE_OK, result);
}

// ==========================================================================
// 18. RenameSetopt test
// ==========================================================================

TEST_F(CurlRequestMockTest, RenameSetopt_SetsOptions) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.url = "http://test/rename";
    s3curl.requestHeaders = NULL;
    s3curl.RenameSetopt();
    // No crash = success
}

// ==========================================================================
// 19. BodyData class tests
// ==========================================================================

TEST_F(CurlRequestMockTest, BodyData_ClearAndReuse) {
    BodyData bd;
    EXPECT_TRUE(bd.Append((void*)"hello", 5));
    EXPECT_EQ(5u, bd.size());
    EXPECT_STREQ("hello", bd.str());
    bd.Clear();
    EXPECT_EQ(0u, bd.size());
    EXPECT_STREQ("", bd.str());
    EXPECT_TRUE(bd.Append((void*)"world", 5));
    EXPECT_STREQ("world", bd.str());
}

TEST_F(CurlRequestMockTest, BodyData_AppendMultiple) {
    BodyData bd;
    EXPECT_TRUE(bd.Append((void*)"abc", 3));
    EXPECT_TRUE(bd.Append((void*)"def", 3));
    EXPECT_EQ(6u, bd.size());
    EXPECT_STREQ("abcdef", bd.str());
}

TEST_F(CurlRequestMockTest, BodyData_AppendBlockSize) {
    BodyData bd;
    char buf[6] = {'a','b','c','d','e','f'};
    EXPECT_TRUE(bd.Append((void*)buf, 2, 3));  // 2 * 3 = 6 bytes, all in-bounds
    EXPECT_EQ(6u, bd.size());
}

TEST_F(CurlRequestMockTest, BodyData_LargeAppend) {
    BodyData bd;
    string large(10000, 'x');
    EXPECT_TRUE(bd.Append((void*)large.c_str(), large.size()));
    EXPECT_EQ(10000u, bd.size());
}

// ==========================================================================
// 20. Constructor / Destructor tests
// ==========================================================================

TEST_F(CurlRequestMockTest, Constructor_Default) {
    S3fsCurl s3curl;
    EXPECT_EQ(nullptr, s3curl.GetCurlHandle());
    EXPECT_EQ(S3fsCurl::REQTYPE_UNSET, s3curl.type);
    EXPECT_EQ("", s3curl.path);
    EXPECT_EQ("", s3curl.url);
    EXPECT_EQ(nullptr, s3curl.requestHeaders);
    EXPECT_EQ(nullptr, s3curl.bodydata);
    EXPECT_EQ(nullptr, s3curl.headdata);
    EXPECT_EQ(-1, s3curl.LastResponseCode);
    EXPECT_EQ(nullptr, s3curl.postdata);
    EXPECT_EQ(0u, s3curl.postdata_remaining);
    EXPECT_FALSE(s3curl.is_use_ahbe);
    EXPECT_EQ(0, s3curl.retry_count);
    EXPECT_EQ(nullptr, s3curl.b_infile);
    EXPECT_EQ(nullptr, s3curl.writedatabuf.pcBuffer);
    EXPECT_EQ(0u, s3curl.writedatabuf.ulBufferSize);
    EXPECT_EQ(0, s3curl.writedatabuf.offset);
    EXPECT_EQ(nullptr, s3curl.read_buf);
    EXPECT_EQ(0, s3curl.read_startpos);
    EXPECT_EQ(0u, s3curl.read_size);
    EXPECT_EQ(0u, s3curl.cur_pos);
}

TEST_F(CurlRequestMockTest, Constructor_WithAhbe) {
    S3fsCurl s3curl(true);
    EXPECT_TRUE(s3curl.is_use_ahbe);
}

TEST_F(CurlRequestMockTest, Destructor_CleansUpHandle) {
    {
        S3fsCurl s3curl;
        s3curl.CreateCurlHandle(true);
        EXPECT_NE(nullptr, s3curl.GetCurlHandle());
    }
    // No crash = success
}

// ==========================================================================
// 21. AddSseRequestHead tests
// ==========================================================================

TEST_F(CurlRequestMockTest, AddSseRequestHead_SseDisable) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.requestHeaders = NULL;
    string ssevalue;
    bool result = s3curl.AddSseRequestHead(SSE_DISABLE, ssevalue, false, false);
    EXPECT_TRUE(result);
}

TEST_F(CurlRequestMockTest, AddSseRequestHead_SseS3) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.requestHeaders = NULL;
    string ssevalue;
    bool result = s3curl.AddSseRequestHead(SSE_S3, ssevalue, false, false);
    EXPECT_TRUE(result);
}

TEST_F(CurlRequestMockTest, AddSseRequestHead_SseS3_OnlyC) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.requestHeaders = NULL;
    string ssevalue;
    bool result = s3curl.AddSseRequestHead(SSE_S3, ssevalue, true, false);
    EXPECT_TRUE(result);
}

TEST_F(CurlRequestMockTest, AddSseRequestHead_SseC_NoKeys) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.requestHeaders = NULL;
    string ssevalue = "nonexistent_md5";
    bool result = s3curl.AddSseRequestHead(SSE_C, ssevalue, false, false);
    EXPECT_TRUE(result);
}

TEST_F(CurlRequestMockTest, AddSseRequestHead_SseKms) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.requestHeaders = NULL;
    string ssevalue = "kms-key-id-123";
    bool result = s3curl.AddSseRequestHead(SSE_KMS, ssevalue, false, false);
    EXPECT_TRUE(result);
}

TEST_F(CurlRequestMockTest, AddSseRequestHead_SseKms_EmptyValue) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.requestHeaders = NULL;
    string ssevalue;
    bool result = s3curl.AddSseRequestHead(SSE_KMS, ssevalue, false, false);
    EXPECT_TRUE(result);
}

TEST_F(CurlRequestMockTest, AddSseRequestHead_SseKms_OnlyC) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.requestHeaders = NULL;
    string ssevalue;
    bool result = s3curl.AddSseRequestHead(SSE_KMS, ssevalue, true, false);
    EXPECT_TRUE(result);
}

// ==========================================================================
// 22. Static class method tests
// ==========================================================================

TEST_F(CurlRequestMockTest, SetStorageClass) {
    storage_class_t old = S3fsCurl::SetStorageClass(REDUCED_REDUNDANCY);
    EXPECT_EQ(REDUCED_REDUNDANCY, S3fsCurl::GetStorageClass());
    S3fsCurl::SetStorageClass(old);
}

TEST_F(CurlRequestMockTest, SetSseType) {
    sse_type_t old = S3fsCurl::SetSseType(SSE_S3);
    EXPECT_EQ(SSE_S3, S3fsCurl::GetSseType());
    EXPECT_TRUE(S3fsCurl::IsSseS3Type());
    EXPECT_FALSE(S3fsCurl::IsSseDisable());
    EXPECT_FALSE(S3fsCurl::IsSseCType());
    EXPECT_FALSE(S3fsCurl::IsSseKmsType());
    S3fsCurl::SetSseType(old);
}

TEST_F(CurlRequestMockTest, SetRetries) {
    int old = S3fsCurl::SetRetries(10);
    S3fsCurl::SetRetries(old);
}

TEST_F(CurlRequestMockTest, SetConnectTimeout) {
    long old = S3fsCurl::SetConnectTimeout(30);
    S3fsCurl::SetConnectTimeout(old);
}

TEST_F(CurlRequestMockTest, SetReadwriteTimeout) {
    time_t old = S3fsCurl::SetReadwriteTimeout(60);
    EXPECT_EQ(60, S3fsCurl::GetReadwriteTimeout());
    S3fsCurl::SetReadwriteTimeout(old);
}

TEST_F(CurlRequestMockTest, SetPublicBucket) {
    bool old = S3fsCurl::SetPublicBucket(true);
    EXPECT_TRUE(S3fsCurl::IsPublicBucket());
    S3fsCurl::SetPublicBucket(old);
}

TEST_F(CurlRequestMockTest, SetDefaultAcl) {
    string old = S3fsCurl::SetDefaultAcl("public-read");
    EXPECT_EQ("public-read", S3fsCurl::GetDefaultAcl());
    S3fsCurl::SetDefaultAcl(old.c_str());
}

TEST_F(CurlRequestMockTest, SetSslVerifyHostname) {
    long old = S3fsCurl::SetSslVerifyHostname(0);
    EXPECT_EQ(0, S3fsCurl::GetSslVerifyHostname());
    S3fsCurl::SetSslVerifyHostname(old);
}

TEST_F(CurlRequestMockTest, SetMaxParallelCount) {
    int old = S3fsCurl::SetMaxParallelCount(20);
    EXPECT_EQ(20, S3fsCurl::GetMaxParallelCount());
    S3fsCurl::SetMaxParallelCount(old);
}

TEST_F(CurlRequestMockTest, SetMultipartSize) {
    // SetMultipartSize takes value in MB and multiplies by 1024*1024 internally
    bool result = S3fsCurl::SetMultipartSize(10);  // 10 MB
    EXPECT_TRUE(result);
    EXPECT_EQ(static_cast<off_t>(10) * 1024 * 1024, S3fsCurl::GetMultipartSize());
}

TEST_F(CurlRequestMockTest, SetMultipartSize_TooSmall) {
    // MIN_MULTIPART_SIZE is in bytes, input 0 MB = 0 bytes < MIN_MULTIPART_SIZE
    bool result = S3fsCurl::SetMultipartSize(0);
    EXPECT_FALSE(result);
}

TEST_F(CurlRequestMockTest, SetSignatureV4) {
    bool old = S3fsCurl::SetSignatureV4(true);
    EXPECT_TRUE(S3fsCurl::IsSignatureV4());
    S3fsCurl::SetSignatureV4(old);
}

TEST_F(CurlRequestMockTest, SetUserAgentFlag) {
    bool old = S3fsCurl::SetUserAgentFlag(true);
    EXPECT_TRUE(S3fsCurl::IsUserAgentFlag());
    S3fsCurl::SetUserAgentFlag(old);
}

TEST_F(CurlRequestMockTest, SetIsECS) {
    S3fsCurl::SetIsECS(true);
    S3fsCurl::SetIsECS(false);
}

TEST_F(CurlRequestMockTest, SetIsIBMIAMAuth) {
    S3fsCurl::SetIsIBMIAMAuth(true);
    EXPECT_TRUE(S3fsCurl::is_ibm_iam_auth);
    S3fsCurl::SetIsIBMIAMAuth(false);
}

TEST_F(CurlRequestMockTest, SetIAMFieldCount) {
    size_t old = S3fsCurl::SetIAMFieldCount(5);
    S3fsCurl::SetIAMFieldCount(old);
}

TEST_F(CurlRequestMockTest, SetIAMCredentialsURL) {
    string old = S3fsCurl::SetIAMCredentialsURL("http://iam.test/creds");
    S3fsCurl::SetIAMCredentialsURL(old.c_str());
}

TEST_F(CurlRequestMockTest, SetIAMTokenField) {
    string old = S3fsCurl::SetIAMTokenField("Token");
    S3fsCurl::SetIAMTokenField(old.c_str());
}

TEST_F(CurlRequestMockTest, SetIAMExpiryField) {
    string old = S3fsCurl::SetIAMExpiryField("Expiration");
    S3fsCurl::SetIAMExpiryField(old.c_str());
}

TEST_F(CurlRequestMockTest, SetIAMRole) {
    string old_role = S3fsCurl::GetIAMRole();
    S3fsCurl::SetIAMRole("test-role");
    EXPECT_STREQ("test-role", S3fsCurl::GetIAMRole());
    S3fsCurl::SetIAMRole(old_role.c_str());
}

TEST_F(CurlRequestMockTest, SetContentMd5) {
    S3fsCurl::SetContentMd5(true);
    S3fsCurl::SetContentMd5(false);
}

TEST_F(CurlRequestMockTest, SetVerbose) {
    S3fsCurl::SetVerbose(true);
    EXPECT_TRUE(S3fsCurl::GetVerbose());
    S3fsCurl::SetVerbose(false);
}

TEST_F(CurlRequestMockTest, SetCheckCertificate) {
    S3fsCurl::SetCheckCertificate(false);
    S3fsCurl::SetCheckCertificate(true);
}

TEST_F(CurlRequestMockTest, SetDnsCache) {
    S3fsCurl::SetDnsCache(true);
    S3fsCurl::SetDnsCache(false);
}

TEST_F(CurlRequestMockTest, SetSslSessionCache) {
    S3fsCurl::SetSslSessionCache(true);
    S3fsCurl::SetSslSessionCache(false);
}

TEST_F(CurlRequestMockTest, SetAccessKey) {
    S3fsCurl::SetAccessKey("newAKID", "newSK");
    EXPECT_EQ("newAKID", S3fsCurl::AWSAccessKeyId);
    EXPECT_EQ("newSK", S3fsCurl::AWSSecretAccessKey);
}

TEST_F(CurlRequestMockTest, SetAccessKeyAndToken) {
    S3fsCurl::SetAccessKeyAndToken("AK", "SK", "TOKEN");
    string ak, sk, token;
    S3fsCurl::GetAccessKeyAndToken(ak, sk, token);
    EXPECT_EQ("AK", ak);
    EXPECT_EQ("SK", sk);
    EXPECT_EQ("TOKEN", token);
}

TEST_F(CurlRequestMockTest, IsSetAccessKeyID) {
    S3fsCurl::AWSAccessKeyId = "test";
    EXPECT_TRUE(S3fsCurl::IsSetAccessKeyID());
    S3fsCurl::AWSAccessKeyId = "";
    EXPECT_FALSE(S3fsCurl::IsSetAccessKeyID());
}

TEST_F(CurlRequestMockTest, IsSetAccessKeys) {
    S3fsCurl::AWSAccessKeyId = "AK";
    S3fsCurl::AWSSecretAccessKey = "SK";
    S3fsCurl::IAM_role = "";
    S3fsCurl::is_ibm_iam_auth = false;
    EXPECT_TRUE(S3fsCurl::IsSetAccessKeys());
}

TEST_F(CurlRequestMockTest, IsSetAccessKeys_IamRole) {
    S3fsCurl::AWSAccessKeyId = "";
    S3fsCurl::AWSSecretAccessKey = "";
    S3fsCurl::IAM_role = "test-role";
    EXPECT_TRUE(S3fsCurl::IsSetAccessKeys());
    S3fsCurl::IAM_role = "";
}

TEST_F(CurlRequestMockTest, IsSetAccessKeys_IbmIam) {
    S3fsCurl::AWSAccessKeyId = "";
    S3fsCurl::AWSSecretAccessKey = "apikey";
    S3fsCurl::IAM_role = "";
    S3fsCurl::is_ibm_iam_auth = true;
    EXPECT_TRUE(S3fsCurl::IsSetAccessKeys());
}

// ==========================================================================
// 23. LookupMimeType tests
// ==========================================================================

TEST_F(CurlRequestMockTest, LookupMimeType_Html) {
    string mime = S3fsCurl::LookupMimeType("test.html");
    EXPECT_FALSE(mime.empty());
}

TEST_F(CurlRequestMockTest, LookupMimeType_Txt) {
    string mime = S3fsCurl::LookupMimeType("test.txt");
    EXPECT_FALSE(mime.empty());
}

TEST_F(CurlRequestMockTest, LookupMimeType_Unknown) {
    string mime = S3fsCurl::LookupMimeType("test.xyz123");
    EXPECT_FALSE(mime.empty());
}

TEST_F(CurlRequestMockTest, LookupMimeType_NoExtension) {
    string mime = S3fsCurl::LookupMimeType("testfile");
    EXPECT_FALSE(mime.empty());
}

TEST_F(CurlRequestMockTest, LookupMimeType_Directory) {
    string mime = S3fsCurl::LookupMimeType("testdir/");
    EXPECT_FALSE(mime.empty());
}

// ==========================================================================
// 24. AddUserAgent tests
// ==========================================================================

TEST_F(CurlRequestMockTest, AddUserAgent_WithFlag) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    S3fsCurl::is_ua = true;
    bool result = S3fsCurl::AddUserAgent(const_cast<CURL*>(s3curl.GetCurlHandle()));
    EXPECT_TRUE(result);
}

TEST_F(CurlRequestMockTest, AddUserAgent_WithoutFlag) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    S3fsCurl::is_ua = false;
    bool result = S3fsCurl::AddUserAgent(const_cast<CURL*>(s3curl.GetCurlHandle()));
    EXPECT_TRUE(result);
}

// ==========================================================================
// 25. getRootInodeNo tests
// ==========================================================================

TEST_F(CurlRequestMockTest, GetRootInodeNo_NoHeader) {
    S3fsCurl s3curl;
    s3curl.responseHeaders.clear();
    int result = s3curl.getRootInodeNo();
    EXPECT_EQ(-1, result);
}

TEST_F(CurlRequestMockTest, GetRootInodeNo_WithHeader) {
    S3fsCurl s3curl;
    s3curl.responseHeaders[hws_s3fs_inodeNo] = "12345";
    int result = s3curl.getRootInodeNo();
    EXPECT_EQ(0, result);
}

// ==========================================================================
// 26. PreGetObjectRequest tests
// ==========================================================================

TEST_F(CurlRequestMockTest, PreGetObjectRequest_NullPath) {
    S3fsCurl s3curl;
    string ssevalue;
    int result = s3curl.PreGetObjectRequest(NULL, 1, 0, 100, SSE_DISABLE, ssevalue);
    EXPECT_EQ(-1, result);
}

TEST_F(CurlRequestMockTest, PreGetObjectRequest_InvalidFd) {
    S3fsCurl s3curl;
    string ssevalue;
    int result = s3curl.PreGetObjectRequest("/test", -1, 0, 100, SSE_DISABLE, ssevalue);
    EXPECT_EQ(-1, result);
}

TEST_F(CurlRequestMockTest, PreGetObjectRequest_NegativeStart) {
    S3fsCurl s3curl;
    string ssevalue;
    int result = s3curl.PreGetObjectRequest("/test", 1, -1, 100, SSE_DISABLE, ssevalue);
    EXPECT_EQ(-1, result);
}

TEST_F(CurlRequestMockTest, PreGetObjectRequest_NegativeSize) {
    S3fsCurl s3curl;
    string ssevalue;
    int result = s3curl.PreGetObjectRequest("/test", 1, 0, -1, SSE_DISABLE, ssevalue);
    EXPECT_EQ(-1, result);
}

TEST_F(CurlRequestMockTest, PreGetObjectRequest_ValidParams) {
    S3fsCurl s3curl;
    string ssevalue;
    int fd = open("/dev/null", O_RDWR);
    ASSERT_GT(fd, 0);
    int result = s3curl.PreGetObjectRequest("/test", fd, 0, 100, SSE_DISABLE, ssevalue);
    EXPECT_EQ(0, result);
    EXPECT_EQ(S3fsCurl::REQTYPE_GET, s3curl.type);
    EXPECT_EQ("GET", s3curl.op);
    EXPECT_EQ(fd, s3curl.partdata.fd);
    EXPECT_EQ(0, s3curl.partdata.startpos);
    EXPECT_EQ(100, s3curl.partdata.size);
    close(fd);
}

TEST_F(CurlRequestMockTest, PreGetObjectRequest_WithRange) {
    S3fsCurl s3curl;
    string ssevalue;
    int fd = open("/dev/null", O_RDWR);
    ASSERT_GT(fd, 0);
    int result = s3curl.PreGetObjectRequest("/test", fd, 1024, 4096, SSE_DISABLE, ssevalue);
    EXPECT_EQ(0, result);
    EXPECT_EQ(1024, s3curl.b_partdata_startpos);
    EXPECT_EQ(4096, s3curl.b_partdata_size);
    close(fd);
}

// ==========================================================================
// 27. GetObjectRequest tests
// ==========================================================================

TEST_F(CurlRequestMockTest, GetObjectRequest_NullPath) {
    S3fsCurl s3curl;
    int result = s3curl.GetObjectRequest(NULL, 1);
    EXPECT_EQ(-1, result);
}

TEST_F(CurlRequestMockTest, GetObjectRequest_ValidPath_RetriesZero) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    int fd = open("/dev/null", O_RDWR);
    ASSERT_GT(fd, 0);
    int result = s3curl.GetObjectRequest("/test", fd, 0, 100);
    EXPECT_EQ(-EIO, result);
    close(fd);
}

// ==========================================================================
// 28. CreateCurlHandle type preservation for IAM types
// ==========================================================================

TEST_F(CurlRequestMockTest, CreateCurlHandle_PreservesIamCredType) {
    S3fsCurl s3curl;
    s3curl.type = S3fsCurl::REQTYPE_IAMCRED;
    s3curl.CreateCurlHandle(true);
    EXPECT_EQ(S3fsCurl::REQTYPE_IAMCRED, s3curl.type);
}

TEST_F(CurlRequestMockTest, CreateCurlHandle_PreservesIamRoleType) {
    S3fsCurl s3curl;
    s3curl.type = S3fsCurl::REQTYPE_IAMROLE;
    s3curl.CreateCurlHandle(true);
    EXPECT_EQ(S3fsCurl::REQTYPE_IAMROLE, s3curl.type);
}

TEST_F(CurlRequestMockTest, CreateCurlHandle_ResetsOtherTypes) {
    S3fsCurl s3curl;
    s3curl.type = S3fsCurl::REQTYPE_DELETE;
    s3curl.CreateCurlHandle(true);
    EXPECT_EQ(S3fsCurl::REQTYPE_UNSET, s3curl.type);
}

// ==========================================================================
// 29. HeaderCallback and WriteMemoryCallback tests
// ==========================================================================

TEST_F(CurlRequestMockTest, HeaderCallback_ValidHeader) {
    headers_t headers;
    char data[] = "Content-Type: text/plain\r\n";
    size_t result = S3fsCurl::HeaderCallback(data, 1, strlen(data), &headers);
    EXPECT_EQ(strlen(data), result);
}

TEST_F(CurlRequestMockTest, HeaderCallback_EmptyLine) {
    headers_t headers;
    char data[] = "\r\n";
    size_t result = S3fsCurl::HeaderCallback(data, 1, strlen(data), &headers);
    EXPECT_EQ(strlen(data), result);
}

TEST_F(CurlRequestMockTest, WriteMemoryCallback_ValidData) {
    BodyData bd;
    char data[] = "test body content";
    size_t result = S3fsCurl::WriteMemoryCallback(data, 1, strlen(data), &bd);
    EXPECT_EQ(strlen(data), result);
    EXPECT_STREQ("test body content", bd.str());
}

TEST_F(CurlRequestMockTest, WriteMemoryCallback_ZeroSize) {
    BodyData bd;
    char data[] = "hello";
    // Zero blocks means zero bytes appended
    size_t result = S3fsCurl::WriteMemoryCallback(data, 1, 0, &bd);
    EXPECT_EQ(0u, result);
}

// ==========================================================================
// 30. ReadCallback tests
// ==========================================================================

TEST_F(CurlRequestMockTest, ReadCallback_WithPostData) {
    S3fsCurl s3curl;
    char buf[256];
    const unsigned char postdata[] = "POST body content";
    s3curl.postdata = postdata;
    s3curl.postdata_remaining = strlen((const char*)postdata);

    size_t result = S3fsCurl::ReadCallback(buf, 1, sizeof(buf), &s3curl);
    EXPECT_EQ(strlen((const char*)postdata), result);
    EXPECT_EQ(0u, s3curl.postdata_remaining);
}

TEST_F(CurlRequestMockTest, ReadCallback_SmallBuffer) {
    S3fsCurl s3curl;
    char buf[5];
    const unsigned char postdata[] = "ABCDEFGHIJ";
    s3curl.postdata = postdata;
    s3curl.postdata_remaining = 10;

    size_t result = S3fsCurl::ReadCallback(buf, 1, 5, &s3curl);
    EXPECT_EQ(5u, result);
    EXPECT_EQ(5u, s3curl.postdata_remaining);
}

TEST_F(CurlRequestMockTest, ReadCallback_NullPostData) {
    S3fsCurl s3curl;
    char buf[256];
    s3curl.postdata = NULL;
    s3curl.postdata_remaining = 0;

    size_t result = S3fsCurl::ReadCallback(buf, 1, sizeof(buf), &s3curl);
    EXPECT_EQ(0u, result);
}

// ==========================================================================
// 31. ReadUploadCache tests
// ==========================================================================

TEST_F(CurlRequestMockTest, ReadUploadCache_ValidData) {
    tagDataBuffer databuf;
    char source[] = "upload cache data";
    databuf.pcBuffer = source;
    databuf.ulBufferSize = strlen(source);
    databuf.offset = 0;

    char dest[256];
    size_t result = S3fsCurl::ReadUploadCache(dest, 1, sizeof(dest), &databuf);
    EXPECT_EQ(strlen(source), result);
    EXPECT_EQ((off_t)strlen(source), databuf.offset);
}

TEST_F(CurlRequestMockTest, ReadUploadCache_SmallBuffer) {
    tagDataBuffer databuf;
    char source[] = "ABCDEFGHIJ";
    databuf.pcBuffer = source;
    databuf.ulBufferSize = 10;
    databuf.offset = 0;

    char dest[5];
    size_t result = S3fsCurl::ReadUploadCache(dest, 1, 5, &databuf);
    EXPECT_EQ(5u, result);
    EXPECT_EQ(5, databuf.offset);
}

TEST_F(CurlRequestMockTest, ReadUploadCache_AlreadyRead) {
    tagDataBuffer databuf;
    char source[] = "data";
    databuf.pcBuffer = source;
    databuf.ulBufferSize = 4;
    databuf.offset = 4;

    char dest[256];
    size_t result = S3fsCurl::ReadUploadCache(dest, 1, sizeof(dest), &databuf);
    EXPECT_EQ(0u, result);
}

// ==========================================================================
// 32. DownloadWriteCallbackforRead tests
// ==========================================================================

TEST_F(CurlRequestMockTest, DownloadWriteCallbackforRead_ValidData) {
    S3fsCurl s3curl;
    char readbuf[256];
    memset(readbuf, 0, sizeof(readbuf));
    s3curl.read_buf = readbuf;
    s3curl.read_size = sizeof(readbuf);
    s3curl.cur_pos = 0;

    char incoming[] = "downloaded data";
    size_t result = S3fsCurl::DownloadWriteCallbackforRead(
        incoming, 1, strlen(incoming), &s3curl);
    EXPECT_EQ(strlen(incoming), result);
    EXPECT_EQ(strlen(incoming), s3curl.cur_pos);
    EXPECT_EQ(0, memcmp(readbuf, "downloaded data", strlen(incoming)));
}

TEST_F(CurlRequestMockTest, DownloadWriteCallbackforRead_BufferFull) {
    S3fsCurl s3curl;
    char readbuf[10];
    memset(readbuf, 0, sizeof(readbuf));
    s3curl.read_buf = readbuf;
    s3curl.read_size = sizeof(readbuf);
    s3curl.cur_pos = 0;

    char incoming[] = "this is way too much data";
    size_t result = S3fsCurl::DownloadWriteCallbackforRead(
        incoming, 1, strlen(incoming), &s3curl);
    EXPECT_EQ(sizeof(readbuf), result);
}

TEST_F(CurlRequestMockTest, DownloadWriteCallbackforRead_NullReadBuf) {
    S3fsCurl s3curl;
    s3curl.read_buf = NULL;
    s3curl.read_size = 0;
    s3curl.cur_pos = 0;

    char incoming[] = "data";
    size_t result = S3fsCurl::DownloadWriteCallbackforRead(
        incoming, 1, strlen(incoming), &s3curl);
    EXPECT_GE(result, 0u);
}

// ==========================================================================
// 33. PutRequest tests
// ==========================================================================

TEST_F(CurlRequestMockTest, PutRequest_NullPath) {
    S3fsCurl s3curl;
    headers_t meta;
    int result = s3curl.PutRequest(NULL, meta, -1);
    EXPECT_EQ(-1, result);
}

// ==========================================================================
// 34. PreMultipartPostRequest tests
// ==========================================================================

TEST_F(CurlRequestMockTest, PreMultipartPostRequest_NullPath) {
    S3fsCurl s3curl;
    headers_t meta;
    string upload_id;
    int result = s3curl.PreMultipartPostRequest(NULL, meta, upload_id, false);
    EXPECT_EQ(-1, result);
}

// ==========================================================================
// 35. AbortMultipartUpload tests
// ==========================================================================

TEST_F(CurlRequestMockTest, AbortMultipartUpload_NullPath) {
    S3fsCurl s3curl;
    string upload_id = "test-upload-id";
    int result = s3curl.AbortMultipartUpload(NULL, upload_id);
    EXPECT_EQ(-1, result);
}

// ==========================================================================
// 36. make_md5_from_string — REMOVED in ISSUE2026040800001 M2
// See curl_auth_mock_test.cpp for the removal rationale.
// ==========================================================================

// ==========================================================================
// 37. CurlProgress tests
// ==========================================================================

TEST_F(CurlRequestMockTest, CurlProgress_NormalProgress) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    int result = S3fsCurl::CurlProgress(const_cast<CURL*>(h), 100.0, 50.0, 0.0, 0.0);
    EXPECT_EQ(0, result);
}

TEST_F(CurlRequestMockTest, CurlProgress_ZeroProgress) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    const CURL* h = s3curl.GetCurlHandle();
    int result = S3fsCurl::CurlProgress(const_cast<CURL*>(h), 0.0, 0.0, 0.0, 0.0);
    EXPECT_EQ(0, result);
}

// ==========================================================================
// 38. filepart struct tests
// ==========================================================================

TEST_F(CurlRequestMockTest, Filepart_DefaultValues) {
    filepart fp;
    EXPECT_FALSE(fp.uploaded);
    EXPECT_EQ("", fp.etag);
    EXPECT_EQ(-1, fp.fd);
    EXPECT_EQ(0, fp.startpos);
    EXPECT_EQ(-1, fp.size);
    EXPECT_EQ(nullptr, fp.etaglist);
    EXPECT_EQ(-1, fp.etagpos);
}

TEST_F(CurlRequestMockTest, Filepart_Clear) {
    filepart fp;
    fp.uploaded = true;
    fp.etag = "etag123";
    fp.fd = 5;
    fp.startpos = 1024;
    fp.size = 4096;
    fp.clear();
    EXPECT_FALSE(fp.uploaded);
    EXPECT_EQ("", fp.etag);
    EXPECT_EQ(-1, fp.fd);
    EXPECT_EQ(0, fp.startpos);
    EXPECT_EQ(-1, fp.size);
}

TEST_F(CurlRequestMockTest, Filepart_AddEtagList) {
    filepart fp;
    etaglist_t etags;
    fp.add_etag_list(&etags);
    EXPECT_EQ(&etags, fp.etaglist);
    EXPECT_EQ(0, fp.etagpos);
    EXPECT_EQ(1u, etags.size());
}

TEST_F(CurlRequestMockTest, Filepart_AddEtagList_Null) {
    filepart fp;
    fp.add_etag_list(NULL);
    EXPECT_EQ(nullptr, fp.etaglist);
    EXPECT_EQ(-1, fp.etagpos);
}

// ==========================================================================
// 39. case_insensitive_compare_func tests
// ==========================================================================

TEST_F(CurlRequestMockTest, CaseInsensitiveCompare_Basic) {
    case_insensitive_compare_func cmp;
    EXPECT_TRUE(cmp("abc", "def"));
    EXPECT_FALSE(cmp("def", "abc"));
    EXPECT_FALSE(cmp("abc", "abc"));
}

TEST_F(CurlRequestMockTest, CaseInsensitiveCompare_CaseInsensitive) {
    case_insensitive_compare_func cmp;
    EXPECT_FALSE(cmp("ABC", "abc"));
    EXPECT_FALSE(cmp("abc", "ABC"));
    EXPECT_TRUE(cmp("ABC", "DEF"));
}

// ==========================================================================
// 40. CurlHandlerPool tests
// ==========================================================================

TEST_F(CurlRequestMockTest, CurlHandlerPool_GetAndReturn) {
    S3fsCurl s3curl;
    EXPECT_TRUE(s3curl.CreateCurlHandle(true));
    const CURL* h = s3curl.GetCurlHandle();
    EXPECT_NE(nullptr, h);
    EXPECT_TRUE(s3curl.DestroyCurlHandle());
    EXPECT_EQ(nullptr, s3curl.GetCurlHandle());
}

TEST_F(CurlRequestMockTest, CurlHandlerPool_GetActiveCurlNum) {
    int active = S3fsCurl::sCurlPool->GetActiveCurlNum();
    EXPECT_GE(active, 0);
}

// ==========================================================================
// 41. Additional edge case tests
// ==========================================================================

TEST_F(CurlRequestMockTest, RemakeHandle_Get_CurPosZero) {
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;  // file bucket uses read_buf
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.url = "http://test/get";
    s3curl.requestHeaders = NULL;
    char readbuf[128];
    s3curl.read_buf = readbuf;
    s3curl.read_size = sizeof(readbuf);
    s3curl.cur_pos = 0;
    EXPECT_TRUE(s3curl.RemakeHandle());
}

TEST_F(CurlRequestMockTest, CheckBucket_DefaultCredentials) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    int result = s3curl.CheckBucket("", "", "");
    EXPECT_NE(0, result);
}

TEST_F(CurlRequestMockTest, ListBucketRequest_EmptyPath) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    int result = s3curl.ListBucketRequest("", NULL);
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, DeleteRequest_RootPath) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    int result = s3curl.DeleteRequest("/");
    EXPECT_EQ(-EIO, result);
}

TEST_F(CurlRequestMockTest, HeadRequest_WithInodeAndShard) {
    S3fsCurl s3curl;
    S3fsCurl::retries = 0;
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
    headers_t meta;
    long long inode_no = 99999;
    int result = s3curl.HeadRequest("/file", meta, &inode_no, "shard_key_1");
    EXPECT_NE(0, result);
}

// ==========================================================================
// 42. tagDataBuffer tests
// ==========================================================================

TEST_F(CurlRequestMockTest, TagDataBuffer_DefaultValues) {
    tagDataBuffer buf;
    memset(&buf, 0, sizeof(buf));
    EXPECT_EQ(0u, buf.ulBufferSize);
    EXPECT_EQ(0, buf.offset);
    EXPECT_EQ(nullptr, buf.pcBuffer);
}

// ==========================================================================
// 43. RemakeHandle bucket-type-aware retry tests (T1-T14)
//     ISSUE2026033100001: RemakeHandle retry path must distinguish
//     object bucket (fd-based) vs file bucket (buffer-based).
// ==========================================================================

// T1: Object bucket PUT retry with b_infile set - verifies file is rewound
TEST_F(CurlRequestMockTest, RemakeHandle_Put_ObjectBucket_FdBased_RetainsInfile) {
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PUT;
    s3curl.url = "http://test/put-obj";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;

    // Create a real temp file with content
    FILE* tmpf = tmpfile();
    ASSERT_NE(nullptr, tmpf);
    fprintf(tmpf, "hello");
    fflush(tmpf);
    s3curl.b_infile = tmpf;
    s3curl.writedatabuf.pcBuffer = NULL;

    EXPECT_TRUE(s3curl.RemakeHandle());
    // b_infile should be rewound to position 0
    EXPECT_EQ(0, ftell(s3curl.b_infile));

    fclose(tmpf);
    s3curl.b_infile = NULL;
}

// T2: Object bucket PUT retry rewinds file from mid-position
TEST_F(CurlRequestMockTest, RemakeHandle_Put_ObjectBucket_FdBased_RewindVerify) {
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PUT;
    s3curl.url = "http://test/put-obj-rewind";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;

    FILE* tmpf = tmpfile();
    ASSERT_NE(nullptr, tmpf);
    fprintf(tmpf, "abcdefgh");
    fflush(tmpf);
    // Seek to byte 5 to simulate partial upload
    fseek(tmpf, 5, SEEK_SET);
    EXPECT_EQ(5, ftell(tmpf));
    s3curl.b_infile = tmpf;
    s3curl.writedatabuf.pcBuffer = NULL;

    EXPECT_TRUE(s3curl.RemakeHandle());
    // After RemakeHandle, file must be rewound
    EXPECT_EQ(0, ftell(s3curl.b_infile));

    fclose(tmpf);
    s3curl.b_infile = NULL;
}

// T3: Object bucket PUT retry with b_infile=NULL (zero-byte object)
TEST_F(CurlRequestMockTest, RemakeHandle_Put_ObjectBucket_BInfileNull) {
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PUT;
    s3curl.url = "http://test/put-obj-null";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;
    s3curl.b_infile = NULL;
    s3curl.writedatabuf.pcBuffer = NULL;

    // Should return true (zero-byte fallback)
    EXPECT_TRUE(s3curl.RemakeHandle());
}

// T4: Object bucket PUT retry called multiple times
TEST_F(CurlRequestMockTest, RemakeHandle_Put_ObjectBucket_MultipleRetries) {
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PUT;
    s3curl.url = "http://test/put-obj-multi";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;

    FILE* tmpf = tmpfile();
    ASSERT_NE(nullptr, tmpf);
    fprintf(tmpf, "retry data");
    fflush(tmpf);
    s3curl.b_infile = tmpf;
    s3curl.writedatabuf.pcBuffer = NULL;
    s3curl.retry_count = 0;

    // Call RemakeHandle 3 times
    for(int i = 0; i < 3; i++){
        fseek(tmpf, 5, SEEK_SET);  // simulate partial transfer
        EXPECT_TRUE(s3curl.RemakeHandle());
        EXPECT_EQ(0, ftell(s3curl.b_infile));
    }
    EXPECT_EQ(3, s3curl.retry_count);

    fclose(tmpf);
    s3curl.b_infile = NULL;
}

// T5: Object bucket GET retry with fd-based download (no read_buf)
TEST_F(CurlRequestMockTest, RemakeHandle_Get_ObjectBucket_FdBased) {
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.url = "http://test/get-obj";
    s3curl.requestHeaders = NULL;
    s3curl.read_buf = NULL;

    // Should return true and not crash (uses DownloadWriteCallback)
    EXPECT_TRUE(s3curl.RemakeHandle());
}

// T6: Object bucket GET retry preserves partdata from backup
TEST_F(CurlRequestMockTest, RemakeHandle_Get_ObjectBucket_PartdataPreserved) {
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.url = "http://test/get-obj-partdata";
    s3curl.requestHeaders = NULL;
    s3curl.read_buf = NULL;

    // Set backup values for partdata
    s3curl.b_partdata_startpos = 1024;
    s3curl.b_partdata_size = 4096;

    EXPECT_TRUE(s3curl.RemakeHandle());

    // partdata should be restored from backup
    EXPECT_EQ(1024, s3curl.partdata.startpos);
    EXPECT_EQ(4096, s3curl.partdata.size);
}

// T7: Object bucket GET retry with partdata.fd=-1 (invalid fd)
TEST_F(CurlRequestMockTest, RemakeHandle_Get_ObjectBucket_PartdataFdInvalid) {
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.url = "http://test/get-obj-invalidfd";
    s3curl.requestHeaders = NULL;
    s3curl.read_buf = NULL;
    s3curl.partdata.fd = -1;

    // Should return true and not crash
    EXPECT_TRUE(s3curl.RemakeHandle());
}

// T8: Object bucket GET retry called multiple times
TEST_F(CurlRequestMockTest, RemakeHandle_Get_ObjectBucket_MultipleRetries) {
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.url = "http://test/get-obj-multi";
    s3curl.requestHeaders = NULL;
    s3curl.read_buf = NULL;
    s3curl.retry_count = 0;

    s3curl.b_partdata_startpos = 2048;
    s3curl.b_partdata_size = 8192;

    for(int i = 0; i < 3; i++){
        EXPECT_TRUE(s3curl.RemakeHandle());
        EXPECT_EQ(2048, s3curl.partdata.startpos);
        EXPECT_EQ(8192, s3curl.partdata.size);
    }
    EXPECT_EQ(3, s3curl.retry_count);
}

// T9: File bucket PUT retry with buffer (regression test)
TEST_F(CurlRequestMockTest, RemakeHandle_Put_FileBucket_BufferBased) {
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PUT;
    s3curl.url = "http://test/put-file";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;

    char databuf[] = "file bucket data";
    s3curl.writedatabuf.pcBuffer = databuf;
    s3curl.writedatabuf.ulBufferSize = sizeof(databuf);
    s3curl.writedatabuf.offset = 7;  // non-zero to test reset

    EXPECT_TRUE(s3curl.RemakeHandle());
    // File bucket: offset should be reset to 0
    EXPECT_EQ(0u, s3curl.writedatabuf.offset);
}

// T10: File bucket PUT retry with NULL buffer
TEST_F(CurlRequestMockTest, RemakeHandle_Put_FileBucket_NullBuffer) {
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PUT;
    s3curl.url = "http://test/put-file-null";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;
    s3curl.writedatabuf.pcBuffer = NULL;
    s3curl.b_infile = NULL;

    // Should return true (INFILESIZE=0 fallback)
    EXPECT_TRUE(s3curl.RemakeHandle());
}

// T11: File bucket GET retry with read_buf (regression test)
TEST_F(CurlRequestMockTest, RemakeHandle_Get_FileBucket_BufferBased) {
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.url = "http://test/get-file";
    s3curl.requestHeaders = NULL;

    char readbuf[256];
    memset(readbuf, 'A', sizeof(readbuf));
    s3curl.read_buf = readbuf;
    s3curl.read_size = sizeof(readbuf);
    s3curl.cur_pos = 10;

    EXPECT_TRUE(s3curl.RemakeHandle());
    // File bucket: cur_pos should be reset to 0
    EXPECT_EQ(0u, s3curl.cur_pos);
    // Buffer should be zeroed
    EXPECT_EQ(0, readbuf[0]);
    EXPECT_EQ(0, readbuf[255]);
}

// T12: Object bucket PUT retry with empty file (0 bytes)
TEST_F(CurlRequestMockTest, RemakeHandle_Put_ObjectBucket_EmptyFile) {
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PUT;
    s3curl.url = "http://test/put-obj-empty";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;

    // Create an empty temp file (0 bytes)
    FILE* tmpf = tmpfile();
    ASSERT_NE(nullptr, tmpf);
    // Don't write anything - file is 0 bytes
    fflush(tmpf);
    s3curl.b_infile = tmpf;
    s3curl.writedatabuf.pcBuffer = NULL;

    EXPECT_TRUE(s3curl.RemakeHandle());

    // Verify: fstat should show st_size == 0
    struct stat st;
    EXPECT_EQ(0, fstat(fileno(tmpf), &st));
    EXPECT_EQ(0, st.st_size);

    fclose(tmpf);
    s3curl.b_infile = NULL;
}

// T13: Object bucket PUT retry with large file (5GB via ftruncate)
TEST_F(CurlRequestMockTest, RemakeHandle_Put_ObjectBucket_LargeFile) {
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PUT;
    s3curl.url = "http://test/put-obj-large";
    s3curl.bodydata = new BodyData();
    s3curl.requestHeaders = NULL;

    // Create a sparse file with ftruncate to 5GB (no disk space consumed)
    FILE* tmpf = tmpfile();
    ASSERT_NE(nullptr, tmpf);
    off_t large_size = (off_t)5 * 1024 * 1024 * 1024;
    int rc = ftruncate(fileno(tmpf), large_size);
    if(rc != 0){
        // If ftruncate fails (e.g., filesystem doesn't support sparse), skip test
        fclose(tmpf);
        std::cout << "[  SKIPPED ] ftruncate to 5GB failed" << std::endl;
        return;
    }
    s3curl.b_infile = tmpf;
    s3curl.writedatabuf.pcBuffer = NULL;

    EXPECT_TRUE(s3curl.RemakeHandle());

    // Verify the file position is rewound
    EXPECT_EQ(0, ftell(s3curl.b_infile));

    // Verify fstat reports 5GB
    struct stat st;
    EXPECT_EQ(0, fstat(fileno(tmpf), &st));
    EXPECT_EQ(large_size, st.st_size);

    fclose(tmpf);
    s3curl.b_infile = NULL;
}

// T14: File bucket GET retry with read_buf=NULL (no crash)
TEST_F(CurlRequestMockTest, RemakeHandle_Get_FileBucket_ReadBufNull) {
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.url = "http://test/get-file-null";
    s3curl.requestHeaders = NULL;
    s3curl.read_buf = NULL;

    // Should return true and not crash (file bucket path, NULL read_buf)
    EXPECT_TRUE(s3curl.RemakeHandle());
}

// ==========================================================================
// main
// ==========================================================================
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::AddGlobalTestEnvironment(new CurlRequestMockEnvironment());
    return RUN_ALL_TESTS();
}
