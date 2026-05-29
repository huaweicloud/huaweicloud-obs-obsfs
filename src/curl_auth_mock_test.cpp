// ==========================================================================
// curl_auth_mock_test.cpp
//
// Supplemental mock tests for curl.cpp covering IAM auth, signature,
// SSE, CurlHandlerPool, RemakeHandle, and other helper functions.
// Complements curl_extended_test.cpp with additional branch coverage.
//
// Approach:
//   1. Pre-include STL headers before #define private public
//   2. #include "curl.cpp" to access private/static members
//   3. Crypto/auth stubs from test_stubs.o
// ==========================================================================

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
#include <fstream>

#define private public
#define protected public
#include "curl.cpp"
#undef private
#undef protected

#include <libxml/parser.h>
#include <libxml/tree.h>

#include "gtest.h"

// ==========================================================================
// Mockable crypto functions for Task B tests (ISSUE2026040300001)
// Override weak symbols from test_stubs.o with controllable behavior
// ==========================================================================
static bool g_mock_sha256_fail = false;
static bool g_mock_hmac256_fail = false;

bool s3fs_sha256(const unsigned char* data, unsigned int data_len,
                 unsigned char** digest, unsigned int* digest_len) {
    if(g_mock_sha256_fail){
        *digest = NULL;
        *digest_len = 0;
        return false;
    }
    (void)data; (void)data_len;
    *digest_len = 32;
    *digest = (unsigned char*)malloc(32);
    if(*digest) memset(*digest, 0, 32);
    return *digest != nullptr;
}

bool s3fs_HMAC256(const void* key, size_t key_len,
                  const unsigned char* data, size_t data_len,
                  unsigned char** digest, unsigned int* digest_len) {
    if(g_mock_hmac256_fail){
        *digest = NULL;
        *digest_len = 0;
        return false;
    }
    (void)key; (void)key_len; (void)data; (void)data_len;
    *digest_len = 32;
    *digest = (unsigned char*)malloc(32);
    if(*digest) memset(*digest, 0, 32);
    return *digest != nullptr;
}

using namespace std;

// ==========================================================================
// Test Fixture - saves/restores all relevant global state
// ==========================================================================
class CurlAuthMockTest : public ::testing::Test {
protected:
    void SetUp() override {
        saved_bucket = bucket;
        saved_host = host;
        saved_pathrequeststyle = pathrequeststyle;
        saved_is_sigv4 = S3fsCurl::is_sigv4;
        saved_is_ibm_iam_auth = S3fsCurl::is_ibm_iam_auth;
        saved_is_ecs = S3fsCurl::is_ecs;
        saved_iam_role = S3fsCurl::IAM_role;
        saved_iam_field_count = S3fsCurl::IAM_field_count;
        saved_iam_token_field = S3fsCurl::IAM_token_field;
        saved_iam_expiry_field = S3fsCurl::IAM_expiry_field;
        saved_ssetype = S3fsCurl::ssetype;
        saved_sseckeys = S3fsCurl::sseckeys;
        saved_ssekmsid = S3fsCurl::ssekmsid;
        saved_is_content_md5 = S3fsCurl::is_content_md5;
        saved_is_verbose = S3fsCurl::is_verbose;
        saved_is_ua = S3fsCurl::is_ua;
        saved_is_cert_check = S3fsCurl::is_cert_check;
        saved_is_dns_cache = S3fsCurl::is_dns_cache;
        saved_is_ssl_session_cache = S3fsCurl::is_ssl_session_cache;
        saved_is_public_bucket = S3fsCurl::is_public_bucket;
        saved_ak = S3fsCurl::AWSAccessKeyId;
        saved_sk = S3fsCurl::AWSSecretAccessKey;
        saved_token = S3fsCurl::AWSAccessToken;
        saved_expire = S3fsCurl::AWSAccessTokenExpire;
        saved_endpoint = endpoint;

        // Set defaults for tests
        S3fsCurl::AWSAccessKeyId = "AKID_TEST";
        S3fsCurl::AWSSecretAccessKey = "SECRET_TEST";
        S3fsCurl::AWSAccessToken = "";
        S3fsCurl::is_ibm_iam_auth = false;
        S3fsCurl::is_ecs = false;
        S3fsCurl::IAM_role = "";
        S3fsCurl::is_public_bucket = false;
        bucket = "test-bucket";
        host = "https://obs.example.com";
        endpoint = "us-east-1";
    }

    void TearDown() override {
        bucket = saved_bucket;
        host = saved_host;
        pathrequeststyle = saved_pathrequeststyle;
        S3fsCurl::is_sigv4 = saved_is_sigv4;
        S3fsCurl::is_ibm_iam_auth = saved_is_ibm_iam_auth;
        S3fsCurl::is_ecs = saved_is_ecs;
        S3fsCurl::IAM_role = saved_iam_role;
        S3fsCurl::IAM_field_count = saved_iam_field_count;
        S3fsCurl::IAM_token_field = saved_iam_token_field;
        S3fsCurl::IAM_expiry_field = saved_iam_expiry_field;
        S3fsCurl::ssetype = saved_ssetype;
        S3fsCurl::sseckeys = saved_sseckeys;
        S3fsCurl::ssekmsid = saved_ssekmsid;
        S3fsCurl::is_content_md5 = saved_is_content_md5;
        S3fsCurl::is_verbose = saved_is_verbose;
        S3fsCurl::is_ua = saved_is_ua;
        S3fsCurl::is_cert_check = saved_is_cert_check;
        S3fsCurl::is_dns_cache = saved_is_dns_cache;
        S3fsCurl::is_ssl_session_cache = saved_is_ssl_session_cache;
        S3fsCurl::is_public_bucket = saved_is_public_bucket;
        S3fsCurl::AWSAccessKeyId = saved_ak;
        S3fsCurl::AWSSecretAccessKey = saved_sk;
        S3fsCurl::AWSAccessToken = saved_token;
        S3fsCurl::AWSAccessTokenExpire = saved_expire;
        endpoint = saved_endpoint;
    }

    string saved_bucket, saved_host, saved_iam_role;
    string saved_iam_token_field, saved_iam_expiry_field;
    string saved_ssekmsid, saved_ak, saved_sk, saved_token;
    string saved_endpoint;
    bool saved_pathrequeststyle, saved_is_sigv4, saved_is_ibm_iam_auth;
    bool saved_is_ecs, saved_is_content_md5, saved_is_verbose;
    bool saved_is_ua, saved_is_cert_check, saved_is_dns_cache;
    bool saved_is_ssl_session_cache, saved_is_public_bucket;
    size_t saved_iam_field_count;
    sse_type_t saved_ssetype;
    sseckeylist_t saved_sseckeys;
    time_t saved_expire;
};

// ==========================================================================
// 1. CalcSignatureV2 - additional branch coverage
// ==========================================================================

TEST_F(CurlAuthMockTest, CalcSignatureV2_EmptyMethod) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignatureV2("", "", "", "date", "/resource");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlAuthMockTest, CalcSignatureV2_WithToken) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    S3fsCurl::AWSAccessToken = "session-token-123";
    string sig = s3curl.CalcSignatureV2(
        "GET", "", "application/xml",
        "Thu, 01 Jan 2026 00:00:00 GMT", "/bucket/key");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlAuthMockTest, CalcSignatureV2_NoToken) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    S3fsCurl::AWSAccessToken = "";
    string sig = s3curl.CalcSignatureV2(
        "GET", "md5val", "text/html",
        "Thu, 01 Jan 2026 00:00:00 GMT", "/bucket/key",
        "", "");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlAuthMockTest, CalcSignatureV2_HEADMethod) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignatureV2(
        "HEAD", "", "", "date", "/bucket/key");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlAuthMockTest, CalcSignatureV2_DELETEMethod) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignatureV2(
        "DELETE", "", "", "date", "/bucket/key");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlAuthMockTest, CalcSignatureV2_ContentTypeOctetStream) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignatureV2(
        "PUT", "", "application/octet-stream", "date", "/bucket/key");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlAuthMockTest, CalcSignatureV2_ContentTypeDirectory) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignatureV2(
        "PUT", "", "application/x-directory", "date", "/bucket/dir/");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

// ==========================================================================
// 2. CalcSignature (V4) - additional branch coverage
// ==========================================================================

TEST_F(CurlAuthMockTest, CalcSignatureV4_GETWithoutSlashUri) {
    // Branch: GET + uriencode not starting with "/" -> "/\n" + urlEncode2
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignature(
        "GET", "no-slash-path", "",
        "20260101", "UNSIGNED-PAYLOAD", "20260101T000000Z");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlAuthMockTest, CalcSignatureV4_EmptyCanonicalUri) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignature(
        "PUT", "", "",
        "20260101", "payload-hash", "20260101T000000Z");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlAuthMockTest, CalcSignatureV4_WithQueryString) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignature(
        "DELETE", "/bucket/key", "versionId=abc123",
        "20260101", "UNSIGNED-PAYLOAD", "20260101T000000Z");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlAuthMockTest, CalcSignatureV4_WithToken) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    S3fsCurl::AWSAccessToken = "session-token-v4";
    string sig = s3curl.CalcSignature(
        "GET", "/bucket/key", "",
        "20260101", "UNSIGNED-PAYLOAD", "20260101T000000Z");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlAuthMockTest, CalcSignatureV4_NoToken) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    S3fsCurl::AWSAccessToken = "";
    string sig = s3curl.CalcSignature(
        "GET", "/bucket/key", "",
        "20260101", "UNSIGNED-PAYLOAD", "20260101T000000Z",
        "", "");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlAuthMockTest, CalcSignatureV4_CustomSk) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignature(
        "HEAD", "/bucket/key", "",
        "20260101", "UNSIGNED-PAYLOAD", "20260101T000000Z",
        "custom-secret-key", "");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

// ==========================================================================
// 3. insertV4Headers - various request types and options
// ==========================================================================

TEST_F(CurlAuthMockTest, InsertV4Headers_Default) {
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "GET";
    s3curl.path = "/test/path";
    s3curl.query_string = "";
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.writedatabuf.pcBuffer = NULL;
    s3curl.writedatabuf.offset = 0;
    s3curl.writedatabuf.ulBufferSize = 0;

    s3curl.insertV4Headers();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

TEST_F(CurlAuthMockTest, InsertV4Headers_WithCustomAkSkToken) {
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "GET";
    s3curl.path = "/test/path";
    s3curl.query_string = "";
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.writedatabuf.pcBuffer = NULL;
    s3curl.writedatabuf.offset = 0;
    s3curl.writedatabuf.ulBufferSize = 0;

    s3curl.insertV4Headers("custom-ak", "custom-sk", "custom-token");
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

TEST_F(CurlAuthMockTest, InsertV4Headers_ListBucketType) {
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "GET";
    s3curl.path = "/some/path";
    s3curl.query_string = "prefix=test";
    s3curl.type = S3fsCurl::REQTYPE_LISTBUCKET;
    s3curl.writedatabuf.pcBuffer = NULL;
    s3curl.writedatabuf.offset = 0;
    s3curl.writedatabuf.ulBufferSize = 0;

    s3curl.insertV4Headers();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

TEST_F(CurlAuthMockTest, InsertV4Headers_PutType) {
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "PUT";
    s3curl.path = "/test/file";
    s3curl.query_string = "";
    s3curl.type = S3fsCurl::REQTYPE_PUT;
    char testbuf[] = "hello";
    s3curl.writedatabuf.pcBuffer = testbuf;
    s3curl.writedatabuf.offset = 0;
    s3curl.writedatabuf.ulBufferSize = 5;

    s3curl.insertV4Headers();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

TEST_F(CurlAuthMockTest, InsertV4Headers_PreMultiPost) {
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "POST";
    s3curl.path = "/test/file";
    s3curl.query_string = "uploads";
    s3curl.type = S3fsCurl::REQTYPE_PREMULTIPOST;
    s3curl.writedatabuf.pcBuffer = NULL;
    s3curl.writedatabuf.offset = 0;
    s3curl.writedatabuf.ulBufferSize = 0;

    s3curl.insertV4Headers();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

TEST_F(CurlAuthMockTest, InsertV4Headers_PublicBucket) {
    S3fsCurl::is_public_bucket = true;
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "GET";
    s3curl.path = "/test/path";
    s3curl.query_string = "";
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.writedatabuf.pcBuffer = NULL;
    s3curl.writedatabuf.offset = 0;
    s3curl.writedatabuf.ulBufferSize = 0;

    s3curl.insertV4Headers();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

// ==========================================================================
// 4. insertV2Headers - additional coverage
// ==========================================================================

TEST_F(CurlAuthMockTest, InsertV2Headers_WithCustomAkSkToken) {
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "GET";
    s3curl.path = "/test/path";
    s3curl.query_string = "";
    s3curl.type = S3fsCurl::REQTYPE_GET;

    s3curl.insertV2Headers("custom-ak", "custom-sk", "custom-token");
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

TEST_F(CurlAuthMockTest, InsertV2Headers_PUTMethod) {
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "PUT";
    s3curl.path = "/test/file";
    s3curl.query_string = "";
    s3curl.type = S3fsCurl::REQTYPE_PUT;

    s3curl.insertV2Headers();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

TEST_F(CurlAuthMockTest, InsertV2Headers_PublicBucket) {
    S3fsCurl::is_public_bucket = true;
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "GET";
    s3curl.path = "/test/path";
    s3curl.query_string = "";
    s3curl.type = S3fsCurl::REQTYPE_GET;

    s3curl.insertV2Headers();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

// ==========================================================================
// 5. insertIBMIAMHeaders
// ==========================================================================

TEST_F(CurlAuthMockTest, InsertIBMIAMHeaders_WithToken) {
    S3fsCurl::AWSAccessToken = "ibm-bearer-token";
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    s3curl.op = "GET";
    s3curl.path = "/test/path";

    s3curl.insertIBMIAMHeaders();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlAuthMockTest, InsertIBMIAMHeaders_BucketCreation) {
    S3fsCurl::AWSAccessToken = "ibm-bearer-token";
    S3fsCurl::AWSAccessKeyId = "ibm-service-id";
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    s3curl.op = "PUT";
    s3curl.path = mount_prefix + "/";

    s3curl.insertIBMIAMHeaders();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlAuthMockTest, InsertIBMIAMHeaders_NonBucketCreation) {
    S3fsCurl::AWSAccessToken = "ibm-bearer-token";
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    s3curl.op = "PUT";
    s3curl.path = "/some/other/path";

    s3curl.insertIBMIAMHeaders();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

// ==========================================================================
// 6. insertAuthHeaders - dispatch coverage
// ==========================================================================

TEST_F(CurlAuthMockTest, InsertAuthHeaders_IBMIAMPath) {
    S3fsCurl::is_ibm_iam_auth = true;
    S3fsCurl::AWSAccessToken = "ibm-token";
    S3fsCurl::AWSAccessTokenExpire = time(NULL) + 86400;
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    s3curl.op = "GET";
    s3curl.path = "/test/path";

    s3curl.insertAuthHeaders();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlAuthMockTest, InsertAuthHeaders_V2Path) {
    S3fsCurl::is_sigv4 = false;
    S3fsCurl::is_ibm_iam_auth = false;
    S3fsCurl::AWSAccessTokenExpire = time(NULL) + 86400;
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    s3curl.requestHeaders = NULL;
    s3curl.op = "GET";
    s3curl.path = "/test/path";
    s3curl.query_string = "";
    s3curl.type = S3fsCurl::REQTYPE_GET;

    s3curl.insertAuthHeaders();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

TEST_F(CurlAuthMockTest, InsertAuthHeaders_V4Path) {
    S3fsCurl::is_sigv4 = true;
    S3fsCurl::is_ibm_iam_auth = false;
    S3fsCurl::AWSAccessTokenExpire = time(NULL) + 86400;
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    s3curl.requestHeaders = NULL;
    s3curl.op = "GET";
    s3curl.path = "/test/path";
    s3curl.query_string = "";
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.writedatabuf.pcBuffer = NULL;
    s3curl.writedatabuf.offset = 0;
    s3curl.writedatabuf.ulBufferSize = 0;

    s3curl.insertAuthHeaders();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

TEST_F(CurlAuthMockTest, InsertAuthHeaders_WithCustomAkSkToken) {
    S3fsCurl::is_sigv4 = false;
    S3fsCurl::is_ibm_iam_auth = false;
    S3fsCurl::AWSAccessTokenExpire = time(NULL) + 86400;
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    s3curl.requestHeaders = NULL;
    s3curl.op = "GET";
    s3curl.path = "/test/path";
    s3curl.query_string = "";
    s3curl.type = S3fsCurl::REQTYPE_GET;

    s3curl.insertAuthHeaders("ak", "sk", "tok");
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

// ==========================================================================
// 7. ParseIAMCredentialResponse - additional edge cases
// ==========================================================================

TEST_F(CurlAuthMockTest, ParseIAMCredentialResponse_PartialFields) {
    iamcredmap_t keyval;
    const char* response = "\"AccessKeyId\" : \"AKID_ONLY\"";
    EXPECT_TRUE(S3fsCurl::ParseIAMCredentialResponse(response, keyval));
    EXPECT_EQ(1u, keyval.size());
    EXPECT_EQ("AKID_ONLY", keyval["AccessKeyId"]);
}

TEST_F(CurlAuthMockTest, ParseIAMCredentialResponse_EmptyValues) {
    iamcredmap_t keyval;
    const char* response =
        "\"AccessKeyId\" : \"\" , \"SecretAccessKey\" : \"\"";
    EXPECT_TRUE(S3fsCurl::ParseIAMCredentialResponse(response, keyval));
    EXPECT_EQ("", keyval["AccessKeyId"]);
    EXPECT_EQ("", keyval["SecretAccessKey"]);
}

TEST_F(CurlAuthMockTest, ParseIAMCredentialResponse_IBMIAMFormat) {
    S3fsCurl::is_ibm_iam_auth = true;
    S3fsCurl::IAM_token_field = "access_token";
    S3fsCurl::IAM_expiry_field = "expiration";

    iamcredmap_t keyval;
    const char* response =
        "{ \"access_token\" : \"ibm-token-value\" , "
        "\"expiration\" : 1735689600 }";
    EXPECT_TRUE(S3fsCurl::ParseIAMCredentialResponse(response, keyval));
    EXPECT_EQ("ibm-token-value", keyval["access_token"]);
    EXPECT_EQ("1735689600", keyval["expiration"]);
}

TEST_F(CurlAuthMockTest, ParseIAMCredentialResponse_IBMExpiryNoDigits) {
    S3fsCurl::is_ibm_iam_auth = true;
    S3fsCurl::IAM_expiry_field = "expiration";

    iamcredmap_t keyval;
    const char* response = "\"expiration\" : no_digits_here";
    EXPECT_TRUE(S3fsCurl::ParseIAMCredentialResponse(response, keyval));
    EXPECT_EQ(0u, keyval.count("expiration"));
}

TEST_F(CurlAuthMockTest, ParseIAMCredentialResponse_MultipleCommas) {
    iamcredmap_t keyval;
    const char* response =
        "\"AccessKeyId\" : \"AK1\" , , \"SecretAccessKey\" : \"SK1\"";
    EXPECT_TRUE(S3fsCurl::ParseIAMCredentialResponse(response, keyval));
    EXPECT_EQ("AK1", keyval["AccessKeyId"]);
    EXPECT_EQ("SK1", keyval["SecretAccessKey"]);
}

// ==========================================================================
// 8. SetIAMCredentials - additional paths
// ==========================================================================

TEST_F(CurlAuthMockTest, SetIAMCredentials_EmptyResponse) {
    EXPECT_FALSE(S3fsCurl::SetIAMCredentials(""));
}

TEST_F(CurlAuthMockTest, SetIAMCredentials_IBMIAMPath) {
    S3fsCurl::is_ibm_iam_auth = true;
    S3fsCurl::IAM_field_count = 2;
    S3fsCurl::IAM_token_field = "access_token";
    S3fsCurl::IAM_expiry_field = "expiration";

    const char* response =
        "{ \"access_token\" : \"ibm-tok\" , \"expiration\" : 9999999999 }";
    bool result = S3fsCurl::SetIAMCredentials(response);
    EXPECT_TRUE(result);
    EXPECT_EQ("ibm-tok", S3fsCurl::AWSAccessToken);
}

TEST_F(CurlAuthMockTest, SetIAMCredentials_MissingFields) {
    S3fsCurl::IAM_field_count = 4;
    S3fsCurl::IAM_token_field = "Token";
    S3fsCurl::IAM_expiry_field = "Expiration";

    const char* response =
        "\"AccessKeyId\" : \"AK\" , \"SecretAccessKey\" : \"SK\"";
    EXPECT_FALSE(S3fsCurl::SetIAMCredentials(response));
}

// ==========================================================================
// 9. ParseIAMRoleFromMetaDataResponse - additional
// ==========================================================================

TEST_F(CurlAuthMockTest, ParseIAMRoleFromMetaData_WhitespaceRole) {
    string rolename;
    bool result = S3fsCurl::ParseIAMRoleFromMetaDataResponse("  role-with-spaces  \n", rolename);
    EXPECT_TRUE(result);
    EXPECT_EQ("  role-with-spaces  ", rolename);
}

// ==========================================================================
// 10. SetIAMRoleFromMetaData - additional
// ==========================================================================

TEST_F(CurlAuthMockTest, SetIAMRoleFromMetaData_MultiLine) {
    string old_role = S3fsCurl::IAM_role;
    bool result = S3fsCurl::SetIAMRoleFromMetaData("first-role\nsecond-role\n");
    EXPECT_TRUE(result);
    EXPECT_STREQ("first-role", S3fsCurl::GetIAMRole());
    S3fsCurl::SetIAMRole(old_role.c_str());
}

// ==========================================================================
// 11. CheckIAMCredentialUpdate - additional paths
// ==========================================================================

TEST_F(CurlAuthMockTest, CheckIAMCredentialUpdate_ECSPath) {
    S3fsCurl::IAM_role = "";
    S3fsCurl::is_ecs = true;
    S3fsCurl::is_ibm_iam_auth = false;
    S3fsCurl::AWSAccessTokenExpire = time(NULL) + 86400;
    EXPECT_TRUE(S3fsCurl::CheckIAMCredentialUpdate());
}

TEST_F(CurlAuthMockTest, CheckIAMCredentialUpdate_IBMIAMPath) {
    S3fsCurl::IAM_role = "";
    S3fsCurl::is_ecs = false;
    S3fsCurl::is_ibm_iam_auth = true;
    S3fsCurl::AWSAccessTokenExpire = time(NULL) + 86400;
    EXPECT_TRUE(S3fsCurl::CheckIAMCredentialUpdate());
}

// ==========================================================================
// 12. SSE - FinalCheckSse
// ==========================================================================

TEST_F(CurlAuthMockTest, FinalCheckSse_Disable) {
    S3fsCurl::ssetype = SSE_DISABLE;
    S3fsCurl::ssekmsid = "should-be-cleared";
    EXPECT_TRUE(S3fsCurl::FinalCheckSse());
    EXPECT_TRUE(S3fsCurl::ssekmsid.empty());
}

TEST_F(CurlAuthMockTest, FinalCheckSse_S3) {
    S3fsCurl::ssetype = SSE_S3;
    S3fsCurl::ssekmsid = "should-be-cleared";
    EXPECT_TRUE(S3fsCurl::FinalCheckSse());
    EXPECT_TRUE(S3fsCurl::ssekmsid.empty());
}

TEST_F(CurlAuthMockTest, FinalCheckSse_C_WithKeys) {
    S3fsCurl::ssetype = SSE_C;
    sseckeymap_t md5map;
    md5map["test-md5"] = "test-key";
    S3fsCurl::sseckeys.push_back(md5map);
    EXPECT_TRUE(S3fsCurl::FinalCheckSse());
    EXPECT_TRUE(S3fsCurl::ssekmsid.empty());
}

TEST_F(CurlAuthMockTest, FinalCheckSse_C_NoKeys) {
    S3fsCurl::ssetype = SSE_C;
    S3fsCurl::sseckeys.clear();
    EXPECT_FALSE(S3fsCurl::FinalCheckSse());
}

TEST_F(CurlAuthMockTest, FinalCheckSse_KMS_ValidV4) {
    S3fsCurl::ssetype = SSE_KMS;
    S3fsCurl::ssekmsid = "kms-key-id";
    S3fsCurl::is_sigv4 = true;
    EXPECT_TRUE(S3fsCurl::FinalCheckSse());
}

TEST_F(CurlAuthMockTest, FinalCheckSse_KMS_NoKmsId) {
    S3fsCurl::ssetype = SSE_KMS;
    S3fsCurl::ssekmsid = "";
    S3fsCurl::is_sigv4 = true;
    EXPECT_FALSE(S3fsCurl::FinalCheckSse());
}

TEST_F(CurlAuthMockTest, FinalCheckSse_KMS_NotV4) {
    S3fsCurl::ssetype = SSE_KMS;
    S3fsCurl::ssekmsid = "kms-key-id";
    S3fsCurl::is_sigv4 = false;
    EXPECT_FALSE(S3fsCurl::FinalCheckSse());
}

TEST_F(CurlAuthMockTest, FinalCheckSse_UnknownType) {
    S3fsCurl::ssetype = static_cast<sse_type_t>(999);
    EXPECT_FALSE(S3fsCurl::FinalCheckSse());
}

// ==========================================================================
// 13. SSE - SetSseKmsid, SetSseType, GetSseKeyCount
// ==========================================================================

TEST_F(CurlAuthMockTest, SetSseKmsid_Valid) {
    EXPECT_TRUE(S3fsCurl::SetSseKmsid("test-kms-id"));
    EXPECT_EQ("test-kms-id", string(S3fsCurl::GetSseKmsId()));
}

TEST_F(CurlAuthMockTest, SetSseKmsid_Null) {
    EXPECT_FALSE(S3fsCurl::SetSseKmsid(NULL));
}

TEST_F(CurlAuthMockTest, SetSseKmsid_Empty) {
    EXPECT_FALSE(S3fsCurl::SetSseKmsid(""));
}

TEST_F(CurlAuthMockTest, SetSseType_AllTypes) {
    sse_type_t old = S3fsCurl::SetSseType(SSE_S3);
    EXPECT_EQ(SSE_S3, S3fsCurl::ssetype);

    S3fsCurl::SetSseType(SSE_C);
    EXPECT_EQ(SSE_C, S3fsCurl::ssetype);

    S3fsCurl::SetSseType(SSE_KMS);
    EXPECT_EQ(SSE_KMS, S3fsCurl::ssetype);

    S3fsCurl::SetSseType(old);
}

TEST_F(CurlAuthMockTest, GetSseKeyCount_Empty) {
    S3fsCurl::sseckeys.clear();
    EXPECT_EQ(0, S3fsCurl::GetSseKeyCount());
}

TEST_F(CurlAuthMockTest, GetSseKeyCount_Multiple) {
    S3fsCurl::sseckeys.clear();
    sseckeymap_t m1, m2;
    m1["md5-1"] = "key-1";
    m2["md5-2"] = "key-2";
    S3fsCurl::sseckeys.push_back(m1);
    S3fsCurl::sseckeys.push_back(m2);
    EXPECT_EQ(2, S3fsCurl::GetSseKeyCount());
}

// ==========================================================================
// 14. SetAccessKey / GetAccessKeyAndToken / SetAccessKeyAndToken
// ==========================================================================

TEST_F(CurlAuthMockTest, SetAccessKey_Valid) {
    EXPECT_TRUE(S3fsCurl::SetAccessKey("myak", "mysk"));
    EXPECT_EQ("myak", S3fsCurl::AWSAccessKeyId);
    EXPECT_EQ("mysk", S3fsCurl::AWSSecretAccessKey);
}

TEST_F(CurlAuthMockTest, SetAccessKeyAndToken_Valid) {
    EXPECT_TRUE(S3fsCurl::SetAccessKeyAndToken("ak1", "sk1", "tok1"));
    EXPECT_EQ("ak1", S3fsCurl::AWSAccessKeyId);
    EXPECT_EQ("sk1", S3fsCurl::AWSSecretAccessKey);
    EXPECT_EQ("tok1", S3fsCurl::AWSAccessToken);
}

TEST_F(CurlAuthMockTest, SetAccessKeyAndToken_NullAccessKeyId) {
    EXPECT_FALSE(S3fsCurl::SetAccessKeyAndToken(NULL, "sk1", "tok1"));
}

TEST_F(CurlAuthMockTest, SetAccessKeyAndToken_EmptyAccessKeyId) {
    EXPECT_FALSE(S3fsCurl::SetAccessKeyAndToken("", "sk1", "tok1"));
}

TEST_F(CurlAuthMockTest, SetAccessKeyAndToken_NullSecretKey) {
    EXPECT_FALSE(S3fsCurl::SetAccessKeyAndToken("ak1", NULL, "tok1"));
}

TEST_F(CurlAuthMockTest, SetAccessKeyAndToken_EmptySecretKey) {
    EXPECT_FALSE(S3fsCurl::SetAccessKeyAndToken("ak1", "", "tok1"));
}

TEST_F(CurlAuthMockTest, SetAccessKeyAndToken_NullToken) {
    EXPECT_FALSE(S3fsCurl::SetAccessKeyAndToken("ak1", "sk1", NULL));
}

TEST_F(CurlAuthMockTest, SetAccessKeyAndToken_EmptyAKWithIBMIAM) {
    S3fsCurl::is_ibm_iam_auth = true;
    EXPECT_TRUE(S3fsCurl::SetAccessKeyAndToken("", "sk1", "tok1"));
}

TEST_F(CurlAuthMockTest, GetAccessKeyAndToken_Valid) {
    S3fsCurl::AWSAccessKeyId = "gettest-ak";
    S3fsCurl::AWSSecretAccessKey = "gettest-sk";
    S3fsCurl::AWSAccessToken = "gettest-tok";

    string ak, sk, tok;
    EXPECT_TRUE(S3fsCurl::GetAccessKeyAndToken(ak, sk, tok));
    EXPECT_EQ("gettest-ak", ak);
    EXPECT_EQ("gettest-sk", sk);
    EXPECT_EQ("gettest-tok", tok);
}

TEST_F(CurlAuthMockTest, GetAccessKeyAndToken_AKEmptySKNot) {
    S3fsCurl::AWSAccessKeyId = "";
    S3fsCurl::AWSSecretAccessKey = "sk-only";

    string ak, sk, tok;
    EXPECT_FALSE(S3fsCurl::GetAccessKeyAndToken(ak, sk, tok));
}

TEST_F(CurlAuthMockTest, GetAccessKeyAndToken_AKNotEmptySKEmpty) {
    S3fsCurl::AWSAccessKeyId = "ak-only";
    S3fsCurl::AWSSecretAccessKey = "";

    string ak, sk, tok;
    EXPECT_FALSE(S3fsCurl::GetAccessKeyAndToken(ak, sk, tok));
}

TEST_F(CurlAuthMockTest, GetAccessKeyAndToken_BothEmpty) {
    S3fsCurl::AWSAccessKeyId = "";
    S3fsCurl::AWSSecretAccessKey = "";

    string ak, sk, tok;
    EXPECT_TRUE(S3fsCurl::GetAccessKeyAndToken(ak, sk, tok));
    EXPECT_EQ("", ak);
    EXPECT_EQ("", sk);
}

// ==========================================================================
// 15. SetCheckCertificate / SetDnsCache / SetSslSessionCache
// ==========================================================================

TEST_F(CurlAuthMockTest, SetCheckCertificate_Toggle) {
    bool old = S3fsCurl::SetCheckCertificate(false);
    EXPECT_FALSE(S3fsCurl::is_cert_check);
    S3fsCurl::SetCheckCertificate(true);
    EXPECT_TRUE(S3fsCurl::is_cert_check);
    S3fsCurl::SetCheckCertificate(old);
}

TEST_F(CurlAuthMockTest, SetDnsCache_Toggle) {
    bool old = S3fsCurl::SetDnsCache(false);
    EXPECT_FALSE(S3fsCurl::is_dns_cache);
    S3fsCurl::SetDnsCache(true);
    EXPECT_TRUE(S3fsCurl::is_dns_cache);
    S3fsCurl::SetDnsCache(old);
}

TEST_F(CurlAuthMockTest, SetSslSessionCache_Toggle) {
    bool old = S3fsCurl::SetSslSessionCache(false);
    EXPECT_FALSE(S3fsCurl::is_ssl_session_cache);
    S3fsCurl::SetSslSessionCache(true);
    EXPECT_TRUE(S3fsCurl::is_ssl_session_cache);
    S3fsCurl::SetSslSessionCache(old);
}

// ==========================================================================
// 16. SetConnectTimeout / SetReadwriteTimeout / SetRetries
// ==========================================================================

TEST_F(CurlAuthMockTest, SetConnectTimeout) {
    long old = S3fsCurl::SetConnectTimeout(42);
    EXPECT_EQ(42, S3fsCurl::connect_timeout);
    S3fsCurl::SetConnectTimeout(old);
}

TEST_F(CurlAuthMockTest, SetReadwriteTimeout) {
    time_t old = S3fsCurl::SetReadwriteTimeout(99);
    EXPECT_EQ(99, S3fsCurl::readwrite_timeout);
    S3fsCurl::SetReadwriteTimeout(old);
}

TEST_F(CurlAuthMockTest, SetRetries) {
    int old = S3fsCurl::SetRetries(5);
    EXPECT_EQ(5, S3fsCurl::retries);
    S3fsCurl::SetRetries(old);
}

// ==========================================================================
// 17. SetPublicBucket / SetDefaultAcl / GetDefaultAcl
// ==========================================================================

TEST_F(CurlAuthMockTest, SetPublicBucket) {
    bool old = S3fsCurl::SetPublicBucket(true);
    EXPECT_TRUE(S3fsCurl::is_public_bucket);
    S3fsCurl::SetPublicBucket(false);
    EXPECT_FALSE(S3fsCurl::is_public_bucket);
    S3fsCurl::SetPublicBucket(old);
}

TEST_F(CurlAuthMockTest, SetDefaultAcl_Valid) {
    string old = S3fsCurl::SetDefaultAcl("private");
    EXPECT_EQ("private", S3fsCurl::GetDefaultAcl());
    S3fsCurl::SetDefaultAcl(old.c_str());
}

TEST_F(CurlAuthMockTest, SetDefaultAcl_Null) {
    string old = S3fsCurl::SetDefaultAcl(NULL);
    EXPECT_EQ("", S3fsCurl::GetDefaultAcl());
    S3fsCurl::SetDefaultAcl(old.c_str());
}

// ==========================================================================
// 18. SetStorageClass
// ==========================================================================

TEST_F(CurlAuthMockTest, SetStorageClass) {
    storage_class_t old = S3fsCurl::SetStorageClass(STANDARD_IA);
    EXPECT_EQ(STANDARD_IA, S3fsCurl::storage_class);
    S3fsCurl::SetStorageClass(REDUCED_REDUNDANCY);
    EXPECT_EQ(REDUCED_REDUNDANCY, S3fsCurl::storage_class);
    S3fsCurl::SetStorageClass(old);
}

// ==========================================================================
// 19. SetContentMd5 / SetVerbose
// ==========================================================================

TEST_F(CurlAuthMockTest, SetContentMd5) {
    bool old = S3fsCurl::SetContentMd5(true);
    EXPECT_TRUE(S3fsCurl::is_content_md5);
    S3fsCurl::SetContentMd5(false);
    EXPECT_FALSE(S3fsCurl::is_content_md5);
    S3fsCurl::SetContentMd5(old);
}

TEST_F(CurlAuthMockTest, SetVerbose) {
    bool old = S3fsCurl::SetVerbose(true);
    EXPECT_TRUE(S3fsCurl::is_verbose);
    S3fsCurl::SetVerbose(false);
    EXPECT_FALSE(S3fsCurl::is_verbose);
    S3fsCurl::SetVerbose(old);
}

// ==========================================================================
// 20. InitUserAgent
// ==========================================================================

TEST_F(CurlAuthMockTest, InitUserAgent_SetsNonEmpty) {
    S3fsCurl::userAgent.clear();
    S3fsCurl::InitUserAgent();
    EXPECT_FALSE(S3fsCurl::userAgent.empty());
    EXPECT_NE(string::npos, S3fsCurl::userAgent.find("/"));
}

TEST_F(CurlAuthMockTest, InitUserAgent_DoesNotOverwrite) {
    S3fsCurl::userAgent = "custom-agent";
    S3fsCurl::InitUserAgent();
    EXPECT_EQ("custom-agent", S3fsCurl::userAgent);
    S3fsCurl::userAgent.clear();
}

// ==========================================================================
// 21. AddUserAgent
// ==========================================================================

TEST_F(CurlAuthMockTest, AddUserAgent_NullHandle) {
    EXPECT_FALSE(S3fsCurl::AddUserAgent(NULL));
}

TEST_F(CurlAuthMockTest, AddUserAgent_UAEnabled) {
    CURL* h = curl_easy_init();
    ASSERT_NE((CURL*)NULL, h);
    S3fsCurl::is_ua = true;
    S3fsCurl::InitUserAgent();
    EXPECT_TRUE(S3fsCurl::AddUserAgent(h));
    curl_easy_cleanup(h);
}

TEST_F(CurlAuthMockTest, AddUserAgent_UADisabled) {
    CURL* h = curl_easy_init();
    ASSERT_NE((CURL*)NULL, h);
    S3fsCurl::is_ua = false;
    EXPECT_TRUE(S3fsCurl::AddUserAgent(h));
    curl_easy_cleanup(h);
}

// ==========================================================================
// 22. SetIsIBMIAMAuth / SetIsECS / SetIAMRole / SetSslVerifyHostname
// ==========================================================================

TEST_F(CurlAuthMockTest, SetIsIBMIAMAuth) {
    bool old = S3fsCurl::SetIsIBMIAMAuth(true);
    EXPECT_TRUE(S3fsCurl::is_ibm_iam_auth);
    S3fsCurl::SetIsIBMIAMAuth(false);
    EXPECT_FALSE(S3fsCurl::is_ibm_iam_auth);
    S3fsCurl::SetIsIBMIAMAuth(old);
}

TEST_F(CurlAuthMockTest, SetIsECS) {
    bool old = S3fsCurl::SetIsECS(true);
    EXPECT_TRUE(S3fsCurl::is_ecs);
    S3fsCurl::SetIsECS(false);
    EXPECT_FALSE(S3fsCurl::is_ecs);
    S3fsCurl::SetIsECS(old);
}

TEST_F(CurlAuthMockTest, SetIAMRole_Valid) {
    string old = S3fsCurl::SetIAMRole("test-role");
    EXPECT_STREQ("test-role", S3fsCurl::GetIAMRole());
    S3fsCurl::SetIAMRole(old.c_str());
}

TEST_F(CurlAuthMockTest, SetIAMRole_Null) {
    string old = S3fsCurl::SetIAMRole(NULL);
    EXPECT_STREQ("", S3fsCurl::GetIAMRole());
    S3fsCurl::SetIAMRole(old.c_str());
}

TEST_F(CurlAuthMockTest, SetSslVerifyHostname_Valid) {
    long old = S3fsCurl::SetSslVerifyHostname(0);
    EXPECT_EQ(0, S3fsCurl::ssl_verify_hostname);
    S3fsCurl::SetSslVerifyHostname(1);
    EXPECT_EQ(1, S3fsCurl::ssl_verify_hostname);
    S3fsCurl::SetSslVerifyHostname(old);
}

TEST_F(CurlAuthMockTest, SetSslVerifyHostname_Invalid) {
    EXPECT_EQ(-1, S3fsCurl::SetSslVerifyHostname(2));
    EXPECT_EQ(-1, S3fsCurl::SetSslVerifyHostname(99));
}

// ==========================================================================
// 23. CurlHandlerPool
// ==========================================================================

TEST_F(CurlAuthMockTest, CurlHandlerPool_InitDestroy) {
    CurlHandlerPool pool(4);
    EXPECT_TRUE(pool.Init());
    EXPECT_TRUE(pool.Destroy());
}

TEST_F(CurlAuthMockTest, CurlHandlerPool_GetHandler) {
    CurlHandlerPool pool(2);
    ASSERT_TRUE(pool.Init());

    CURL* h1 = pool.GetHandler();
    EXPECT_NE((CURL*)NULL, h1);
    CURL* h2 = pool.GetHandler();
    EXPECT_NE((CURL*)NULL, h2);

    // Pool is empty now, next should create new
    CURL* h3 = pool.GetHandler();
    EXPECT_NE((CURL*)NULL, h3);

    pool.ReturnHandler(h1);
    pool.ReturnHandler(h2);
    pool.ReturnHandler(h3);

    pool.Destroy();
}

TEST_F(CurlAuthMockTest, CurlHandlerPool_ReturnHandlerPoolFull) {
    CurlHandlerPool pool(1);
    ASSERT_TRUE(pool.Init());

    CURL* h1 = pool.GetHandler();
    EXPECT_NE((CURL*)NULL, h1);

    CURL* h2 = curl_easy_init();
    EXPECT_NE((CURL*)NULL, h2);

    pool.ReturnHandler(h1);
    // Pool full, h2 should be cleaned up
    pool.ReturnHandler(h2);

    pool.Destroy();
}

TEST_F(CurlAuthMockTest, CurlHandlerPool_GetActiveCurlNum) {
    CurlHandlerPool pool(4);
    ASSERT_TRUE(pool.Init());

    int active = pool.GetActiveCurlNum();
    EXPECT_GT(active, 0);

    CURL* h = pool.GetHandler();
    int active_after = pool.GetActiveCurlNum();
    EXPECT_EQ(active + 1, active_after);

    pool.ReturnHandler(h);
    pool.Destroy();
}

// ==========================================================================
// 24. CurlDebugFunc - tab indentation
// ==========================================================================

TEST_F(CurlAuthMockTest, CurlDebugFunc_TextWithTabs) {
    CURL* h = curl_easy_init();
    ASSERT_NE((CURL*)NULL, h);
    char data[] = "\t\tTabbed text\n";
    int result = S3fsCurl::CurlDebugFunc(h, CURLINFO_TEXT, data, strlen(data), NULL);
    EXPECT_EQ(0, result);
    curl_easy_cleanup(h);
}

TEST_F(CurlAuthMockTest, CurlDebugFunc_HeaderMultiLine) {
    CURL* h = curl_easy_init();
    ASSERT_NE((CURL*)NULL, h);
    char data[] = "Header1: val1\r\nHeader2: val2\r\n";
    int result = S3fsCurl::CurlDebugFunc(h, CURLINFO_HEADER_IN, data, strlen(data), NULL);
    EXPECT_EQ(0, result);
    curl_easy_cleanup(h);
}

TEST_F(CurlAuthMockTest, CurlDebugFunc_HeaderNoNewline) {
    CURL* h = curl_easy_init();
    ASSERT_NE((CURL*)NULL, h);
    char data[] = "Header-Only-No-Newline";
    int result = S3fsCurl::CurlDebugFunc(h, CURLINFO_HEADER_OUT, data, strlen(data), NULL);
    EXPECT_EQ(0, result);
    curl_easy_cleanup(h);
}

TEST_F(CurlAuthMockTest, CurlDebugFunc_EmptyData) {
    CURL* h = curl_easy_init();
    ASSERT_NE((CURL*)NULL, h);
    char data[] = "";
    int result = S3fsCurl::CurlDebugFunc(h, CURLINFO_TEXT, data, 0, NULL);
    EXPECT_EQ(0, result);
    curl_easy_cleanup(h);
}

// ==========================================================================
// 25. DetectBucketTypeFromHeaders - additional cases
// ==========================================================================

TEST_F(CurlAuthMockTest, DetectBucketType_WithObsBucketType) {
    headers_t headers;
    headers["x-obs-bucket-type"] = "POSIX";
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_TRUE(result);
    EXPECT_EQ(BUCKET_TYPE_FILE_SEMANTIC, g_bucket_type);
    g_bucket_type = BUCKET_TYPE_UNKNOWN;
}

TEST_F(CurlAuthMockTest, DetectBucketType_WithFsFileInterface) {
    headers_t headers;
    headers["x-obs-fs-file-interface"] = "Enabled";
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_TRUE(result);
    EXPECT_EQ(BUCKET_TYPE_FILE_SEMANTIC, g_bucket_type);
    g_bucket_type = BUCKET_TYPE_UNKNOWN;
}

TEST_F(CurlAuthMockTest, DetectBucketType_NoFileHeaders) {
    headers_t headers;
    headers["content-type"] = "application/xml";
    headers["x-amz-request-id"] = "REQ123";
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_FALSE(result);
}

// ==========================================================================
// 26. CreateCurlHandle / DestroyCurlHandle / ClearInternalData
// ==========================================================================

TEST_F(CurlAuthMockTest, CreateCurlHandle_New) {
    S3fsCurl s3curl;
    EXPECT_TRUE(s3curl.CreateCurlHandle(true));
    EXPECT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, CreateCurlHandle_ForceTrue) {
    S3fsCurl s3curl;
    EXPECT_TRUE(s3curl.CreateCurlHandle(true));
    EXPECT_NE((CURL*)NULL, s3curl.hCurl);
    EXPECT_TRUE(s3curl.CreateCurlHandle(true));
    EXPECT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, CreateCurlHandle_ForceFalse_AlreadyExists) {
    S3fsCurl s3curl;
    EXPECT_TRUE(s3curl.CreateCurlHandle(true));
    EXPECT_FALSE(s3curl.CreateCurlHandle(false));
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, DestroyCurlHandle_NullHandle) {
    S3fsCurl s3curl;
    EXPECT_FALSE(s3curl.DestroyCurlHandle());
}

TEST_F(CurlAuthMockTest, DestroyCurlHandle_Valid) {
    S3fsCurl s3curl;
    EXPECT_TRUE(s3curl.CreateCurlHandle(true));
    EXPECT_TRUE(s3curl.DestroyCurlHandle());
    EXPECT_EQ((CURL*)NULL, s3curl.hCurl);
}

TEST_F(CurlAuthMockTest, ClearInternalData_WithHandle) {
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    EXPECT_FALSE(s3curl.ClearInternalData());
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

TEST_F(CurlAuthMockTest, ClearInternalData_WithoutHandle) {
    S3fsCurl s3curl;
    s3curl.path = "/some/path";
    s3curl.op = "GET";
    s3curl.url = "https://example.com";
    s3curl.LastResponseCode = 200;
    EXPECT_TRUE(s3curl.ClearInternalData());
    EXPECT_EQ("", s3curl.path);
    EXPECT_EQ("", s3curl.op);
    EXPECT_EQ("", s3curl.url);
    EXPECT_EQ(-1, s3curl.LastResponseCode);
}

// ==========================================================================
// 27. SetUseAhbe
// ==========================================================================

TEST_F(CurlAuthMockTest, SetUseAhbe) {
    S3fsCurl s3curl;
    bool old = s3curl.SetUseAhbe(true);
    EXPECT_TRUE(s3curl.is_use_ahbe);
    s3curl.SetUseAhbe(false);
    EXPECT_FALSE(s3curl.is_use_ahbe);
    s3curl.SetUseAhbe(old);
}

// ==========================================================================
// 28. RemakeHandle - various request types
// ==========================================================================

TEST_F(CurlAuthMockTest, RemakeHandle_UnsetType) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_UNSET;
    EXPECT_FALSE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_DeleteType) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_DELETE;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.requestHeaders = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_HeadType) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_HEAD;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.requestHeaders = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_PutHeadType) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PUTHEAD;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.requestHeaders = NULL;
    s3curl.bodydata = new BodyData();
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_PutType_NullBuffer) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PUT;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.requestHeaders = NULL;
    s3curl.bodydata = new BodyData();
    s3curl.writedatabuf.pcBuffer = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_PutType_WithBuffer) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PUT;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.requestHeaders = NULL;
    s3curl.bodydata = new BodyData();
    char buf[] = "data";
    s3curl.writedatabuf.pcBuffer = buf;
    s3curl.writedatabuf.offset = 0;
    s3curl.writedatabuf.ulBufferSize = 4;
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.writedatabuf.pcBuffer = NULL;
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_GetType) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.requestHeaders = NULL;
    s3curl.read_buf = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_GetType_WithReadBuf) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.requestHeaders = NULL;
    char readbuf[64];
    s3curl.read_buf = readbuf;
    s3curl.read_size = sizeof(readbuf);
    s3curl.cur_pos = 10;
    EXPECT_TRUE(s3curl.RemakeHandle());
    EXPECT_EQ(0, s3curl.cur_pos);
    s3curl.read_buf = NULL;
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_ChkBucketType) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_CHKBUCKET;
    s3curl.url = "https://obs.example.com/";
    s3curl.requestHeaders = NULL;
    s3curl.bodydata = new BodyData();
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_ListBucketType) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_LISTBUCKET;
    s3curl.url = "https://obs.example.com/?prefix=test";
    s3curl.requestHeaders = NULL;
    s3curl.bodydata = new BodyData();
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_PreMultiPost) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_PREMULTIPOST;
    s3curl.url = "https://obs.example.com/bucket/key?uploads";
    s3curl.requestHeaders = NULL;
    s3curl.bodydata = new BodyData();
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_CompleteMultiPost) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_COMPLETEMULTIPOST;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.requestHeaders = NULL;
    s3curl.bodydata = new BodyData();
    s3curl.b_postdata = (const unsigned char*)"post-data";
    s3curl.b_postdata_remaining = 9;
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_UploadMultiPost) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_UPLOADMULTIPOST;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.requestHeaders = NULL;
    s3curl.bodydata = new BodyData();
    s3curl.b_partdata_startpos = 0;
    s3curl.b_partdata_size = 1024;
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_CopyMultiPost) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_COPYMULTIPOST;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.requestHeaders = NULL;
    s3curl.bodydata = new BodyData();
    s3curl.headdata = new BodyData();
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_MultiList) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_MULTILIST;
    s3curl.url = "https://obs.example.com/bucket?uploads";
    s3curl.requestHeaders = NULL;
    s3curl.bodydata = new BodyData();
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_IAMCred) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_IAMCRED;
    s3curl.url = "http://169.254.169.254/iam/cred";
    s3curl.requestHeaders = NULL;
    s3curl.bodydata = new BodyData();
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_IAMCred_IBMIAM) {
    S3fsCurl::is_ibm_iam_auth = true;
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_IAMCRED;
    s3curl.url = "https://iam.example.com/token";
    s3curl.requestHeaders = NULL;
    s3curl.bodydata = new BodyData();
    s3curl.b_postdata = (const unsigned char*)"grant_type=refresh";
    s3curl.b_postdata_remaining = 18;
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_AbortMultiUpload) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_ABORTMULTIUPLOAD;
    s3curl.url = "https://obs.example.com/bucket/key?uploadId=abc";
    s3curl.requestHeaders = NULL;
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_IAMRole) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_IAMROLE;
    s3curl.url = "http://169.254.169.254/iam/role";
    s3curl.requestHeaders = NULL;
    s3curl.bodydata = new BodyData();
    EXPECT_TRUE(s3curl.RemakeHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, RemakeHandle_RetryCount) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_DELETE;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.requestHeaders = NULL;
    s3curl.retry_count = 0;
    EXPECT_TRUE(s3curl.RemakeHandle());
    EXPECT_EQ(1, s3curl.retry_count);
    EXPECT_TRUE(s3curl.RemakeHandle());
    EXPECT_EQ(2, s3curl.retry_count);
    s3curl.DestroyCurlHandle();
}

// ==========================================================================
// 29. S3fsMultiCurl - additional coverage
// ==========================================================================

TEST_F(CurlAuthMockTest, S3fsMultiCurl_SetMaxMultiRequest_Various) {
    int old = S3fsMultiCurl::SetMaxMultiRequest(1);
    EXPECT_EQ(1, S3fsMultiCurl::max_multireq);

    S3fsMultiCurl::SetMaxMultiRequest(100);
    EXPECT_EQ(100, S3fsMultiCurl::max_multireq);

    S3fsMultiCurl::SetMaxMultiRequest(0);
    EXPECT_EQ(0, S3fsMultiCurl::max_multireq);

    S3fsMultiCurl::SetMaxMultiRequest(old);
}

TEST_F(CurlAuthMockTest, S3fsMultiCurl_ClearExFalse) {
    S3fsMultiCurl multi;
    EXPECT_TRUE(multi.ClearEx(false));
}

TEST_F(CurlAuthMockTest, S3fsMultiCurl_ClearExTrue) {
    S3fsMultiCurl multi;
    EXPECT_TRUE(multi.ClearEx(true));
}

TEST_F(CurlAuthMockTest, S3fsMultiCurl_SetSuccessCallback) {
    S3fsMultiCurl multi;
    S3fsMultiSuccessCallback old = multi.SetSuccessCallback(NULL);
    EXPECT_EQ((S3fsMultiSuccessCallback)NULL, multi.SuccessCallback);
    multi.SetSuccessCallback(old);
}

TEST_F(CurlAuthMockTest, S3fsMultiCurl_SetRetryCallback) {
    S3fsMultiCurl multi;
    S3fsMultiRetryCallback old = multi.SetRetryCallback(NULL);
    EXPECT_EQ((S3fsMultiRetryCallback)NULL, multi.RetryCallback);
    multi.SetRetryCallback(old);
}

// ==========================================================================
// 30. ResetHandle - covers various option branches
// ==========================================================================

TEST_F(CurlAuthMockTest, ResetHandle_WithVerbose) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    S3fsCurl::is_verbose = true;
    S3fsCurl::is_cert_check = false;
    s3curl.type = S3fsCurl::REQTYPE_GET;
    EXPECT_TRUE(s3curl.ResetHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, ResetHandle_IAMCredType) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_IAMCRED;
    EXPECT_TRUE(s3curl.ResetHandle());
    s3curl.DestroyCurlHandle();
}

TEST_F(CurlAuthMockTest, ResetHandle_IAMRoleType) {
    S3fsCurl s3curl;
    s3curl.CreateCurlHandle(true);
    s3curl.type = S3fsCurl::REQTYPE_IAMROLE;
    EXPECT_TRUE(s3curl.ResetHandle());
    s3curl.DestroyCurlHandle();
}

// ==========================================================================
// 31. CurlProgress callback
// ==========================================================================

TEST_F(CurlAuthMockTest, CurlProgress_ValidClientp) {
    CURL* h = curl_easy_init();
    ASSERT_NE((CURL*)NULL, h);

    pthread_spin_lock(&S3fsCurl::curl_handles_spinlock);
    S3fsCurl::curl_times[h] = time(0);
    S3fsCurl::curl_progress[h] = progress_t(-1, -1);
    pthread_spin_unlock(&S3fsCurl::curl_handles_spinlock);

    int result = S3fsCurl::CurlProgress(h, 100.0, 50.0, 0.0, 0.0);
    EXPECT_EQ(0, result);

    pthread_spin_lock(&S3fsCurl::curl_handles_spinlock);
    S3fsCurl::curl_times.erase(h);
    S3fsCurl::curl_progress.erase(h);
    pthread_spin_unlock(&S3fsCurl::curl_handles_spinlock);

    curl_easy_cleanup(h);
}

// ==========================================================================
// 32. url_to_host (static function, accessible via #include)
// ==========================================================================

TEST_F(CurlAuthMockTest, UrlToHost_Http) {
    string result = url_to_host("http://obs.example.com/bucket");
    EXPECT_EQ("obs.example.com", result);
}

TEST_F(CurlAuthMockTest, UrlToHost_Https) {
    string result = url_to_host("https://obs.example.com:443/bucket");
    EXPECT_EQ("obs.example.com:443", result);
}

TEST_F(CurlAuthMockTest, UrlToHost_HttpsNoPath) {
    string result = url_to_host("https://obs.example.com");
    EXPECT_EQ("obs.example.com", result);
}

// ==========================================================================
// 33. make_md5_from_string — REMOVED in ISSUE2026040800001 M2
// The function was replaced by the in-memory s3fs_get_content_md5_hws_obs
// at its single call site (PushbackSseKeys). See common_auth_test.cpp for
// the new SseCKeyMD5 test suite (Test 0/1/1b/2/3/6) that covers the same
// code path with KAT + cross-path equivalence + chunking boundary + NUL-byte
// latent bug tests.
// ==========================================================================

// ==========================================================================
// Task B: sha256/HMAC return value checks (ISSUE2026040300001)
// ==========================================================================

TEST_F(CurlAuthMockTest, CalcSignature_Sha256Fail_ReturnsEmpty) {
    // When s3fs_sha256 fails, CalcSignature should return empty string
    g_mock_sha256_fail = true;
    g_mock_hmac256_fail = false;

    S3fsCurl s3curl;
    string sig = s3curl.CalcSignature("GET", "/test", "", "20260101", "payload", "20260101T000000Z", "", "");
    EXPECT_TRUE(sig.empty()) << "CalcSignature must return empty when sha256 fails";

    g_mock_sha256_fail = false;
}

TEST_F(CurlAuthMockTest, CalcSignature_Hmac256Fail_ReturnsEmpty) {
    // When s3fs_HMAC256 fails, CalcSignature should return empty string
    g_mock_sha256_fail = false;
    g_mock_hmac256_fail = true;

    S3fsCurl s3curl;
    string sig = s3curl.CalcSignature("GET", "/test", "", "20260101", "payload", "20260101T000000Z", "", "");
    EXPECT_TRUE(sig.empty()) << "CalcSignature must return empty when HMAC256 chain fails";

    g_mock_hmac256_fail = false;
}

TEST_F(CurlAuthMockTest, InsertV4Headers_CompleteMPPost_Sha256Fail_EmptyPayloadHash) {
    // When sha256 fails in COMPLETEMULTIPOST branch, payload_hash stays empty
    g_mock_sha256_fail = true;
    g_mock_hmac256_fail = false;

    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "POST";
    s3curl.path = "/test/key";
    s3curl.query_string = "uploads";
    s3curl.type = S3fsCurl::REQTYPE_COMPLETEMULTIPOST;
    const char* xml = "<Complete></Complete>";
    s3curl.b_postdata = reinterpret_cast<const unsigned char*>(xml);

    // Should not crash; sha256 failure means payload_hash is empty,
    // falls through to empty_payload_hash usage
    s3curl.insertV4Headers();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;

    g_mock_sha256_fail = false;
}

// ==========================================================================
// Task E: b_postdata NULL defense (ISSUE2026040300001)
// ==========================================================================

TEST_F(CurlAuthMockTest, InsertV4Headers_CompleteMPPost_NullPostdata_NoCrash) {
    // b_postdata=NULL should not crash; payload_hash remains empty
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "POST";
    s3curl.path = "/test/key";
    s3curl.query_string = "uploads";
    s3curl.type = S3fsCurl::REQTYPE_COMPLETEMULTIPOST;
    s3curl.b_postdata = NULL;  // NULL defense test

    // Should not crash; empty payload_hash falls through to empty_payload_hash
    s3curl.insertV4Headers();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

TEST_F(CurlAuthMockTest, InsertV4Headers_CompleteMPPost_NonNullPostdata) {
    // b_postdata non-NULL: payload_hash should be computed (regression guard)
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "POST";
    s3curl.path = "/test/key";
    s3curl.query_string = "uploads";
    s3curl.type = S3fsCurl::REQTYPE_COMPLETEMULTIPOST;
    const char* xml = "<CompleteMultipartUpload></CompleteMultipartUpload>";
    s3curl.b_postdata = reinterpret_cast<const unsigned char*>(xml);

    s3curl.insertV4Headers();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

// ==========================================================================
// Main
// ==========================================================================

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    curl_global_init(CURL_GLOBAL_ALL);
    xmlInitParser();

    pthread_spin_init(&S3fsCurl::curl_handles_spinlock, PTHREAD_PROCESS_PRIVATE);
    pthread_mutex_init(&S3fsCurl::curl_handles_lock, NULL);

    CurlHandlerPool pool(8);
    pool.Init();
    S3fsCurl::sCurlPool = &pool;

    int result = RUN_ALL_TESTS();

    S3fsCurl::sCurlPool = NULL;
    pool.Destroy();

    pthread_spin_destroy(&S3fsCurl::curl_handles_spinlock);
    pthread_mutex_destroy(&S3fsCurl::curl_handles_lock);

    xmlCleanupParser();
    curl_global_cleanup();
    return result;
}
