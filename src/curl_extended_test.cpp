// ==========================================================================
// curl_extended_test.cpp
//
// Extended tests for curl.cpp using #include approach.
// Tests IAM credential parsing, signature computation, SSE key management,
// XML parsing (GetUploadId), request setup (PreHeadRequest), multi-curl,
// and error/debug helpers.
//
// Approach:
//   1. #include "curl.cpp" to access private/static functions
//   2. Crypto/auth stubs provided by test_stubs.o (linked separately)
//   3. Link against real .o files for all dependencies
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

// XML parsing headers for GetUploadId tests
#include <libxml/parser.h>
#include <libxml/tree.h>

// ==========================================================================
// Google Test
// ==========================================================================
#include "gtest.h"

using namespace std;

// ==========================================================================
// Test Fixture
// ==========================================================================
class CurlExtendedTest : public ::testing::Test {
protected:
    void SetUp() override {
        saved_bucket = bucket;
        saved_host = host;
        saved_pathrequeststyle = pathrequeststyle;
        saved_is_sigv4 = S3fsCurl::is_sigv4;
        // Set default credentials for signature tests
        S3fsCurl::AWSAccessKeyId = "AKID_TEST";
        S3fsCurl::AWSSecretAccessKey = "SECRET_TEST";
        S3fsCurl::AWSAccessToken = "";
    }
    void TearDown() override {
        bucket = saved_bucket;
        host = saved_host;
        pathrequeststyle = saved_pathrequeststyle;
        S3fsCurl::is_sigv4 = saved_is_sigv4;
    }
    string saved_bucket;
    string saved_host;
    bool saved_pathrequeststyle;
    bool saved_is_sigv4;
};

// ==========================================================================
// 1. IAM Credential Parsing
// ==========================================================================

TEST_F(CurlExtendedTest, ParseIAMCredentialResponse_Valid) {
    iamcredmap_t keyval;
    // The parser looks for JSON-like "key" : "value" pairs separated by commas
    const char* response =
        "{ \"AccessKeyId\" : \"AKID123\" , "
        "\"SecretAccessKey\" : \"SK456\" , "
        "\"Token\" : \"TOK789\" , "
        "\"Expiration\" : \"2026-12-31T00:00:00Z\" }";
    bool result = S3fsCurl::ParseIAMCredentialResponse(response, keyval);
    EXPECT_TRUE(result);
    // Verify that AccessKeyId and SecretAccessKey were extracted
    EXPECT_EQ("AKID123", keyval["AccessKeyId"]);
    EXPECT_EQ("SK456", keyval["SecretAccessKey"]);
}

TEST_F(CurlExtendedTest, ParseIAMCredentialResponse_Empty) {
    iamcredmap_t keyval;
    // Empty string should still return true (just no keys extracted)
    bool result = S3fsCurl::ParseIAMCredentialResponse("", keyval);
    EXPECT_TRUE(result);
    EXPECT_TRUE(keyval.empty());
}

TEST_F(CurlExtendedTest, ParseIAMCredentialResponse_Null) {
    iamcredmap_t keyval;
    EXPECT_FALSE(S3fsCurl::ParseIAMCredentialResponse(NULL, keyval));
}

TEST_F(CurlExtendedTest, ParseIAMCredentialResponse_NoMatchingKeys) {
    iamcredmap_t keyval;
    const char* response = "\"SomeOtherKey\" : \"value1\" , \"AnotherKey\" : \"value2\"";
    bool result = S3fsCurl::ParseIAMCredentialResponse(response, keyval);
    EXPECT_TRUE(result);
    EXPECT_TRUE(keyval.empty());
}

TEST_F(CurlExtendedTest, ParseIAMCredentialResponse_MissingColonAfterKey) {
    iamcredmap_t keyval;
    // AccessKeyId found, but no colon after it in expected position
    const char* response = "\"AccessKeyId\" \"AKID123\"";
    bool result = S3fsCurl::ParseIAMCredentialResponse(response, keyval);
    EXPECT_TRUE(result);
    // May or may not extract the key depending on colon search
}

TEST_F(CurlExtendedTest, ParseIAMCredentialResponse_MissingQuotes) {
    iamcredmap_t keyval;
    // Value has colon but no quotes around value
    const char* response = "\"AccessKeyId\" : AKID_NO_QUOTES";
    bool result = S3fsCurl::ParseIAMCredentialResponse(response, keyval);
    EXPECT_TRUE(result);
    // Value without quotes should not be extracted
    EXPECT_TRUE(keyval.find("AccessKeyId") == keyval.end());
}

TEST_F(CurlExtendedTest, ParseIAMCredentialResponse_WithRoleArn) {
    iamcredmap_t keyval;
    const char* response =
        "{ \"AccessKeyId\" : \"AK\" , "
        "\"SecretAccessKey\" : \"SK\" , "
        "\"Token\" : \"TK\" , "
        "\"Expiration\" : \"2026-12-31T00:00:00Z\" , "
        "\"RoleArn\" : \"arn:aws:iam::123456:role/myrole\" }";
    bool result = S3fsCurl::ParseIAMCredentialResponse(response, keyval);
    EXPECT_TRUE(result);
    EXPECT_EQ("arn:aws:iam::123456:role/myrole", keyval["RoleArn"]);
}

TEST_F(CurlExtendedTest, SetIAMCredentials_ValidResponse) {
    // SetIAMCredentials requires IAM_field_count to match keyval.size()
    size_t old_field_count = S3fsCurl::IAM_field_count;
    string old_token_field = S3fsCurl::IAM_token_field;
    string old_expiry_field = S3fsCurl::IAM_expiry_field;

    S3fsCurl::IAM_field_count = 4;
    S3fsCurl::IAM_token_field = "Token";
    S3fsCurl::IAM_expiry_field = "Expiration";

    const char* response =
        "{ \"AccessKeyId\" : \"TESTAKID\" , "
        "\"SecretAccessKey\" : \"TESTSK\" , "
        "\"Token\" : \"TESTTOKEN\" , "
        "\"Expiration\" : \"2026-12-31T00:00:00Z\" }";
    bool result = S3fsCurl::SetIAMCredentials(response);
    EXPECT_TRUE(result);
    EXPECT_EQ("TESTAKID", S3fsCurl::AWSAccessKeyId);
    EXPECT_EQ("TESTSK", S3fsCurl::AWSSecretAccessKey);
    EXPECT_EQ("TESTTOKEN", S3fsCurl::AWSAccessToken);

    // Restore
    S3fsCurl::IAM_field_count = old_field_count;
    S3fsCurl::IAM_token_field = old_token_field;
    S3fsCurl::IAM_expiry_field = old_expiry_field;
    S3fsCurl::AWSAccessKeyId = "AKID_TEST";
    S3fsCurl::AWSSecretAccessKey = "SECRET_TEST";
    S3fsCurl::AWSAccessToken = "";
}

TEST_F(CurlExtendedTest, SetIAMCredentials_FieldCountMismatch) {
    size_t old_field_count = S3fsCurl::IAM_field_count;
    S3fsCurl::IAM_field_count = 99;  // won't match

    const char* response =
        "{ \"AccessKeyId\" : \"AK\" , \"SecretAccessKey\" : \"SK\" }";
    bool result = S3fsCurl::SetIAMCredentials(response);
    EXPECT_FALSE(result);

    S3fsCurl::IAM_field_count = old_field_count;
}

TEST_F(CurlExtendedTest, SetIAMCredentials_NullResponse) {
    bool result = S3fsCurl::SetIAMCredentials(NULL);
    EXPECT_FALSE(result);
}

TEST_F(CurlExtendedTest, ParseIAMRoleFromMetaDataResponse_Valid) {
    string rolename;
    bool result = S3fsCurl::ParseIAMRoleFromMetaDataResponse("test-role-name\n", rolename);
    EXPECT_TRUE(result);
    EXPECT_EQ("test-role-name", rolename);
}

TEST_F(CurlExtendedTest, ParseIAMRoleFromMetaDataResponse_NoNewline) {
    string rolename;
    bool result = S3fsCurl::ParseIAMRoleFromMetaDataResponse("my-role", rolename);
    EXPECT_TRUE(result);
    EXPECT_EQ("my-role", rolename);
}

TEST_F(CurlExtendedTest, ParseIAMRoleFromMetaDataResponse_Empty) {
    string rolename;
    EXPECT_FALSE(S3fsCurl::ParseIAMRoleFromMetaDataResponse("", rolename));
}

TEST_F(CurlExtendedTest, ParseIAMRoleFromMetaDataResponse_Null) {
    string rolename;
    EXPECT_FALSE(S3fsCurl::ParseIAMRoleFromMetaDataResponse(NULL, rolename));
}

TEST_F(CurlExtendedTest, ParseIAMRoleFromMetaDataResponse_MultiLine) {
    string rolename;
    bool result = S3fsCurl::ParseIAMRoleFromMetaDataResponse("first-role\nsecond-role\n", rolename);
    EXPECT_TRUE(result);
    EXPECT_EQ("first-role", rolename);
}

TEST_F(CurlExtendedTest, SetIAMRoleFromMetaData_Valid) {
    string old_role = S3fsCurl::IAM_role;
    bool result = S3fsCurl::SetIAMRoleFromMetaData("my-iam-role\n");
    EXPECT_TRUE(result);
    EXPECT_STREQ("my-iam-role", S3fsCurl::GetIAMRole());
    S3fsCurl::SetIAMRole(old_role.c_str());
}

TEST_F(CurlExtendedTest, SetIAMRoleFromMetaData_Empty) {
    bool result = S3fsCurl::SetIAMRoleFromMetaData("");
    EXPECT_FALSE(result);
}

TEST_F(CurlExtendedTest, SetIAMRoleFromMetaData_Null) {
    bool result = S3fsCurl::SetIAMRoleFromMetaData(NULL);
    EXPECT_FALSE(result);
}

// ==========================================================================
// 2. Signature Computation
// ==========================================================================

TEST_F(CurlExtendedTest, CalcSignatureV2_Basic) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    // CalcSignatureV2 uses s3fs_HMAC (stubbed) and s3fs_base64 (real from string_util)
    string sig = s3curl.CalcSignatureV2(
        "GET", "", "application/xml",
        "Thu, 01 Jan 2026 00:00:00 GMT",
        "/bucket/key");
    // With stub HMAC returning zeros, we still get a base64-encoded result
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlExtendedTest, CalcSignatureV2_WithCustomSkAndToken) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignatureV2(
        "PUT", "md5hash", "text/plain",
        "Thu, 01 Jan 2026 00:00:00 GMT",
        "/bucket/key",
        "custom_sk", "custom_token");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlExtendedTest, CalcSignatureV2_EmptySecretKey) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    // Uses AWSSecretAccessKey from class static if sk is empty
    string sig = s3curl.CalcSignatureV2(
        "DELETE", "", "",
        "Thu, 01 Jan 2026 00:00:00 GMT",
        "/bucket/key",
        "", "");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlExtendedTest, CalcSignatureV2_POSTMethod) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignatureV2(
        "POST", "", "application/xml",
        "Thu, 01 Jan 2026 00:00:00 GMT",
        "/bucket?uploads");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlExtendedTest, CalcSignatureV4_Basic) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    // CalcSignature (V4) uses s3fs_HMAC256, s3fs_sha256 (stubbed)
    string sig = s3curl.CalcSignature(
        "GET", "/bucket/key", "",
        "20260101", "UNSIGNED-PAYLOAD", "20260101T000000Z");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlExtendedTest, CalcSignatureV4_HEADMethod) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignature(
        "HEAD", "/bucket/key", "",
        "20260101", "UNSIGNED-PAYLOAD", "20260101T000000Z");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlExtendedTest, CalcSignatureV4_PUTMethod) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignature(
        "PUT", "/bucket/key", "",
        "20260101", "payload-hash", "20260101T000000Z");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlExtendedTest, CalcSignatureV4_DELETEMethod) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignature(
        "DELETE", "/bucket/key", "",
        "20260101", "UNSIGNED-PAYLOAD", "20260101T000000Z");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlExtendedTest, CalcSignatureV4_POSTMethod) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignature(
        "POST", "/bucket", "uploads",
        "20260101", "UNSIGNED-PAYLOAD", "20260101T000000Z");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlExtendedTest, CalcSignatureV4_GETEmptyUri) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    // Tests the branch: GET with empty uriencode -> uses "/"
    string sig = s3curl.CalcSignature(
        "GET", "", "",
        "20260101", "UNSIGNED-PAYLOAD", "20260101T000000Z");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlExtendedTest, CalcSignatureV4_GETWithSlashUri) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    // Tests the branch: GET with uri starting with "/"
    string sig = s3curl.CalcSignature(
        "GET", "/bucket/key", "prefix=test",
        "20260101", "UNSIGNED-PAYLOAD", "20260101T000000Z");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlExtendedTest, CalcSignatureV4_WithCustomSkAndToken) {
    S3fsCurl s3curl;
    s3curl.requestHeaders = NULL;
    string sig = s3curl.CalcSignature(
        "GET", "/bucket/key", "",
        "20260101", "UNSIGNED-PAYLOAD", "20260101T000000Z",
        "custom_sk", "custom_token");
    EXPECT_FALSE(sig.empty());
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
}

TEST_F(CurlExtendedTest, InsertAuthHeaders_V2) {
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "GET";
    s3curl.path = "/test/path";
    s3curl.query_string = "";
    s3curl.url = "https://obs.example.com/bucket/test/path";
    s3curl.type = S3fsCurl::REQTYPE_GET;
    bucket = "bucket";
    host = "https://obs.example.com";
    S3fsCurl::is_sigv4 = false;
    S3fsCurl::is_ibm_iam_auth = false;
    S3fsCurl::IAM_role = "";
    S3fsCurl::is_ecs = false;
    S3fsCurl::AWSAccessTokenExpire = time(NULL) + 86400;

    s3curl.insertAuthHeaders();
    // Verify headers were added (at minimum Date and Authorization)
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

TEST_F(CurlExtendedTest, InsertAuthHeaders_V4) {
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "GET";
    s3curl.path = "/test/path";
    s3curl.query_string = "";
    s3curl.url = "https://obs.example.com/bucket/test/path";
    s3curl.type = S3fsCurl::REQTYPE_GET;
    s3curl.writedatabuf.pcBuffer = NULL;
    s3curl.writedatabuf.offset = 0;
    s3curl.writedatabuf.ulBufferSize = 0;
    bucket = "bucket";
    host = "https://obs.example.com";
    S3fsCurl::is_sigv4 = true;
    S3fsCurl::is_ibm_iam_auth = false;
    S3fsCurl::IAM_role = "";
    S3fsCurl::is_ecs = false;
    S3fsCurl::AWSAccessTokenExpire = time(NULL) + 86400;

    s3curl.insertAuthHeaders();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
    S3fsCurl::is_sigv4 = false;
}

TEST_F(CurlExtendedTest, InsertV2Headers_ListBucket) {
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "GET";
    s3curl.path = "/";
    s3curl.query_string = "prefix=test&delimiter=/";
    s3curl.type = S3fsCurl::REQTYPE_LISTBUCKET;
    bucket = "mybucket";
    host = "https://obs.example.com";

    s3curl.insertV2Headers();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

TEST_F(CurlExtendedTest, InsertV2Headers_WithQueryString) {
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    s3curl.requestHeaders = NULL;
    s3curl.op = "PUT";
    s3curl.path = "/test/object";
    s3curl.query_string = "partNumber=1&uploadId=abc";
    s3curl.type = S3fsCurl::REQTYPE_UPLOADMULTIPOST;
    bucket = "mybucket";
    host = "https://obs.example.com";

    s3curl.insertV2Headers();
    EXPECT_NE(s3curl.requestHeaders, (struct curl_slist*)NULL);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

// ==========================================================================
// 3. SSE Keys
// ==========================================================================

TEST_F(CurlExtendedTest, LoadEnvSseCKeys_NoEnv) {
    unsetenv("AWSSSECKEYS");
    EXPECT_TRUE(S3fsCurl::LoadEnvSseCKeys());
}

TEST_F(CurlExtendedTest, LoadEnvSseKmsid_NoEnv) {
    unsetenv("AWSSSEKMSID");
    EXPECT_TRUE(S3fsCurl::LoadEnvSseKmsid());
}

TEST_F(CurlExtendedTest, LoadEnvSseKmsid_WithEnv) {
    setenv("AWSSSEKMSID", "my-kms-key-id", 1);
    bool result = S3fsCurl::LoadEnvSseKmsid();
    EXPECT_TRUE(result);
    EXPECT_EQ("my-kms-key-id", string(S3fsCurl::GetSseKmsId()));
    unsetenv("AWSSSEKMSID");
    S3fsCurl::ssekmsid.clear();
}

TEST_F(CurlExtendedTest, LoadEnvSseCKeys_WithEnv) {
    // Set a key in the environment. PushbackSseKeys may fail
    // with stubs but we still exercise the code path.
    setenv("AWSSSECKEYS", "somekey1:somekey2", 1);
    bool result = S3fsCurl::LoadEnvSseCKeys();
    // Result depends on whether PushbackSseKeys succeeds with stubs
    (void)result;
    unsetenv("AWSSSECKEYS");
}

// [ISSUE2026040700001 P0-1] LoadEnvSseCKeys must NOT log the envkeys contents
// when it fails to parse any valid SSE-C key.
// Only the input length should appear in the ERR log.
class LoadEnvSseCKeysRedactTest : public CurlExtendedTest {
protected:
    s3fs_log_mode saved_log_mode;
    void SetUp() override {
        CurlExtendedTest::SetUp();
        saved_log_mode = debug_log_mode;
        debug_log_mode = LOG_MODE_FOREGROUND;  // So log goes to stdout
    }
    void TearDown() override {
        debug_log_mode = saved_log_mode;
        unsetenv("AWSSSECKEYS");
        CurlExtendedTest::TearDown();
    }
};

TEST_F(LoadEnvSseCKeysRedactTest, EmptyValue_NoLeakToLog) {
    setenv("AWSSSECKEYS", "", 1);
    testing::internal::CaptureStdout();
    bool result = S3fsCurl::LoadEnvSseCKeys();
    std::string log = testing::internal::GetCapturedStdout();

    EXPECT_FALSE(result);
    // Positive assertion: length must appear in log for diagnostic value
    EXPECT_NE(std::string::npos, log.find("input length=0"));
    // Negative assertion: the old format string "AWSSSECKEYS=" must NOT be present
    EXPECT_EQ(std::string::npos, log.find("AWSSSECKEYS="));
}

TEST_F(LoadEnvSseCKeysRedactTest, OnlyColons_NoLeakToLog) {
    setenv("AWSSSECKEYS", "::::", 1);
    testing::internal::CaptureStdout();
    bool result = S3fsCurl::LoadEnvSseCKeys();
    std::string log = testing::internal::GetCapturedStdout();

    EXPECT_FALSE(result);
    EXPECT_NE(std::string::npos, log.find("input length=4"));
    // The colons themselves should not appear embedded in the key field
    EXPECT_EQ(std::string::npos, log.find("AWSSSECKEYS=::::"));
}

TEST_F(LoadEnvSseCKeysRedactTest, BinarySentinel_NoLeakToLog) {
    // Sentinel string that should NEVER appear in any log
    const char* sentinel = "SENTINEL_SSE_KEY_DO_NOT_LEAK_0123456789";
    setenv("AWSSSECKEYS", sentinel, 1);
    testing::internal::CaptureStdout();
    bool result = S3fsCurl::LoadEnvSseCKeys();
    std::string log = testing::internal::GetCapturedStdout();

    // Either parses (unlikely with stubs) or fails; in either case the
    // sentinel must not appear in any log output at all.
    (void)result;
    EXPECT_EQ(std::string::npos, log.find(sentinel));
}

TEST_F(CurlExtendedTest, PushbackSseKeys_EmptyKey) {
    string key = "";
    EXPECT_FALSE(S3fsCurl::PushbackSseKeys(key));
}

TEST_F(CurlExtendedTest, PushbackSseKeys_CommentKey) {
    string key = "# this is a comment";
    EXPECT_FALSE(S3fsCurl::PushbackSseKeys(key));
}

TEST_F(CurlExtendedTest, PushbackSseKeys_WhitespaceOnly) {
    string key = "   ";
    EXPECT_FALSE(S3fsCurl::PushbackSseKeys(key));
}

TEST_F(CurlExtendedTest, PushbackSseKeys_ValidKey) {
    // [ISSUE2026040800001 M1] PushbackSseKeys now calls s3fs_base64 (real) and
    // s3fs_get_content_md5_hws_obs (real, in-memory) instead of the removed
    // make_md5_from_string tmpfile path.
    size_t old_size = S3fsCurl::sseckeys.size();
    string key = "my-secret-sse-key-32byteslong!!X";
    bool result = S3fsCurl::PushbackSseKeys(key);
    // May succeed or fail depending on stub md5 behavior
    (void)result;
    // Restore
    while(S3fsCurl::sseckeys.size() > old_size) {
        S3fsCurl::sseckeys.pop_back();
    }
}

TEST_F(CurlExtendedTest, GetSseKey_NoKeys) {
    sseckeylist_t old_keys = S3fsCurl::sseckeys;
    S3fsCurl::sseckeys.clear();

    string md5, ssekey;
    EXPECT_FALSE(S3fsCurl::GetSseKey(md5, ssekey));

    S3fsCurl::sseckeys = old_keys;
}

TEST_F(CurlExtendedTest, GetSseKey_WithKeys) {
    sseckeylist_t old_keys = S3fsCurl::sseckeys;
    S3fsCurl::sseckeys.clear();

    // Manually insert a key
    sseckeymap_t md5map;
    md5map["test-md5"] = "test-base64-key";
    S3fsCurl::sseckeys.push_back(md5map);

    string md5, ssekey;
    // Empty md5 should return first key
    EXPECT_TRUE(S3fsCurl::GetSseKey(md5, ssekey));
    EXPECT_EQ("test-md5", md5);
    EXPECT_EQ("test-base64-key", ssekey);

    S3fsCurl::sseckeys = old_keys;
}

TEST_F(CurlExtendedTest, GetSseKey_WithMd5Match) {
    sseckeylist_t old_keys = S3fsCurl::sseckeys;
    S3fsCurl::sseckeys.clear();

    sseckeymap_t md5map1;
    md5map1["md5-first"] = "key-first";
    S3fsCurl::sseckeys.push_back(md5map1);

    sseckeymap_t md5map2;
    md5map2["md5-second"] = "key-second";
    S3fsCurl::sseckeys.push_back(md5map2);

    string md5 = "md5-second";
    string ssekey;
    EXPECT_TRUE(S3fsCurl::GetSseKey(md5, ssekey));
    EXPECT_EQ("key-second", ssekey);

    S3fsCurl::sseckeys = old_keys;
}

TEST_F(CurlExtendedTest, GetSseKey_NoMatch) {
    sseckeylist_t old_keys = S3fsCurl::sseckeys;
    S3fsCurl::sseckeys.clear();

    sseckeymap_t md5map;
    md5map["md5-one"] = "key-one";
    S3fsCurl::sseckeys.push_back(md5map);

    string md5 = "nonexistent-md5";
    string ssekey;
    EXPECT_FALSE(S3fsCurl::GetSseKey(md5, ssekey));

    S3fsCurl::sseckeys = old_keys;
}

TEST_F(CurlExtendedTest, GetSseKeyMd5_ValidPos) {
    sseckeylist_t old_keys = S3fsCurl::sseckeys;
    S3fsCurl::sseckeys.clear();

    sseckeymap_t md5map;
    md5map["test-md5-value"] = "test-key";
    S3fsCurl::sseckeys.push_back(md5map);

    string md5;
    EXPECT_TRUE(S3fsCurl::GetSseKeyMd5(0, md5));
    EXPECT_EQ("test-md5-value", md5);

    S3fsCurl::sseckeys = old_keys;
}

TEST_F(CurlExtendedTest, GetSseKeyMd5_NegativePos) {
    string md5;
    EXPECT_FALSE(S3fsCurl::GetSseKeyMd5(-1, md5));
}

TEST_F(CurlExtendedTest, GetSseKeyMd5_OutOfRange) {
    string md5;
    EXPECT_FALSE(S3fsCurl::GetSseKeyMd5(9999, md5));
}

// ==========================================================================
// 4. XML Parsing - GetUploadId
// ==========================================================================

TEST_F(CurlExtendedTest, GetUploadId_ValidXml) {
    S3fsCurl s3curl;
    s3curl.bodydata = new BodyData();
    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<InitiateMultipartUploadResult>"
        "<Bucket>mybucket</Bucket>"
        "<Key>mykey</Key>"
        "<UploadId>upload-id-12345</UploadId>"
        "</InitiateMultipartUploadResult>";
    s3curl.bodydata->Append((void*)xml, strlen(xml));

    string upload_id;
    bool result = s3curl.GetUploadId(upload_id);
    EXPECT_TRUE(result);
    EXPECT_EQ("upload-id-12345", upload_id);

    delete s3curl.bodydata;
    s3curl.bodydata = NULL;
}

TEST_F(CurlExtendedTest, GetUploadId_MissingElement) {
    S3fsCurl s3curl;
    s3curl.bodydata = new BodyData();
    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<InitiateMultipartUploadResult>"
        "<Bucket>mybucket</Bucket>"
        "<Key>mykey</Key>"
        "</InitiateMultipartUploadResult>";
    s3curl.bodydata->Append((void*)xml, strlen(xml));

    string upload_id;
    bool result = s3curl.GetUploadId(upload_id);
    EXPECT_FALSE(result);
    EXPECT_TRUE(upload_id.empty());

    delete s3curl.bodydata;
    s3curl.bodydata = NULL;
}

TEST_F(CurlExtendedTest, GetUploadId_NullBody) {
    S3fsCurl s3curl;
    s3curl.bodydata = NULL;
    string upload_id;
    bool result = s3curl.GetUploadId(upload_id);
    EXPECT_FALSE(result);
}

TEST_F(CurlExtendedTest, GetUploadId_EmptyBody) {
    S3fsCurl s3curl;
    s3curl.bodydata = new BodyData();
    // Empty body - no data appended

    string upload_id;
    bool result = s3curl.GetUploadId(upload_id);
    EXPECT_FALSE(result);

    delete s3curl.bodydata;
    s3curl.bodydata = NULL;
}

TEST_F(CurlExtendedTest, GetUploadId_InvalidXml) {
    S3fsCurl s3curl;
    s3curl.bodydata = new BodyData();
    const char* xml = "this is not valid xml at all";
    s3curl.bodydata->Append((void*)xml, strlen(xml));

    string upload_id;
    bool result = s3curl.GetUploadId(upload_id);
    EXPECT_FALSE(result);

    delete s3curl.bodydata;
    s3curl.bodydata = NULL;
}

TEST_F(CurlExtendedTest, GetUploadId_EmptyUploadIdElement) {
    S3fsCurl s3curl;
    s3curl.bodydata = new BodyData();
    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<InitiateMultipartUploadResult>"
        "<UploadId></UploadId>"
        "</InitiateMultipartUploadResult>";
    s3curl.bodydata->Append((void*)xml, strlen(xml));

    string upload_id;
    bool result = s3curl.GetUploadId(upload_id);
    // Empty element has no text child node, so should return false
    EXPECT_FALSE(result);

    delete s3curl.bodydata;
    s3curl.bodydata = NULL;
}

TEST_F(CurlExtendedTest, GetUploadId_LongUploadId) {
    S3fsCurl s3curl;
    s3curl.bodydata = new BodyData();
    string long_id(256, 'A');
    string xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<InitiateMultipartUploadResult>"
        "<UploadId>" + long_id + "</UploadId>"
        "</InitiateMultipartUploadResult>";
    s3curl.bodydata->Append((void*)xml.c_str(), xml.size());

    string upload_id;
    bool result = s3curl.GetUploadId(upload_id);
    EXPECT_TRUE(result);
    EXPECT_EQ(long_id, upload_id);

    delete s3curl.bodydata;
    s3curl.bodydata = NULL;
}

// ==========================================================================
// 5. Request Setup - PreHeadRequest
//    Note: PreHeadRequest calls CreateCurlHandle which needs sCurlPool.
//    We test it by manually initializing the pool first.
// ==========================================================================

TEST_F(CurlExtendedTest, PreHeadRequest_NullPath) {
    S3fsCurl s3curl;
    // Even without a valid pool, NULL path should return false immediately
    EXPECT_FALSE(s3curl.PreHeadRequest(NULL));
}

TEST_F(CurlExtendedTest, PreHeadRequest_ValidPath_WithPool) {
    // Initialize a minimal curl pool so CreateCurlHandle works
    CurlHandlerPool* pool = new CurlHandlerPool(4);
    ASSERT_TRUE(pool->Init());
    CurlHandlerPool* old_pool = S3fsCurl::sCurlPool;
    S3fsCurl::sCurlPool = pool;

    bucket = "testbucket";
    host = "https://obs.example.com";
    pathrequeststyle = true;
    S3fsCurl::is_sigv4 = false;
    S3fsCurl::is_ibm_iam_auth = false;
    S3fsCurl::IAM_role = "";
    S3fsCurl::is_ecs = false;
    S3fsCurl::AWSAccessTokenExpire = time(NULL) + 86400;

    S3fsCurl s3curl;
    bool result = s3curl.PreHeadRequest("/test/file.txt");
    EXPECT_TRUE(result);
    EXPECT_EQ("/test/file.txt", s3curl.path);
    EXPECT_EQ("HEAD", s3curl.op);

    // Cleanup
    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    if(s3curl.hCurl) {
        pool->ReturnHandler(s3curl.hCurl);
        s3curl.hCurl = NULL;
    }

    S3fsCurl::sCurlPool = old_pool;
    pool->Destroy();
    delete pool;
}

TEST_F(CurlExtendedTest, PreHeadRequest_WithBasePath) {
    CurlHandlerPool* pool = new CurlHandlerPool(4);
    ASSERT_TRUE(pool->Init());
    CurlHandlerPool* old_pool = S3fsCurl::sCurlPool;
    S3fsCurl::sCurlPool = pool;

    bucket = "testbucket";
    host = "https://obs.example.com";
    pathrequeststyle = true;
    S3fsCurl::is_sigv4 = false;
    S3fsCurl::is_ibm_iam_auth = false;
    S3fsCurl::IAM_role = "";
    S3fsCurl::is_ecs = false;
    S3fsCurl::AWSAccessTokenExpire = time(NULL) + 86400;

    S3fsCurl s3curl;
    bool result = s3curl.PreHeadRequest("/test/file.txt", "/base", "/saved");
    EXPECT_TRUE(result);
    EXPECT_EQ("/base", s3curl.base_path);
    EXPECT_EQ("/saved", s3curl.saved_path);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    if(s3curl.hCurl) {
        pool->ReturnHandler(s3curl.hCurl);
        s3curl.hCurl = NULL;
    }

    S3fsCurl::sCurlPool = old_pool;
    pool->Destroy();
    delete pool;
}

TEST_F(CurlExtendedTest, PreHeadRequest_WithNullBasePath) {
    CurlHandlerPool* pool = new CurlHandlerPool(4);
    ASSERT_TRUE(pool->Init());
    CurlHandlerPool* old_pool = S3fsCurl::sCurlPool;
    S3fsCurl::sCurlPool = pool;

    bucket = "testbucket";
    host = "https://obs.example.com";
    pathrequeststyle = true;
    S3fsCurl::is_sigv4 = false;
    S3fsCurl::is_ibm_iam_auth = false;
    S3fsCurl::IAM_role = "";
    S3fsCurl::is_ecs = false;
    S3fsCurl::AWSAccessTokenExpire = time(NULL) + 86400;

    S3fsCurl s3curl;
    bool result = s3curl.PreHeadRequest("/test/file.txt", NULL, NULL);
    EXPECT_TRUE(result);
    EXPECT_TRUE(s3curl.base_path.empty());
    EXPECT_TRUE(s3curl.saved_path.empty());

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    if(s3curl.hCurl) {
        pool->ReturnHandler(s3curl.hCurl);
        s3curl.hCurl = NULL;
    }

    S3fsCurl::sCurlPool = old_pool;
    pool->Destroy();
    delete pool;
}

TEST_F(CurlExtendedTest, PreHeadRequest_WithSseKeyPos_NoKeys) {
    CurlHandlerPool* pool = new CurlHandlerPool(4);
    ASSERT_TRUE(pool->Init());
    CurlHandlerPool* old_pool = S3fsCurl::sCurlPool;
    S3fsCurl::sCurlPool = pool;

    bucket = "testbucket";
    host = "https://obs.example.com";
    pathrequeststyle = true;
    S3fsCurl::is_sigv4 = false;
    S3fsCurl::is_ibm_iam_auth = false;
    S3fsCurl::IAM_role = "";
    S3fsCurl::is_ecs = false;
    S3fsCurl::AWSAccessTokenExpire = time(NULL) + 86400;

    S3fsCurl s3curl;
    // ssekey_pos=0 but no SSE keys loaded -> should fail
    bool result = s3curl.PreHeadRequest("/test/file.txt", NULL, NULL, 0);
    EXPECT_FALSE(result);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    if(s3curl.hCurl) {
        pool->ReturnHandler(s3curl.hCurl);
        s3curl.hCurl = NULL;
    }

    S3fsCurl::sCurlPool = old_pool;
    pool->Destroy();
    delete pool;
}

TEST_F(CurlExtendedTest, PreHeadRequest_WithSseKeyPos_DefaultNeg1) {
    CurlHandlerPool* pool = new CurlHandlerPool(4);
    ASSERT_TRUE(pool->Init());
    CurlHandlerPool* old_pool = S3fsCurl::sCurlPool;
    S3fsCurl::sCurlPool = pool;

    bucket = "testbucket";
    host = "https://obs.example.com";
    pathrequeststyle = true;
    S3fsCurl::is_sigv4 = false;
    S3fsCurl::is_ibm_iam_auth = false;
    S3fsCurl::IAM_role = "";
    S3fsCurl::is_ecs = false;
    S3fsCurl::AWSAccessTokenExpire = time(NULL) + 86400;

    S3fsCurl s3curl;
    // Default ssekey_pos=-1 should skip SSE key handling
    bool result = s3curl.PreHeadRequest("/test/file.txt", NULL, NULL, -1);
    EXPECT_TRUE(result);
    EXPECT_EQ(-1, s3curl.b_ssekey_pos);

    curl_slist_free_all(s3curl.requestHeaders);
    s3curl.requestHeaders = NULL;
    if(s3curl.hCurl) {
        pool->ReturnHandler(s3curl.hCurl);
        s3curl.hCurl = NULL;
    }

    S3fsCurl::sCurlPool = old_pool;
    pool->Destroy();
    delete pool;
}

// ==========================================================================
// 6. Multi-curl
// ==========================================================================

TEST_F(CurlExtendedTest, S3fsMultiCurl_SetS3fsCurlObject_Valid) {
    S3fsMultiCurl multi;
    S3fsCurl* s3curl = new S3fsCurl();
    s3curl->hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl->hCurl);

    EXPECT_TRUE(multi.SetS3fsCurlObject(s3curl));
    // Object is now owned by multi, Clear will handle cleanup
    multi.Clear();
}

TEST_F(CurlExtendedTest, S3fsMultiCurl_SetS3fsCurlObject_Null) {
    S3fsMultiCurl multi;
    EXPECT_FALSE(multi.SetS3fsCurlObject(NULL));
}

TEST_F(CurlExtendedTest, S3fsMultiCurl_SetS3fsCurlObject_NoHandle) {
    S3fsMultiCurl multi;
    S3fsCurl* s3curl = new S3fsCurl();
    // No curl handle set (hCurl is NULL)
    // SetS3fsCurlObject uses hCurl as map key, NULL key causes find to fail
    // and it may or may not add it (depends on implementation)
    bool result = multi.SetS3fsCurlObject(s3curl);
    if(!result) {
        delete s3curl;
    } else {
        multi.Clear();
    }
}

TEST_F(CurlExtendedTest, S3fsMultiCurl_SetS3fsCurlObject_Duplicate) {
    S3fsMultiCurl multi;
    S3fsCurl* s3curl = new S3fsCurl();
    s3curl->hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl->hCurl);

    EXPECT_TRUE(multi.SetS3fsCurlObject(s3curl));
    // Adding same object again should fail (same hCurl key)
    EXPECT_FALSE(multi.SetS3fsCurlObject(s3curl));

    multi.Clear();
}

TEST_F(CurlExtendedTest, S3fsMultiCurl_SetMaxMultiRequest) {
    int old = S3fsMultiCurl::SetMaxMultiRequest(42);
    EXPECT_EQ(42, S3fsMultiCurl::GetMaxMultiRequest());
    S3fsMultiCurl::SetMaxMultiRequest(old);
}

TEST_F(CurlExtendedTest, S3fsMultiCurl_SetCallbacks) {
    S3fsMultiCurl multi;
    S3fsMultiSuccessCallback old_success = multi.SetSuccessCallback(NULL);
    S3fsMultiRetryCallback old_retry = multi.SetRetryCallback(NULL);
    // Restore
    multi.SetSuccessCallback(old_success);
    multi.SetRetryCallback(old_retry);
}

TEST_F(CurlExtendedTest, S3fsMultiCurl_ClearEmpty) {
    S3fsMultiCurl multi;
    // Clearing an empty multi should work fine
    EXPECT_TRUE(multi.Clear());
}

TEST_F(CurlExtendedTest, S3fsMultiCurl_MultipleObjects) {
    S3fsMultiCurl multi;

    S3fsCurl* s3curl1 = new S3fsCurl();
    s3curl1->hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl1->hCurl);

    S3fsCurl* s3curl2 = new S3fsCurl();
    s3curl2->hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl2->hCurl);

    EXPECT_TRUE(multi.SetS3fsCurlObject(s3curl1));
    EXPECT_TRUE(multi.SetS3fsCurlObject(s3curl2));

    multi.Clear();
}

// ==========================================================================
// 7. Error/Debug
// ==========================================================================

TEST_F(CurlExtendedTest, PrintRequestId4ResponseErr_NoHeaders) {
    S3fsCurl s3curl;
    s3curl.LastResponseCode = 500;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.responseHeaders.clear();
    string result = s3curl.PrintRequestId4ResponseErr();
    // Should return empty string when no x-amz-request-id header
    EXPECT_TRUE(result.empty());
}

TEST_F(CurlExtendedTest, PrintRequestId4ResponseErr_WithRequestId) {
    S3fsCurl s3curl;
    s3curl.LastResponseCode = 500;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.responseHeaders["x-amz-request-id"] = "REQ123ABC";
    string result = s3curl.PrintRequestId4ResponseErr();
    EXPECT_EQ("REQ123ABC", result);
}

TEST_F(CurlExtendedTest, PrintRequestId4ResponseErr_Response404) {
    S3fsCurl s3curl;
    s3curl.LastResponseCode = 404;
    s3curl.url = "https://obs.example.com/bucket/missing-key";
    s3curl.responseHeaders["x-amz-request-id"] = "REQ404";
    string result = s3curl.PrintRequestId4ResponseErr();
    EXPECT_EQ("REQ404", result);
}

TEST_F(CurlExtendedTest, PrintRequestId4ResponseErr_Response416) {
    S3fsCurl s3curl;
    s3curl.LastResponseCode = 416;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.responseHeaders["x-amz-request-id"] = "REQ416";
    string result = s3curl.PrintRequestId4ResponseErr();
    EXPECT_EQ("REQ416", result);
}

TEST_F(CurlExtendedTest, PrintRequestId4ResponseErr_SuccessResponse) {
    S3fsCurl s3curl;
    s3curl.LastResponseCode = 200;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.responseHeaders["x-amz-request-id"] = "REQ200";
    string result = s3curl.PrintRequestId4ResponseErr();
    // Response < 400 and > 0 means logflag=false, but we still return requestIdStr
    // Actually, the code breaks before finding the header when logflag=false
    // Let me re-read: it breaks out of do-while, then checks 404/416, else if logflag
    // So the return value is still "" because it breaks before finding the header
    EXPECT_TRUE(result.empty());
}

TEST_F(CurlExtendedTest, PrintRequestId4ResponseErr_ResponseZero) {
    S3fsCurl s3curl;
    s3curl.LastResponseCode = 0;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.responseHeaders.clear();
    string result = s3curl.PrintRequestId4ResponseErr();
    // LastResponseCode 0 is not < 400 && > 0, so continues to search headers
    EXPECT_TRUE(result.empty());
}

TEST_F(CurlExtendedTest, PrintRequestId4ResponseErr_NegativeResponse) {
    S3fsCurl s3curl;
    s3curl.LastResponseCode = -1;
    s3curl.url = "https://obs.example.com/bucket/key";
    s3curl.responseHeaders["x-amz-request-id"] = "REQ-NEG";
    string result = s3curl.PrintRequestId4ResponseErr();
    EXPECT_EQ("REQ-NEG", result);
}

// ==========================================================================
// 8. Additional Coverage: CheckIAMCredentialUpdate
// ==========================================================================

TEST_F(CurlExtendedTest, CheckIAMCredentialUpdate_NoRole) {
    string old_role = S3fsCurl::IAM_role;
    bool old_ecs = S3fsCurl::is_ecs;
    bool old_ibm = S3fsCurl::is_ibm_iam_auth;

    S3fsCurl::IAM_role = "";
    S3fsCurl::is_ecs = false;
    S3fsCurl::is_ibm_iam_auth = false;

    // No role, no ECS, no IBM IAM -> returns true immediately
    EXPECT_TRUE(S3fsCurl::CheckIAMCredentialUpdate());

    S3fsCurl::IAM_role = old_role;
    S3fsCurl::is_ecs = old_ecs;
    S3fsCurl::is_ibm_iam_auth = old_ibm;
}

TEST_F(CurlExtendedTest, CheckIAMCredentialUpdate_NotExpired) {
    string old_role = S3fsCurl::IAM_role;
    time_t old_expire = S3fsCurl::AWSAccessTokenExpire;

    S3fsCurl::IAM_role = "some-role";
    S3fsCurl::AWSAccessTokenExpire = time(NULL) + 86400;  // Far in the future

    EXPECT_TRUE(S3fsCurl::CheckIAMCredentialUpdate());

    S3fsCurl::IAM_role = old_role;
    S3fsCurl::AWSAccessTokenExpire = old_expire;
}

// ==========================================================================
// 9. Additional Coverage: Hws_curl_easy_perform
// ==========================================================================

TEST_F(CurlExtendedTest, HwsCurlEasyPerform_NullHandle) {
    S3fsCurl s3curl;
    s3curl.hCurl = NULL;
    // Calling with NULL handle - curl_easy_perform will return error
    // We just verify it doesn't crash
    CURLcode code = s3curl.Hws_curl_easy_perform();
    EXPECT_NE(CURLE_OK, code);
}

// ==========================================================================
// 10. Additional Coverage: GetResponseCode
// ==========================================================================

TEST_F(CurlExtendedTest, GetResponseCode_NoHandle) {
    S3fsCurl s3curl;
    s3curl.hCurl = NULL;
    long code = 0;
    EXPECT_FALSE(s3curl.GetResponseCode(code));
}

TEST_F(CurlExtendedTest, GetResponseCode_WithHandle) {
    S3fsCurl s3curl;
    s3curl.hCurl = curl_easy_init();
    ASSERT_NE((CURL*)NULL, s3curl.hCurl);
    long code = 0;
    bool result = s3curl.GetResponseCode(code);
    // Should succeed but code is 0 since no request was made
    EXPECT_TRUE(result);
    EXPECT_EQ(0, code);
    curl_easy_cleanup(s3curl.hCurl);
    s3curl.hCurl = NULL;
}

// ==========================================================================
// 11. Additional Coverage: DetectBucketTypeFromHeaders
// ==========================================================================

TEST_F(CurlExtendedTest, DetectBucketTypeFromHeaders_EmptyHeaders) {
    headers_t headers;
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_FALSE(result);
}

TEST_F(CurlExtendedTest, DetectBucketTypeFromHeaders_WithObsBucketType) {
    headers_t headers;
    headers["x-obs-bucket-type"] = "POSIX";
    bucket_type_t saved = g_bucket_type;
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_TRUE(result);
    EXPECT_EQ(BUCKET_TYPE_FILE_SEMANTIC, g_bucket_type);
    g_bucket_type = saved;
}

TEST_F(CurlExtendedTest, DetectBucketTypeFromHeaders_WithFsFileInterface) {
    headers_t headers;
    headers["x-obs-fs-file-interface"] = "Enabled";
    bucket_type_t saved = g_bucket_type;
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_TRUE(result);
    EXPECT_EQ(BUCKET_TYPE_FILE_SEMANTIC, g_bucket_type);
    g_bucket_type = saved;
}

TEST_F(CurlExtendedTest, DetectBucketTypeFromHeaders_NonPosixBucketType) {
    headers_t headers;
    headers["x-obs-bucket-type"] = "STANDARD";
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_FALSE(result);
}

// ==========================================================================
// 12. Additional Coverage: IBMIAMAuth path in ParseIAMCredentialResponse
// ==========================================================================

TEST_F(CurlExtendedTest, ParseIAMCredentialResponse_IBMIAMAuth) {
    bool old_ibm = S3fsCurl::is_ibm_iam_auth;
    string old_expiry = S3fsCurl::IAM_expiry_field;

    S3fsCurl::is_ibm_iam_auth = true;
    S3fsCurl::IAM_expiry_field = "expiration";

    iamcredmap_t keyval;
    // IBM IAM uses integer expiry, not quoted string
    const char* response =
        "{ \"access_token\" : \"ibm-token\" , "
        "\"expiration\" : 1735689600 }";

    // IAM_token_field needs to match what's in response
    string old_token_field = S3fsCurl::IAM_token_field;
    S3fsCurl::IAM_token_field = "access_token";

    bool result = S3fsCurl::ParseIAMCredentialResponse(response, keyval);
    EXPECT_TRUE(result);

    // Check that expiration was parsed as integer
    if(keyval.find("expiration") != keyval.end()) {
        EXPECT_EQ("1735689600", keyval["expiration"]);
    }

    S3fsCurl::is_ibm_iam_auth = old_ibm;
    S3fsCurl::IAM_expiry_field = old_expiry;
    S3fsCurl::IAM_token_field = old_token_field;
}

// ==========================================================================
// 13. Additional Coverage: AddUserAgent
// ==========================================================================

TEST_F(CurlExtendedTest, AddUserAgent_WithHandle) {
    CURL* h = curl_easy_init();
    ASSERT_NE((CURL*)NULL, h);

    bool old_ua = S3fsCurl::is_ua;
    S3fsCurl::is_ua = true;
    S3fsCurl::InitUserAgent();

    bool result = S3fsCurl::AddUserAgent(h);
    EXPECT_TRUE(result);

    S3fsCurl::is_ua = old_ua;
    curl_easy_cleanup(h);
}

TEST_F(CurlExtendedTest, AddUserAgent_Disabled) {
    CURL* h = curl_easy_init();
    ASSERT_NE((CURL*)NULL, h);

    bool old_ua = S3fsCurl::is_ua;
    S3fsCurl::is_ua = false;

    bool result = S3fsCurl::AddUserAgent(h);
    // When UA is disabled, AddUserAgent should still return true
    EXPECT_TRUE(result);

    S3fsCurl::is_ua = old_ua;
    curl_easy_cleanup(h);
}

// ==========================================================================
// 14. Additional Coverage: CurlDebugFunc
// ==========================================================================

TEST_F(CurlExtendedTest, CurlDebugFunc_NullHandle) {
    int result = S3fsCurl::CurlDebugFunc(NULL, CURLINFO_TEXT, (char*)"test", 4, NULL);
    EXPECT_EQ(0, result);
}

TEST_F(CurlExtendedTest, CurlDebugFunc_TextType) {
    CURL* h = curl_easy_init();
    ASSERT_NE((CURL*)NULL, h);
    char data[] = "Connection established\n";
    int result = S3fsCurl::CurlDebugFunc(h, CURLINFO_TEXT, data, strlen(data), NULL);
    EXPECT_EQ(0, result);
    curl_easy_cleanup(h);
}

TEST_F(CurlExtendedTest, CurlDebugFunc_HeaderIn) {
    CURL* h = curl_easy_init();
    ASSERT_NE((CURL*)NULL, h);
    char data[] = "HTTP/1.1 200 OK\r\n";
    int result = S3fsCurl::CurlDebugFunc(h, CURLINFO_HEADER_IN, data, strlen(data), NULL);
    EXPECT_EQ(0, result);
    curl_easy_cleanup(h);
}

TEST_F(CurlExtendedTest, CurlDebugFunc_HeaderOut) {
    CURL* h = curl_easy_init();
    ASSERT_NE((CURL*)NULL, h);
    char data[] = "GET /bucket/key HTTP/1.1\r\n";
    int result = S3fsCurl::CurlDebugFunc(h, CURLINFO_HEADER_OUT, data, strlen(data), NULL);
    EXPECT_EQ(0, result);
    curl_easy_cleanup(h);
}

TEST_F(CurlExtendedTest, CurlDebugFunc_DataTypes) {
    CURL* h = curl_easy_init();
    ASSERT_NE((CURL*)NULL, h);
    char data[] = "binary data";
    EXPECT_EQ(0, S3fsCurl::CurlDebugFunc(h, CURLINFO_DATA_IN, data, 11, NULL));
    EXPECT_EQ(0, S3fsCurl::CurlDebugFunc(h, CURLINFO_DATA_OUT, data, 11, NULL));
    EXPECT_EQ(0, S3fsCurl::CurlDebugFunc(h, CURLINFO_SSL_DATA_IN, data, 11, NULL));
    EXPECT_EQ(0, S3fsCurl::CurlDebugFunc(h, CURLINFO_SSL_DATA_OUT, data, 11, NULL));
    EXPECT_EQ(0, S3fsCurl::CurlDebugFunc(h, CURLINFO_END, data, 11, NULL));
    curl_easy_cleanup(h);
}

// ==========================================================================
// 15. Additional Coverage: DeleteRequest (null path)
// ==========================================================================

TEST_F(CurlExtendedTest, DeleteRequest_NullPath) {
    S3fsCurl s3curl;
    int result = s3curl.DeleteRequest(NULL);
    EXPECT_EQ(-1, result);
}

// ==========================================================================
// 16. Additional Coverage: CurlProgress callback
// ==========================================================================

TEST_F(CurlExtendedTest, CurlProgress_NullClientp) {
    int result = S3fsCurl::CurlProgress(NULL, 100.0, 50.0, 0.0, 0.0);
    EXPECT_EQ(0, result);
}

// ==========================================================================
// ISSUE2026040700001 P0-2: CurlDebugFunc sensitive-header redaction
// ==========================================================================
// All 18 edge cases for the CurlDebugFunc header redaction: Authorization,
// x-amz-security-token, SSE-C keys (including copy-source variants), case
// insensitivity, partial-match non-redaction, and robustness boundaries.

class CurlDebugFuncRedactTest : public ::testing::Test {
protected:
    s3fs_log_mode saved_log_mode;
    s3fs_log_level saved_level;
    CURL* h;

    void SetUp() override {
        saved_log_mode = debug_log_mode;
        saved_level    = debug_level;
        // In foreground mode, CurlDebugFunc writes to stderr (matches libcurl
        // native convention and Unix standard for verbose/debug output).
        debug_log_mode = LOG_MODE_FOREGROUND;
        debug_level    = S3FS_LOG_DBG;          // Max verbosity
        h = curl_easy_init();
        ASSERT_NE(nullptr, h);
    }
    void TearDown() override {
        if(h) curl_easy_cleanup(h);
        debug_level    = saved_level;
        debug_log_mode = saved_log_mode;
    }

    // Helper: run CurlDebugFunc with a header buffer and return captured log.
    // Foreground mode writes to stderr, so we CaptureStderr here.
    std::string RunHeaderOut(const char* header) {
        size_t len = strlen(header);
        testing::internal::CaptureStderr();
        S3fsCurl::CurlDebugFunc(h, CURLINFO_HEADER_OUT,
                                const_cast<char*>(header), len, nullptr);
        return testing::internal::GetCapturedStderr();
    }
    std::string RunHeaderIn(const char* header) {
        size_t len = strlen(header);
        testing::internal::CaptureStderr();
        S3fsCurl::CurlDebugFunc(h, CURLINFO_HEADER_IN,
                                const_cast<char*>(header), len, nullptr);
        return testing::internal::GetCapturedStderr();
    }
};

TEST_F(CurlDebugFuncRedactTest, RedactAuthorization) {
    const char* hdr =
        "Authorization: AWS4-HMAC-SHA256 "
        "Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request, "
        "SignedHeaders=host;x-amz-date, "
        "Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024\r\n";
    std::string log = RunHeaderOut(hdr);

    // Negative: no secrets
    EXPECT_EQ(std::string::npos, log.find("Signature="));
    EXPECT_EQ(std::string::npos, log.find("AKIAIOSFODNN7EXAMPLE"));
    EXPECT_EQ(std::string::npos, log.find("fe5f80f77d5fa3beca038a248ff027d0"));
    // Positive: header name preserved + redaction marker present
    EXPECT_NE(std::string::npos, log.find("Authorization:"));
    EXPECT_NE(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, RedactSecurityToken) {
    const char* hdr = "x-amz-security-token: FQoGZXIvYXdzEJr//////////wEaDGV4YW1wbGU\r\n";
    std::string log = RunHeaderOut(hdr);
    EXPECT_EQ(std::string::npos, log.find("FQoGZXIvYXdz"));
    EXPECT_NE(std::string::npos, log.find("x-amz-security-token:"));
    EXPECT_NE(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, RedactSseCKey) {
    const char* hdr = "x-amz-server-side-encryption-customer-key: dGVzdGtleWluYmFzZTY0\r\n";
    std::string log = RunHeaderOut(hdr);
    EXPECT_EQ(std::string::npos, log.find("dGVzdGtleWluYmFzZTY0"));
    EXPECT_NE(std::string::npos, log.find("x-amz-server-side-encryption-customer-key:"));
    EXPECT_NE(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, RedactSseCKeyMd5) {
    const char* hdr = "x-amz-server-side-encryption-customer-key-md5: S6GgYQ7vZvVE3u6WSVVdWA==\r\n";
    std::string log = RunHeaderOut(hdr);
    EXPECT_EQ(std::string::npos, log.find("S6GgYQ7vZvVE3u6WSVVdWA=="));
    EXPECT_NE(std::string::npos, log.find("x-amz-server-side-encryption-customer-key-md5:"));
    EXPECT_NE(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, RedactCopySourceSseCKey) {
    const char* hdr = "x-amz-copy-source-server-side-encryption-customer-key: Y29weS1zb3VyY2Uta2V5\r\n";
    std::string log = RunHeaderOut(hdr);
    EXPECT_EQ(std::string::npos, log.find("Y29weS1zb3VyY2Uta2V5"));
    EXPECT_NE(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, RedactCopySourceSseCKeyMd5) {
    const char* hdr = "x-amz-copy-source-server-side-encryption-customer-key-md5: UMY0nxdEcU3Ehm3K8kOYYQ==\r\n";
    std::string log = RunHeaderOut(hdr);
    EXPECT_EQ(std::string::npos, log.find("UMY0nxdEcU3Ehm3K8kOYYQ=="));
    EXPECT_NE(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, CaseInsensitive_Lower) {
    const char* hdr = "authorization: SECRET_VALUE_LOWER\r\n";
    std::string log = RunHeaderOut(hdr);
    EXPECT_EQ(std::string::npos, log.find("SECRET_VALUE_LOWER"));
    EXPECT_NE(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, CaseInsensitive_Upper) {
    const char* hdr = "AUTHORIZATION: SECRET_VALUE_UPPER\r\n";
    std::string log = RunHeaderOut(hdr);
    EXPECT_EQ(std::string::npos, log.find("SECRET_VALUE_UPPER"));
    EXPECT_NE(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, CaseInsensitive_Mixed) {
    const char* hdr = "AuThOrIzAtIoN: SECRET_VALUE_MIXED\r\n";
    std::string log = RunHeaderOut(hdr);
    EXPECT_EQ(std::string::npos, log.find("SECRET_VALUE_MIXED"));
    EXPECT_NE(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, NormalHeaderUnchanged) {
    const char* hdr = "Content-Type: application/xml\r\n";
    std::string log = RunHeaderOut(hdr);
    // Normal header passes through unchanged
    EXPECT_NE(std::string::npos, log.find("Content-Type:"));
    EXPECT_NE(std::string::npos, log.find("application/xml"));
    EXPECT_EQ(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, PartialMatchNotRedacted) {
    // "X-My-Authorization-Header" contains "Authorization" but is NOT the
    // "Authorization:" header — must not be redacted.
    const char* hdr = "X-My-Authorization-Header: harmless_value\r\n";
    std::string log = RunHeaderOut(hdr);
    EXPECT_NE(std::string::npos, log.find("harmless_value"));
    EXPECT_EQ(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, EmptyHeaderLine) {
    const char* hdr = "\r\n";
    // Should not crash; output may or may not contain anything meaningful
    std::string log = RunHeaderIn(hdr);
    (void)log;
    SUCCEED();
}

TEST_F(CurlDebugFuncRedactTest, HeaderNoColon_StatusLine) {
    // HTTP status line has no colon -> cannot match any sensitive prefix
    const char* hdr = "HTTP/1.1 200 OK\r\n";
    std::string log = RunHeaderIn(hdr);
    EXPECT_NE(std::string::npos, log.find("HTTP/1.1 200 OK"));
    EXPECT_EQ(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, MixedHeaders_OneSensitiveOnePlain) {
    const char* hdr =
        "Content-Type: application/xml\r\n"
        "Authorization: AWS4-HMAC-SHA256 Credential=AKIA_SECRET_KEY, Signature=deadbeef\r\n";
    std::string log = RunHeaderOut(hdr);
    // Plain header passes through
    EXPECT_NE(std::string::npos, log.find("Content-Type:"));
    EXPECT_NE(std::string::npos, log.find("application/xml"));
    // Sensitive header is redacted
    EXPECT_EQ(std::string::npos, log.find("AKIA_SECRET_KEY"));
    EXPECT_EQ(std::string::npos, log.find("deadbeef"));
    EXPECT_NE(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, EmptyValueAfterColon) {
    const char* hdr = "Authorization:\r\n";
    std::string log = RunHeaderOut(hdr);
    // Should redact without crashing, even with empty value
    EXPECT_NE(std::string::npos, log.find("Authorization:"));
    EXPECT_NE(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, LongValueOver4KB) {
    std::string longval(4096, 'A');
    std::string hdr = "Authorization: " + longval + "\r\n";
    std::string log = RunHeaderOut(hdr.c_str());
    // Long 4096-'A' value must not appear in full — key is that
    // the "AAAA..." sequence of 4096 chars is absent.
    EXPECT_EQ(std::string::npos, log.find(longval));
    EXPECT_NE(std::string::npos, log.find("Authorization:"));
    EXPECT_NE(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, Foreground_WritesToStderrWithRedaction) {
    // Regression for ISSUE2026040700001 P0-2:
    // (1) CurlDebugFunc must be registered and callable in foreground mode
    //     (the pre-fix code skipped registration, letting libcurl write raw
    //     headers to stderr unredacted).
    // (2) The verbose output must go to stderr (libcurl native convention,
    //     Unix standard for debug output) — NOT stdout.
    // (3) Sensitive header values must be redacted.
    const char* hdr = "Authorization: SECRET_FG\r\n";

    // Capture both streams to verify the routing
    testing::internal::CaptureStdout();
    testing::internal::CaptureStderr();
    S3fsCurl::CurlDebugFunc(h, CURLINFO_HEADER_OUT,
                            const_cast<char*>(hdr), strlen(hdr), nullptr);
    std::string out = testing::internal::GetCapturedStdout();
    std::string err = testing::internal::GetCapturedStderr();

    // Verbose output must go to stderr, not stdout
    EXPECT_TRUE(out.empty()) << "CurlDebugFunc leaked to stdout: [" << out << "]";
    EXPECT_FALSE(err.empty()) << "CurlDebugFunc produced no stderr output in foreground mode";

    // Secret not present anywhere
    EXPECT_EQ(std::string::npos, err.find("SECRET_FG"));
    EXPECT_EQ(std::string::npos, out.find("SECRET_FG"));

    // Redaction marker present on stderr
    EXPECT_NE(std::string::npos, err.find("***REDACTED***"));
    EXPECT_NE(std::string::npos, err.find("Authorization:"));
}

// ==========================================================================
// ISSUE2026040700001 P2-1: CurlDebugFunc no-newline input fix
// ==========================================================================
// Pre-fix code did `length = eol - p` when eol was NULL (no '\n' or '\r' in
// the buffer), which is undefined behavior. Combined with the new
// is_sensitive_header_line(p, line_len) call, it could cause OOB reads for
// inputs shorter than the longest sensitive prefix (55 bytes).
// The fix sets `length = remaining` when eol is NULL so the full buffer is
// treated as the final line. These tests verify the behavior, not just
// that CurlDebugFunc returns 0 (which the pre-existing NoNewline test did).

TEST_F(CurlDebugFuncRedactTest, NoNewline_NormalHeader_FullyEmitted) {
    // Non-sensitive header with no trailing \r\n — must emit the full content
    const char* hdr = "Content-Type: application/xml";  // len=29, no newline
    std::string log = RunHeaderOut(hdr);
    EXPECT_NE(std::string::npos, log.find("Content-Type:"));
    EXPECT_NE(std::string::npos, log.find("application/xml"));
    EXPECT_EQ(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, NoNewline_SensitiveHeader_Redacted) {
    // Sensitive header with no trailing \r\n — must be redacted
    const char* hdr = "Authorization: SECRET_NO_NEWLINE";  // len=32, no newline
    std::string log = RunHeaderOut(hdr);
    EXPECT_EQ(std::string::npos, log.find("SECRET_NO_NEWLINE"));
    EXPECT_NE(std::string::npos, log.find("Authorization:"));
    EXPECT_NE(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, NoNewline_ShortInput_NoCrashNoOOB) {
    // Input shorter than the longest sensitive prefix (55 bytes), no newline.
    // Pre-fix code would dereference `length = NULL - p` giving a huge garbage
    // size_t, then is_sensitive_header_line would strncasecmp 14..55 bytes
    // past the end of the buffer. With the fix, length = remaining = 4 and
    // all prefix checks fail cleanly via the `len >= plen` guard.
    char small_buf[4];
    small_buf[0] = 'A'; small_buf[1] = 'u'; small_buf[2] = 't'; small_buf[3] = 'h';
    // Note: NOT NUL-terminated on purpose. libcurl buffers aren't guaranteed to be.
    testing::internal::CaptureStderr();
    int result = S3fsCurl::CurlDebugFunc(h, CURLINFO_HEADER_OUT,
                                          small_buf, sizeof(small_buf), nullptr);
    std::string log = testing::internal::GetCapturedStderr();
    EXPECT_EQ(0, result);
    // 4 bytes "Auth" should be emitted as-is (not redacted — no colon, no
    // complete match against any sensitive prefix)
    EXPECT_NE(std::string::npos, log.find("Auth"));
    EXPECT_EQ(std::string::npos, log.find("***REDACTED***"));
}

TEST_F(CurlDebugFuncRedactTest, NoNewline_EmptyInput_NoCrash) {
    // Edge case: size=0. Pre-fix this was also UB (NULL - data pointer).
    // After the fix, length = remaining = 0, loop exits cleanly.
    char dummy = 0;
    testing::internal::CaptureStderr();
    int result = S3fsCurl::CurlDebugFunc(h, CURLINFO_HEADER_OUT,
                                          &dummy, 0, nullptr);
    std::string log = testing::internal::GetCapturedStderr();
    EXPECT_EQ(0, result);
    (void)log;
}

TEST_F(CurlDebugFuncRedactTest, IsSensitiveHeaderLine_DirectCheck) {
    // Sanity check on the static helper (accessible because we #include curl.cpp)
    EXPECT_TRUE(is_sensitive_header_line("Authorization: foo", 19));
    EXPECT_TRUE(is_sensitive_header_line("x-amz-security-token: bar", 25));
    EXPECT_TRUE(is_sensitive_header_line("x-amz-server-side-encryption-customer-key: baz", 46));
    EXPECT_FALSE(is_sensitive_header_line("Content-Type: application/xml", 29));
    EXPECT_FALSE(is_sensitive_header_line("X-My-Authorization-Header: foo", 30));
    EXPECT_FALSE(is_sensitive_header_line("HTTP/1.1 200 OK", 15));
    // Too short to match any prefix
    EXPECT_FALSE(is_sensitive_header_line("Auth", 4));
}

// ==========================================================================
// ISSUE2026040700001 P0-2 (P2-2 补测): curl_verbose_emit 非 foreground 分支
// ==========================================================================
// 上面的 CurlDebugFuncRedactTest 只覆盖 foreground 模式（vfprintf(stderr)）。
// 这一组测试覆盖 curl_verbose_emit 的 else 分支：vsnprintf + S3FS_PRN_CURL
// → syslog/IndexLog，确保：
//   (1) 非 foreground 模式下 CurlDebugFunc 不崩溃
//   (2) 输出不会意外泄漏到 stdout/stderr（只应进 syslog/IndexLog）
//   (3) 敏感 header 的密钥内容不出现在 stdout/stderr 任一通道
// IndexLogEntry 在 test 环境下是早退的 no-op（g_log_service_enable_flag=FALSE），
// syslog 写 /dev/log，都不干扰 stdout/stderr 捕获，所以 CaptureStdout/Stderr
// 用来验证"路由没有意外回流到本地流"是可靠的。

class CurlDebugFuncNonForegroundTest : public ::testing::Test {
protected:
    s3fs_log_mode saved_log_mode;
    s3fs_log_level saved_level;
    CURL* h;

    void SetUp() override {
        saved_log_mode = debug_log_mode;
        saved_level    = debug_level;
        // LOG_MODE_OBSFS → 走 IndexLog 路径（stub 早退），无 /dev/log 依赖
        debug_log_mode = LOG_MODE_OBSFS;
        debug_level    = S3FS_LOG_DBG;
        h = curl_easy_init();
        ASSERT_NE(nullptr, h);
    }
    void TearDown() override {
        if(h) curl_easy_cleanup(h);
        debug_level    = saved_level;
        debug_log_mode = saved_log_mode;
    }

    // Returns pair<stdout, stderr>; both should be empty in non-foreground mode.
    std::pair<std::string,std::string> RunCapturingBoth(curl_infotype type,
                                                         const char* data, size_t len) {
        testing::internal::CaptureStdout();
        testing::internal::CaptureStderr();
        int result = S3fsCurl::CurlDebugFunc(h, type,
                                              const_cast<char*>(data), len, nullptr);
        std::string out = testing::internal::GetCapturedStdout();
        std::string err = testing::internal::GetCapturedStderr();
        EXPECT_EQ(0, result);
        return std::make_pair(out, err);
    }
};

TEST_F(CurlDebugFuncNonForegroundTest, NormalHeader_NoLeakToStdStreams) {
    const char* hdr = "Content-Type: application/xml\r\n";
    auto pair = RunCapturingBoth(CURLINFO_HEADER_OUT, hdr, strlen(hdr));
    // Output must go to IndexLog, NOT stdout/stderr
    EXPECT_TRUE(pair.first.empty()) << "leaked to stdout: [" << pair.first << "]";
    EXPECT_TRUE(pair.second.empty()) << "leaked to stderr: [" << pair.second << "]";
}

TEST_F(CurlDebugFuncNonForegroundTest, SensitiveHeader_NoLeakToStdStreams) {
    const char* hdr = "Authorization: AWS4-HMAC-SHA256 Signature=SECRET_NONFG\r\n";
    auto pair = RunCapturingBoth(CURLINFO_HEADER_OUT, hdr, strlen(hdr));
    // The secret must not surface on any local stream (even if redaction logic
    // somehow misrouted output back to stdout/stderr by mistake).
    EXPECT_EQ(std::string::npos, pair.first.find("SECRET_NONFG"));
    EXPECT_EQ(std::string::npos, pair.second.find("SECRET_NONFG"));
    EXPECT_TRUE(pair.first.empty());
    EXPECT_TRUE(pair.second.empty());
}

TEST_F(CurlDebugFuncNonForegroundTest, TextInfo_NoLeakToStdStreams) {
    const char* txt = "* Connected to example.com (1.2.3.4) port 443\n";
    auto pair = RunCapturingBoth(CURLINFO_TEXT, txt, strlen(txt));
    EXPECT_TRUE(pair.first.empty());
    EXPECT_TRUE(pair.second.empty());
}

TEST_F(CurlDebugFuncNonForegroundTest, MixedHeaders_NoLeakToStdStreams) {
    // Exercises the do-while loop over multiple headers in a single callback
    const char* hdr =
        "Content-Type: application/xml\r\n"
        "Authorization: AWS4-HMAC-SHA256 Signature=SECRET_MIX_NONFG\r\n"
        "x-amz-security-token: TOKEN_NONFG\r\n";
    auto pair = RunCapturingBoth(CURLINFO_HEADER_OUT, hdr, strlen(hdr));
    EXPECT_EQ(std::string::npos, pair.first.find("SECRET_MIX_NONFG"));
    EXPECT_EQ(std::string::npos, pair.second.find("SECRET_MIX_NONFG"));
    EXPECT_EQ(std::string::npos, pair.first.find("TOKEN_NONFG"));
    EXPECT_EQ(std::string::npos, pair.second.find("TOKEN_NONFG"));
    EXPECT_TRUE(pair.first.empty());
    EXPECT_TRUE(pair.second.empty());
}

TEST_F(CurlDebugFuncNonForegroundTest, LongNonSensitiveHeader_NoCrashNoLeak) {
    // Exercises the 4KB vsnprintf buffer in curl_verbose_emit with a long
    // non-sensitive header. Note: >4KB content IS truncated in the non-FG
    // path (known limitation), but must not crash and must not leak to
    // stdout/stderr. Picking 2KB to stay under the buffer limit.
    std::string longval(2048, 'X');
    std::string hdr = "X-Custom-Long: " + longval + "\r\n";
    auto pair = RunCapturingBoth(CURLINFO_HEADER_OUT, hdr.c_str(), hdr.length());
    EXPECT_TRUE(pair.first.empty());
    EXPECT_TRUE(pair.second.empty());
}

TEST_F(CurlDebugFuncNonForegroundTest, LongSensitiveHeader_NoLeakNoBufferIssue) {
    // 4KB Authorization value — sensitive path produces a SHORT output
    // ("> Authorization: ***REDACTED***" ~34 chars) so the 4KB buf has
    // plenty of room even though the input was 4KB.
    std::string longval(4096, 'A');
    std::string hdr = "Authorization: " + longval + "\r\n";
    auto pair = RunCapturingBoth(CURLINFO_HEADER_OUT, hdr.c_str(), hdr.length());
    // The 4096-A value must not appear on either local stream
    EXPECT_EQ(std::string::npos, pair.first.find(longval));
    EXPECT_EQ(std::string::npos, pair.second.find(longval));
    EXPECT_TRUE(pair.first.empty());
    EXPECT_TRUE(pair.second.empty());
}

TEST_F(CurlDebugFuncNonForegroundTest, LogModeSyslog_NoCrashNoLeak) {
    // Exercise the syslog branch of the S3FS_PRN_CURL macro (different
    // code path inside S3FS_LOW_LOGPRN2 than LOG_MODE_OBSFS).
    // Real syslog() writes to /dev/log which does not affect stdout/stderr.
    debug_log_mode = LOG_MODE_SYSLOG;

    const char* hdr = "Authorization: SECRET_SYSLOG\r\n";
    auto pair = RunCapturingBoth(CURLINFO_HEADER_OUT, hdr, strlen(hdr));
    EXPECT_EQ(std::string::npos, pair.first.find("SECRET_SYSLOG"));
    EXPECT_EQ(std::string::npos, pair.second.find("SECRET_SYSLOG"));
    EXPECT_TRUE(pair.first.empty());
    EXPECT_TRUE(pair.second.empty());

    debug_log_mode = LOG_MODE_OBSFS;  // restore for TearDown consistency
}

// ==========================================================================
// Main
// ==========================================================================

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    curl_global_init(CURL_GLOBAL_ALL);
    xmlInitParser();

    // Initialize locks used by S3fsCurl (normally done by InitS3fsCurl)
    pthread_spin_init(&S3fsCurl::curl_handles_spinlock, PTHREAD_PROCESS_PRIVATE);
    pthread_mutex_init(&S3fsCurl::curl_handles_lock, NULL);

    // Create a default handler pool so DestroyCurlHandle() can return handles
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
