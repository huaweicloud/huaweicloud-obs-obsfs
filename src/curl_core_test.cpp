/*
 * s3fs - FUSE-based file system backed by Amazon S3
 *
 * Core unit tests for curl (HTTP client) functionality
 * This test focuses on S3fsCurl class methods to improve coverage
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
 */

#include "gtest.h"
#include "curl.h"
#include "common.h"
#include <string>
#include <cstring>

// Stub variables and functions needed for linking
s3fs_log_level debug_level = S3FS_LOG_INFO;
s3fs_log_mode debug_log_mode = LOG_MODE_FOREGROUND;
const char* s3fs_log_nest[S3FS_LOG_NEST_MAX] = {"", "  ", "    ", "      "};

extern "C" {
void IndexLogEntry(const uint64_t, const char*, const uint32_t,
                   const char*, const char*, const uint32_t,
                   const char*, ...) {}
}
void s3fsStatisLogModeNotObsfs() {}

// Global variables provided by test_stubs.o
extern std::string host;
extern std::string bucket;
extern bool pathrequeststyle;

// ========================================================================
// Tests for S3fsCurl static methods
// ========================================================================

TEST(CurlCore, SetCheckCertificate) {
    // Test default value
    bool original = S3fsCurl::SetCheckCertificate(true);
    EXPECT_TRUE(original);

    // Test setting to false
    bool result = S3fsCurl::SetCheckCertificate(false);
    EXPECT_TRUE(result);  // Should return the previous value (true)

    // Test setting back to true
    result = S3fsCurl::SetCheckCertificate(true);
    EXPECT_FALSE(result);  // Should return the previous value (false)
}

TEST(CurlCore, SetDnsCache) {
    // Test default value
    bool original = S3fsCurl::SetDnsCache(true);
    EXPECT_TRUE(original);

    // Test setting to false
    bool result = S3fsCurl::SetDnsCache(false);
    EXPECT_TRUE(result);  // Should return the previous value (true)

    // Test setting back to true
    result = S3fsCurl::SetDnsCache(true);
    EXPECT_FALSE(result);  // Should return the previous value (false)
}

TEST(CurlCore, SetSslSessionCache) {
    // Test default value
    bool original = S3fsCurl::SetSslSessionCache(true);
    EXPECT_TRUE(original);

    // Test setting to false
    bool result = S3fsCurl::SetSslSessionCache(false);
    EXPECT_TRUE(result);  // Should return the previous value (true)

    // Test setting back to true
    result = S3fsCurl::SetSslSessionCache(true);
    EXPECT_FALSE(result);  // Should return the previous value (false)
}

TEST(CurlCore, SetRetries) {
    // Test default value
    int original = S3fsCurl::SetRetries(5);
    EXPECT_EQ(original, 3);  // Default is 3

    // Test setting to 10
    int result = S3fsCurl::SetRetries(10);
    EXPECT_EQ(result, 5);  // Should return the previous value (5)

    // Test setting to 0
    result = S3fsCurl::SetRetries(0);
    EXPECT_EQ(result, 10);  // Should return the previous value (10)

    // Test setting to negative (should return previous value)
    result = S3fsCurl::SetRetries(-1);
    EXPECT_EQ(result, 0);  // Should return the previous value (0)
}

TEST(CurlCore, SetPublicBucket) {
    // Test default value
    bool original = S3fsCurl::SetPublicBucket(true);
    EXPECT_FALSE(original);  // Default is false

    // Test setting to false
    bool result = S3fsCurl::SetPublicBucket(false);
    EXPECT_TRUE(result);  // Should return the previous value (true)

    // Test setting back to true
    result = S3fsCurl::SetPublicBucket(true);
    EXPECT_FALSE(result);  // Should return the previous value (false)
}

TEST(CurlCore, SetContentMd5) {
    // Test default value
    bool original = S3fsCurl::SetContentMd5(false);
    EXPECT_TRUE(original);  // Default is true

    // Test setting to true
    bool result = S3fsCurl::SetContentMd5(true);
    EXPECT_FALSE(result);  // Should return the previous value (false)
}

TEST(CurlCore, SetVerbose) {
    // Test default value
    bool original = S3fsCurl::SetVerbose(true);
    EXPECT_FALSE(original);  // Default is false

    // Test setting to false
    bool result = S3fsCurl::SetVerbose(false);
    EXPECT_TRUE(result);  // Should return the previous value (true)
}

TEST(CurlCore, SetConnectTimeout) {
    // Test setting timeout
    long original = S3fsCurl::SetConnectTimeout(120);
    EXPECT_GT(original, 0);  // Should have some default value

    // Test setting to 60
    long result = S3fsCurl::SetConnectTimeout(60);
    EXPECT_EQ(result, 120);  // Should return the previous value (120)

    // Test setting to 0
    result = S3fsCurl::SetConnectTimeout(0);
    EXPECT_EQ(result, 60);  // Should return the previous value (60)
}

TEST(CurlCore, SetReadwriteTimeout) {
    // Test setting timeout
    time_t original = S3fsCurl::SetReadwriteTimeout(300);
    EXPECT_GT(original, 0);  // Should have some default value

    // Test setting to 60
    time_t result = S3fsCurl::SetReadwriteTimeout(60);
    EXPECT_EQ(result, 300);  // Should return the previous value (300)

    // Test setting to 0
    result = S3fsCurl::SetReadwriteTimeout(0);
    EXPECT_EQ(result, 60);  // Should return the previous value (60)
}

TEST(CurlCore, GetReadwriteTimeout) {
    // Set a known value
    S3fsCurl::SetReadwriteTimeout(120);

    // Get the value
    time_t timeout = S3fsCurl::GetReadwriteTimeout();
    EXPECT_EQ(timeout, 120);
}

TEST(CurlCore, SetStorageClass) {
    // Test default value
    storage_class_t original = S3fsCurl::SetStorageClass(STANDARD_IA);
    EXPECT_EQ(original, STANDARD);  // Default is STANDARD

    // Test setting to REDUCED_REDUNDANCY
    storage_class_t result = S3fsCurl::SetStorageClass(REDUCED_REDUNDANCY);
    EXPECT_EQ(result, STANDARD_IA);  // Should return the previous value

    // Test getting the current value
    storage_class_t current = S3fsCurl::GetStorageClass();
    EXPECT_EQ(current, REDUCED_REDUNDANCY);

    // Reset to default
    S3fsCurl::SetStorageClass(STANDARD);
}

TEST(CurlCore, SetMaxParallelCount) {
    // Test setting max parallel count
    int original = S3fsCurl::SetMaxParallelCount(10);
    EXPECT_EQ(original, 5);  // Default is 5

    // Test setting to 20
    int result = S3fsCurl::SetMaxParallelCount(20);
    EXPECT_EQ(result, 10);  // Should return the previous value

    // Test setting to 1
    result = S3fsCurl::SetMaxParallelCount(1);
    EXPECT_EQ(result, 20);  // Should return the previous value
}

TEST(CurlCore, SetMultipartSize) {
    // Test setting multipart size
    // Note: SetMultipartSize multiplies input by 1024*1024 internally
    // And returns false if size < MIN_MULTIPART_SIZE (5MB)

    // First set to 10MB (input is in MB, function multiplies by 1024*1024)
    bool result1 = S3fsCurl::SetMultipartSize(10);  // 10MB
    EXPECT_TRUE(result1);  // 10MB >= 5MB MIN_MULTIPART_SIZE, should succeed

    // Set to 6MB
    bool result2 = S3fsCurl::SetMultipartSize(6);  // 6MB
    EXPECT_TRUE(result2);  // 6MB >= 5MB, should succeed

    // Set to 3MB (should fail - below minimum)
    bool result3 = S3fsCurl::SetMultipartSize(3);  // 3MB
    EXPECT_FALSE(result3);  // 3MB < 5MB MIN_MULTIPART_SIZE, should fail
}

TEST(CurlCore, SetSignatureV4) {
    // Test default value
    bool original = S3fsCurl::SetSignatureV4(true);
    EXPECT_FALSE(original);  // Default is false (V2)

    // Test setting to false
    bool result = S3fsCurl::SetSignatureV4(false);
    EXPECT_TRUE(result);  // Should return the previous value (true)
}

TEST(CurlCore, SetDefaultAcl) {
    // Test setting default ACL
    std::string acl = "public-read";
    std::string result = S3fsCurl::SetDefaultAcl(acl.c_str());
    EXPECT_TRUE(result.empty() || result == "private");  // Should return previous value or empty

    // Test setting to empty
    result = S3fsCurl::SetDefaultAcl("");
    EXPECT_TRUE(result.empty() || result == "public-read");
}

TEST(CurlCore, SetAccessKey) {
    // Test setting access key
    const char* access_key = "test_access_key";
    const char* secret_key = "test_secret_key";

    bool result = S3fsCurl::SetAccessKey(access_key, secret_key);
    EXPECT_TRUE(result);
}

TEST(CurlCore, SetAccessKeyAndToken) {
    // Test setting access key with token
    const char* access_key = "test_access_key";
    const char* secret_key = "test_secret_key";
    const char* access_token = "test_access_token";

    bool result = S3fsCurl::SetAccessKeyAndToken(access_key, secret_key, access_token);
    EXPECT_TRUE(result);

    // Test with NULL token - should fail (token is required for this function)
    result = S3fsCurl::SetAccessKeyAndToken(access_key, secret_key, NULL);
    EXPECT_FALSE(result);  // NULL token is not allowed

    // Test with empty access key - should fail
    result = S3fsCurl::SetAccessKeyAndToken(NULL, secret_key, access_token);
    EXPECT_FALSE(result);  // NULL access key is not allowed

    // Test with empty secret key - should fail
    result = S3fsCurl::SetAccessKeyAndToken(access_key, NULL, access_token);
    EXPECT_FALSE(result);  // NULL secret key is not allowed
}

// ========================================================================
// Tests for SSE (Server Side Encryption) functionality
// ========================================================================

TEST(CurlCore, SetSseType) {
    // Test default value
    sse_type_t original = S3fsCurl::SetSseType(SSE_S3);
    EXPECT_EQ(original, SSE_DISABLE);  // Default is SSE_DISABLE

    // Test setting to SSE_C
    sse_type_t result = S3fsCurl::SetSseType(SSE_C);
    EXPECT_EQ(result, SSE_S3);  // Should return the previous value

    // Test setting to SSE_KMS
    result = S3fsCurl::SetSseType(SSE_KMS);
    EXPECT_EQ(result, SSE_C);  // Should return the previous value

    // Reset to default
    S3fsCurl::SetSseType(SSE_DISABLE);
}

TEST(CurlCore, SetSseKmsid) {
    // Test setting SSE KMS ID
    const char* kmsid = "test-kms-key-id";
    bool result = S3fsCurl::SetSseKmsid(kmsid);
    EXPECT_TRUE(result);  // Valid KMS ID should succeed

    // Test with empty KMS ID - should fail
    result = S3fsCurl::SetSseKmsid("");
    EXPECT_FALSE(result);  // Empty KMS ID should fail

    // Test with NULL - should fail
    result = S3fsCurl::SetSseKmsid(NULL);
    EXPECT_FALSE(result);  // NULL KMS ID should fail
}

TEST(CurlCore, FinalCheckSse) {
    // Test with SSE disabled (should succeed)
    S3fsCurl::SetSseType(SSE_DISABLE);
    bool result = S3fsCurl::FinalCheckSse();
    EXPECT_TRUE(result);
}

TEST(CurlCore, GetSseKeyCount) {
    // Test default (no keys set)
    int count = S3fsCurl::GetSseKeyCount();
    EXPECT_EQ(count, 0);
}

TEST(CurlCore, GetSseKeyMd5) {
    // Test getting SSE key MD5 when no keys are set
    std::string md5;
    bool result = S3fsCurl::GetSseKeyMd5(0, md5);
    EXPECT_FALSE(result);  // Should fail when no keys are set

    // Test with negative index
    result = S3fsCurl::GetSseKeyMd5(-1, md5);
    EXPECT_FALSE(result);  // Should fail
}

// ========================================================================
// Tests for filepart structure
// ========================================================================

TEST(CurlCore, Filepart_DefaultConstructor) {
    filepart part;

    EXPECT_FALSE(part.uploaded);
    EXPECT_EQ(part.fd, -1);
    EXPECT_EQ(part.startpos, 0);
    EXPECT_EQ(part.size, -1);
    EXPECT_EQ(part.etaglist, (etaglist_t*)NULL);
    EXPECT_EQ(part.etagpos, -1);
    EXPECT_TRUE(part.etag.empty());
}

TEST(CurlCore, Filepart_Clear) {
    filepart part;
    part.uploaded = true;
    part.fd = 10;
    part.startpos = 100;
    part.size = 1000;
    part.etag = "test-etag";

    part.clear();

    EXPECT_FALSE(part.uploaded);
    EXPECT_EQ(part.fd, -1);
    EXPECT_EQ(part.startpos, 0);
    EXPECT_EQ(part.size, -1);
    EXPECT_EQ(part.etaglist, (etaglist_t*)NULL);
    EXPECT_EQ(part.etagpos, -1);
    EXPECT_TRUE(part.etag.empty());
}

TEST(CurlCore, Filepart_AddEtagList) {
    filepart part;
    etaglist_t list;

    part.add_etag_list(&list);

    EXPECT_EQ(part.etaglist, &list);
    EXPECT_EQ(part.etagpos, 0);  // First position

    // Add more items
    list.push_back("etag1");
    list.push_back("etag2");

    filepart part2;
    part2.add_etag_list(&list);

    EXPECT_EQ(part2.etaglist, &list);
    EXPECT_EQ(part2.etagpos, 3);  // After adding 3 items total
}

TEST(CurlCore, Filepart_AddEtagListNull) {
    filepart part;

    part.add_etag_list(NULL);

    EXPECT_EQ(part.etaglist, (etaglist_t*)NULL);
    EXPECT_EQ(part.etagpos, -1);
}

// ========================================================================
// Tests for utility functions
// ========================================================================

TEST(CurlCore, CaseInsensitiveCompareFunc) {
    case_insensitive_compare_func comp;

    // case_insensitive_compare_func returns true if strcasecmp(a, b) < 0
    // This means it returns true when a < b (lexicographically, case insensitive)

    // Equal strings should return false (not less than)
    EXPECT_FALSE(comp("abc", "ABC"));
    EXPECT_FALSE(comp("abc", "abc"));

    // "abc" < "xyz" should return true
    EXPECT_TRUE(comp("abc", "xyz"));

    // "xyz" > "abc" should return false
    EXPECT_FALSE(comp("xyz", "abc"));

    // Case should not matter
    EXPECT_TRUE(comp("ABC", "xyz"));
    EXPECT_FALSE(comp("xyz", "ABC"));
}

TEST(CurlCore, EmptyPayloadHash) {
    // Test the empty payload hash constant
    // This is used in AWS Signature V4
    const std::string empty_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    // Verify it's a valid SHA256 hash (64 hex characters)
    EXPECT_EQ(empty_hash.length(), static_cast<size_t>(64));

    // Verify all characters are hex
    for (char c : empty_hash) {
        EXPECT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
    }
}

TEST(CurlCore, MinMultipartSize) {
    // Test the minimum multipart size constant
    EXPECT_EQ(MIN_MULTIPART_SIZE, 5 * 1024 * 1024);  // 5MB
}

TEST(CurlCore, LongResponseTimeUs) {
    // Test the long response time constant
    EXPECT_EQ(LONG_RESPONSE_TIME_US, 1000000);  // 1 second in microseconds
}

TEST(CurlCore, UsPerSecond) {
    // Test the microseconds per second constant
    EXPECT_EQ(US_PER_SECOND, 1000000);  // 1 million microseconds per second
}

// ========================================================================
// Tests for constants and enums
// ========================================================================

TEST(CurlCore, StorageClassEnum) {
    // Test storage class enum values
    EXPECT_EQ(STANDARD, (storage_class_t)0);
    EXPECT_EQ(STANDARD_IA, (storage_class_t)1);
    EXPECT_EQ(REDUCED_REDUNDANCY, (storage_class_t)2);
}

TEST(CurlCore, SseTypeEnum) {
    // Test SSE type enum values
    EXPECT_EQ(SSE_DISABLE, (sse_type_t)0);
    EXPECT_EQ(SSE_S3, (sse_type_t)1);
    EXPECT_EQ(SSE_C, (sse_type_t)2);
    EXPECT_EQ(SSE_KMS, (sse_type_t)3);
}

TEST(CurlCore, ShareMutexEnum) {
    // Test share mutex enum values
    EXPECT_EQ(SHARE_MUTEX_DNS, 0);
    EXPECT_EQ(SHARE_MUTEX_SSL_SESSION, 1);
    EXPECT_EQ(SHARE_MUTEX_MAX, 2);
}

// ========================================================================
// Additional tests for S3fsCurl static methods
// ========================================================================

TEST(CurlCore, SetSslVerifyHostname) {
    // Test setting to 0
    long original = S3fsCurl::SetSslVerifyHostname(0);
    (void)original;  // Just verify no crash

    // Test setting to 1
    long result = S3fsCurl::SetSslVerifyHostname(1);
    (void)result;  // Just verify no crash

    // Reset to default
    S3fsCurl::SetSslVerifyHostname(1);
}

TEST(CurlCore, GetMultipartSize) {
    // Set a known value
    S3fsCurl::SetMultipartSize(10);  // 10MB

    // Get the value
    off_t size = S3fsCurl::GetMultipartSize();
    EXPECT_EQ(size, 10 * 1024 * 1024);  // 10MB in bytes
}

TEST(CurlCore, GetMaxParallelCount) {
    // Set a known value
    S3fsCurl::SetMaxParallelCount(15);

    // Get the value
    int count = S3fsCurl::GetMaxParallelCount();
    EXPECT_EQ(count, 15);
}

TEST(CurlCore, IsPublicBucket) {
    // Set to false first
    S3fsCurl::SetPublicBucket(false);
    EXPECT_FALSE(S3fsCurl::IsPublicBucket());

    // Set to true
    S3fsCurl::SetPublicBucket(true);
    EXPECT_TRUE(S3fsCurl::IsPublicBucket());

    // Reset to default
    S3fsCurl::SetPublicBucket(false);
}

TEST(CurlCore, GetDefaultAcl) {
    // Set a known ACL
    std::string acl = "private";
    S3fsCurl::SetDefaultAcl(acl.c_str());

    // Get the value
    std::string result = S3fsCurl::GetDefaultAcl();
    EXPECT_EQ(result, "private");
}

TEST(CurlCore, GetVerbose) {
    // Set to true
    S3fsCurl::SetVerbose(true);
    EXPECT_TRUE(S3fsCurl::GetVerbose());

    // Set to false
    S3fsCurl::SetVerbose(false);
    EXPECT_FALSE(S3fsCurl::GetVerbose());
}

// ========================================================================
// Tests for additional S3fsCurl functionality
// ========================================================================

TEST(CurlCore, SetIsIBMIAMAuth) {
    // Test default (should be false)
    bool original = S3fsCurl::SetIsIBMIAMAuth(true);
    EXPECT_FALSE(original);  // Default is false

    // Set to false
    bool result = S3fsCurl::SetIsIBMIAMAuth(false);
    EXPECT_TRUE(result);  // Should return previous value (true)

    // Reset to default
    S3fsCurl::SetIsIBMIAMAuth(false);
}

TEST(CurlCore, SetIsECS) {
    // Test default (should be false)
    bool original = S3fsCurl::SetIsECS(true);
    EXPECT_FALSE(original);  // Default is false

    // Set to false
    bool result = S3fsCurl::SetIsECS(false);
    EXPECT_TRUE(result);  // Should return previous value (true)

    // Reset to default
    S3fsCurl::SetIsECS(false);
}

TEST(CurlCore, SetUserAgentFlag) {
    // Test setting user agent flag
    S3fsCurl::SetUserAgentFlag(true);
    EXPECT_TRUE(S3fsCurl::IsUserAgentFlag());

    S3fsCurl::SetUserAgentFlag(false);
    EXPECT_FALSE(S3fsCurl::IsUserAgentFlag());

    // Reset to default
    S3fsCurl::SetUserAgentFlag(false);
}

// ========================================================================
// Tests for Mime type functionality
// Note: LookupMimeType requires InitMimeType to be called first.
// Without initialization, it returns "application/octet-stream" as default.
// ========================================================================

TEST(CurlCore, LookupMimeType_DefaultBehavior) {
    // Without initialization, all types return the default
    std::string mime = S3fsCurl::LookupMimeType("test.txt");
    EXPECT_FALSE(mime.empty());  // Should return some default (application/octet-stream)
}

TEST(CurlCore, LookupMimeType_UnknownExtension) {
    // Unknown extension should return default
    std::string mime = S3fsCurl::LookupMimeType("test.unknownext");
    EXPECT_FALSE(mime.empty());  // Should return some default
}

TEST(CurlCore, LookupMimeType_NoExtension) {
    // No extension should return default
    std::string mime = S3fsCurl::LookupMimeType("testfile");
    EXPECT_FALSE(mime.empty());  // Should return some default
}

TEST(CurlCore, LookupMimeType_EmptyPath) {
    // Empty path should return default
    std::string mime = S3fsCurl::LookupMimeType("");
    EXPECT_FALSE(mime.empty());  // Should return some default
}

// ========================================================================
// Tests for DetectBucketTypeFromHeaders
// ========================================================================

TEST(CurlCore, DetectBucketTypeFromHeaders_NoSpecialHeaders) {
    headers_t headers;
    headers["Content-Type"] = "application/xml";

    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_FALSE(result);
}

TEST(CurlCore, DetectBucketTypeFromHeaders_WithObsBucketType) {
    headers_t headers;
    headers["x-obs-bucket-type"] = "POSIX";

    bucket_type_t saved_type = g_bucket_type;
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_TRUE(result);
    EXPECT_EQ(g_bucket_type, BUCKET_TYPE_FILE_SEMANTIC);
    g_bucket_type = saved_type;
}

TEST(CurlCore, DetectBucketTypeFromHeaders_WithFsFileInterface) {
    headers_t headers;
    headers["x-obs-fs-file-interface"] = "Enabled";

    bucket_type_t saved_type = g_bucket_type;
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_TRUE(result);
    EXPECT_EQ(g_bucket_type, BUCKET_TYPE_FILE_SEMANTIC);
    g_bucket_type = saved_type;
}

TEST(CurlCore, DetectBucketTypeFromHeaders_BothObsHeaders) {
    headers_t headers;
    headers["x-obs-bucket-type"] = "POSIX";
    headers["x-obs-fs-file-interface"] = "Enabled";

    // Both headers - should return true
    bucket_type_t saved_type = g_bucket_type;
    bool result = S3fsCurl::DetectBucketTypeFromHeaders(headers);
    EXPECT_TRUE(result);
    EXPECT_EQ(g_bucket_type, BUCKET_TYPE_FILE_SEMANTIC);

    // Restore
    g_bucket_type = saved_type;
}

// ========================================================================
// Main function (if not using gtest main)
// ========================================================================

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
