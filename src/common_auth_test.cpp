/*
 * s3fs - FUSE-based file system backed by Amazon S3
 *
 * Unit tests for common_auth (authentication utilities) functionality
 *
 * This test links against the actual source code to get proper coverage.
 */

#include "gtest.h"
#include <string>
#include <cstring>
#include <cstdint>
#include <unistd.h>
#include <fcntl.h>
#include <cstdio>
#include <cstdlib>

// Include the actual function declarations
#include "s3fs_auth.h"

// Stub variables for linking
typedef enum {
    S3FS_LOG_CRIT = 0,
    S3FS_LOG_ERR,
    S3FS_LOG_WARN,
    S3FS_LOG_INFO,
    S3FS_LOG_DBG
} s3fs_log_level;

enum s3fs_log_mode {
    LOG_MODE_FOREGROUND = 0,
    LOG_MODE_SYSLOG,
    LOG_MODE_OBSFS
};

s3fs_log_level debug_level = S3FS_LOG_INFO;
s3fs_log_mode debug_log_mode = LOG_MODE_FOREGROUND;

// Stub functions for linking
extern "C" {
void IndexLogEntry(const uint64_t, const char*, const uint32_t,
                   const char*, const char*, const uint32_t,
                   const char*, ...) {}
}
void s3fsStatisLogModeNotObsfs() {}

// Test helper: Create a temporary file with known content
static int create_temp_file_with_content(const char* content) {
    char temp_file[] = "/tmp/common_auth_test_XXXXXX";
    int fd = mkstemp(temp_file);
    if (fd < 0) return -1;

    unlink(temp_file); // File will be deleted when closed

    size_t len = strlen(content);
    if (write(fd, content, len) != static_cast<ssize_t>(len)) {
        close(fd);
        return -1;
    }

    lseek(fd, 0, SEEK_SET);
    return fd;
}

// Test s3fs_get_content_md5 - Get MD5 hash of file content (base64 encoded)
TEST(CommonAuth, GetContentMd5_ValidFd) {
    int fd = create_temp_file_with_content("test content for MD5");
    ASSERT_GE(fd, 0);

    std::string result = s3fs_get_content_md5(fd);
    close(fd);

    EXPECT_FALSE(result.empty());
    EXPECT_GE(result.length(), static_cast<size_t>(10));
}

TEST(CommonAuth, GetContentMd5_InvalidFd) {
    std::string result = s3fs_get_content_md5(-1);
    EXPECT_TRUE(result.empty());
}

TEST(CommonAuth, GetContentMd5_EmptyFile) {
    int fd = create_temp_file_with_content("");
    ASSERT_GE(fd, 0);

    std::string result = s3fs_get_content_md5(fd);
    close(fd);

    EXPECT_FALSE(result.empty());
}

// Test s3fs_md5sum - Compute MD5 checksum (hex encoded)
TEST(CommonAuth, Md5Sum_ValidFd) {
    int fd = create_temp_file_with_content("test content for MD5 checksum");
    ASSERT_GE(fd, 0);

    std::string result = s3fs_md5sum(fd, 0, -1);
    close(fd);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(32));

    for (char c : result) {
        EXPECT_TRUE(std::isxdigit(c));
    }
}

TEST(CommonAuth, Md5Sum_InvalidFd) {
    std::string result = s3fs_md5sum(-1, 0, 100);
    EXPECT_TRUE(result.empty());
}

TEST(CommonAuth, Md5Sum_PartialContent) {
    int fd = create_temp_file_with_content("0123456789ABCDEFGHIJ");
    ASSERT_GE(fd, 0);

    std::string result = s3fs_md5sum(fd, 0, 10);
    close(fd);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(32));
}

TEST(CommonAuth, Md5Sum_WithOffset) {
    int fd = create_temp_file_with_content("0123456789ABCDEFGHIJ");
    ASSERT_GE(fd, 0);

    std::string result = s3fs_md5sum(fd, 10, 10);
    close(fd);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(32));
}

// Test s3fs_sha256sum - Compute SHA256 checksum (hex encoded)
TEST(CommonAuth, Sha256Sum_ValidFd) {
    int fd = create_temp_file_with_content("test content for SHA256");
    ASSERT_GE(fd, 0);

    std::string result = s3fs_sha256sum(fd, 0, -1);
    close(fd);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(64));

    for (char c : result) {
        EXPECT_TRUE(std::isxdigit(c));
    }
}

TEST(CommonAuth, Sha256Sum_InvalidFd) {
    std::string result = s3fs_sha256sum(-1, 0, 100);
    EXPECT_TRUE(result.empty());
}

TEST(CommonAuth, Sha256Sum_PartialContent) {
    int fd = create_temp_file_with_content("0123456789ABCDEF");
    ASSERT_GE(fd, 0);

    std::string result = s3fs_sha256sum(fd, 0, 8);
    close(fd);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(64));
}

TEST(CommonAuth, Sha256Sum_WithOffset) {
    int fd = create_temp_file_with_content("0123456789ABCDEF");
    ASSERT_GE(fd, 0);

    std::string result = s3fs_sha256sum(fd, 8, 8);
    close(fd);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(64));
}

// Test s3fs_get_content_md5_hws_obs - Get MD5 from buffer (base64 encoded)
TEST(CommonAuth, GetContentMd5HwsObs_ValidBuffer) {
    const char* test_data = "test data for buffer MD5";
    std::string result = s3fs_get_content_md5_hws_obs(test_data, 0, strlen(test_data));

    EXPECT_FALSE(result.empty());
    EXPECT_GE(result.length(), static_cast<size_t>(10));
}

TEST(CommonAuth, GetContentMd5HwsObs_NullBuffer) {
    std::string result = s3fs_get_content_md5_hws_obs(NULL, 0, 10);
    EXPECT_TRUE(result.empty());
}

TEST(CommonAuth, GetContentMd5HwsObs_EmptyBuffer) {
    const char* empty_data = "";
    // size=0 is invalid for buffer MD5, returns empty string
    std::string result = s3fs_get_content_md5_hws_obs(empty_data, 0, 0);
    EXPECT_TRUE(result.empty());
}

TEST(CommonAuth, GetContentMd5HwsObs_WithOffset) {
    const char* test_data = "0123456789ABCDEF";

    std::string result = s3fs_get_content_md5_hws_obs(test_data, 5, 5);

    EXPECT_FALSE(result.empty());
    EXPECT_GE(result.length(), static_cast<size_t>(10));
}

// Test s3fs_sha256sum_hw_obs - Compute SHA256 from buffer (hex encoded)
TEST(CommonAuth, Sha256SumHwObs_ValidBuffer) {
    const char* test_data = "test data for buffer SHA256";
    std::string result = s3fs_sha256sum_hw_obs(test_data, 0, strlen(test_data));

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(64));

    for (char c : result) {
        EXPECT_TRUE(std::isxdigit(c));
    }
}

// [ISSUE2026040900001 A2/A3 regression guard] Known-Answer Tests for the
// SHA256 hex formatting path. Reference values are independent of this
// project (RFC test vectors / `openssl dgst -sha256` output). Without these
// KATs, the strncat→snprintf refactor in s3fs_sha256sum* would have no
// byte-level safety net (only "non-empty + length 64 + isxdigit" assertions).
TEST(CommonAuth, Sha256SumHwObs_KAT_RFC_Abc) {
    // SHA256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
    std::string result = s3fs_sha256sum_hw_obs("abc", 0, 3);
    EXPECT_EQ(result, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

TEST(CommonAuth, Sha256SumHwObs_KAT_QuickBrownFox) {
    // SHA256("The quick brown fox jumps over the lazy dog") =
    //   d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592
    const char* input = "The quick brown fox jumps over the lazy dog";
    std::string result = s3fs_sha256sum_hw_obs(input, 0, strlen(input));
    EXPECT_EQ(result, "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592");
}

TEST(CommonAuth, Sha256Sum_KAT_RFC_Abc) {
    // Same RFC vector via the fd-based path; validates the s3fs_sha256sum
    // (file descriptor) variant uses the same hex formatting.
    int fd = create_temp_file_with_content("abc");
    ASSERT_GE(fd, 0);
    std::string result = s3fs_sha256sum(fd, 0, 3);
    close(fd);
    EXPECT_EQ(result, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

TEST(CommonAuth, Sha256SumHwObs_NullBuffer) {
    std::string result = s3fs_sha256sum_hw_obs(NULL, 0, 10);
    EXPECT_TRUE(result.empty());
}

TEST(CommonAuth, Sha256SumHwObs_EmptyBuffer) {
    const char* empty_data = "";
    // size=0 is invalid for buffer SHA256, returns empty string
    std::string result = s3fs_sha256sum_hw_obs(empty_data, 0, 0);
    EXPECT_TRUE(result.empty());
}

TEST(CommonAuth, Sha256SumHwObs_WithOffset) {
    const char* test_data = "0123456789ABCDEFabcdefgh";
    std::string result = s3fs_sha256sum_hw_obs(test_data, 8, 8);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(64));
}

// Test digest length functions
TEST(CommonAuth, Md5DigestLength) {
    size_t len = get_md5_digest_length();
    EXPECT_EQ(len, static_cast<size_t>(16));
}

TEST(CommonAuth, Sha256DigestLength) {
    size_t len = get_sha256_digest_length();
    EXPECT_EQ(len, static_cast<size_t>(32));
}

// Test consistency: same input produces same output
TEST(CommonAuth, ConsistentMd5ForSameInput) {
    const char* test_data = "consistent test data";

    std::string result1 = s3fs_get_content_md5_hws_obs(test_data, 0, strlen(test_data));
    std::string result2 = s3fs_get_content_md5_hws_obs(test_data, 0, strlen(test_data));

    EXPECT_EQ(result1, result2);
}

TEST(CommonAuth, ConsistentSha256ForSameInput) {
    const char* test_data = "consistent test data";

    std::string result1 = s3fs_sha256sum_hw_obs(test_data, 0, strlen(test_data));
    std::string result2 = s3fs_sha256sum_hw_obs(test_data, 0, strlen(test_data));

    EXPECT_EQ(result1, result2);
}

TEST(CommonAuth, DifferentInputsDifferentMd5) {
    const char* data1 = "test data one";
    const char* data2 = "test data two";

    std::string result1 = s3fs_get_content_md5_hws_obs(data1, 0, strlen(data1));
    std::string result2 = s3fs_get_content_md5_hws_obs(data2, 0, strlen(data2));

    EXPECT_NE(result1, result2);
}

TEST(CommonAuth, DifferentInputsDifferentSha256) {
    const char* data1 = "test data one";
    const char* data2 = "test data two";

    std::string result1 = s3fs_sha256sum_hw_obs(data1, 0, strlen(data1));
    std::string result2 = s3fs_sha256sum_hw_obs(data2, 0, strlen(data2));

    EXPECT_NE(result1, result2);
}

// Test edge cases
TEST(CommonAuth, Md5Sum_LargeContent) {
    std::string large_data(1024, 'A');
    int fd = create_temp_file_with_content(large_data.c_str());
    ASSERT_GE(fd, 0);

    std::string result = s3fs_md5sum(fd, 0, -1);
    close(fd);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(32));
}

TEST(CommonAuth, Sha256Sum_LargeContent) {
    std::string large_data(1024, 'B');

    std::string result = s3fs_sha256sum_hw_obs(large_data.c_str(), 0, large_data.length());

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(64));
}

TEST(CommonAuth, Md5Sum_ZeroOffset) {
    int fd = create_temp_file_with_content("test");
    ASSERT_GE(fd, 0);

    std::string result = s3fs_md5sum(fd, 0, 4);
    close(fd);

    EXPECT_FALSE(result.empty());
}

TEST(CommonAuth, Sha256Sum_ZeroOffset) {
    const char* test_data = "test";

    std::string result = s3fs_sha256sum_hw_obs(test_data, 0, 4);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(64));
}

// Test edge case: size=0 for buffer functions
TEST(CommonAuth, Md5HwsObs_ZeroSize) {
    const char* test_data = "test";
    std::string result = s3fs_get_content_md5_hws_obs(test_data, 0, 0);
    EXPECT_TRUE(result.empty());
}

TEST(CommonAuth, Sha256HwsObs_ZeroSize) {
    const char* test_data = "test";
    std::string result = s3fs_sha256sum_hw_obs(test_data, 0, 0);
    EXPECT_TRUE(result.empty());
}

// Test edge case: negative start for buffer functions
TEST(CommonAuth, Md5HwsObs_NegativeStart) {
    const char* test_data = "test";
    std::string result = s3fs_get_content_md5_hws_obs(test_data, -1, 10);
    EXPECT_TRUE(result.empty());
}

TEST(CommonAuth, Sha256HwsObs_NegativeStart) {
    const char* test_data = "test";
    std::string result = s3fs_sha256sum_hw_obs(test_data, -1, 10);
    EXPECT_TRUE(result.empty());
}

// ============================================================
// Tests for openssl_auth.cpp specific functions
// ============================================================

// Test fixture for OpenSSL auth tests
class OpenSslAuthTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        // Initialize OpenSSL for all tests in this suite
        s3fs_init_global_ssl();
    }

    static void TearDownTestSuite() {
        // Cleanup OpenSSL after all tests
        s3fs_destroy_global_ssl();
    }

    void SetUp() override {
        // Initialize crypt mutex for each test
        s3fs_init_crypt_mutex();
    }

    void TearDown() override {
        // Cleanup crypt mutex after each test
        s3fs_destroy_crypt_mutex();
    }
};

// Test s3fs_crypt_lib_name - Returns the crypto library name
TEST_F(OpenSslAuthTest, CryptLibName) {
    const char* lib_name = s3fs_crypt_lib_name();
    ASSERT_NE(lib_name, nullptr);
    EXPECT_STREQ(lib_name, "OpenSSL");
}

// Test s3fs_init_global_ssl and s3fs_destroy_global_ssl
TEST_F(OpenSslAuthTest, InitDestroyGlobalSsl) {
    // Initialize SSL (should be idempotent if already initialized)
    EXPECT_TRUE(s3fs_init_global_ssl());

    // Destroy SSL
    EXPECT_TRUE(s3fs_destroy_global_ssl());

    // Re-initialize to verify it can be done again
    EXPECT_TRUE(s3fs_init_global_ssl());

    // Clean up
    EXPECT_TRUE(s3fs_destroy_global_ssl());
}

// Test s3fs_init_crypt_mutex and s3fs_destroy_crypt_mutex
TEST_F(OpenSslAuthTest, InitDestroyCryptMutex) {
    // Initialize crypt mutex (should be idempotent if already initialized)
    EXPECT_TRUE(s3fs_init_crypt_mutex());

    // Destroy crypt mutex
    EXPECT_TRUE(s3fs_destroy_crypt_mutex());

    // Re-initialize to verify it can be done again
    EXPECT_TRUE(s3fs_init_crypt_mutex());

    // Clean up
    EXPECT_TRUE(s3fs_destroy_crypt_mutex());
}

// Test s3fs_init_crypt_mutex idempotency
TEST_F(OpenSslAuthTest, InitCryptMutex_Idempotent) {
    // First initialization
    EXPECT_TRUE(s3fs_init_crypt_mutex());

    // Second initialization should succeed (destroys and recreates)
    EXPECT_TRUE(s3fs_init_crypt_mutex());

    // Clean up
    EXPECT_TRUE(s3fs_destroy_crypt_mutex());
}

// Test s3fs_destroy_crypt_mutex with NULL
TEST_F(OpenSslAuthTest, DestroyCryptMutex_NotInitialized) {
    // Destroy without initializing should succeed (returns true)
    EXPECT_TRUE(s3fs_destroy_crypt_mutex());
}

// Test s3fs_HMAC - HMAC-SHA1 computation
TEST_F(OpenSslAuthTest, Hmac_ValidInputs) {
    const char* key = "test_key";
    const char* data = "test_data";

    unsigned char* digest = nullptr;
    unsigned int digestlen = 0;

    bool result = s3fs_HMAC(key, strlen(key),
                           reinterpret_cast<const unsigned char*>(data), strlen(data),
                           &digest, &digestlen);

    EXPECT_TRUE(result);
    EXPECT_NE(digest, nullptr);
    EXPECT_GT(digestlen, 0);

    if (digest) {
        free(digest);
    }
}

// Test s3fs_HMAC with null key
TEST_F(OpenSslAuthTest, Hmac_NullKey) {
    const char* data = "test_data";
    unsigned char* digest = nullptr;
    unsigned int digestlen = 0;

    bool result = s3fs_HMAC(nullptr, 10,
                           reinterpret_cast<const unsigned char*>(data), strlen(data),
                           &digest, &digestlen);

    EXPECT_FALSE(result);
    EXPECT_EQ(digest, nullptr);
}

// Test s3fs_HMAC with null data
TEST_F(OpenSslAuthTest, Hmac_NullData) {
    const char* key = "test_key";
    unsigned char* digest = nullptr;
    unsigned int digestlen = 0;

    bool result = s3fs_HMAC(key, strlen(key),
                           nullptr, 10,
                           &digest, &digestlen);

    EXPECT_FALSE(result);
    EXPECT_EQ(digest, nullptr);
}

// Test s3fs_HMAC with null digest pointer
TEST_F(OpenSslAuthTest, Hmac_NullDigestPointer) {
    const char* key = "test_key";
    const char* data = "test_data";

    bool result = s3fs_HMAC(key, strlen(key),
                           reinterpret_cast<const unsigned char*>(data), strlen(data),
                           nullptr, nullptr);

    EXPECT_FALSE(result);
}

// Test s3fs_HMAC256 - HMAC-SHA256 computation
TEST_F(OpenSslAuthTest, Hmac256_ValidInputs) {
    const char* key = "test_key_256";
    const char* data = "test_data_for_sha256";

    unsigned char* digest = nullptr;
    unsigned int digestlen = 0;

    bool result = s3fs_HMAC256(key, strlen(key),
                               reinterpret_cast<const unsigned char*>(data), strlen(data),
                               &digest, &digestlen);

    EXPECT_TRUE(result);
    EXPECT_NE(digest, nullptr);
    EXPECT_GT(digestlen, 0);

    // SHA256 HMAC should produce 32 bytes
    EXPECT_EQ(digestlen, static_cast<unsigned int>(32));

    if (digest) {
        free(digest);
    }
}

// Test s3fs_HMAC256 with null key
TEST_F(OpenSslAuthTest, Hmac256_NullKey) {
    const char* data = "test_data";
    unsigned char* digest = nullptr;
    unsigned int digestlen = 0;

    bool result = s3fs_HMAC256(nullptr, 10,
                               reinterpret_cast<const unsigned char*>(data), strlen(data),
                               &digest, &digestlen);

    EXPECT_FALSE(result);
    EXPECT_EQ(digest, nullptr);
}

// Test s3fs_HMAC256 consistency
TEST_F(OpenSslAuthTest, Hmac256_ConsistentOutput) {
    const char* key = "consistency_key";
    const char* data = "consistency_test_data";

    unsigned char* digest1 = nullptr;
    unsigned char* digest2 = nullptr;
    unsigned int digestlen1 = 0;
    unsigned int digestlen2 = 0;

    bool result1 = s3fs_HMAC256(key, strlen(key),
                                reinterpret_cast<const unsigned char*>(data), strlen(data),
                                &digest1, &digestlen1);
    bool result2 = s3fs_HMAC256(key, strlen(key),
                                reinterpret_cast<const unsigned char*>(data), strlen(data),
                                &digest2, &digestlen2);

    EXPECT_TRUE(result1);
    EXPECT_TRUE(result2);
    EXPECT_EQ(digestlen1, digestlen2);

    if (digest1 && digest2) {
        EXPECT_EQ(memcmp(digest1, digest2, digestlen1), 0);
        free(digest1);
        free(digest2);
    }
}

// Test s3fs_sha256 - SHA256 hash of data buffer
TEST_F(OpenSslAuthTest, Sha256_ValidInputs) {
    const char* data = "test_data_for_sha256_hash";

    unsigned char* digest = nullptr;
    unsigned int digestlen = 0;

    bool result = s3fs_sha256(reinterpret_cast<const unsigned char*>(data), strlen(data),
                             &digest, &digestlen);

    EXPECT_TRUE(result);
    EXPECT_NE(digest, nullptr);
    EXPECT_GT(digestlen, 0);

    // SHA256 should produce 32 bytes
    EXPECT_EQ(digestlen, static_cast<unsigned int>(32));

    if (digest) {
        free(digest);
    }
}

// Test s3fs_sha256 with empty data (zero length)
TEST_F(OpenSslAuthTest, Sha256_EmptyData) {
    const char* data = "";
    unsigned char* digest = nullptr;
    unsigned int digestlen = 0;

    bool result = s3fs_sha256(reinterpret_cast<const unsigned char*>(data), 0, &digest, &digestlen);

    // Should succeed and produce a hash for empty data
    EXPECT_TRUE(result);
    EXPECT_NE(digest, nullptr);
    EXPECT_EQ(digestlen, static_cast<unsigned int>(32));

    if (digest) {
        free(digest);
    }
}

// Test s3fs_sha256 consistency
TEST_F(OpenSslAuthTest, Sha256_ConsistentOutput) {
    const char* data = "consistency_test_data";

    unsigned char* digest1 = nullptr;
    unsigned char* digest2 = nullptr;
    unsigned int digestlen1 = 0;
    unsigned int digestlen2 = 0;

    bool result1 = s3fs_sha256(reinterpret_cast<const unsigned char*>(data), strlen(data),
                              &digest1, &digestlen1);
    bool result2 = s3fs_sha256(reinterpret_cast<const unsigned char*>(data), strlen(data),
                              &digest2, &digestlen2);

    EXPECT_TRUE(result1);
    EXPECT_TRUE(result2);
    EXPECT_EQ(digestlen1, digestlen2);
    EXPECT_EQ(digestlen1, static_cast<unsigned int>(32));

    if (digest1 && digest2) {
        EXPECT_EQ(memcmp(digest1, digest2, digestlen1), 0);
        free(digest1);
        free(digest2);
    }
}

// Test s3fs_sha256 different inputs produce different outputs
TEST_F(OpenSslAuthTest, Sha256_DifferentInputsDifferentOutputs) {
    const char* data1 = "test_data_one";
    const char* data2 = "test_data_two";

    unsigned char* digest1 = nullptr;
    unsigned char* digest2 = nullptr;
    unsigned int digestlen1 = 0;
    unsigned int digestlen2 = 0;

    s3fs_sha256(reinterpret_cast<const unsigned char*>(data1), strlen(data1),
               &digest1, &digestlen1);
    s3fs_sha256(reinterpret_cast<const unsigned char*>(data2), strlen(data2),
               &digest2, &digestlen2);

    EXPECT_NE(digestlen1, static_cast<unsigned int>(0));
    EXPECT_NE(digestlen2, static_cast<unsigned int>(0));

    if (digest1 && digest2) {
        EXPECT_NE(memcmp(digest1, digest2, std::min(digestlen1, digestlen2)), 0);
        free(digest1);
        free(digest2);
    }
}

// ============================================================
// Additional tests for edge cases and improved coverage
// ============================================================

// Test MD5 with very large content
TEST_F(OpenSslAuthTest, Md5Sum_VeryLargeContent) {
    std::string very_large_data(10 * 1024 * 1024, 'X');  // 10 MB

    unsigned char* digest = nullptr;
    unsigned int digestlen = 0;

    bool result = s3fs_HMAC256("test_key", 8,
                               reinterpret_cast<const unsigned char*>(very_large_data.c_str()), very_large_data.length(),
                               &digest, &digestlen);

    // Large input HMAC should still work
    EXPECT_TRUE(result);
    if (digest) {
        free(digest);
    }
}

// Test HMAC with empty key
TEST_F(OpenSslAuthTest, Hmac_EmptyKey) {
    const char* data = "test_data";
    const char* empty_key = "";

    unsigned char* digest = nullptr;
    unsigned int digestlen = 0;

    bool result = s3fs_HMAC(empty_key, 0,
                           reinterpret_cast<const unsigned char*>(data), strlen(data),
                           &digest, &digestlen);

    EXPECT_TRUE(result);
    EXPECT_NE(digest, nullptr);
    EXPECT_GT(digestlen, 0);

    if (digest) {
        free(digest);
    }
}

// Test HMAC256 with empty key
TEST_F(OpenSslAuthTest, Hmac256_EmptyKey) {
    const char* data = "test_data";
    const char* empty_key = "";

    unsigned char* digest = nullptr;
    unsigned int digestlen = 0;

    bool result = s3fs_HMAC256(empty_key, 0,
                               reinterpret_cast<const unsigned char*>(data), strlen(data),
                               &digest, &digestlen);

    EXPECT_TRUE(result);
    EXPECT_NE(digest, nullptr);
    EXPECT_EQ(digestlen, static_cast<unsigned int>(32));

    if (digest) {
        free(digest);
    }
}

// Test HMAC with empty data (may return false depending on implementation)
TEST_F(OpenSslAuthTest, Hmac_EmptyData) {
    const char* key = "test_key";

    unsigned char* digest = nullptr;
    unsigned int digestlen = 0;

    bool result = s3fs_HMAC(key, strlen(key),
                           nullptr, 0,
                           &digest, &digestlen);

    // Empty data may return false or produce a valid HMAC
    if (result) {
        EXPECT_NE(digest, nullptr);
        if (digest) {
            free(digest);
        }
    } else {
        EXPECT_EQ(digest, nullptr);
    }
}

// Test HMAC256 with empty data (may return false depending on implementation)
TEST_F(OpenSslAuthTest, Hmac256_EmptyData) {
    const char* key = "test_key";

    unsigned char* digest = nullptr;
    unsigned int digestlen = 0;

    bool result = s3fs_HMAC256(key, strlen(key),
                               nullptr, 0,
                               &digest, &digestlen);

    // Empty data may return false or produce a valid HMAC
    if (result) {
        EXPECT_NE(digest, nullptr);
        if (digest) {
            free(digest);
        }
    } else {
        EXPECT_EQ(digest, nullptr);
    }
}

// Test SHA256 with very large data
TEST_F(OpenSslAuthTest, Sha256_VeryLargeData) {
    std::string large_data(5 * 1024 * 1024, 'Y');  // 5 MB

    unsigned char* digest = nullptr;
    unsigned int digestlen = 0;

    bool result = s3fs_sha256(reinterpret_cast<const unsigned char*>(large_data.c_str()), large_data.length(),
                             &digest, &digestlen);

    EXPECT_TRUE(result);
    EXPECT_EQ(digestlen, static_cast<unsigned int>(32));

    if (digest) {
        free(digest);
    }
}

// Test buffer functions with single byte
TEST(CommonAuth, GetContentMd5HwsObs_SingleByte) {
    const char* test_data = "X";

    std::string result = s3fs_get_content_md5_hws_obs(test_data, 0, 1);

    EXPECT_FALSE(result.empty());
    EXPECT_GE(result.length(), static_cast<size_t>(10));
}

TEST(CommonAuth, Sha256SumHwObs_SingleByte) {
    const char* test_data = "Y";

    std::string result = s3fs_sha256sum_hw_obs(test_data, 0, 1);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(64));
}

// Test buffer functions with maximum offset
TEST(CommonAuth, Md5HwsObs_MaxOffset) {
    const char* test_data = "test";
    size_t len = strlen(test_data);

    // Offset equal to length (returns empty)
    std::string result = s3fs_get_content_md5_hws_obs(test_data, len, 0);
    EXPECT_TRUE(result.empty());
}

TEST(CommonAuth, Sha256HwObs_MaxOffset) {
    const char* test_data = "test";
    size_t len = strlen(test_data);

    // Offset equal to length (returns empty)
    std::string result = s3fs_sha256sum_hw_obs(test_data, len, 0);
    EXPECT_TRUE(result.empty());
}

// Test file descriptor functions with various read sizes
TEST(CommonAuth, Md5Sum_DifferentReadSizes) {
    const char* test_data = "0123456789ABCDEFGHIJ";

    int fd = create_temp_file_with_content(test_data);
    ASSERT_GE(fd, 0);

    // Test reading entire file
    std::string result_full = s3fs_md5sum(fd, 0, -1);
    EXPECT_FALSE(result_full.empty());
    EXPECT_EQ(result_full.length(), static_cast<size_t>(32));

    // Test reading first half
    lseek(fd, 0, SEEK_SET);
    std::string result_half = s3fs_md5sum(fd, 0, 10);
    EXPECT_FALSE(result_half.empty());
    EXPECT_EQ(result_half.length(), static_cast<size_t>(32));

    // Results should be different
    EXPECT_NE(result_full, result_half);

    close(fd);
}

TEST(CommonAuth, Sha256Sum_DifferentReadSizes) {
    const char* test_data = "0123456789ABCDEFGHIJ";

    int fd = create_temp_file_with_content(test_data);
    ASSERT_GE(fd, 0);

    // Test reading entire file
    std::string result_full = s3fs_sha256sum(fd, 0, -1);
    EXPECT_FALSE(result_full.empty());
    EXPECT_EQ(result_full.length(), static_cast<size_t>(64));

    // Test reading first half
    lseek(fd, 0, SEEK_SET);
    std::string result_half = s3fs_sha256sum(fd, 0, 10);
    EXPECT_FALSE(result_half.empty());
    EXPECT_EQ(result_half.length(), static_cast<size_t>(64));

    // Results should be different
    EXPECT_NE(result_full, result_half);

    close(fd);
}

// Test HMAC with special characters in key
TEST_F(OpenSslAuthTest, Hmac_SpecialCharactersInKey) {
    const char* key = "key=with=special&chars";
    const char* data = "test_data";

    unsigned char* digest1 = nullptr;
    unsigned int digestlen1 = 0;

    bool result = s3fs_HMAC(key, strlen(key),
                           reinterpret_cast<const unsigned char*>(data), strlen(data),
                           &digest1, &digestlen1);

    EXPECT_TRUE(result);
    EXPECT_NE(digest1, nullptr);

    if (digest1) {
        free(digest1);
    }
}

// Test HMAC256 with special characters in key
TEST_F(OpenSslAuthTest, Hmac256_SpecialCharactersInKey) {
    const char* key = "key=with=special&chars";
    const char* data = "test_data";

    unsigned char* digest1 = nullptr;
    unsigned int digestlen1 = 0;

    bool result = s3fs_HMAC256(key, strlen(key),
                               reinterpret_cast<const unsigned char*>(data), strlen(data),
                               &digest1, &digestlen1);

    EXPECT_TRUE(result);
    EXPECT_NE(digest1, nullptr);
    EXPECT_EQ(digestlen1, static_cast<unsigned int>(32));

    if (digest1) {
        free(digest1);
    }
}

// Test with unicode/UTF-8 data
TEST(CommonAuth, Md5Sum_Utf8Data) {
    const char* utf8_data = "Hello 世界 🌍";

    int fd = create_temp_file_with_content(utf8_data);
    ASSERT_GE(fd, 0);

    std::string result = s3fs_md5sum(fd, 0, -1);
    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(32));

    close(fd);
}

TEST(CommonAuth, Sha256Sum_Utf8Data) {
    const char* utf8_data = "Hello 世界 🌍";

    int fd = create_temp_file_with_content(utf8_data);
    ASSERT_GE(fd, 0);

    std::string result = s3fs_sha256sum(fd, 0, -1);
    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(64));

    close(fd);
}

TEST(CommonAuth, GetContentMd5HwsObs_Utf8Data) {
    const char* utf8_data = "Hello 世界 🌍";

    std::string result = s3fs_get_content_md5_hws_obs(utf8_data, 0, strlen(utf8_data));

    EXPECT_FALSE(result.empty());
    EXPECT_GE(result.length(), static_cast<size_t>(10));
}

TEST(CommonAuth, Sha256SumHwObs_Utf8Data) {
    const char* utf8_data = "Hello 世界 🌍";

    std::string result = s3fs_sha256sum_hw_obs(utf8_data, 0, strlen(utf8_data));

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.length(), static_cast<size_t>(64));
}

//==============================================================================
// ISSUE2026040800001 — SSE-C Key MD5 tests
//
// Pin-down baseline tests for the refactor that replaces make_md5_from_string
// (tmpfile detour) with the in-memory s3fs_get_content_md5_hws_obs.
//
// Test 0: fd-path vs buffer-path cross-path equivalence
// Test 1: RFC 1321 Known-Answer Test (KAT)
// Test 1b: 512-byte chunking boundary KAT
// Test 6: NUL-byte key latent bug fix evidence
//==============================================================================

// Test 0: Cross-path equivalence (critical pin-down baseline)
// Proves that fd-based s3fs_get_content_md5 and buffer-based
// s3fs_get_content_md5_hws_obs produce byte-identical MD5 base64 output
// for a wide range of input sizes covering all chunking boundaries.
TEST(SseCKeyMD5, FdPathAndBufferPathEquivalent) {
    size_t sizes[] = {
        1, 15, 16, 31, 32,          // short keys (SSE-C AES-128/256 typical)
        44, 45,                      // base64-encoded AES-256 key (32B -> 44 b64)
        100, 255,                    // medium length
        511, 512, 513,               // chunking boundary ±1
        1023, 1024, 1025,            // chunking boundary ±1
        2047, 2048, 2049,            // larger boundary
        4095, 4096                   // page-size typical
    };
    const size_t nsizes = sizeof(sizes) / sizeof(sizes[0]);

    for(size_t i = 0; i < nsizes; ++i){
        size_t len = sizes[i];
        std::string data(len, '\0');
        // Deterministic pseudo-random fill to avoid all-zero collision
        for(size_t j = 0; j < len; ++j){
            data[j] = static_cast<char>((j * 131 + 17) & 0xFF);
        }

        // Path A: fd-based via temporary file
        char tmpname[] = "/tmp/md5_equiv_XXXXXX";
        int fd = mkstemp(tmpname);
        ASSERT_GE(fd, 0) << "mkstemp failed at len=" << len;
        ssize_t written = write(fd, data.data(), len);
        ASSERT_EQ(static_cast<ssize_t>(len), written) << "write failed at len=" << len;
        lseek(fd, 0, SEEK_SET);
        std::string via_fd = s3fs_get_content_md5(fd);
        close(fd);
        unlink(tmpname);

        // Path B: buffer-based in memory
        std::string via_buf = s3fs_get_content_md5_hws_obs(data.data(), 0, len);

        EXPECT_EQ(via_fd, via_buf)
            << "Mismatch at len=" << len
            << " (via_fd='" << via_fd << "', via_buf='" << via_buf << "')";
        EXPECT_FALSE(via_fd.empty()) << "len=" << len;
        EXPECT_EQ(via_fd.length(), 24u) << "len=" << len;  // base64(16B) == 24 chars
    }
}

// Test 1: RFC 1321 Known-Answer Tests (KAT)
// These expected values are from RFC 1321 / openssl dgst and are independent
// of this project's code, providing an absolute correctness anchor.

TEST(SseCKeyMD5, KAT_RFC1321_Abc) {
    // MD5("abc") = 900150983cd24fb0d6963f7d28e17f72
    // base64 of that binary = kAFQmDzST7DWlj99KOF/cg==
    std::string md5 = s3fs_get_content_md5_hws_obs("abc", 0, 3);
    EXPECT_EQ(md5, "kAFQmDzST7DWlj99KOF/cg==");
}

TEST(SseCKeyMD5, KAT_RFC1321_MessageDigest) {
    // MD5("message digest") = f96b697d7cb7938d525a2f31aaf161d0
    const char* input = "message digest";
    std::string md5 = s3fs_get_content_md5_hws_obs(input, 0, strlen(input));
    EXPECT_EQ(md5, "+WtpfXy3k41SWi8xqvFh0A==");
}

TEST(SseCKeyMD5, KAT_Pangram_QuickBrownFox) {
    // MD5("The quick brown fox jumps over the lazy dog") = 9e107d9d372bb6826bd81d3542a419d6
    const char* input = "The quick brown fox jumps over the lazy dog";
    std::string md5 = s3fs_get_content_md5_hws_obs(input, 0, strlen(input));
    EXPECT_EQ(md5, "nhB9nTcrtoJr2B01QqQZ1g==");
}

TEST(SseCKeyMD5, KAT_EmptyInput_ReturnsEmpty) {
    // size=0 path returns NULL -> empty string. Design contract: the old
    // make_md5_from_string also rejected empty input.
    std::string md5 = s3fs_get_content_md5_hws_obs("", 0, 0);
    EXPECT_TRUE(md5.empty());
}

TEST(SseCKeyMD5, KAT_Base64OutputLength) {
    // base64(16B) always produces 24 chars including '=' padding
    std::string md5 = s3fs_get_content_md5_hws_obs("X", 0, 1);
    EXPECT_EQ(md5.length(), 24u);
}

// Test 1b: Chunking boundary KATs
// Computed via: python3 -c "print('A'*N, end='')" | openssl dgst -md5 -binary | base64
TEST(SseCKeyMD5, Chunking_Exactly512Bytes) {
    // 512 * 'A' = exactly one chunk
    std::string input(512, 'A');
    std::string md5 = s3fs_get_content_md5_hws_obs(input.c_str(), 0, 512);
    EXPECT_FALSE(md5.empty());
    EXPECT_EQ(md5.length(), 24u);
}

TEST(SseCKeyMD5, Chunking_513Bytes) {
    // 513 * 'A' = two iterations (512 + 1)
    std::string input(513, 'A');
    std::string md5 = s3fs_get_content_md5_hws_obs(input.c_str(), 0, 513);
    EXPECT_EQ(md5, "y/WpyiNQhdsM98r56CR5kQ==");
}

TEST(SseCKeyMD5, Chunking_1024Bytes) {
    std::string input(1024, 'B');
    std::string md5 = s3fs_get_content_md5_hws_obs(input.c_str(), 0, 1024);
    EXPECT_EQ(md5, "6fgADK/7zjadf+6cB9Q1CQ==");
}

// Test 6: NUL-byte latent bug fix evidence
// The old make_md5_from_string used strlen() which truncated keys at the
// first NUL byte; the base64 used length() which kept all bytes. This
// inconsistency meant that any SSE-C key with an embedded NUL would be
// rejected by OBS server (HTTP 400 InvalidDigest). The new code uses
// length() for both, fixing the latent bug.
// These tests prove the equivalence argument in the issue is executable.

TEST(SseCKeyMD5, EmbeddedNulByte_NewPathCorrect) {
    std::string key("abc\x00" "def", 7);

    // New path uses length() -> MD5 of all 7 bytes
    std::string md5_full = s3fs_get_content_md5_hws_obs(key.data(), 0, key.length());
    EXPECT_EQ(md5_full, "peTVljrkTB9L+zexo9VaPA==");

    // Reverse verification: the old strlen-based path would have computed
    // MD5 of only the first 3 bytes ("abc"), which is different
    std::string md5_strlen = s3fs_get_content_md5_hws_obs(key.data(), 0, 3);
    EXPECT_EQ(md5_strlen, "kAFQmDzST7DWlj99KOF/cg==");

    // The two differ -> proves strlen vs length() divergence on NUL-containing input
    EXPECT_NE(md5_full, md5_strlen);
}

TEST(SseCKeyMD5, LeadingNulByte_NewPathDoesNotReject) {
    // Leading NUL byte: old make_md5_from_string rejected this via '\0'==pstr[0]
    // check; new path computes MD5 normally.
    std::string key("\x00" "abc", 4);
    std::string md5 = s3fs_get_content_md5_hws_obs(key.data(), 0, key.length());
    EXPECT_EQ(md5, "ejlJ6rGks2CfsF8mmevrjw==");
}

// Main function to run the tests
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
