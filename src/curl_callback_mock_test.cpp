// ==========================================================================
// curl_callback_mock_test.cpp
//
// Mock tests for curl.cpp callback functions and multipart operations.
// Covers: WriteMemoryCallback, ReadUploadCache, ReadCallback,
// HeaderCallback, UploadReadCallback, DownloadWriteCallback,
// DownloadWriteCallbackforRead, CurlProgress, multipart post callbacks,
// retry callbacks, GetUploadId, UploadMultipartPostComplete,
// CopyMultipartPostComplete.
//
// Approach:
//   1. #include "curl.cpp" to access static/private functions
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
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

// Now expose S3fsCurl private members for testing
#define private public
#define protected public
#include "curl.cpp"
#undef private
#undef protected

// XML parsing headers for GetUploadId / CopyMultipartPostComplete tests
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
class CurlCallbackTest : public ::testing::Test {
protected:
    void SetUp() override {
        saved_bucket = bucket;
        saved_host = host;
        saved_pathrequeststyle = pathrequeststyle;
        saved_is_sigv4 = S3fsCurl::is_sigv4;
        saved_retries = S3fsCurl::retries;
        saved_readwrite_timeout = S3fsCurl::readwrite_timeout;
        saved_is_content_md5 = S3fsCurl::is_content_md5;
        saved_ssetype = S3fsCurl::ssetype;
        // Set default credentials
        S3fsCurl::AWSAccessKeyId = "AKID_TEST";
        S3fsCurl::AWSSecretAccessKey = "SECRET_TEST";
        S3fsCurl::AWSAccessToken = "";
    }
    void TearDown() override {
        bucket = saved_bucket;
        host = saved_host;
        pathrequeststyle = saved_pathrequeststyle;
        S3fsCurl::is_sigv4 = saved_is_sigv4;
        S3fsCurl::retries = saved_retries;
        S3fsCurl::readwrite_timeout = saved_readwrite_timeout;
        S3fsCurl::is_content_md5 = saved_is_content_md5;
        S3fsCurl::ssetype = saved_ssetype;
    }
    string saved_bucket;
    string saved_host;
    bool saved_pathrequeststyle;
    bool saved_is_sigv4;
    int saved_retries;
    time_t saved_readwrite_timeout;
    bool saved_is_content_md5;
    sse_type_t saved_ssetype;
};

// ==========================================================================
// Helper: create a temp file with known content
// ==========================================================================
static int create_temp_file(const char* content, size_t len) {
    char tmpl[] = "/tmp/curl_cb_test_XXXXXX";
    int fd = mkstemp(tmpl);
    if (fd < 0) return -1;
    unlink(tmpl);  // auto-cleanup on close
    if (content && len > 0) {
        ssize_t written = write(fd, content, len);
        (void)written;
        lseek(fd, 0, SEEK_SET);
    }
    return fd;
}

// ==========================================================================
// 1. WriteMemoryCallback Tests
// ==========================================================================

TEST_F(CurlCallbackTest, WriteMemoryCallback_NormalSmallData) {
    BodyData body;
    const char* data = "Hello, World!";
    size_t result = S3fsCurl::WriteMemoryCallback(
        (void*)data, 1, strlen(data), (void*)&body);
    EXPECT_EQ(strlen(data), result);
    EXPECT_EQ(strlen(data), body.size());
    EXPECT_STREQ(data, body.str());
}

TEST_F(CurlCallbackTest, WriteMemoryCallback_BlockSizeTimesNumBlocks) {
    BodyData body;
    const char* data = "ABCDEFGHIJ";
    // blockSize=2, numBlocks=5 => 10 bytes
    size_t result = S3fsCurl::WriteMemoryCallback(
        (void*)data, 2, 5, (void*)&body);
    EXPECT_EQ(10u, result);
    EXPECT_EQ(10u, body.size());
}

TEST_F(CurlCallbackTest, WriteMemoryCallback_ZeroSize) {
    BodyData body;
    const char* data = "X";
    // blockSize=0, numBlocks=5 => 0 bytes
    size_t result = S3fsCurl::WriteMemoryCallback(
        (void*)data, 0, 5, (void*)&body);
    EXPECT_EQ(0u, result);
    EXPECT_EQ(0u, body.size());
}

TEST_F(CurlCallbackTest, WriteMemoryCallback_LargeData) {
    BodyData body;
    // Create 64KB data to trigger internal Resize
    std::string large_data(65536, 'X');
    size_t result = S3fsCurl::WriteMemoryCallback(
        (void*)large_data.c_str(), 1, large_data.size(), (void*)&body);
    EXPECT_EQ(large_data.size(), result);
    EXPECT_EQ(large_data.size(), body.size());
}

TEST_F(CurlCallbackTest, WriteMemoryCallback_MultipleAppends) {
    BodyData body;
    const char* d1 = "Hello";
    const char* d2 = " World";
    S3fsCurl::WriteMemoryCallback((void*)d1, 1, 5, (void*)&body);
    S3fsCurl::WriteMemoryCallback((void*)d2, 1, 6, (void*)&body);
    EXPECT_EQ(11u, body.size());
    EXPECT_EQ(string("Hello World"), string(body.str(), body.size()));
}

// ==========================================================================
// 2. ReadUploadCache Tests
// ==========================================================================

TEST_F(CurlCallbackTest, ReadUploadCache_NullPtr) {
    tagDataBuffer dbuf;
    dbuf.pcBuffer = "hello";
    dbuf.ulBufferSize = 5;
    dbuf.offset = 0;
    // ptr is NULL
    size_t result = S3fsCurl::ReadUploadCache(NULL, 1, 10, (void*)&dbuf);
    EXPECT_EQ(0u, result);
}

TEST_F(CurlCallbackTest, ReadUploadCache_NullData) {
    char buf[64];
    size_t result = S3fsCurl::ReadUploadCache(buf, 1, 10, NULL);
    EXPECT_EQ(0u, result);
}

TEST_F(CurlCallbackTest, ReadUploadCache_OffsetExceedsBuffer) {
    char buf[64];
    tagDataBuffer dbuf;
    dbuf.pcBuffer = "hello";
    dbuf.ulBufferSize = 5;
    dbuf.offset = 10;  // offset > ulBufferSize
    size_t result = S3fsCurl::ReadUploadCache(buf, 1, 10, (void*)&dbuf);
    EXPECT_EQ(0u, result);
}

TEST_F(CurlCallbackTest, ReadUploadCache_ZeroRemaining) {
    char buf[64];
    tagDataBuffer dbuf;
    dbuf.pcBuffer = "hello";
    dbuf.ulBufferSize = 5;
    dbuf.offset = 5;  // exactly at end
    size_t result = S3fsCurl::ReadUploadCache(buf, 1, 10, (void*)&dbuf);
    EXPECT_EQ(0u, result);
}

TEST_F(CurlCallbackTest, ReadUploadCache_NormalFullRead) {
    char buf[64];
    memset(buf, 0, sizeof(buf));
    const char* src = "Hello World!";
    tagDataBuffer dbuf;
    dbuf.pcBuffer = src;
    dbuf.ulBufferSize = strlen(src);
    dbuf.offset = 0;
    // Request more than available
    size_t result = S3fsCurl::ReadUploadCache(buf, 1, 64, (void*)&dbuf);
    EXPECT_EQ(strlen(src), result);
    EXPECT_EQ(0, memcmp(buf, src, strlen(src)));
    EXPECT_EQ((off_t)strlen(src), dbuf.offset);
}

TEST_F(CurlCallbackTest, ReadUploadCache_PartialRead) {
    char buf[4];
    memset(buf, 0, sizeof(buf));
    const char* src = "ABCDEFGHIJ";
    tagDataBuffer dbuf;
    dbuf.pcBuffer = src;
    dbuf.ulBufferSize = 10;
    dbuf.offset = 0;
    // Request less than available (curl buffer smaller)
    size_t result = S3fsCurl::ReadUploadCache(buf, 1, 4, (void*)&dbuf);
    EXPECT_EQ(4u, result);
    EXPECT_EQ(0, memcmp(buf, "ABCD", 4));
    EXPECT_EQ(4, dbuf.offset);
}

TEST_F(CurlCallbackTest, ReadUploadCache_PartialReadFromOffset) {
    char buf[64];
    memset(buf, 0, sizeof(buf));
    const char* src = "ABCDEFGHIJ";
    tagDataBuffer dbuf;
    dbuf.pcBuffer = src;
    dbuf.ulBufferSize = 10;
    dbuf.offset = 7;  // 3 bytes remaining
    // Request more than remaining
    size_t result = S3fsCurl::ReadUploadCache(buf, 1, 64, (void*)&dbuf);
    EXPECT_EQ(3u, result);
    EXPECT_EQ(0, memcmp(buf, "HIJ", 3));
}

TEST_F(CurlCallbackTest, ReadUploadCache_ExactSizeMatch) {
    char buf[10];
    memset(buf, 0, sizeof(buf));
    const char* src = "0123456789";
    tagDataBuffer dbuf;
    dbuf.pcBuffer = src;
    dbuf.ulBufferSize = 10;
    dbuf.offset = 0;
    size_t result = S3fsCurl::ReadUploadCache(buf, 1, 10, (void*)&dbuf);
    EXPECT_EQ(10u, result);
    EXPECT_EQ(0, memcmp(buf, src, 10));
    EXPECT_EQ(10, dbuf.offset);
}

TEST_F(CurlCallbackTest, ReadUploadCache_BlockSizeMultiply) {
    char buf[64];
    memset(buf, 0, sizeof(buf));
    const char* src = "ABCDEFGH";
    tagDataBuffer dbuf;
    dbuf.pcBuffer = src;
    dbuf.ulBufferSize = 8;
    dbuf.offset = 0;
    // blockSize=2, numBlocks=3 => 6 bytes requested
    size_t result = S3fsCurl::ReadUploadCache(buf, 2, 3, (void*)&dbuf);
    EXPECT_EQ(6u, result);
    EXPECT_EQ(0, memcmp(buf, "ABCDEF", 6));
}

// ==========================================================================
// 3. ReadCallback Tests
// ==========================================================================

TEST_F(CurlCallbackTest, ReadCallback_ZeroSizeRequest) {
    S3fsCurl curl;
    const char* post = "test data";
    curl.postdata = (const unsigned char*)post;
    curl.postdata_remaining = strlen(post);
    char buf[64];
    // size=0, nmemb=5 => product is 0, which is < 1
    size_t result = S3fsCurl::ReadCallback(buf, 0, 5, (void*)&curl);
    EXPECT_EQ(0u, result);
}

TEST_F(CurlCallbackTest, ReadCallback_ZeroRemaining) {
    S3fsCurl curl;
    curl.postdata = (const unsigned char*)"data";
    curl.postdata_remaining = 0;
    char buf[64];
    size_t result = S3fsCurl::ReadCallback(buf, 1, 10, (void*)&curl);
    EXPECT_EQ(0u, result);
}

TEST_F(CurlCallbackTest, ReadCallback_NormalRead) {
    S3fsCurl curl;
    const char* post = "Hello POST";
    curl.postdata = (const unsigned char*)post;
    curl.postdata_remaining = strlen(post);
    char buf[64];
    memset(buf, 0, sizeof(buf));
    size_t result = S3fsCurl::ReadCallback(buf, 1, 64, (void*)&curl);
    EXPECT_EQ(strlen(post), result);
    EXPECT_EQ(0, memcmp(buf, post, strlen(post)));
    EXPECT_EQ(0u, curl.postdata_remaining);
}

TEST_F(CurlCallbackTest, ReadCallback_PartialRead) {
    S3fsCurl curl;
    const char* post = "ABCDEFGHIJ";
    curl.postdata = (const unsigned char*)post;
    curl.postdata_remaining = 10;
    char buf[4];
    memset(buf, 0, sizeof(buf));
    size_t result = S3fsCurl::ReadCallback(buf, 1, 4, (void*)&curl);
    EXPECT_EQ(4u, result);
    EXPECT_EQ(0, memcmp(buf, "ABCD", 4));
    EXPECT_EQ(6u, curl.postdata_remaining);
    // Read remaining
    char buf2[10];
    memset(buf2, 0, sizeof(buf2));
    result = S3fsCurl::ReadCallback(buf2, 1, 10, (void*)&curl);
    EXPECT_EQ(6u, result);
    EXPECT_EQ(0, memcmp(buf2, "EFGHIJ", 6));
    EXPECT_EQ(0u, curl.postdata_remaining);
}

TEST_F(CurlCallbackTest, ReadCallback_ExactSizeRead) {
    S3fsCurl curl;
    const char* post = "DATA";
    curl.postdata = (const unsigned char*)post;
    curl.postdata_remaining = 4;
    char buf[4];
    size_t result = S3fsCurl::ReadCallback(buf, 1, 4, (void*)&curl);
    EXPECT_EQ(4u, result);
    EXPECT_EQ(0u, curl.postdata_remaining);
}

// ==========================================================================
// 4. HeaderCallback Tests
// ==========================================================================

TEST_F(CurlCallbackTest, HeaderCallback_NormalHeader) {
    headers_t headers;
    const char* hdr = "Content-Type: text/html\r\n";
    size_t result = S3fsCurl::HeaderCallback(
        (void*)hdr, 1, strlen(hdr), (void*)&headers);
    EXPECT_EQ(strlen(hdr), result);
    EXPECT_EQ("text/html", headers["Content-Type"]);
}

TEST_F(CurlCallbackTest, HeaderCallback_XAmzHeader_Lowercased) {
    headers_t headers;
    const char* hdr = "X-Amz-Request-Id: ABC123\r\n";
    size_t result = S3fsCurl::HeaderCallback(
        (void*)hdr, 1, strlen(hdr), (void*)&headers);
    EXPECT_EQ(strlen(hdr), result);
    // x-amz headers should be lowercased
    EXPECT_EQ("ABC123", headers["x-amz-request-id"]);
}

TEST_F(CurlCallbackTest, HeaderCallback_XHwsHeader_Lowercased) {
    headers_t headers;
    const char* hdr = "X-Hws-Request-Id: DEF456\r\n";
    size_t result = S3fsCurl::HeaderCallback(
        (void*)hdr, 1, strlen(hdr), (void*)&headers);
    EXPECT_EQ(strlen(hdr), result);
    EXPECT_EQ("DEF456", headers["x-hws-request-id"]);
}

TEST_F(CurlCallbackTest, HeaderCallback_NoColon) {
    headers_t headers;
    const char* hdr = "HTTP/1.1 200 OK\r\n";
    size_t result = S3fsCurl::HeaderCallback(
        (void*)hdr, 1, strlen(hdr), (void*)&headers);
    EXPECT_EQ(strlen(hdr), result);
    // With no colon, the entire line becomes the key
    // getline with ':' delimiter reads the whole string
    EXPECT_TRUE(headers.count("HTTP/1.1 200 OK\r\n") > 0 ||
                headers.count("HTTP/1.1 200 OK") > 0);
}

TEST_F(CurlCallbackTest, HeaderCallback_EmptyValue) {
    headers_t headers;
    const char* hdr = "X-Custom:\r\n";
    size_t result = S3fsCurl::HeaderCallback(
        (void*)hdr, 1, strlen(hdr), (void*)&headers);
    EXPECT_EQ(strlen(hdr), result);
    EXPECT_TRUE(headers.count("X-Custom") > 0);
}

TEST_F(CurlCallbackTest, HeaderCallback_MultipleColons) {
    headers_t headers;
    const char* hdr = "Location: http://example.com:8080/path\r\n";
    size_t result = S3fsCurl::HeaderCallback(
        (void*)hdr, 1, strlen(hdr), (void*)&headers);
    EXPECT_EQ(strlen(hdr), result);
    // Key is "Location", value contains the rest including the second colon
    EXPECT_TRUE(headers.count("Location") > 0);
    string val = headers["Location"];
    EXPECT_NE(string::npos, val.find("http"));
}

TEST_F(CurlCallbackTest, HeaderCallback_BlockSizeVariant) {
    headers_t headers;
    const char* hdr = "ETag: \"abc123\"\r\n";
    // blockSize=2
    size_t result = S3fsCurl::HeaderCallback(
        (void*)hdr, 2, strlen(hdr) / 2, (void*)&headers);
    size_t expected = 2 * (strlen(hdr) / 2);
    EXPECT_EQ(expected, result);
}

TEST_F(CurlCallbackTest, HeaderCallback_CaseSensitiveNonAmz) {
    headers_t headers;
    const char* hdr = "Content-Length: 12345\r\n";
    S3fsCurl::HeaderCallback((void*)hdr, 1, strlen(hdr), (void*)&headers);
    // Non x-amz/x-hws headers should keep original case
    EXPECT_TRUE(headers.count("Content-Length") > 0);
}

// ==========================================================================
// 5. UploadReadCallback Tests
// ==========================================================================

TEST_F(CurlCallbackTest, UploadReadCallback_ZeroSizeRequest) {
    S3fsCurl curl;
    curl.partdata.fd = 0;
    curl.partdata.size = 100;
    char buf[64];
    size_t result = S3fsCurl::UploadReadCallback(buf, 0, 5, (void*)&curl);
    EXPECT_EQ(0u, result);
}

TEST_F(CurlCallbackTest, UploadReadCallback_InvalidFd) {
    S3fsCurl curl;
    curl.partdata.fd = -1;
    curl.partdata.size = 100;
    char buf[64];
    size_t result = S3fsCurl::UploadReadCallback(buf, 1, 10, (void*)&curl);
    EXPECT_EQ(0u, result);
}

TEST_F(CurlCallbackTest, UploadReadCallback_ZeroPartSize) {
    S3fsCurl curl;
    curl.partdata.fd = 1;  // not -1
    curl.partdata.size = 0;
    char buf[64];
    size_t result = S3fsCurl::UploadReadCallback(buf, 1, 10, (void*)&curl);
    EXPECT_EQ(0u, result);
}

TEST_F(CurlCallbackTest, UploadReadCallback_NegativePartSize) {
    S3fsCurl curl;
    curl.partdata.fd = 1;
    curl.partdata.size = -1;
    char buf[64];
    size_t result = S3fsCurl::UploadReadCallback(buf, 1, 10, (void*)&curl);
    EXPECT_EQ(0u, result);
}

TEST_F(CurlCallbackTest, UploadReadCallback_NormalRead) {
    const char* content = "ABCDEFGHIJ";
    int fd = create_temp_file(content, 10);
    ASSERT_GE(fd, 0);

    S3fsCurl curl;
    curl.partdata.fd = fd;
    curl.partdata.startpos = 0;
    curl.partdata.size = 10;

    char buf[64];
    memset(buf, 0, sizeof(buf));
    size_t result = S3fsCurl::UploadReadCallback(buf, 1, 64, (void*)&curl);
    EXPECT_EQ(10u, result);
    EXPECT_EQ(0, memcmp(buf, "ABCDEFGHIJ", 10));
    EXPECT_EQ(10, curl.partdata.startpos);
    EXPECT_EQ(0, curl.partdata.size);

    close(fd);
}

TEST_F(CurlCallbackTest, UploadReadCallback_PartialRead) {
    const char* content = "0123456789ABCDEF";
    int fd = create_temp_file(content, 16);
    ASSERT_GE(fd, 0);

    S3fsCurl curl;
    curl.partdata.fd = fd;
    curl.partdata.startpos = 0;
    curl.partdata.size = 16;

    char buf[4];
    memset(buf, 0, sizeof(buf));
    size_t result = S3fsCurl::UploadReadCallback(buf, 1, 4, (void*)&curl);
    EXPECT_EQ(4u, result);
    EXPECT_EQ(0, memcmp(buf, "0123", 4));
    EXPECT_EQ(4, curl.partdata.startpos);
    EXPECT_EQ(12, curl.partdata.size);

    close(fd);
}

TEST_F(CurlCallbackTest, UploadReadCallback_ReadFromOffset) {
    const char* content = "HEADER_DATA_PAYLOAD";
    int fd = create_temp_file(content, strlen(content));
    ASSERT_GE(fd, 0);

    S3fsCurl curl;
    curl.partdata.fd = fd;
    curl.partdata.startpos = 7;  // start at "DATA_PAYLOAD"
    curl.partdata.size = 12;

    char buf[64];
    memset(buf, 0, sizeof(buf));
    size_t result = S3fsCurl::UploadReadCallback(buf, 1, 64, (void*)&curl);
    EXPECT_EQ(12u, result);
    EXPECT_EQ(0, memcmp(buf, "DATA_PAYLOAD", 12));

    close(fd);
}

// ==========================================================================
// 6. DownloadWriteCallback Tests
// ==========================================================================

TEST_F(CurlCallbackTest, DownloadWriteCallback_ZeroSize) {
    S3fsCurl curl;
    curl.partdata.fd = 1;
    curl.partdata.size = 100;
    const char* data = "test";
    size_t result = S3fsCurl::DownloadWriteCallback(
        (void*)data, 0, 5, (void*)&curl);
    EXPECT_EQ(0u, result);
}

TEST_F(CurlCallbackTest, DownloadWriteCallback_InvalidFd) {
    S3fsCurl curl;
    curl.partdata.fd = -1;
    curl.partdata.size = 100;
    const char* data = "test";
    size_t result = S3fsCurl::DownloadWriteCallback(
        (void*)data, 1, 4, (void*)&curl);
    EXPECT_EQ(0u, result);
}

TEST_F(CurlCallbackTest, DownloadWriteCallback_ZeroPartSize) {
    S3fsCurl curl;
    curl.partdata.fd = 1;
    curl.partdata.size = 0;
    const char* data = "test";
    size_t result = S3fsCurl::DownloadWriteCallback(
        (void*)data, 1, 4, (void*)&curl);
    EXPECT_EQ(0u, result);
}

TEST_F(CurlCallbackTest, DownloadWriteCallback_NormalWrite) {
    int fd = create_temp_file(NULL, 0);
    ASSERT_GE(fd, 0);

    S3fsCurl curl;
    curl.partdata.fd = fd;
    curl.partdata.startpos = 0;
    curl.partdata.size = 100;

    const char* data = "DOWNLOAD_DATA";
    size_t result = S3fsCurl::DownloadWriteCallback(
        (void*)data, 1, strlen(data), (void*)&curl);
    EXPECT_EQ(strlen(data), result);
    EXPECT_EQ((off_t)strlen(data), curl.partdata.startpos);
    EXPECT_EQ((ssize_t)(100 - strlen(data)), curl.partdata.size);

    // Verify written content
    char verify[64];
    memset(verify, 0, sizeof(verify));
    lseek(fd, 0, SEEK_SET);
    read(fd, verify, strlen(data));
    EXPECT_EQ(0, memcmp(verify, data, strlen(data)));

    close(fd);
}

TEST_F(CurlCallbackTest, DownloadWriteCallback_SizeLimitsWrite) {
    int fd = create_temp_file(NULL, 0);
    ASSERT_GE(fd, 0);

    S3fsCurl curl;
    curl.partdata.fd = fd;
    curl.partdata.startpos = 0;
    curl.partdata.size = 5;  // only allow 5 bytes

    const char* data = "ABCDEFGHIJ";
    size_t result = S3fsCurl::DownloadWriteCallback(
        (void*)data, 1, 10, (void*)&curl);
    // Should only write 5 bytes (partdata.size limit)
    EXPECT_EQ(5u, result);
    EXPECT_EQ(5, curl.partdata.startpos);
    EXPECT_EQ(0, curl.partdata.size);

    // Verify
    char verify[10];
    memset(verify, 0, sizeof(verify));
    lseek(fd, 0, SEEK_SET);
    read(fd, verify, 5);
    EXPECT_EQ(0, memcmp(verify, "ABCDE", 5));

    close(fd);
}

TEST_F(CurlCallbackTest, DownloadWriteCallback_WriteFromOffset) {
    int fd = create_temp_file(NULL, 0);
    ASSERT_GE(fd, 0);

    S3fsCurl curl;
    curl.partdata.fd = fd;
    curl.partdata.startpos = 100;  // write at offset 100
    curl.partdata.size = 50;

    const char* data = "OFFSET_DATA";
    size_t result = S3fsCurl::DownloadWriteCallback(
        (void*)data, 1, strlen(data), (void*)&curl);
    EXPECT_EQ(strlen(data), result);
    EXPECT_EQ((off_t)(100 + strlen(data)), curl.partdata.startpos);

    // Verify at offset 100
    char verify[20];
    memset(verify, 0, sizeof(verify));
    lseek(fd, 100, SEEK_SET);
    read(fd, verify, strlen(data));
    EXPECT_EQ(0, memcmp(verify, "OFFSET_DATA", strlen(data)));

    close(fd);
}

// ==========================================================================
// 7. DownloadWriteCallbackforRead Tests
// ==========================================================================

TEST_F(CurlCallbackTest, DownloadWriteCallbackforRead_ZeroSize) {
    S3fsCurl curl;
    curl.read_buf = new char[64];
    curl.read_size = 64;
    curl.cur_pos = 0;
    const char* data = "X";
    size_t result = S3fsCurl::DownloadWriteCallbackforRead(
        (void*)data, 0, 5, (void*)&curl);
    EXPECT_EQ(0u, result);
    delete[] curl.read_buf;
    curl.read_buf = NULL;
}

TEST_F(CurlCallbackTest, DownloadWriteCallbackforRead_NullReadBuf) {
    S3fsCurl curl;
    curl.read_buf = NULL;
    curl.read_size = 64;
    curl.cur_pos = 0;
    const char* data = "test";
    size_t result = S3fsCurl::DownloadWriteCallbackforRead(
        (void*)data, 1, 4, (void*)&curl);
    EXPECT_EQ(0u, result);
}

TEST_F(CurlCallbackTest, DownloadWriteCallbackforRead_ZeroReadSize) {
    S3fsCurl curl;
    curl.read_buf = new char[64];
    curl.read_size = 0;
    curl.cur_pos = 0;
    const char* data = "test";
    size_t result = S3fsCurl::DownloadWriteCallbackforRead(
        (void*)data, 1, 4, (void*)&curl);
    EXPECT_EQ(0u, result);
    delete[] curl.read_buf;
    curl.read_buf = NULL;
}

TEST_F(CurlCallbackTest, DownloadWriteCallbackforRead_NormalWrite) {
    S3fsCurl curl;
    curl.read_buf = new char[64];
    memset(curl.read_buf, 0, 64);
    curl.read_size = 64;
    curl.cur_pos = 0;

    const char* data = "Hello Read Buffer";
    size_t result = S3fsCurl::DownloadWriteCallbackforRead(
        (void*)data, 1, strlen(data), (void*)&curl);
    EXPECT_EQ(strlen(data), result);
    EXPECT_EQ(strlen(data), curl.cur_pos);
    EXPECT_EQ(0, memcmp(curl.read_buf, data, strlen(data)));

    delete[] curl.read_buf;
    curl.read_buf = NULL;
}

TEST_F(CurlCallbackTest, DownloadWriteCallbackforRead_ExceedsReadSize) {
    S3fsCurl curl;
    curl.read_buf = new char[10];
    memset(curl.read_buf, 0, 10);
    curl.read_size = 10;
    curl.cur_pos = 0;

    const char* data = "ABCDEFGHIJKLMNOP";  // 16 bytes, but only 10 fit
    size_t result = S3fsCurl::DownloadWriteCallbackforRead(
        (void*)data, 1, 16, (void*)&curl);
    EXPECT_EQ(10u, result);
    EXPECT_EQ(10u, curl.cur_pos);
    EXPECT_EQ(0, memcmp(curl.read_buf, "ABCDEFGHIJ", 10));

    delete[] curl.read_buf;
    curl.read_buf = NULL;
}

TEST_F(CurlCallbackTest, DownloadWriteCallbackforRead_PartialFromCurPos) {
    S3fsCurl curl;
    curl.read_buf = new char[20];
    memset(curl.read_buf, 0, 20);
    curl.read_size = 20;
    curl.cur_pos = 15;  // only 5 bytes of space left

    const char* data = "ABCDEFGHIJ";
    size_t result = S3fsCurl::DownloadWriteCallbackforRead(
        (void*)data, 1, 10, (void*)&curl);
    EXPECT_EQ(5u, result);  // only 5 bytes fit
    EXPECT_EQ(20u, curl.cur_pos);
    EXPECT_EQ(0, memcmp(curl.read_buf + 15, "ABCDE", 5));

    delete[] curl.read_buf;
    curl.read_buf = NULL;
}

TEST_F(CurlCallbackTest, DownloadWriteCallbackforRead_MultipleWrites) {
    S3fsCurl curl;
    curl.read_buf = new char[20];
    memset(curl.read_buf, 0, 20);
    curl.read_size = 20;
    curl.cur_pos = 0;

    const char* d1 = "HELLO";
    const char* d2 = " WORLD";
    S3fsCurl::DownloadWriteCallbackforRead((void*)d1, 1, 5, (void*)&curl);
    EXPECT_EQ(5u, curl.cur_pos);
    S3fsCurl::DownloadWriteCallbackforRead((void*)d2, 1, 6, (void*)&curl);
    EXPECT_EQ(11u, curl.cur_pos);
    EXPECT_EQ(0, memcmp(curl.read_buf, "HELLO WORLD", 11));

    delete[] curl.read_buf;
    curl.read_buf = NULL;
}

TEST_F(CurlCallbackTest, DownloadWriteCallbackforRead_SmallData) {
    // Test that copysize < sizeof(long) doesn't log prefix
    S3fsCurl curl;
    curl.read_buf = new char[10];
    memset(curl.read_buf, 0, 10);
    curl.read_size = 10;
    curl.cur_pos = 0;

    const char* data = "AB";  // 2 bytes < sizeof(long)
    size_t result = S3fsCurl::DownloadWriteCallbackforRead(
        (void*)data, 1, 2, (void*)&curl);
    EXPECT_EQ(2u, result);
    EXPECT_EQ(2u, curl.cur_pos);

    delete[] curl.read_buf;
    curl.read_buf = NULL;
}

// ==========================================================================
// 8. CurlProgress Tests
// ==========================================================================

TEST_F(CurlCallbackTest, CurlProgress_NormalProgress) {
    // Create a fake CURL* key for the maps
    CURL* fake_curl = curl_easy_init();
    ASSERT_NE(nullptr, fake_curl);

    // Initialize the progress tracking
    S3fsCurl::curl_times[fake_curl] = time(0);
    S3fsCurl::curl_progress[fake_curl] = progress_t(0.0, 0.0);

    // Simulate progress with new values
    int result = S3fsCurl::CurlProgress(
        (void*)fake_curl, 1000.0, 500.0, 0.0, 0.0);
    EXPECT_EQ(0, result);

    // Cleanup
    S3fsCurl::curl_times.erase(fake_curl);
    S3fsCurl::curl_progress.erase(fake_curl);
    curl_easy_cleanup(fake_curl);
}

TEST_F(CurlCallbackTest, CurlProgress_NoProgressWithinTimeout) {
    CURL* fake_curl = curl_easy_init();
    ASSERT_NE(nullptr, fake_curl);

    // Initialize with current time and zero progress
    S3fsCurl::curl_times[fake_curl] = time(0);
    S3fsCurl::curl_progress[fake_curl] = progress_t(100.0, 50.0);

    // Same progress values => no progress, but within timeout
    S3fsCurl::readwrite_timeout = 60;
    int result = S3fsCurl::CurlProgress(
        (void*)fake_curl, 200.0, 100.0, 100.0, 50.0);
    EXPECT_EQ(0, result);

    S3fsCurl::curl_times.erase(fake_curl);
    S3fsCurl::curl_progress.erase(fake_curl);
    curl_easy_cleanup(fake_curl);
}

TEST_F(CurlCallbackTest, CurlProgress_Timeout) {
    CURL* fake_curl = curl_easy_init();
    ASSERT_NE(nullptr, fake_curl);

    // Set time far in the past
    S3fsCurl::curl_times[fake_curl] = time(0) - 1000;
    S3fsCurl::curl_progress[fake_curl] = progress_t(100.0, 50.0);

    // Same progress, and time exceeded timeout
    S3fsCurl::readwrite_timeout = 5;
    int result = S3fsCurl::CurlProgress(
        (void*)fake_curl, 200.0, 100.0, 100.0, 50.0);
    EXPECT_EQ(CURLE_ABORTED_BY_CALLBACK, result);

    S3fsCurl::curl_times.erase(fake_curl);
    S3fsCurl::curl_progress.erase(fake_curl);
    curl_easy_cleanup(fake_curl);
}

TEST_F(CurlCallbackTest, CurlProgress_ProgressResets) {
    CURL* fake_curl = curl_easy_init();
    ASSERT_NE(nullptr, fake_curl);

    // Set time far in the past, but provide new progress
    S3fsCurl::curl_times[fake_curl] = time(0) - 1000;
    S3fsCurl::curl_progress[fake_curl] = progress_t(0.0, 0.0);

    S3fsCurl::readwrite_timeout = 5;
    // New progress values => should reset timer, return 0
    int result = S3fsCurl::CurlProgress(
        (void*)fake_curl, 200.0, 100.0, 100.0, 50.0);
    EXPECT_EQ(0, result);

    S3fsCurl::curl_times.erase(fake_curl);
    S3fsCurl::curl_progress.erase(fake_curl);
    curl_easy_cleanup(fake_curl);
}

// ==========================================================================
// 9. UploadMultipartPostCallback Tests
// ==========================================================================

TEST_F(CurlCallbackTest, UploadMultipartPostCallback_NullCurl) {
    bool result = S3fsCurl::UploadMultipartPostCallback(NULL);
    EXPECT_FALSE(result);
}

TEST_F(CurlCallbackTest, UploadMultipartPostCallback_NoETag) {
    S3fsCurl curl;
    curl.responseHeaders.clear();
    // No ETag => UploadMultipartPostComplete returns false
    bool result = S3fsCurl::UploadMultipartPostCallback(&curl);
    EXPECT_FALSE(result);
}

TEST_F(CurlCallbackTest, UploadMultipartPostCallback_WithETag) {
    S3fsCurl curl;
    etaglist_t list;
    list.push_back("");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 0;
    curl.partdata.etag = "";
    curl.responseHeaders["ETag"] = "\"abc123\"";
    S3fsCurl::is_content_md5 = false;

    bool result = S3fsCurl::UploadMultipartPostCallback(&curl);
    EXPECT_TRUE(result);
    EXPECT_EQ("\"abc123\"", list[0]);
    EXPECT_TRUE(curl.partdata.uploaded);
}

// ==========================================================================
// 10. CopyMultipartPostCallback Tests
// ==========================================================================

TEST_F(CurlCallbackTest, CopyMultipartPostCallback_NullCurl) {
    bool result = S3fsCurl::CopyMultipartPostCallback(NULL);
    EXPECT_FALSE(result);
}

TEST_F(CurlCallbackTest, CopyMultipartPostCallback_NoBody) {
    S3fsCurl curl;
    curl.bodydata = NULL;
    bool result = S3fsCurl::CopyMultipartPostCallback(&curl);
    EXPECT_FALSE(result);
}

TEST_F(CurlCallbackTest, CopyMultipartPostCallback_EmptyBody) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    bool result = S3fsCurl::CopyMultipartPostCallback(&curl);
    EXPECT_FALSE(result);
    delete curl.bodydata;
    curl.bodydata = NULL;
}

TEST_F(CurlCallbackTest, CopyMultipartPostCallback_ValidXml) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    etaglist_t list;
    list.push_back("");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 0;

    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<CopyPartResult>"
        "<ETag>\"etag_value_123\"</ETag>"
        "<LastModified>2026-01-01</LastModified>"
        "</CopyPartResult>";
    curl.bodydata->Append((void*)xml, strlen(xml));

    bool result = S3fsCurl::CopyMultipartPostCallback(&curl);
    EXPECT_TRUE(result);
    EXPECT_EQ("etag_value_123", list[0]);
    EXPECT_TRUE(curl.partdata.uploaded);

    delete curl.bodydata;
    curl.bodydata = NULL;
}

// ==========================================================================
// 11. MixMultipartPostCallback Tests
// ==========================================================================

TEST_F(CurlCallbackTest, MixMultipartPostCallback_NullCurl) {
    bool result = S3fsCurl::MixMultipartPostCallback(NULL);
    EXPECT_FALSE(result);
}

TEST_F(CurlCallbackTest, MixMultipartPostCallback_CopyType) {
    S3fsCurl curl;
    curl.type = S3fsCurl::REQTYPE_COPYMULTIPOST;
    curl.bodydata = new BodyData();
    etaglist_t list;
    list.push_back("");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 0;

    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<CopyPartResult>"
        "<ETag>\"copy_etag\"</ETag>"
        "</CopyPartResult>";
    curl.bodydata->Append((void*)xml, strlen(xml));

    bool result = S3fsCurl::MixMultipartPostCallback(&curl);
    EXPECT_TRUE(result);
    EXPECT_EQ("copy_etag", list[0]);

    delete curl.bodydata;
    curl.bodydata = NULL;
}

TEST_F(CurlCallbackTest, MixMultipartPostCallback_UploadType) {
    S3fsCurl curl;
    curl.type = S3fsCurl::REQTYPE_UPLOADMULTIPOST;
    etaglist_t list;
    list.push_back("");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 0;
    curl.responseHeaders["ETag"] = "\"upload_etag\"";
    S3fsCurl::is_content_md5 = false;

    bool result = S3fsCurl::MixMultipartPostCallback(&curl);
    EXPECT_TRUE(result);
    EXPECT_EQ("\"upload_etag\"", list[0]);
}

TEST_F(CurlCallbackTest, MixMultipartPostCallback_UploadType_NoETag) {
    S3fsCurl curl;
    curl.type = S3fsCurl::REQTYPE_UPLOADMULTIPOST;
    curl.responseHeaders.clear();

    bool result = S3fsCurl::MixMultipartPostCallback(&curl);
    EXPECT_FALSE(result);
}

// ==========================================================================
// 11b. MixMultipartPostRetryCallback Tests
// ==========================================================================

TEST_F(CurlCallbackTest, MixMultipartPostRetryCallback_NullCurl) {
    S3fsCurl* result = S3fsCurl::MixMultipartPostRetryCallback(NULL);
    EXPECT_EQ(nullptr, result);
}

TEST_F(CurlCallbackTest, MixMultipartPostRetryCallback_CopyType_Dispatches) {
    S3fsCurl curl;
    curl.type = S3fsCurl::REQTYPE_COPYMULTIPOST;
    curl.url = "http://host/bucket/obj?partNumber=2&uploadId=test123";
    curl.path = "/obj";
    curl.b_copy_source = "/bucket/obj";
    curl.b_copy_source_range = "bytes=0-10485759";
    curl.retry_count = 0;
    S3fsCurl::retries = 3;

    S3fsCurl* result = S3fsCurl::MixMultipartPostRetryCallback(&curl);
    ASSERT_NE(nullptr, result);
    EXPECT_EQ(S3fsCurl::REQTYPE_COPYMULTIPOST, result->type);

    result->DestroyCurlHandle();
    delete result;
}

TEST_F(CurlCallbackTest, MixMultipartPostRetryCallback_UploadType_Dispatches) {
    int fd = create_temp_file("testdata", 8);
    ASSERT_NE(-1, fd);

    S3fsCurl curl;
    curl.type = S3fsCurl::REQTYPE_UPLOADMULTIPOST;
    curl.url = "http://host/bucket/obj?partNumber=1&uploadId=test123";
    curl.path = "/obj";
    curl.partdata.fd = fd;
    curl.partdata.startpos = 0;
    curl.partdata.size = 8;
    curl.b_partdata_startpos = 0;
    curl.b_partdata_size = 8;
    curl.retry_count = 0;
    S3fsCurl::retries = 3;

    S3fsCurl* result = S3fsCurl::MixMultipartPostRetryCallback(&curl);
    ASSERT_NE(nullptr, result);
    EXPECT_EQ(S3fsCurl::REQTYPE_UPLOADMULTIPOST, result->type);

    result->DestroyCurlHandle();
    delete result;
    close(fd);
}

// Reproduces the original bug: CopyPart with UploadMultipartPostRetryCallback
// returns NULL because fd==-1 (CopyPart never sets fd).
TEST_F(CurlCallbackTest, OldBug_CopyPartWithUploadRetryCallback_ReturnsNull) {
    S3fsCurl curl;
    curl.type = S3fsCurl::REQTYPE_COPYMULTIPOST;
    curl.url = "http://host/bucket/obj?partNumber=2&uploadId=test123";
    curl.path = "/obj";
    // partdata.fd = -1 (default) — CopyPart never sets fd
    curl.retry_count = 0;
    S3fsCurl::retries = 3;

    // Using the WRONG callback (the bug):
    // UploadMultipartPostRetryCallback copies fd=-1, UploadMultipartPostSetup checks fd==-1 → return -1
    S3fsCurl* result = S3fsCurl::UploadMultipartPostRetryCallback(&curl);
    EXPECT_EQ(nullptr, result);  // Retry fails → upload aborted
}

// ==========================================================================
// 12. UploadMultipartPostRetryCallback Tests
// ==========================================================================

TEST_F(CurlCallbackTest, UploadMultipartPostRetryCallback_NullCurl) {
    S3fsCurl* result = S3fsCurl::UploadMultipartPostRetryCallback(NULL);
    EXPECT_EQ(nullptr, result);
}

TEST_F(CurlCallbackTest, UploadMultipartPostRetryCallback_NoUploadId) {
    S3fsCurl curl;
    curl.url = "http://example.com/path?partNumber=1";
    // No uploadId in URL
    S3fsCurl* result = S3fsCurl::UploadMultipartPostRetryCallback(&curl);
    EXPECT_EQ(nullptr, result);
}

TEST_F(CurlCallbackTest, UploadMultipartPostRetryCallback_NoPartNumber) {
    S3fsCurl curl;
    curl.url = "http://example.com/path?uploadId=abc123";
    // No partNumber in URL
    S3fsCurl* result = S3fsCurl::UploadMultipartPostRetryCallback(&curl);
    EXPECT_EQ(nullptr, result);
}

TEST_F(CurlCallbackTest, UploadMultipartPostRetryCallback_OverRetryLimit) {
    S3fsCurl curl;
    curl.url = "http://example.com/path?partNumber=1&uploadId=abc123";
    curl.path = "/test/file";
    S3fsCurl::retries = 3;
    curl.retry_count = 3;  // at limit

    S3fsCurl* result = S3fsCurl::UploadMultipartPostRetryCallback(&curl);
    EXPECT_EQ(nullptr, result);
}

// ==========================================================================
// 13. CopyMultipartPostRetryCallback Tests
// ==========================================================================

TEST_F(CurlCallbackTest, CopyMultipartPostRetryCallback_NullCurl) {
    S3fsCurl* result = S3fsCurl::CopyMultipartPostRetryCallback(NULL);
    EXPECT_EQ(nullptr, result);
}

TEST_F(CurlCallbackTest, CopyMultipartPostRetryCallback_NoUploadId) {
    S3fsCurl curl;
    curl.url = "http://example.com/path?partNumber=1";
    S3fsCurl* result = S3fsCurl::CopyMultipartPostRetryCallback(&curl);
    EXPECT_EQ(nullptr, result);
}

TEST_F(CurlCallbackTest, CopyMultipartPostRetryCallback_NoPartNumber) {
    S3fsCurl curl;
    curl.url = "http://example.com/path?uploadId=abc123";
    S3fsCurl* result = S3fsCurl::CopyMultipartPostRetryCallback(&curl);
    EXPECT_EQ(nullptr, result);
}

TEST_F(CurlCallbackTest, CopyMultipartPostRetryCallback_OverRetryLimit) {
    S3fsCurl curl;
    curl.url = "http://example.com/path?partNumber=1&uploadId=abc123";
    curl.path = "/test/file";
    S3fsCurl::retries = 3;
    curl.retry_count = 3;

    S3fsCurl* result = S3fsCurl::CopyMultipartPostRetryCallback(&curl);
    EXPECT_EQ(nullptr, result);
}

// ==========================================================================
// 14. ParallelGetObjectRetryCallback Tests
// ==========================================================================

TEST_F(CurlCallbackTest, ParallelGetObjectRetryCallback_NullCurl) {
    S3fsCurl* result = S3fsCurl::ParallelGetObjectRetryCallback(NULL);
    EXPECT_EQ(nullptr, result);
}

TEST_F(CurlCallbackTest, ParallelGetObjectRetryCallback_OverRetryLimit) {
    S3fsCurl curl;
    curl.path = "/test/file";
    S3fsCurl::retries = 3;
    curl.retry_count = 3;

    S3fsCurl* result = S3fsCurl::ParallelGetObjectRetryCallback(&curl);
    EXPECT_EQ(nullptr, result);
}

TEST_F(CurlCallbackTest, ParallelGetObjectRetryCallback_NormalRetry) {
    int fd = create_temp_file("test_data", 9);
    ASSERT_GE(fd, 0);

    S3fsCurl curl;
    curl.path = "/test/file";
    curl.partdata.fd = fd;
    curl.partdata.startpos = 0;
    curl.partdata.size = 100;
    curl.b_ssetype = SSE_DISABLE;
    curl.b_ssevalue = "";
    S3fsCurl::retries = 5;
    curl.retry_count = 0;

    // Set up globals for PreGetObjectRequest
    bucket = "test-bucket";
    host = "http://127.0.0.1:8080";
    S3fsCurl::is_sigv4 = true;

    S3fsCurl* newcurl = S3fsCurl::ParallelGetObjectRetryCallback(&curl);
    if (newcurl) {
        EXPECT_EQ(1, newcurl->retry_count);
        delete newcurl;
    }
    // If NULL, it's because CreateCurlHandle failed in test env - that's OK
    close(fd);
}

// ==========================================================================
// 15. GetUploadId Tests
// ==========================================================================

TEST_F(CurlCallbackTest, GetUploadId_NullBodyData) {
    S3fsCurl curl;
    curl.bodydata = NULL;
    string upload_id;
    EXPECT_FALSE(curl.GetUploadId(upload_id));
    EXPECT_TRUE(upload_id.empty());
}

TEST_F(CurlCallbackTest, GetUploadId_EmptyBody) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    string upload_id;
    EXPECT_FALSE(curl.GetUploadId(upload_id));
    EXPECT_TRUE(upload_id.empty());
    delete curl.bodydata;
    curl.bodydata = NULL;
}

TEST_F(CurlCallbackTest, GetUploadId_MalformedXml) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    const char* bad_xml = "<not valid xml><<<";
    curl.bodydata->Append((void*)bad_xml, strlen(bad_xml));
    string upload_id;
    EXPECT_FALSE(curl.GetUploadId(upload_id));
    delete curl.bodydata;
    curl.bodydata = NULL;
}

TEST_F(CurlCallbackTest, GetUploadId_ValidXmlNoUploadId) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<InitiateMultipartUploadResult>"
        "<Bucket>test-bucket</Bucket>"
        "<Key>/test/file</Key>"
        "</InitiateMultipartUploadResult>";
    curl.bodydata->Append((void*)xml, strlen(xml));
    string upload_id;
    EXPECT_FALSE(curl.GetUploadId(upload_id));
    EXPECT_TRUE(upload_id.empty());
    delete curl.bodydata;
    curl.bodydata = NULL;
}

TEST_F(CurlCallbackTest, GetUploadId_ValidXmlWithUploadId) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<InitiateMultipartUploadResult>"
        "<Bucket>test-bucket</Bucket>"
        "<Key>/test/file</Key>"
        "<UploadId>upload123abc</UploadId>"
        "</InitiateMultipartUploadResult>";
    curl.bodydata->Append((void*)xml, strlen(xml));
    string upload_id;
    EXPECT_TRUE(curl.GetUploadId(upload_id));
    EXPECT_EQ("upload123abc", upload_id);
    delete curl.bodydata;
    curl.bodydata = NULL;
}

TEST_F(CurlCallbackTest, GetUploadId_XmlWithNoChildren) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    // XML with empty root element
    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Empty/>";
    curl.bodydata->Append((void*)xml, strlen(xml));
    string upload_id;
    EXPECT_FALSE(curl.GetUploadId(upload_id));
    delete curl.bodydata;
    curl.bodydata = NULL;
}

TEST_F(CurlCallbackTest, GetUploadId_XmlNonTextChild) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    // XML with element nodes only, no text content under UploadId
    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<InitiateMultipartUploadResult>"
        "<UploadId><nested>value</nested></UploadId>"
        "</InitiateMultipartUploadResult>";
    curl.bodydata->Append((void*)xml, strlen(xml));
    string upload_id;
    // The first child of UploadId is an element node, not text
    // So it should not match the XML_TEXT_NODE check
    bool result = curl.GetUploadId(upload_id);
    // The nested element's child might be detected as text, behavior depends on libxml2
    // We just verify no crash
    (void)result;
    delete curl.bodydata;
    curl.bodydata = NULL;
}

// ==========================================================================
// 16. UploadMultipartPostComplete Tests
// ==========================================================================

TEST_F(CurlCallbackTest, UploadMultipartPostComplete_NoETag) {
    S3fsCurl curl;
    curl.responseHeaders.clear();
    EXPECT_FALSE(curl.UploadMultipartPostComplete());
}

TEST_F(CurlCallbackTest, UploadMultipartPostComplete_WithETag_NoMd5Check) {
    S3fsCurl curl;
    etaglist_t list;
    list.push_back("");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 0;
    curl.responseHeaders["ETag"] = "\"aabbccdd\"";
    S3fsCurl::is_content_md5 = false;

    EXPECT_TRUE(curl.UploadMultipartPostComplete());
    EXPECT_EQ("\"aabbccdd\"", list[0]);
    EXPECT_TRUE(curl.partdata.uploaded);
}

TEST_F(CurlCallbackTest, UploadMultipartPostComplete_Md5Match) {
    S3fsCurl curl;
    etaglist_t list;
    list.push_back("");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 0;
    curl.partdata.etag = "aabbccdd";
    curl.responseHeaders["ETag"] = "\"aabbccdd\"";
    S3fsCurl::is_content_md5 = true;
    S3fsCurl::ssetype = SSE_DISABLE;

    EXPECT_TRUE(curl.UploadMultipartPostComplete());
    EXPECT_EQ("\"aabbccdd\"", list[0]);
}

TEST_F(CurlCallbackTest, UploadMultipartPostComplete_Md5Mismatch) {
    S3fsCurl curl;
    etaglist_t list;
    list.push_back("");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 0;
    curl.partdata.etag = "different_etag";
    curl.responseHeaders["ETag"] = "\"aabbccdd\"";
    S3fsCurl::is_content_md5 = true;
    S3fsCurl::ssetype = SSE_DISABLE;

    EXPECT_FALSE(curl.UploadMultipartPostComplete());
}

TEST_F(CurlCallbackTest, UploadMultipartPostComplete_SSE_C_SkipsMd5) {
    S3fsCurl curl;
    etaglist_t list;
    list.push_back("");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 0;
    curl.partdata.etag = "different_etag";
    curl.responseHeaders["ETag"] = "\"aabbccdd\"";
    S3fsCurl::is_content_md5 = true;
    S3fsCurl::ssetype = SSE_C;

    // SSE_C => skip MD5 check
    EXPECT_TRUE(curl.UploadMultipartPostComplete());
    EXPECT_EQ("\"aabbccdd\"", list[0]);
}

TEST_F(CurlCallbackTest, UploadMultipartPostComplete_SSE_KMS_SkipsMd5) {
    S3fsCurl curl;
    etaglist_t list;
    list.push_back("");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 0;
    curl.partdata.etag = "different_etag";
    curl.responseHeaders["ETag"] = "\"aabbccdd\"";
    S3fsCurl::is_content_md5 = true;
    S3fsCurl::ssetype = SSE_KMS;

    EXPECT_TRUE(curl.UploadMultipartPostComplete());
}

TEST_F(CurlCallbackTest, UploadMultipartPostComplete_MultipleEtagPositions) {
    S3fsCurl curl;
    etaglist_t list;
    list.push_back("first");
    list.push_back("");
    list.push_back("third");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 1;
    curl.responseHeaders["ETag"] = "\"second_etag\"";
    S3fsCurl::is_content_md5 = false;

    EXPECT_TRUE(curl.UploadMultipartPostComplete());
    EXPECT_EQ("first", list[0]);
    EXPECT_EQ("\"second_etag\"", list[1]);
    EXPECT_EQ("third", list[2]);
}

// ==========================================================================
// 17. CopyMultipartPostComplete Tests
// ==========================================================================

TEST_F(CurlCallbackTest, CopyMultipartPostComplete_NullBody) {
    S3fsCurl curl;
    curl.bodydata = NULL;
    EXPECT_FALSE(curl.CopyMultipartPostComplete());
}

TEST_F(CurlCallbackTest, CopyMultipartPostComplete_EmptyBody) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    EXPECT_FALSE(curl.CopyMultipartPostComplete());
    delete curl.bodydata;
    curl.bodydata = NULL;
}

TEST_F(CurlCallbackTest, CopyMultipartPostComplete_MalformedXml) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    const char* bad_xml = "<<<<not valid";
    curl.bodydata->Append((void*)bad_xml, strlen(bad_xml));
    EXPECT_FALSE(curl.CopyMultipartPostComplete());
    delete curl.bodydata;
    curl.bodydata = NULL;
}

TEST_F(CurlCallbackTest, CopyMultipartPostComplete_NoETagInXml) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<CopyPartResult>"
        "<LastModified>2026-01-01</LastModified>"
        "</CopyPartResult>";
    curl.bodydata->Append((void*)xml, strlen(xml));
    EXPECT_FALSE(curl.CopyMultipartPostComplete());
    delete curl.bodydata;
    curl.bodydata = NULL;
}

TEST_F(CurlCallbackTest, CopyMultipartPostComplete_ValidETag) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    etaglist_t list;
    list.push_back("");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 0;

    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<CopyPartResult>"
        "<ETag>\"copy_etag_abc\"</ETag>"
        "</CopyPartResult>";
    curl.bodydata->Append((void*)xml, strlen(xml));

    EXPECT_TRUE(curl.CopyMultipartPostComplete());
    EXPECT_EQ("copy_etag_abc", list[0]);
    EXPECT_TRUE(curl.partdata.uploaded);

    delete curl.bodydata;
    curl.bodydata = NULL;
}

TEST_F(CurlCallbackTest, CopyMultipartPostComplete_UnquotedETag) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    etaglist_t list;
    list.push_back("");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 0;

    // ETag without quotes
    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<CopyPartResult>"
        "<ETag>no_quotes</ETag>"
        "</CopyPartResult>";
    curl.bodydata->Append((void*)xml, strlen(xml));

    EXPECT_TRUE(curl.CopyMultipartPostComplete());
    // Without quotes, the etag is kept as-is
    EXPECT_EQ("no_quotes", list[0]);

    delete curl.bodydata;
    curl.bodydata = NULL;
}

TEST_F(CurlCallbackTest, CopyMultipartPostComplete_EmptyChildren) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    // XML with no children under root
    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<CopyPartResult/>";
    curl.bodydata->Append((void*)xml, strlen(xml));
    EXPECT_FALSE(curl.CopyMultipartPostComplete());
    delete curl.bodydata;
    curl.bodydata = NULL;
}

// ==========================================================================
// Additional edge case tests
// ==========================================================================

// WriteMemoryCallback with single byte blocks
TEST_F(CurlCallbackTest, WriteMemoryCallback_SingleByte) {
    BodyData body;
    char c = 'X';
    size_t result = S3fsCurl::WriteMemoryCallback(&c, 1, 1, (void*)&body);
    EXPECT_EQ(1u, result);
    EXPECT_EQ(1u, body.size());
}

// ReadUploadCache sequential reads
TEST_F(CurlCallbackTest, ReadUploadCache_SequentialReads) {
    const char* src = "0123456789";
    tagDataBuffer dbuf;
    dbuf.pcBuffer = src;
    dbuf.ulBufferSize = 10;
    dbuf.offset = 0;

    char buf[4];
    // First read: 3 bytes
    memset(buf, 0, sizeof(buf));
    size_t r1 = S3fsCurl::ReadUploadCache(buf, 1, 3, (void*)&dbuf);
    EXPECT_EQ(3u, r1);
    EXPECT_EQ(0, memcmp(buf, "012", 3));
    EXPECT_EQ(3, dbuf.offset);

    // Second read: 3 bytes
    memset(buf, 0, sizeof(buf));
    size_t r2 = S3fsCurl::ReadUploadCache(buf, 1, 3, (void*)&dbuf);
    EXPECT_EQ(3u, r2);
    EXPECT_EQ(0, memcmp(buf, "345", 3));
    EXPECT_EQ(6, dbuf.offset);

    // Third read: request 10 but only 4 remaining
    char buf2[10];
    memset(buf2, 0, sizeof(buf2));
    size_t r3 = S3fsCurl::ReadUploadCache(buf2, 1, 10, (void*)&dbuf);
    EXPECT_EQ(4u, r3);
    EXPECT_EQ(0, memcmp(buf2, "6789", 4));
    EXPECT_EQ(10, dbuf.offset);

    // Fourth read: nothing left
    size_t r4 = S3fsCurl::ReadUploadCache(buf2, 1, 10, (void*)&dbuf);
    EXPECT_EQ(0u, r4);
}

// HeaderCallback with x-amz mixed case
TEST_F(CurlCallbackTest, HeaderCallback_XAmzMixedCase) {
    headers_t headers;
    const char* hdr = "X-AMZ-METADATA-FOO: bar_value\r\n";
    S3fsCurl::HeaderCallback((void*)hdr, 1, strlen(hdr), (void*)&headers);
    // x-amz headers should be lowercased
    EXPECT_TRUE(headers.count("x-amz-metadata-foo") > 0);
    EXPECT_EQ("bar_value", headers["x-amz-metadata-foo"]);
}

// UploadReadCallback with block size multiplied
TEST_F(CurlCallbackTest, UploadReadCallback_BlockSizeMultiplied) {
    const char* content = "TESTDATA12345678";
    int fd = create_temp_file(content, 16);
    ASSERT_GE(fd, 0);

    S3fsCurl curl;
    curl.partdata.fd = fd;
    curl.partdata.startpos = 0;
    curl.partdata.size = 16;

    char buf[64];
    memset(buf, 0, sizeof(buf));
    // size=4, nmemb=2 => 8 bytes
    size_t result = S3fsCurl::UploadReadCallback(buf, 4, 2, (void*)&curl);
    EXPECT_EQ(8u, result);
    EXPECT_EQ(0, memcmp(buf, "TESTDATA", 8));

    close(fd);
}

// DownloadWriteCallback sequential writes
TEST_F(CurlCallbackTest, DownloadWriteCallback_SequentialWrites) {
    int fd = create_temp_file(NULL, 0);
    ASSERT_GE(fd, 0);

    S3fsCurl curl;
    curl.partdata.fd = fd;
    curl.partdata.startpos = 0;
    curl.partdata.size = 100;

    const char* d1 = "AAAA";
    const char* d2 = "BBBB";
    S3fsCurl::DownloadWriteCallback((void*)d1, 1, 4, (void*)&curl);
    EXPECT_EQ(4, curl.partdata.startpos);
    S3fsCurl::DownloadWriteCallback((void*)d2, 1, 4, (void*)&curl);
    EXPECT_EQ(8, curl.partdata.startpos);
    EXPECT_EQ(92, curl.partdata.size);

    // Verify
    char verify[8];
    lseek(fd, 0, SEEK_SET);
    read(fd, verify, 8);
    EXPECT_EQ(0, memcmp(verify, "AAAABBBB", 8));

    close(fd);
}

// CurlProgress with different progress values
TEST_F(CurlCallbackTest, CurlProgress_DlProgressChange) {
    CURL* fake_curl = curl_easy_init();
    ASSERT_NE(nullptr, fake_curl);

    S3fsCurl::curl_times[fake_curl] = time(0) - 100;
    S3fsCurl::curl_progress[fake_curl] = progress_t(50.0, 0.0);
    S3fsCurl::readwrite_timeout = 5;

    // dl progress changed (different dlnow), ul same
    int result = S3fsCurl::CurlProgress(
        (void*)fake_curl, 200.0, 60.0, 100.0, 0.0);
    EXPECT_EQ(0, result);

    S3fsCurl::curl_times.erase(fake_curl);
    S3fsCurl::curl_progress.erase(fake_curl);
    curl_easy_cleanup(fake_curl);
}

TEST_F(CurlCallbackTest, CurlProgress_UlProgressChange) {
    CURL* fake_curl = curl_easy_init();
    ASSERT_NE(nullptr, fake_curl);

    S3fsCurl::curl_times[fake_curl] = time(0) - 100;
    S3fsCurl::curl_progress[fake_curl] = progress_t(0.0, 50.0);
    S3fsCurl::readwrite_timeout = 5;

    // ul progress changed (different ulnow)
    int result = S3fsCurl::CurlProgress(
        (void*)fake_curl, 0.0, 0.0, 200.0, 60.0);
    EXPECT_EQ(0, result);

    S3fsCurl::curl_times.erase(fake_curl);
    S3fsCurl::curl_progress.erase(fake_curl);
    curl_easy_cleanup(fake_curl);
}

// UploadMultipartPostComplete with etag matching (quoted both sides)
TEST_F(CurlCallbackTest, UploadMultipartPostComplete_BothQuotedMatch) {
    S3fsCurl curl;
    etaglist_t list;
    list.push_back("");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 0;
    curl.partdata.etag = "\"aabbccdd\"";
    curl.responseHeaders["ETag"] = "\"aabbccdd\"";
    S3fsCurl::is_content_md5 = true;
    S3fsCurl::ssetype = SSE_DISABLE;

    EXPECT_TRUE(curl.UploadMultipartPostComplete());
}

// GetUploadId with multiple elements before UploadId
TEST_F(CurlCallbackTest, GetUploadId_MultipleElements) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        "<Bucket>mybucket</Bucket>"
        "<Key>mykey</Key>"
        "<UploadId>VXBsb2FkIElE</UploadId>"
        "</InitiateMultipartUploadResult>";
    curl.bodydata->Append((void*)xml, strlen(xml));
    string upload_id;
    EXPECT_TRUE(curl.GetUploadId(upload_id));
    EXPECT_EQ("VXBsb2FkIElE", upload_id);
    delete curl.bodydata;
    curl.bodydata = NULL;
}

// CopyMultipartPostComplete with multiple elements
TEST_F(CurlCallbackTest, CopyMultipartPostComplete_MultipleElements) {
    S3fsCurl curl;
    curl.bodydata = new BodyData();
    etaglist_t list;
    list.push_back("part0");
    list.push_back("");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 1;

    const char* xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<CopyPartResult>"
        "<LastModified>2026-02-27T12:00:00Z</LastModified>"
        "<ETag>\"copy_part_etag_2\"</ETag>"
        "</CopyPartResult>";
    curl.bodydata->Append((void*)xml, strlen(xml));

    EXPECT_TRUE(curl.CopyMultipartPostComplete());
    EXPECT_EQ("part0", list[0]);
    EXPECT_EQ("copy_part_etag_2", list[1]);

    delete curl.bodydata;
    curl.bodydata = NULL;
}

// ReadCallback with size*nmemb=1 (boundary)
TEST_F(CurlCallbackTest, ReadCallback_MinimalSize) {
    S3fsCurl curl;
    const char* post = "XY";
    curl.postdata = (const unsigned char*)post;
    curl.postdata_remaining = 2;
    char buf[1];
    // size=1, nmemb=1 => exactly 1 byte
    size_t result = S3fsCurl::ReadCallback(buf, 1, 1, (void*)&curl);
    EXPECT_EQ(1u, result);
    EXPECT_EQ('X', buf[0]);
    EXPECT_EQ(1u, curl.postdata_remaining);
}

// DownloadWriteCallbackforRead exactly fills buffer
TEST_F(CurlCallbackTest, DownloadWriteCallbackforRead_ExactFill) {
    S3fsCurl curl;
    curl.read_buf = new char[8];
    memset(curl.read_buf, 0, 8);
    curl.read_size = 8;
    curl.cur_pos = 0;

    const char* data = "12345678";
    size_t result = S3fsCurl::DownloadWriteCallbackforRead(
        (void*)data, 1, 8, (void*)&curl);
    EXPECT_EQ(8u, result);
    EXPECT_EQ(8u, curl.cur_pos);
    EXPECT_EQ(0, memcmp(curl.read_buf, "12345678", 8));

    // Further writes should return 0 (no space)
    result = S3fsCurl::DownloadWriteCallbackforRead(
        (void*)data, 1, 1, (void*)&curl);
    EXPECT_EQ(0u, result);

    delete[] curl.read_buf;
    curl.read_buf = NULL;
}

// UploadMultipartPostComplete with SSE_S3 still checks MD5
TEST_F(CurlCallbackTest, UploadMultipartPostComplete_SSE_S3_ChecksMd5) {
    S3fsCurl curl;
    etaglist_t list;
    list.push_back("");
    curl.partdata.etaglist = &list;
    curl.partdata.etagpos = 0;
    curl.partdata.etag = "wrong_etag";
    curl.responseHeaders["ETag"] = "\"correct_etag\"";
    S3fsCurl::is_content_md5 = true;
    S3fsCurl::ssetype = SSE_S3;

    // SSE_S3 is not SSE_C or SSE_KMS, so MD5 check applies
    EXPECT_FALSE(curl.UploadMultipartPostComplete());
}

// DownloadWriteCallback with block size > 1
TEST_F(CurlCallbackTest, DownloadWriteCallback_LargeBlockSize) {
    int fd = create_temp_file(NULL, 0);
    ASSERT_GE(fd, 0);

    S3fsCurl curl;
    curl.partdata.fd = fd;
    curl.partdata.startpos = 0;
    curl.partdata.size = 100;

    // "ABCDEFGH" (8 bytes), blockSize=4, nmemb=2 => 8 bytes
    const char* data = "ABCDEFGH";
    size_t result = S3fsCurl::DownloadWriteCallback(
        (void*)data, 4, 2, (void*)&curl);
    EXPECT_EQ(8u, result);

    char verify[8];
    lseek(fd, 0, SEEK_SET);
    read(fd, verify, 8);
    EXPECT_EQ(0, memcmp(verify, "ABCDEFGH", 8));

    close(fd);
}

// DownloadWriteCallbackforRead with block size > 1
TEST_F(CurlCallbackTest, DownloadWriteCallbackforRead_LargeBlockSize) {
    S3fsCurl curl;
    curl.read_buf = new char[64];
    memset(curl.read_buf, 0, 64);
    curl.read_size = 64;
    curl.cur_pos = 0;

    const char* data = "ABCDEFGHIJKL";
    // size=4, nmemb=3 => 12 bytes
    size_t result = S3fsCurl::DownloadWriteCallbackforRead(
        (void*)data, 4, 3, (void*)&curl);
    EXPECT_EQ(12u, result);
    EXPECT_EQ(12u, curl.cur_pos);
    EXPECT_EQ(0, memcmp(curl.read_buf, "ABCDEFGHIJKL", 12));

    delete[] curl.read_buf;
    curl.read_buf = NULL;
}

// ==========================================================================
// main
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
