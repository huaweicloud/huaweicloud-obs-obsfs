/*
 * curl_util_test.cpp
 *
 * Unit tests for utility functions in curl.cpp that are not yet tested
 * Focus on small utility functions that don't require complex mocking
 *
 * Tests:
 * - etag_equals() - ETag comparison ignoring quotes
 * - adjust_block() - Block size alignment calculation
 * - url_to_host() - Host extraction from URL
 *
 * Copyright(C) 2025
 * This program is free software; you can redistribute it and/or
 * modify it under terms of GNU General Public License
 * as published by Free Software Foundation; either version 2
 * of License, or (at your option) any later version.
 */

// ==========================================================================
// Stubs for external dependencies
// ==========================================================================

#include <string>
#include <cstring>

// Minimal stubs needed for curl.cpp linkage
#define main curl_main_disabled
#include "curl.cpp"
#undef main

// ==========================================================================
// Google Test framework
// ==========================================================================
#include "gtest.h"

// ==========================================================================
// Test Fixture
// ==========================================================================
class CurlUtilTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Reset globals if needed
    }
    void TearDown() override {
    }
};

// ==========================================================================
// Tests for etag_equals()
// ==========================================================================

TEST_F(CurlUtilTest, EtagEquals_IdenticalPlain) {
    std::string etag1 = "abc123";
    std::string etag2 = "abc123";
    EXPECT_TRUE(etag_equals(etag1, etag2));
}

TEST_F(CurlUtilTest, EtagEquals_IdenticalQuoted) {
    std::string etag1 = "\"abc123\"";
    std::string etag2 = "\"abc123\"";
    EXPECT_TRUE(etag_equals(etag1, etag2));
}

TEST_F(CurlUtilTest, EtagEquals_OneQuotedOnePlain) {
    std::string etag1 = "\"abc123\"";
    std::string etag2 = "abc123";
    EXPECT_TRUE(etag_equals(etag1, etag2));
}

TEST_F(CurlUtilTest, EtagEquals_DifferentValues) {
    std::string etag1 = "\"abc123\"";
    std::string etag2 = "\"def456\"";
    EXPECT_FALSE(etag_equals(etag1, etag2));
}

TEST_F(CurlUtilTest, EtagEquals_EmptyStrings) {
    std::string etag1 = "";
    std::string etag2 = "";
    EXPECT_TRUE(etag_equals(etag1, etag2));
}

TEST_F(CurlUtilTest, EtagEquals_QuotesOnly) {
    std::string etag1 = "\"\"";
    std::string etag2 = "\"\"";
    // After stripping quotes from both, both become empty
    EXPECT_TRUE(etag_equals(etag1, etag2));
}

TEST_F(CurlUtilTest, EtagEquals_SingleQuoteOne) {
    std::string etag1 = "\"abc123";
    std::string etag2 = "abc123";
    // Only etag1 has leading quote, not trailing
    EXPECT_FALSE(etag_equals(etag1, etag2));
}

// ==========================================================================
// Tests for adjust_block()
// ==========================================================================

TEST_F(CurlUtilTest, AdjustBlock_MultipleOfBlockSize) {
    // 100 bytes with block size 50 -> 2 blocks = 100 bytes
    EXPECT_EQ(100, adjust_block(100, 50));
}

TEST_F(CurlUtilTest, AdjustBlock_PartialBlock) {
    // 75 bytes with block size 50 -> 2 blocks = 100 bytes
    EXPECT_EQ(100, adjust_block(75, 50));
}

TEST_F(CurlUtilTest, AdjustBlock_ZeroBytes) {
    EXPECT_EQ(0, adjust_block(0, 50));
}

TEST_F(CurlUtilTest, AdjustBlock_OneByte) {
    EXPECT_EQ(50, adjust_block(1, 50));
}

TEST_F(CurlUtilTest, AdjustBlock_BlockSize1) {
    EXPECT_EQ(100, adjust_block(100, 1));
    EXPECT_EQ(50, adjust_block(50, 1));
}

TEST_F(CurlUtilTest, AdjustBlock_LargeValues) {
    // 1025 bytes with block size 1024 -> 2 blocks = 2048 bytes
    EXPECT_EQ(2048, adjust_block(1025, 1024));
}

// ==========================================================================
// Tests for url_to_host()
// ==========================================================================

TEST_F(CurlUtilTest, UrlToHost_HttpUrl) {
    std::string url = "http://example.com:8080/path";
    std::string host = url_to_host(url);
    EXPECT_EQ("example.com:8080", host);
}

TEST_F(CurlUtilTest, UrlToHost_HttpsUrl) {
    std::string url = "https://s3.amazonaws.com/bucket/key";
    std::string host = url_to_host(url);
    EXPECT_EQ("s3.amazonaws.com", host);
}

TEST_F(CurlUtilTest, UrlToHost_WithPath) {
    std::string url = "https://obs.example.com/obs/path";
    std::string host = url_to_host(url);
    EXPECT_EQ("obs.example.com", host);
}

TEST_F(CurlUtilTest, UrlToHost_NoSlash) {
    std::string url = "http://example.com";
    std::string host = url_to_host(url);
    EXPECT_EQ("example.com", host);
}

TEST_F(CurlUtilTest, UrlToHost_WithPort) {
    std::string url = "http://example.com:9000";
    std::string host = url_to_host(url);
    EXPECT_EQ("example.com:9000", host);
}

TEST_F(CurlUtilTest, UrlToHost_HttpsNoPath) {
    std::string url = "https://secure.example.com";
    std::string host = url_to_host(url);
    EXPECT_EQ("secure.example.com", host);
}

// ==========================================================================
// Tests for get_bucket_host()
// ==========================================================================

TEST_F(CurlUtilTest, GetBucketHost_PathRequestStyleFalse) {
    // Save original values
    bool orig_pathrequeststyle = pathrequeststyle;
    std::string orig_bucket = bucket;
    std::string orig_host = host;

    // Test with pathrequeststyle = false (virtual-hosted style)
    pathrequeststyle = false;
    bucket = "mybucket";
    host = "https://s3.amazonaws.com";

    std::string result = get_bucket_host();
    EXPECT_EQ("mybucket.s3.amazonaws.com", result);

    // Restore original values
    pathrequeststyle = orig_pathrequeststyle;
    bucket = orig_bucket;
    host = orig_host;
}

TEST_F(CurlUtilTest, GetBucketHost_PathRequestStyleTrue) {
    // Save original values
    bool orig_pathrequeststyle = pathrequeststyle;
    std::string orig_bucket = bucket;
    std::string orig_host = host;

    // Test with pathrequeststyle = true (path style)
    pathrequeststyle = true;
    bucket = "mybucket";
    host = "https://s3.amazonaws.com";

    std::string result = get_bucket_host();
    EXPECT_EQ("s3.amazonaws.com", result);

    // Restore original values
    pathrequeststyle = orig_pathrequeststyle;
    bucket = orig_bucket;
    host = orig_host;
}

TEST_F(CurlUtilTest, GetBucketHost_WithPort) {
    // Save original values
    bool orig_pathrequeststyle = pathrequeststyle;
    std::string orig_bucket = bucket;
    std::string orig_host = host;

    // Test with port in URL
    pathrequeststyle = false;
    bucket = "testbucket";
    host = "http://localhost:9000";

    std::string result = get_bucket_host();
    EXPECT_EQ("testbucket.localhost:9000", result);

    // Restore original values
    pathrequeststyle = orig_pathrequeststyle;
    bucket = orig_bucket;
    host = orig_host;
}

// ==========================================================================
// Main function
// ==========================================================================
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
