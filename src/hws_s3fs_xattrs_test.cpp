// ==========================================================================
// hws_s3fs_xattrs_test.cpp
//
// Tests xattrs functions in hws_s3fs.cpp using #include approach for real
// gcov coverage. Tests parse_xattr_keyval, parse_xattrs, build_xattrs,
// free_xattrs (all static functions in hws_s3fs.cpp).
//
// Approach:
//   1. #define main hws_s3fs_main_disabled to avoid main() conflict
//   2. #include "hws_s3fs.cpp" to access static functions
//   3. Provide crypto/auth stubs and fuse_get_context stub
//   4. Link against real .o files for all dependencies
// ==========================================================================

// ---- Rename main() to avoid conflict ----
#define main hws_s3fs_main_disabled

// ---- Include the source under test ----
#include "hws_s3fs.cpp"

// ---- Restore main ----
#undef main

// ==========================================================================
// Stubs for crypto/auth functions (normally from openssl_auth.cpp)
// ==========================================================================

std::string s3fs_get_content_md5(int fd) {
    (void)fd;
    return "";
}

std::string s3fs_get_content_md5_hws_obs(const char* path, off_t offset, size_t size) {
    (void)path; (void)offset; (void)size;
    return "";
}

unsigned char* s3fs_md5hexsum(int fd, off_t start, ssize_t size) {
    (void)fd; (void)start; (void)size;
    unsigned char* result = (unsigned char*)malloc(16);
    if (result) memset(result, 0, 16);
    return result;
}

size_t get_md5_digest_length() { return 16; }

bool s3fs_init_crypt_mutex() { return true; }
bool s3fs_destroy_crypt_mutex() { return true; }

const char* s3fs_crypt_lib_name() { return "openssl"; }

bool s3fs_HMAC256(const void* key, size_t key_len,
                  const unsigned char* data, size_t data_len,
                  unsigned char** digest, unsigned int* digest_len) {
    (void)key; (void)key_len; (void)data; (void)data_len;
    *digest_len = 32;
    *digest = (unsigned char*)malloc(32);
    if (*digest) memset(*digest, 0, 32);
    return *digest != nullptr;
}

bool s3fs_HMAC(const void* key, size_t key_len,
               const unsigned char* data, size_t data_len,
               unsigned char** digest, unsigned int* digest_len) {
    (void)key; (void)key_len; (void)data; (void)data_len;
    *digest_len = 32;
    *digest = (unsigned char*)malloc(32);
    if (*digest) memset(*digest, 0, 32);
    return *digest != nullptr;
}

std::string s3fs_sha256sum_hw_obs(const char* path, off_t start, size_t size) {
    (void)path; (void)start; (void)size;
    return "";
}

std::string s3fs_sha256sum(int fd, off_t start, ssize_t size) {
    (void)fd; (void)start; (void)size;
    return "";
}

bool s3fs_sha256(const unsigned char* data, unsigned int data_len,
                 unsigned char** digest, unsigned int* digest_len) {
    (void)data; (void)data_len;
    *digest_len = 32;
    *digest = (unsigned char*)malloc(32);
    if (*digest) memset(*digest, 0, 32);
    return *digest != nullptr;
}

bool s3fs_init_global_ssl(void) { return true; }
bool s3fs_destroy_global_ssl(void) { return true; }

// ==========================================================================
// Stub for fuse_get_context (normally from libfuse)
// ==========================================================================
extern "C" struct fuse_context* fuse_get_context() {
    static struct fuse_context g_test_fuse_ctx;
    g_test_fuse_ctx.uid = getuid();
    g_test_fuse_ctx.gid = getgid();
    return &g_test_fuse_ctx;
}

// ---- Google Test ----
#include "gtest.h"

// ==========================================================================
// Test Suite - tests real static functions from hws_s3fs.cpp
// ==========================================================================

class HwsS3fsXattrsTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

// ============================================================
// Tests for free_xattrs()
// ============================================================

TEST_F(HwsS3fsXattrsTest, FreeXattrs_EmptyMap) {
  xattrs_t xattrs;
  free_xattrs(xattrs);
  EXPECT_TRUE(xattrs.empty());
}

TEST_F(HwsS3fsXattrsTest, FreeXattrs_SingleEntry) {
  xattrs_t xattrs;

  PXATTRVAL pval = new XATTRVAL;
  pval->length = 5;
  pval->pvalue = (unsigned char*)malloc(6);
  strcpy((char*)pval->pvalue, "hello");

  xattrs["test.key"] = pval;

  EXPECT_EQ(xattrs.size(), 1u);
  free_xattrs(xattrs);
  EXPECT_TRUE(xattrs.empty());
}

TEST_F(HwsS3fsXattrsTest, FreeXattrs_MultipleEntries) {
  xattrs_t xattrs;

  for(int i = 0; i < 5; i++){
    PXATTRVAL pval = new XATTRVAL;
    pval->length = 4;
    pval->pvalue = (unsigned char*)malloc(5);
    strcpy((char*)pval->pvalue, "test");
    char key[32];
    snprintf(key, sizeof(key), "key%d", i);
    xattrs[key] = pval;
  }

  EXPECT_EQ(xattrs.size(), 5u);
  free_xattrs(xattrs);
  EXPECT_TRUE(xattrs.empty());
}

TEST_F(HwsS3fsXattrsTest, FreeXattrs_WithNullValue) {
  xattrs_t xattrs;

  PXATTRVAL pval = new XATTRVAL;
  pval->length = 0;
  pval->pvalue = NULL;

  xattrs["null.value"] = pval;

  EXPECT_EQ(xattrs.size(), 1u);
  free_xattrs(xattrs);
  EXPECT_TRUE(xattrs.empty());
}

// ============================================================
// Tests for parse_xattr_keyval()
// ============================================================

TEST_F(HwsS3fsXattrsTest, ParseXattrKeyval_ValidFormat) {
  string key;
  PXATTRVAL pval = NULL;

  string test_pair = "\"user.comment\":\"SGVsbG8gV29ybGQ=\"";
  bool result = parse_xattr_keyval(test_pair, key, pval);

  EXPECT_TRUE(result);
  EXPECT_EQ(key, "user.comment");
  EXPECT_NE(pval, (PXATTRVAL)NULL);

  if(pval){
    EXPECT_GT(pval->length, 0u);
    EXPECT_NE(pval->pvalue, (unsigned char*)NULL);
    delete pval;
  }
}

TEST_F(HwsS3fsXattrsTest, ParseXattrKeyval_NoColon) {
  string key;
  PXATTRVAL pval = NULL;

  string test_pair = "\"user.comment\"\"SGVsbG8gV29ybGQ=\"";
  bool result = parse_xattr_keyval(test_pair, key, pval);

  EXPECT_FALSE(result);
  EXPECT_TRUE(key.empty());
  EXPECT_EQ(pval, (PXATTRVAL)NULL);
}

TEST_F(HwsS3fsXattrsTest, ParseXattrKeyval_EmptyKey) {
  string key;
  PXATTRVAL pval = NULL;

  string test_pair = "\":\"SGVsbG8gV29ybGQ=\"";
  bool result = parse_xattr_keyval(test_pair, key, pval);

  EXPECT_FALSE(result);
  EXPECT_TRUE(key.empty());
  EXPECT_EQ(pval, (PXATTRVAL)NULL);
}

TEST_F(HwsS3fsXattrsTest, ParseXattrKeyval_EmptyValue) {
  string key;
  PXATTRVAL pval = NULL;

  string test_pair = "\"user.key\":\"\"";
  bool result = parse_xattr_keyval(test_pair, key, pval);

  EXPECT_TRUE(result);
  EXPECT_EQ(key, "user.key");
  if(pval){
    delete pval;
  }
}

// ============================================================
// Tests for parse_xattrs()
// ============================================================

TEST_F(HwsS3fsXattrsTest, ParseXattrs_EmptyString) {
  xattrs_t xattrs;
  string empty_str = "";
  size_t count = parse_xattrs(empty_str, xattrs);

  EXPECT_EQ(count, 0u);
  EXPECT_TRUE(xattrs.empty());
}

TEST_F(HwsS3fsXattrsTest, ParseXattrs_SingleEntry) {
  xattrs_t xattrs;

  string json_str = "{\"user.name\":\"SGVsbG8=\"}";
  string encoded = urlEncode(json_str);

  size_t count = parse_xattrs(encoded, xattrs);

  EXPECT_EQ(count, 1u);
  EXPECT_EQ(xattrs.size(), 1u);

  free_xattrs(xattrs);
}

TEST_F(HwsS3fsXattrsTest, ParseXattrs_MultipleEntries) {
  xattrs_t xattrs;

  string json_str = "{\"user.name\":\"dmFsdWUx\",\"user.comment\":\"dmFsdWUy\",\"user.test\":\"dmFsdWUz\"}";
  string encoded = urlEncode(json_str);

  size_t count = parse_xattrs(encoded, xattrs);

  EXPECT_EQ(count, 3u);
  EXPECT_EQ(xattrs.size(), 3u);

  free_xattrs(xattrs);
}

TEST_F(HwsS3fsXattrsTest, ParseXattrs_InvalidFormat) {
  xattrs_t xattrs;

  string invalid_str = "user.name:value";
  string encoded = urlEncode(invalid_str);

  size_t count = parse_xattrs(encoded, xattrs);

  EXPECT_EQ(count, 0u);
  EXPECT_TRUE(xattrs.empty());
}

// ============================================================
// Tests for build_xattrs()
// ============================================================

TEST_F(HwsS3fsXattrsTest, BuildXattrs_EmptyMap) {
  xattrs_t xattrs;
  string result = build_xattrs(xattrs);

  EXPECT_FALSE(result.empty());
  EXPECT_NE(result.find("%7B"), string::npos);
  EXPECT_NE(result.find("%7D"), string::npos);
}

TEST_F(HwsS3fsXattrsTest, BuildXattrs_SingleEntry) {
  xattrs_t xattrs;

  PXATTRVAL pval = new XATTRVAL;
  pval->length = 5;
  pval->pvalue = (unsigned char*)malloc(6);
  strcpy((char*)pval->pvalue, "hello");

  xattrs["user.name"] = pval;

  string result = build_xattrs(xattrs);

  EXPECT_FALSE(result.empty());
  EXPECT_NE(result.find("%7B"), string::npos);
  EXPECT_NE(result.find("%7D"), string::npos);
  EXPECT_NE(result.find("%22"), string::npos);

  free_xattrs(xattrs);
}

TEST_F(HwsS3fsXattrsTest, BuildXattrs_WithNullValue) {
  xattrs_t xattrs;

  PXATTRVAL pval = new XATTRVAL;
  pval->length = 0;
  pval->pvalue = NULL;

  xattrs["empty.attr"] = pval;

  string result = build_xattrs(xattrs);

  EXPECT_FALSE(result.empty());

  free_xattrs(xattrs);
}

TEST_F(HwsS3fsXattrsTest, BuildXattrs_MultipleEntries) {
  xattrs_t xattrs;

  const char* keys[] = {"user.one", "user.two", "user.three"};
  const char* values[] = {"val1", "val2", "val3"};

  for(int i = 0; i < 3; i++){
    PXATTRVAL pval = new XATTRVAL;
    pval->length = strlen(values[i]);
    pval->pvalue = (unsigned char*)malloc(pval->length + 1);
    strcpy((char*)pval->pvalue, values[i]);
    xattrs[keys[i]] = pval;
  }

  string result = build_xattrs(xattrs);

  EXPECT_FALSE(result.empty());
  EXPECT_NE(result.find("%7B"), string::npos);
  EXPECT_NE(result.find("%7D"), string::npos);
  EXPECT_NE(result.find("%2C"), string::npos);

  free_xattrs(xattrs);
}

// ============================================================
// Round-trip tests (parse -> build -> parse)
// ============================================================

TEST_F(HwsS3fsXattrsTest, RoundTrip_SingleEntry) {
  xattrs_t original;
  PXATTRVAL pval = new XATTRVAL;
  pval->length = 5;
  pval->pvalue = (unsigned char*)malloc(6);
  strcpy((char*)pval->pvalue, "hello");
  original["user.test"] = pval;

  string built = build_xattrs(original);

  xattrs_t parsed;
  size_t count = parse_xattrs(built, parsed);

  EXPECT_EQ(count, 1u);
  EXPECT_EQ(parsed.size(), 1u);

  if(parsed.size() == 1 && parsed["user.test"]){
    EXPECT_EQ(parsed["user.test"]->length, 5u);
    EXPECT_EQ(memcmp(parsed["user.test"]->pvalue, "hello", 5), 0);
  }

  free_xattrs(original);
  free_xattrs(parsed);
}

TEST_F(HwsS3fsXattrsTest, RoundTrip_MultipleEntries) {
  xattrs_t original;
  const char* keys[] = {"key1", "key2", "key3"};
  const char* values[] = {"val1", "val2", "val3"};

  for(int i = 0; i < 3; i++){
    PXATTRVAL pval = new XATTRVAL;
    pval->length = strlen(values[i]);
    pval->pvalue = (unsigned char*)malloc(pval->length + 1);
    strcpy((char*)pval->pvalue, values[i]);
    original[keys[i]] = pval;
  }

  string built = build_xattrs(original);

  xattrs_t parsed;
  size_t count = parse_xattrs(built, parsed);

  EXPECT_EQ(count, 3u);

  for(int i = 0; i < 3; i++){
    EXPECT_NE(parsed.find(keys[i]), parsed.end());
    if(parsed[keys[i]]){
      EXPECT_EQ(parsed[keys[i]]->length, strlen(values[i]));
      EXPECT_EQ(memcmp(parsed[keys[i]]->pvalue, values[i], strlen(values[i])), 0);
    }
  }

  free_xattrs(original);
  free_xattrs(parsed);
}

TEST_F(HwsS3fsXattrsTest, RoundTrip_BinaryData) {
  xattrs_t original;
  PXATTRVAL pval = new XATTRVAL;
  pval->length = 4;
  pval->pvalue = (unsigned char*)malloc(4);
  pval->pvalue[0] = 0x00;
  pval->pvalue[1] = 0x01;
  pval->pvalue[2] = 0x02;
  pval->pvalue[3] = 0x03;
  original["user.binary"] = pval;

  string built = build_xattrs(original);

  xattrs_t parsed;
  size_t count = parse_xattrs(built, parsed);

  EXPECT_EQ(count, 1u);
  EXPECT_EQ(parsed.size(), 1u);

  if(parsed.size() == 1 && parsed["user.binary"]){
    EXPECT_EQ(parsed["user.binary"]->length, 4u);
    EXPECT_EQ(parsed["user.binary"]->pvalue[0], 0x00);
    EXPECT_EQ(parsed["user.binary"]->pvalue[1], 0x01);
    EXPECT_EQ(parsed["user.binary"]->pvalue[2], 0x02);
    EXPECT_EQ(parsed["user.binary"]->pvalue[3], 0x03);
  }

  free_xattrs(original);
  free_xattrs(parsed);
}

// ============================================================
// Edge cases
// ============================================================

TEST_F(HwsS3fsXattrsTest, ParseXattrs_WithSpecialCharactersInKey) {
  xattrs_t xattrs;

  string json_str = "{\"user.special.key\":\"dmFsdWU=\"}";
  string encoded = urlEncode(json_str);

  size_t count = parse_xattrs(encoded, xattrs);

  EXPECT_EQ(count, 1u);
  EXPECT_EQ(xattrs.size(), 1u);
  EXPECT_NE(xattrs.find("user.special.key"), xattrs.end());

  free_xattrs(xattrs);
}

TEST_F(HwsS3fsXattrsTest, ParseXattrs_LargeValue) {
  xattrs_t xattrs;

  string large_base64(1024, 'A');
  string json_str = "{\"user.large\":\"" + large_base64 + "\"}";
  string encoded = urlEncode(json_str);

  size_t count = parse_xattrs(encoded, xattrs);

  EXPECT_EQ(count, 1u);
  EXPECT_EQ(xattrs.size(), 1u);

  free_xattrs(xattrs);
}

TEST_F(HwsS3fsXattrsTest, FreeXattrs_DuringIteration) {
  xattrs_t xattrs;

  for(int i = 0; i < 100; i++){
    PXATTRVAL pval = new XATTRVAL;
    pval->length = 4;
    pval->pvalue = (unsigned char*)malloc(5);
    strcpy((char*)pval->pvalue, "test");
    char key[32];
    snprintf(key, sizeof(key), "key%d", i);
    xattrs[key] = pval;
  }

  EXPECT_EQ(xattrs.size(), 100u);

  free_xattrs(xattrs);

  EXPECT_TRUE(xattrs.empty());
}

// ============================================================
// Main function
// ============================================================

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
