/*
 * s3fs_operations include-based unit test
 *
 * Strategy: #include "s3fs_operations.cpp" directly so that all static
 * functions become visible and gcov records real coverage.
 *
 * External class dependencies (S3fsCurl, FdManager, StatCache, etc.)
 * are satisfied by linking real .o files + test_stubs.o (which
 * provides globals from hws_s3fs.cpp).
 *
 * Build: link with string_util.o s3fs_util.o cache.o fdcache.o curl.o
 *        addhead.o hws_*.o test_stubs.o + gtest-all.cc
 *        Do NOT link s3fs_operations.o (it is #included here).
 */

// Provide stubs for symbols that s3fs_operations.cpp needs but are
// normally defined in hws_s3fs.cpp (and NOT in test_stubs.cpp).
// We must define them BEFORE including s3fs_operations.cpp.

// fuse_get_context stub — s3fs_operations.cpp calls this for uid/gid checks
#include <fuse.h>
#include <sys/types.h>
#include <unistd.h>

static struct fuse_context g_test_fuse_ctx;
extern "C" struct fuse_context* fuse_get_context() {
    g_test_fuse_ctx.uid = getuid();
    g_test_fuse_ctx.gid = getgid();
    return &g_test_fuse_ctx;
}

// get_realpath stub — s3fs_operations.cpp calls get_realpath(path) on some paths
// Use weak attribute so s3fs_util.o's real implementation wins if linked.
#include <string>
__attribute__((weak)) std::string get_realpath(const char* path) {
    return path ? std::string(path) : std::string("/");
}

// Now include the source under test.
// This makes ALL static functions visible in this compilation unit.
// gcov will record coverage on real s3fs_operations.cpp lines.
#include "s3fs_operations.cpp"

// Google Test framework
#include "gtest.h"

// =====================================================================
// Stub reset helper
// =====================================================================
extern bucket_type_t g_bucket_type;
extern std::string bucket;

class S3fsOpsIncludeTest : public ::testing::Test {
protected:
    void SetUp() override {
        g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
        bucket = "test-bucket";
        StatCache::getStatCacheData()->DelStatWildcard("/");
    }
    void TearDown() override {
        g_bucket_type = BUCKET_TYPE_UNKNOWN;
    }
};

// ==========================================================================
// Tests for STATIC helper functions — these are key coverage wins
// ==========================================================================

// [ISSUE2026040900001 C1/C2] Removed map_obs_error_to_errno and
// is_retryable_error tests (the underlying helpers were unused production
// code and have been deleted from s3fs_operations.cpp).

// --- str_to_off_t ---
TEST_F(S3fsOpsIncludeTest, StrToOffT_Normal) {
    EXPECT_EQ(123, str_to_off_t("123"));
    EXPECT_EQ(0, str_to_off_t("0"));
    EXPECT_EQ(-1, str_to_off_t("-1"));
}
TEST_F(S3fsOpsIncludeTest, StrToOffT_Null) {
    EXPECT_EQ(0, str_to_off_t(nullptr));
}
TEST_F(S3fsOpsIncludeTest, StrToOffT_Empty) {
    EXPECT_EQ(0, str_to_off_t(""));
}
TEST_F(S3fsOpsIncludeTest, StrToOffT_Large) {
    EXPECT_EQ(536870912LL, str_to_off_t("536870912"));
}

// --- build_standard_metadata ---
TEST_F(S3fsOpsIncludeTest, BuildStandardMetadata_Default) {
    headers_t meta;
    build_standard_metadata(meta, 0644, "text/plain", false);
    EXPECT_FALSE(meta.empty());
    EXPECT_NE(meta.end(), meta.find("Content-Type"));
    EXPECT_EQ("text/plain", meta["Content-Type"]);
}
TEST_F(S3fsOpsIncludeTest, BuildStandardMetadata_PreserveMode) {
    headers_t meta;
    build_standard_metadata(meta, 0755, "application/octet-stream", true);
    EXPECT_NE(meta.end(), meta.find(hws_obs_meta_mode));
}

// --- s3fs_str_realtime ---
TEST_F(S3fsOpsIncludeTest, StrRealtime_NotEmpty) {
    std::string rt = s3fs_str_realtime();
    EXPECT_FALSE(rt.empty());
}

// --- pending_dir_entries (add / get / remove) ---
// [ISSUE2026051100001 cleanup] PendingDirEntries_* tests removed — the
// underlying helpers and static map have been deleted.

// --- GetXmlNsUrl ---
TEST_F(S3fsOpsIncludeTest, GetXmlNsUrl_NullDoc) {
    std::string nsurl;
    EXPECT_FALSE(GetXmlNsUrl(nullptr, nsurl));
}
TEST_F(S3fsOpsIncludeTest, GetXmlNsUrl_ValidS3Response) {
    const char* xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        "<Name>test</Name></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    std::string nsurl;
    EXPECT_TRUE(GetXmlNsUrl(doc, nsurl));
    EXPECT_EQ("http://s3.amazonaws.com/doc/2006-03-01/", nsurl);
    xmlFreeDoc(doc);
}

// --- is_truncated_s3ops ---
TEST_F(S3fsOpsIncludeTest, IsTruncated_True) {
    const char* xml = "<?xml version=\"1.0\"?>"
        "<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    EXPECT_TRUE(is_truncated_s3ops(doc));
    xmlFreeDoc(doc);
}
TEST_F(S3fsOpsIncludeTest, IsTruncated_False) {
    const char* xml = "<?xml version=\"1.0\"?>"
        "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    EXPECT_FALSE(is_truncated_s3ops(doc));
    xmlFreeDoc(doc);
}

// --- get_next_marker_s3ops ---
TEST_F(S3fsOpsIncludeTest, GetNextMarker_Present) {
    const char* xml = "<?xml version=\"1.0\"?>"
        "<ListBucketResult><NextMarker>key123</NextMarker></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    EXPECT_EQ("key123", get_next_marker_s3ops(doc));
    xmlFreeDoc(doc);
}

// --- get_base_exp_s3ops ---
TEST_F(S3fsOpsIncludeTest, GetBaseExp_NullDoc) {
    EXPECT_EQ(nullptr, get_base_exp_s3ops(nullptr, "Name"));
}
TEST_F(S3fsOpsIncludeTest, GetBaseExp_ValidElement) {
    const char* xml = "<?xml version=\"1.0\"?>"
        "<ListBucketResult><Name>my-bucket</Name></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    xmlChar* val = get_base_exp_s3ops(doc, "Name");
    ASSERT_NE(nullptr, val);
    EXPECT_STREQ("my-bucket", (const char*)val);
    xmlFree(val);
    xmlFreeDoc(doc);
}

// --- check_parent_object_access_s3 ---
TEST_F(S3fsOpsIncludeTest, CheckParentAccess_RootPath) {
    EXPECT_EQ(0, check_parent_object_access_s3("/", W_OK));
}

// [ISSUE2026040900001 C1/C2] Removed extended map_obs_error_to_errno /
// is_retryable_error coverage suites — underlying helpers deleted as dead
// code in s3fs_operations.cpp.

// [ISSUE2026051100001 cleanup] PendingDirEntries_* additional-coverage tests
// removed — helpers and static map no longer exist.

// ==========================================================================
// Tests for str_to_off_t -- additional edge cases
// ==========================================================================

TEST_F(S3fsOpsIncludeTest, StrToOffT_LeadingWhitespace) {
    EXPECT_EQ(123, str_to_off_t("  123"));
    EXPECT_EQ(123, str_to_off_t("\t123"));
}

TEST_F(S3fsOpsIncludeTest, StrToOffT_NegativeNumber) {
    EXPECT_EQ(-456, str_to_off_t("-456"));
}

TEST_F(S3fsOpsIncludeTest, StrToOffT_LargeNumber) {
    EXPECT_EQ(9223372036854775807LL, str_to_off_t("9223372036854775807"));
}

TEST_F(S3fsOpsIncludeTest, StrToOffT_InvalidString) {
    // Non-numeric should return 0
    EXPECT_EQ(0, str_to_off_t("abc"));
    EXPECT_EQ(0, str_to_off_t(""));
    EXPECT_EQ(0, str_to_off_t(nullptr));
}

// ==========================================================================
// Tests for build_standard_metadata -- additional coverage
// ==========================================================================

TEST_F(S3fsOpsIncludeTest, BuildStandardMetadata_ContentType) {
    headers_t meta;
    build_standard_metadata(meta, 0644, "application/json", false);
    EXPECT_EQ("application/json", meta["Content-Type"]);
}

TEST_F(S3fsOpsIncludeTest, BuildStandardMetadata_DirectoryMode) {
    headers_t meta;
    build_standard_metadata(meta, 0755, "application/x-directory", true);
    // Should include mode header for directories
    EXPECT_NE(meta.end(), meta.find(hws_obs_meta_mode));
}

TEST_F(S3fsOpsIncludeTest, BuildStandardMetadata_ExecutableMode) {
    headers_t meta;
    build_standard_metadata(meta, 0755, "application/x-executable", false);
    // Executable files should have mode stored
    EXPECT_NE(meta.end(), meta.find(hws_obs_meta_mode));
}

// ==========================================================================
// Tests for check_parent_object_access_s3 -- additional coverage
// ==========================================================================

TEST_F(S3fsOpsIncludeTest, CheckParentAccess_RootPathNoParent) {
    // Root path "/" has no parent, should return 0
    EXPECT_EQ(0, check_parent_object_access_s3("/", W_OK));
}

TEST_F(S3fsOpsIncludeTest, CheckParentAccess_SingleLevelPath) {
    // Single level like "/dir" has parent "/"
    int result = check_parent_object_access_s3("/testdir", W_OK);
    // Result depends on whether "/" is accessible
    (void)result;
}

// ==========================================================================
// Tests for XML parsing functions -- more coverage
// ==========================================================================

TEST_F(S3fsOpsIncludeTest, IsTruncated_InvalidDocTest) {
    EXPECT_FALSE(is_truncated_s3ops(nullptr));
}

TEST_F(S3fsOpsIncludeTest, GetNextMarker_NullDocTest) {
    EXPECT_TRUE(get_next_marker_s3ops(nullptr).empty());
}

TEST_F(S3fsOpsIncludeTest, GetXmlNsUrl_EmptyDocTest) {
    const char* xml = "<?xml version=\"1.0\"?><root></root>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    std::string nsurl;
    bool result = GetXmlNsUrl(doc, nsurl);
    // Empty doc may not have namespace
    (void)result;
    xmlFreeDoc(doc);
}

// ==========================================================================
// Tests for str_to_off_t -- additional edge cases
// ==========================================================================

TEST_F(S3fsOpsIncludeTest, StrToOffT_MaxValue) {
    EXPECT_EQ(2147483647, str_to_off_t("2147483647"));
}
TEST_F(S3fsOpsIncludeTest, StrToOffT_WithLeadingSpace) {
    EXPECT_EQ(42, str_to_off_t(" 42"));
}
TEST_F(S3fsOpsIncludeTest, StrToOffT_TrailingChars) {
    EXPECT_EQ(123, str_to_off_t("123abc"));
}

// ==========================================================================
// Tests for build_standard_metadata -- additional coverage
// ==========================================================================

TEST_F(S3fsOpsIncludeTest, BuildStandardMetadata_EmptyContentType) {
    headers_t meta;
    build_standard_metadata(meta, 0644, "", false);
    EXPECT_EQ(meta.end(), meta.find("Content-Type"));
    EXPECT_NE(meta.end(), meta.find(hws_obs_meta_uid));
    EXPECT_NE(meta.end(), meta.find(hws_obs_meta_gid));
    EXPECT_NE(meta.end(), meta.find(hws_obs_meta_mode));
    EXPECT_NE(meta.end(), meta.find(hws_obs_meta_mtime));
}

// ==========================================================================
// Tests for XML helper functions -- additional coverage
// ==========================================================================

TEST_F(S3fsOpsIncludeTest, GetBaseExp_EmptyDoc) {
    const char* xml = "<?xml version=\"1.0\"?><Root/>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    EXPECT_EQ(nullptr, get_base_exp_s3ops(doc, "Name"));
    xmlFreeDoc(doc);
}

TEST_F(S3fsOpsIncludeTest, GetBaseExp_MultipleChildren) {
    const char* xml = "<?xml version=\"1.0\"?>"
        "<ListBucketResult><Name>bucket1</Name><Prefix>pfx</Prefix></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    xmlChar* val = get_base_exp_s3ops(doc, "Prefix");
    ASSERT_NE(nullptr, val);
    EXPECT_STREQ("pfx", (const char*)val);
    xmlFree(val);
    xmlFreeDoc(doc);
}

TEST_F(S3fsOpsIncludeTest, GetNextMarker_WithNamespace) {
    const char* xml = "<?xml version=\"1.0\"?>"
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        "<NextMarker>dir/key999</NextMarker></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    EXPECT_EQ("dir/key999", get_next_marker_s3ops(doc));
    xmlFreeDoc(doc);
}

TEST_F(S3fsOpsIncludeTest, GetNextMarker_NullDoc) {
    EXPECT_TRUE(get_next_marker_s3ops(nullptr).empty());
}

TEST_F(S3fsOpsIncludeTest, IsTruncated_WithNamespace) {
    const char* xml = "<?xml version=\"1.0\"?>"
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        "<IsTruncated>true</IsTruncated></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    EXPECT_TRUE(is_truncated_s3ops(doc));
    xmlFreeDoc(doc);
}

TEST_F(S3fsOpsIncludeTest, GetNextMarker_Missing) {
    const char* xml = "<?xml version=\"1.0\"?>"
        "<ListBucketResult><NextMarker></NextMarker></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    EXPECT_TRUE(get_next_marker_s3ops(doc).empty());
    xmlFreeDoc(doc);
}

// ==========================================================================
// Tests for public API functions (also real coverage via #include)
// ==========================================================================

// --- s3fs_getattr_s3 ---
TEST_F(S3fsOpsIncludeTest, Getattr_RootPath) {
    struct stat stbuf;
    memset(&stbuf, 0, sizeof(stbuf));
    int result = s3fs_getattr_s3("/", &stbuf);
    EXPECT_EQ(0, result);
    EXPECT_TRUE(S_ISDIR(stbuf.st_mode));
    EXPECT_EQ(2u, stbuf.st_nlink);
    EXPECT_EQ(getuid(), stbuf.st_uid);
    EXPECT_EQ(getgid(), stbuf.st_gid);
    EXPECT_EQ(4096, stbuf.st_size);
    EXPECT_NE(0, stbuf.st_ino);
}

// --- s3fs_mkdir_s3 ---
TEST_F(S3fsOpsIncludeTest, Mkdir_RootPath) {
    EXPECT_EQ(0, s3fs_mkdir_s3("/", 0755));
}

// --- s3fs_rename_s3 ---
TEST_F(S3fsOpsIncludeTest, Rename_NullFrom) {
    EXPECT_EQ(-EINVAL, s3fs_rename_s3(NULL, "/to"));
}
TEST_F(S3fsOpsIncludeTest, Rename_NullTo) {
    EXPECT_EQ(-EINVAL, s3fs_rename_s3("/from", NULL));
}
TEST_F(S3fsOpsIncludeTest, Rename_RootPath) {
    EXPECT_EQ(-EINVAL, s3fs_rename_s3("/", "/to"));
}

// --- s3fs_link_s3 ---
TEST_F(S3fsOpsIncludeTest, Link_NotSupported) {
    EXPECT_EQ(-EOPNOTSUPP, s3fs_link_s3("/from", "/to"));
}

// --- s3fs_chmod_s3 ---
TEST_F(S3fsOpsIncludeTest, Chmod_RootPath) {
    EXPECT_EQ(0, s3fs_chmod_s3("/", 0755));
}

// --- s3fs_chmod_nocopy_s3 ---
TEST_F(S3fsOpsIncludeTest, ChmodNocopy_ReturnsZero) {
    EXPECT_EQ(0, s3fs_chmod_nocopy_s3("/file", 0644));
}

// --- s3fs_chown_s3 ---
TEST_F(S3fsOpsIncludeTest, Chown_RootPath) {
    EXPECT_EQ(0, s3fs_chown_s3("/", getuid(), getgid()));
}

// --- s3fs_chown_nocopy_s3 ---
TEST_F(S3fsOpsIncludeTest, ChownNocopy_ReturnsZero) {
    EXPECT_EQ(0, s3fs_chown_nocopy_s3("/file", 1000, 1000));
}

// --- s3fs_utimens_s3 ---
TEST_F(S3fsOpsIncludeTest, Utimens_RootPath) {
    struct timespec ts[2] = {{0, 0}, {0, 0}};
    EXPECT_EQ(0, s3fs_utimens_s3("/", ts));
}

// --- s3fs_utimens_nocopy_s3 ---
TEST_F(S3fsOpsIncludeTest, UtimensNocopy_ReturnsZero) {
    struct timespec ts[2] = {{0, 0}, {0, 0}};
    EXPECT_EQ(0, s3fs_utimens_nocopy_s3("/file", ts));
}

// --- s3fs_statfs_s3 ---
TEST_F(S3fsOpsIncludeTest, Statfs_Success) {
    struct statvfs stbuf;
    memset(&stbuf, 0, sizeof(stbuf));
    int result = s3fs_statfs_s3("/", &stbuf);
    EXPECT_EQ(0, result);
    EXPECT_EQ(4096u, stbuf.f_bsize);
    EXPECT_EQ(4096u, stbuf.f_frsize);
    EXPECT_EQ(0xFFFFFFFFu, stbuf.f_blocks);
    EXPECT_EQ(0xFFFFFFFFu, stbuf.f_bfree);
    EXPECT_EQ(0xFFFFFFFFu, stbuf.f_bavail);
    EXPECT_EQ((unsigned long)NAME_MAX, stbuf.f_namemax);
}

// --- s3fs_access_s3 ---
TEST_F(S3fsOpsIncludeTest, Access_RootPath_FOK) {
    EXPECT_EQ(0, s3fs_access_s3("/", F_OK));
}
TEST_F(S3fsOpsIncludeTest, Access_RootPath_ROK) {
    EXPECT_EQ(0, s3fs_access_s3("/", R_OK));
}
TEST_F(S3fsOpsIncludeTest, Access_RootPath_WOK) {
    EXPECT_EQ(0, s3fs_access_s3("/", W_OK));
}

// --- s3fs_mknod_s3 ---
TEST_F(S3fsOpsIncludeTest, Mknod_UnsupportedType_FIFO) {
    EXPECT_EQ(-ENOTSUP, s3fs_mknod_s3("/fifo", S_IFIFO | 0644, 0));
}
TEST_F(S3fsOpsIncludeTest, Mknod_UnsupportedType_BlockDev) {
    EXPECT_EQ(-ENOTSUP, s3fs_mknod_s3("/bdev", S_IFBLK | 0644, 0));
}
TEST_F(S3fsOpsIncludeTest, Mknod_UnsupportedType_CharDev) {
    EXPECT_EQ(-ENOTSUP, s3fs_mknod_s3("/cdev", S_IFCHR | 0644, 0));
}
TEST_F(S3fsOpsIncludeTest, Mknod_UnsupportedType_Socket) {
    EXPECT_EQ(-ENOTSUP, s3fs_mknod_s3("/sock", S_IFSOCK | 0644, 0));
}

// --- s3fs_str_realtime ---
TEST_F(S3fsOpsIncludeTest, StrRealtime_ContainsDigits) {
    std::string rt = s3fs_str_realtime();
    bool has_digit = false;
    for(size_t i = 0; i < rt.size(); i++) {
        if(isdigit(rt[i])) { has_digit = true; break; }
    }
    EXPECT_TRUE(has_digit);
}

// --- check_parent_object_access_s3 ---
TEST_F(S3fsOpsIncludeTest, CheckParentAccess_EmptyMask) {
    EXPECT_EQ(0, check_parent_object_access_s3("/", 0));
}
TEST_F(S3fsOpsIncludeTest, CheckParentAccess_RootPathXOK) {
    EXPECT_EQ(0, check_parent_object_access_s3("/", X_OK));
}

// --- directory_empty ---
TEST_F(S3fsOpsIncludeTest, DirectoryEmpty_NullPath) {
    EXPECT_EQ(-EINVAL, directory_empty(NULL));
}
TEST_F(S3fsOpsIncludeTest, DirectoryEmpty_StatCacheWitnessShortCircuit) {
    // ISSUE2026051100001 cleanup: directory_empty() short-circuits to
    // -ENOTEMPTY when StatCache witnesses any direct child — no ListBucket
    // round-trip needed.
    headers_t meta;
    meta["Content-Length"] = "0";
    meta["Content-Type"] = "application/octet-stream";
    std::string key("/check_dir/file1.txt");
    StatCache::getStatCacheData()->AddStat(key, meta, false, true);
    EXPECT_EQ(-ENOTEMPTY, directory_empty("/check_dir"));
    StatCache::getStatCacheData()->DelStat(key);
}

// --- Constants ---
TEST_F(S3fsOpsIncludeTest, Constants_XattrMetaPrefix) {
    EXPECT_EQ("x-amz-meta-xattr-", XATTR_META_PREFIX);
}
TEST_F(S3fsOpsIncludeTest, Constants_MultipartThreshold) {
    EXPECT_EQ(25 * 1024 * 1024, MULTIPART_THRESHOLD);
}
TEST_F(S3fsOpsIncludeTest, Constants_MultipartPartSize) {
    EXPECT_EQ(10 * 1024 * 1024, MULTIPART_PART_SIZE);
}
TEST_F(S3fsOpsIncludeTest, Constants_WriteRetry) {
    EXPECT_EQ(8, WRITE_RETRY_MAX_ATTEMPTS);
    EXPECT_EQ(4000, WRITE_RETRY_DELAY_MS);
}

// [ISSUE2026040900001 C1/C2] Removed lone is_retryable_error HTTP 200 test —
// helper deleted as dead code.

// [ISSUE2026051100001 cleanup] AddPendingEntry / RemovePendingEntry /
// GetPendingEntries additional-code-path tests removed — helpers and static
// map no longer exist.

// ==========================================================================
// Tests for build_standard_metadata -- additional edge cases
// ==========================================================================

TEST_F(S3fsOpsIncludeTest, BuildStandardMetadata_BinaryFile) {
    headers_t meta;
    build_standard_metadata(meta, 0644, "application/octet-stream", false);
    EXPECT_NE(meta.end(), meta.find("Content-Type"));
    EXPECT_EQ("application/octet-stream", meta["Content-Type"]);
}

TEST_F(S3fsOpsIncludeTest, BuildStandardMetadata_JsonFile) {
    headers_t meta;
    build_standard_metadata(meta, 0644, "application/json", true);
    EXPECT_EQ("application/json", meta["Content-Type"]);
    // With preserve=true, mode should be in metadata
    EXPECT_NE(meta.end(), meta.find(hws_obs_meta_mode));
}

// ==========================================================================
// Tests for s3fs_str_realtime -- format validation
// ==========================================================================

TEST_F(S3fsOpsIncludeTest, StrRealtime_FormatIsValid) {
    std::string rt = s3fs_str_realtime();
    // Should be numeric string (epoch seconds)
    bool all_digits = true;
    for(size_t i = 0; i < rt.size(); i++) {
        if(!isdigit(rt[i]) && rt[i] != '-') {
            all_digits = false;
            break;
        }
    }
    EXPECT_FALSE(rt.empty());
    // May contain '-' for negative values on some systems
    EXPECT_TRUE(all_digits || rt[0] == '-');
}

// ==========================================================================
// Tests for XML parsing edge cases
// ==========================================================================

TEST_F(S3fsOpsIncludeTest, GetXmlNsUrl_MissingNs) {
    const char* xml = "<?xml version=\"1.0\"?><ListBucketResult><Name>test</Name></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    std::string nsurl;
    // No namespace in document
    EXPECT_FALSE(GetXmlNsUrl(doc, nsurl));
    xmlFreeDoc(doc);
}

TEST_F(S3fsOpsIncludeTest, IsTruncated_MissingElement) {
    const char* xml = "<?xml version=\"1.0\"?><ListBucketResult><Name>test</Name></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    // No IsTruncated element - should return false
    EXPECT_FALSE(is_truncated_s3ops(doc));
    xmlFreeDoc(doc);
}

TEST_F(S3fsOpsIncludeTest, GetBaseExp_NullDocReturnsNull) {
    xmlChar* val = get_base_exp_s3ops(nullptr, "Name");
    EXPECT_EQ(nullptr, val);
}

// ==========================================================================
// Tests for str_to_off_t -- more edge cases
// ==========================================================================

TEST_F(S3fsOpsIncludeTest, StrToOffT_NegativeLarge) {
    EXPECT_EQ(-1000000, str_to_off_t("-1000000"));
}

TEST_F(S3fsOpsIncludeTest, StrToOffT_Whitespace) {
    EXPECT_EQ(0, str_to_off_t("   "));
}

// [ISSUE2026040900001 C1/C2] Removed final is_retryable_error /
// map_obs_error_to_errno coverage suites — helpers deleted as dead code.

// ==========================================================================
// Tests for get_next_marker_s3ops -- edge cases
// ==========================================================================
TEST_F(S3fsOpsIncludeTest, GetNextMarker_EmptyDoc) {
    const char* xml = "<?xml version=\"1.0\"?><ListBucketResult></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    std::string marker = get_next_marker_s3ops(doc);
    EXPECT_TRUE(marker.empty());
    xmlFreeDoc(doc);
}

TEST_F(S3fsOpsIncludeTest, GetNextMarker_MissingElement) {
    const char* xml = "<?xml version=\"1.0\"?><ListBucketResult><Name>test</Name></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    std::string marker = get_next_marker_s3ops(doc);
    EXPECT_TRUE(marker.empty());
    xmlFreeDoc(doc);
}

// ==========================================================================
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
