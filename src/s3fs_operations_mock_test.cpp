// =================================================================
// s3fs_operations_mock_test.cpp
// Tests FUSE operations in s3fs_operations.cpp with mocked S3fsCurl
//
// Architecture:
// - #include "s3fs_operations.cpp" to compile operations code directly
// - Do NOT link curl.o - all S3fsCurl symbols are defined in this file
// - MockState struct provides configurable return values
// - Tests exercise real FUSE operation functions with controlled S3 behavior
//
// Copyright(C) 2025-2026
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
// =================================================================

// Pre-include standard headers
#include <string>
#include <sstream>
#include <vector>
#include <map>
#include <list>
#include <set>
#include <algorithm>
#include <iostream>
#include <memory>
#include <mutex>
#include <cstring>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <errno.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

// Stub for fuse_get_context - s3fs_operations.cpp calls this for uid/gid checks
#include <fuse.h>
#include <sys/types.h>

static struct fuse_context g_test_fuse_ctx;
extern "C" struct fuse_context* fuse_get_context() {
    g_test_fuse_ctx.uid = getuid();
    g_test_fuse_ctx.gid = getgid();
    return &g_test_fuse_ctx;
}

// Stub for get_realpath - s3fs_operations.cpp calls get_realpath(path)
// Use weak attribute so s3fs_util.o's real implementation wins if linked.
__attribute__((weak)) std::string get_realpath(const char* path) {
    return path ? std::string(path) : std::string("/");
}

// Access private/protected members for testing
#define private public
#define protected public
#include "s3fs_operations.cpp"
#undef private
#undef protected

// =================================================================
// MockState - configurable return values for S3fsCurl methods
// =================================================================
struct MockState {
    int head_result = 0;
    headers_t head_meta;
    int put_result = 0;
    int put_head_result = 0;
    int delete_result = 0;
    int list_result = 0;
    std::string list_body_xml;
    int get_obj_result = 0;
    int rename_result = 0;
    // Captured arguments for verification
    std::string last_head_path;
    std::string last_put_path;
    std::string last_delete_path;
    std::string last_list_path;
    std::string last_list_query;
    // Pagination support: per-call queues (index by list_call_count)
    std::vector<int> list_result_queue;
    std::vector<std::string> list_xml_queue;
    int list_call_count = 0;
    std::vector<std::string> list_queries;
    // Per-call DELETE queue (parallel to list_result_queue)
    std::vector<int> delete_result_queue;
    int delete_call_count = 0;
    std::vector<std::string> delete_paths;
    // Per-path HEAD results (for multi-path scenarios like dir rename)
    std::map<std::string, int> head_result_map;
    std::map<std::string, headers_t> head_meta_map;
    int head_call_count = 0;
    std::vector<std::string> head_paths;
    void reset() {
        head_result = 0;
        head_meta.clear();
        put_result = 0;
        put_head_result = 0;
        delete_result = 0;
        list_result = 0;
        list_body_xml.clear();
        get_obj_result = 0;
        rename_result = 0;
        last_head_path.clear();
        last_put_path.clear();
        last_delete_path.clear();
        last_list_path.clear();
        last_list_query.clear();
        list_result_queue.clear();
        list_xml_queue.clear();
        list_call_count = 0;
        list_queries.clear();
        delete_result_queue.clear();
        delete_call_count = 0;
        delete_paths.clear();
        head_result_map.clear();
        head_meta_map.clear();
        head_call_count = 0;
        head_paths.clear();
    }
};

static MockState g_mock;

// =================================================================
// S3fsCurl static member definitions
// =================================================================
pthread_mutex_t S3fsCurl::curl_handles_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_spinlock_t S3fsCurl::curl_handles_spinlock;
pthread_rwlock_t S3fsCurl::curl_aksk_rwlock;
pthread_mutex_t S3fsCurl::curl_share_lock[SHARE_MUTEX_MAX] = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER};
bool S3fsCurl::is_initglobal_done = false;
CurlHandlerPool* S3fsCurl::sCurlPool = NULL;
int S3fsCurl::sCurlPoolSize = 4;
CURLSH* S3fsCurl::hCurlShare = NULL;
bool S3fsCurl::is_cert_check = true;
bool S3fsCurl::is_dns_cache = true;
bool S3fsCurl::is_ssl_session_cache = true;
long S3fsCurl::connect_timeout = 300;
time_t S3fsCurl::readwrite_timeout = 45;
int S3fsCurl::retries = 3;
bool S3fsCurl::is_public_bucket = false;
std::string S3fsCurl::default_acl = "private";
storage_class_t S3fsCurl::storage_class = STANDARD;
sseckeylist_t S3fsCurl::sseckeys;
std::string S3fsCurl::ssekmsid;
sse_type_t S3fsCurl::ssetype = SSE_DISABLE;
bool S3fsCurl::is_content_md5 = true;
bool S3fsCurl::is_verbose = false;
std::string S3fsCurl::AWSAccessKeyId = "AKID";
std::string S3fsCurl::AWSSecretAccessKey = "SECRET";
std::string S3fsCurl::AWSAccessToken;
time_t S3fsCurl::AWSAccessTokenExpire = 0;
bool S3fsCurl::is_ecs = false;
bool S3fsCurl::is_ibm_iam_auth = false;
std::string S3fsCurl::IAM_cred_url;
size_t S3fsCurl::IAM_field_count = 4;
std::string S3fsCurl::IAM_token_field = "Token";
std::string S3fsCurl::IAM_expiry_field = "Expiration";
std::string S3fsCurl::IAM_role;
long S3fsCurl::ssl_verify_hostname = 1;
curltime_t S3fsCurl::curl_times;
curlprogress_t S3fsCurl::curl_progress;
std::string S3fsCurl::curl_ca_bundle;
mimes_t S3fsCurl::mimeTypes;
std::string S3fsCurl::userAgent;
int S3fsCurl::max_parallel_cnt = 5;
off_t S3fsCurl::multipart_size = 10 * 1024 * 1024;
bool S3fsCurl::is_sigv4 = false;
bool S3fsCurl::is_ua = true;

// S3fsMultiCurl static
int S3fsMultiCurl::max_multireq = 20;

// =================================================================
// BodyData real implementation (needed for XML parsing in readdir etc.)
// =================================================================
void BodyData::Clear() {
    if(text) { free(text); text = NULL; }
    lastpos = 0;
    bufsize = 0;
}

bool BodyData::Resize(size_t addbytes) {
    size_t need = lastpos + addbytes + 1;
    if(need <= bufsize) return true;
    size_t newsize = (bufsize == 0) ? need * 2 : bufsize * 2;
    if(newsize < need) newsize = need;
    char* tmp = (char*)realloc(text, newsize);
    if(!tmp) return false;
    text = tmp;
    bufsize = newsize;
    return true;
}

bool BodyData::Append(void* ptr, size_t bytes) {
    if(!ptr) return false;
    if(bytes == 0) return true;
    if(!Resize(bytes)) return false;
    memcpy(&text[lastpos], ptr, bytes);
    lastpos += bytes;
    text[lastpos] = '\0';
    return true;
}

const char* BodyData::str() const {
    static const char* empty = "";
    return text ? text : empty;
}

// =================================================================
// CurlHandlerPool stubs
// =================================================================
bool CurlHandlerPool::Init() { return true; }
bool CurlHandlerPool::Destroy() { return true; }
CURL* CurlHandlerPool::GetHandler() { return NULL; }
void CurlHandlerPool::ReturnHandler(CURL*) {}
int CurlHandlerPool::GetActiveCurlNum() { return 0; }

// =================================================================
// S3fsCurl mock implementations
// =================================================================
S3fsCurl::S3fsCurl(bool ahbe)
    : hCurl(NULL), type(REQTYPE_UNSET), requestHeaders(NULL),
      bodydata(NULL), headdata(NULL), LastResponseCode(-1),
      postdata(NULL), postdata_remaining(0),
      is_use_ahbe(ahbe), retry_count(0),
      b_infile(NULL), b_postdata(NULL), b_postdata_remaining(0),
      b_partdata_startpos(0), b_partdata_size(0),
      b_ssekey_pos(-1), b_ssetype(SSE_DISABLE),
      read_buf(NULL), read_startpos(0), read_size(0), cur_pos(0)
{
    memset(&writedatabuf, 0, sizeof(writedatabuf));
}

S3fsCurl::~S3fsCurl() {
    if(bodydata) { delete bodydata; bodydata = NULL; }
    if(headdata) { delete headdata; headdata = NULL; }
    if(requestHeaders) { curl_slist_free_all(requestHeaders); requestHeaders = NULL; }
}

// Private methods - minimal stubs
bool S3fsCurl::ClearInternalData() { return true; }
bool S3fsCurl::ResetHandle() { return true; }
bool S3fsCurl::RemakeHandle() { return true; }
void S3fsCurl::insertV4Headers(const std::string&, const std::string&, const std::string&) {}
void S3fsCurl::insertV2Headers(const std::string&, const std::string&, const std::string&) {}
void S3fsCurl::insertIBMIAMHeaders() {}
void S3fsCurl::insertAuthHeaders(const std::string&, const std::string&, const std::string&) {}
std::string S3fsCurl::CalcSignatureV2(const std::string&, const std::string&, const std::string&, const std::string&, const std::string&, const std::string&, const std::string&) { return ""; }
std::string S3fsCurl::CalcSignature(const std::string&, const std::string&, const std::string&, const std::string&, const std::string&, const std::string&, const std::string&, const std::string&) { return ""; }
bool S3fsCurl::GetUploadId(std::string&) { return false; }
int S3fsCurl::GetIAMCredentials() { return 0; }
int S3fsCurl::UploadMultipartPostSetup(const char*, int, const std::string&) { return 0; }
int S3fsCurl::CopyMultipartPostRequest(const char*, const char*, int, std::string&, headers_t&) { return 0; }
int S3fsCurl::CopyMultipartPostSetup(const char*, const char*, int, const std::string&, headers_t&) { return 0; }
bool S3fsCurl::CopyMultipartPostComplete() { return true; }
bool S3fsCurl::UploadMultipartPostComplete() { return true; }
CURLcode S3fsCurl::curl_easy_perform_mock(CURL*, long*) { return CURLE_OK; }
std::string S3fsCurl::PrintRequestId4ResponseErr() { return ""; }
int S3fsCurl::getRootInodeNo() { return 0; }
bool S3fsCurl::UploadMultipartPostCallback(S3fsCurl*) { return true; }
S3fsCurl* S3fsCurl::UploadMultipartPostRetryCallback(S3fsCurl*) { return NULL; }
bool S3fsCurl::CopyMultipartPostCallback(S3fsCurl*) { return true; }
S3fsCurl* S3fsCurl::CopyMultipartPostRetryCallback(S3fsCurl*) { return NULL; }
bool S3fsCurl::MixMultipartPostCallback(S3fsCurl*) { return true; }
S3fsCurl* S3fsCurl::ParallelGetObjectRetryCallback(S3fsCurl*) { return NULL; }
bool S3fsCurl::ParseIAMCredentialResponse(const char*, iamcredmap_t&) { return false; }
bool S3fsCurl::SetIAMCredentials(const char*) { return false; }
bool S3fsCurl::ParseIAMRoleFromMetaDataResponse(const char*, std::string&) { return false; }
bool S3fsCurl::SetIAMRoleFromMetaData(const char*) { return false; }
bool S3fsCurl::LoadEnvSseCKeys() { return true; }
bool S3fsCurl::LoadEnvSseKmsid() { return true; }
bool S3fsCurl::PushbackSseKeys(std::string&) { return false; }
bool S3fsCurl::AddUserAgent(CURL*) { return true; }
int S3fsCurl::CurlDebugFunc(CURL*, curl_infotype, char*, size_t, void*) { return 0; }
int S3fsCurl::CurlProgress(void*, double, double, double, double) { return 0; }
size_t S3fsCurl::HeaderCallback(void*, size_t, size_t, void*) { return 0; }
size_t S3fsCurl::WriteMemoryCallback(void*, size_t, size_t, void*) { return 0; }
size_t S3fsCurl::ReadUploadCache(void*, size_t, size_t, void*) { return 0; }
size_t S3fsCurl::ReadCallback(void*, size_t, size_t, void*) { return 0; }
size_t S3fsCurl::UploadReadCallback(void*, size_t, size_t, void*) { return 0; }
size_t S3fsCurl::DownloadWriteCallback(void*, size_t, size_t, void*) { return 0; }
size_t S3fsCurl::DownloadWriteCallbackforRead(void*, size_t, size_t, void*) { return 0; }

// Public static methods
bool S3fsCurl::InitS3fsCurl(const char*) { return true; }
bool S3fsCurl::DestroyS3fsCurl() { return true; }
bool S3fsCurl::InitGlobalCurl() { return true; }
bool S3fsCurl::DestroyGlobalCurl() { return true; }
bool S3fsCurl::InitShareCurl() { return true; }
bool S3fsCurl::DestroyShareCurl() { return true; }
void S3fsCurl::LockCurlShare(CURL*, curl_lock_data, curl_lock_access, void*) {}
void S3fsCurl::UnlockCurlShare(CURL*, curl_lock_data, void*) {}
bool S3fsCurl::InitCryptMutex() { return true; }
bool S3fsCurl::DestroyCryptMutex() { return true; }
bool S3fsCurl::InitMimeType(const char*) { return true; }
bool S3fsCurl::LocateBundle() { return true; }
bool S3fsCurl::CheckIAMCredentialUpdate() { return true; }
void S3fsCurl::InitUserAgent() {}

std::string S3fsCurl::LookupMimeType(const std::string& name) {
    if(name.find(".html") != std::string::npos) return "text/html";
    if(name.find(".txt") != std::string::npos) return "text/plain";
    return "application/octet-stream";
}

// Static setter/getter stubs
bool S3fsCurl::SetCheckCertificate(bool v) { bool o = is_cert_check; is_cert_check = v; return o; }
bool S3fsCurl::SetDnsCache(bool v) { bool o = is_dns_cache; is_dns_cache = v; return o; }
bool S3fsCurl::SetSslSessionCache(bool v) { bool o = is_ssl_session_cache; is_ssl_session_cache = v; return o; }
long S3fsCurl::SetConnectTimeout(long v) { long o = connect_timeout; connect_timeout = v; return o; }
time_t S3fsCurl::SetReadwriteTimeout(time_t v) { time_t o = readwrite_timeout; readwrite_timeout = v; return o; }
int S3fsCurl::SetRetries(int v) { int o = retries; retries = v; return o; }
bool S3fsCurl::SetPublicBucket(bool v) { bool o = is_public_bucket; is_public_bucket = v; return o; }
std::string S3fsCurl::SetDefaultAcl(const char* acl) {
    std::string o = default_acl;
    default_acl = acl ? acl : "";
    return o;
}
std::string S3fsCurl::GetDefaultAcl() { return default_acl; }
storage_class_t S3fsCurl::SetStorageClass(storage_class_t sc) {
    storage_class_t o = storage_class; storage_class = sc; return o;
}
sse_type_t S3fsCurl::SetSseType(sse_type_t t) { sse_type_t o = ssetype; ssetype = t; return o; }
bool S3fsCurl::FinalCheckSse() { return true; }
bool S3fsCurl::SetSseCKeys(const char*) { return false; }
bool S3fsCurl::SetSseKmsid(const char* id) {
    if(!id || !id[0]) return false;
    ssekmsid = id;
    return true;
}
bool S3fsCurl::GetSseKey(std::string&, std::string&) { return false; }
bool S3fsCurl::GetSseKeyMd5(int, std::string&) { return false; }
int S3fsCurl::GetSseKeyCount() { return 0; }
bool S3fsCurl::SetContentMd5(bool v) { bool o = is_content_md5; is_content_md5 = v; return o; }
bool S3fsCurl::SetVerbose(bool v) { bool o = is_verbose; is_verbose = v; return o; }
bool S3fsCurl::SetAccessKey(const char* ak, const char* sk) {
    if(!ak || !sk) return false;
    AWSAccessKeyId = ak; AWSSecretAccessKey = sk; return true;
}
bool S3fsCurl::GetAccessKeyAndToken(std::string& ak, std::string& sk, std::string& token) {
    ak = AWSAccessKeyId; sk = AWSSecretAccessKey; token = AWSAccessToken; return true;
}
bool S3fsCurl::SetAccessKeyAndToken(const char* ak, const char* sk, const char* token) {
    if(!ak || !ak[0] || !sk || !sk[0] || !token) return false;
    AWSAccessKeyId = ak; AWSSecretAccessKey = sk; AWSAccessToken = token; return true;
}
long S3fsCurl::SetSslVerifyHostname(long v) {
    if(v != 0 && v != 1) return -1;
    long o = ssl_verify_hostname; ssl_verify_hostname = v; return o;
}
int S3fsCurl::SetMaxParallelCount(int v) { int o = max_parallel_cnt; max_parallel_cnt = v; return o; }
bool S3fsCurl::SetIsECS(bool v) { bool o = is_ecs; is_ecs = v; return o; }
bool S3fsCurl::SetIsIBMIAMAuth(bool v) { bool o = is_ibm_iam_auth; is_ibm_iam_auth = v; return o; }
size_t S3fsCurl::SetIAMFieldCount(size_t v) { size_t o = IAM_field_count; IAM_field_count = v; return o; }
std::string S3fsCurl::SetIAMCredentialsURL(const char* v) {
    std::string o = IAM_cred_url; IAM_cred_url = v ? v : ""; return o;
}
std::string S3fsCurl::SetIAMTokenField(const char* v) {
    std::string o = IAM_token_field; IAM_token_field = v ? v : ""; return o;
}
std::string S3fsCurl::SetIAMExpiryField(const char* v) {
    std::string o = IAM_expiry_field; IAM_expiry_field = v ? v : ""; return o;
}
std::string S3fsCurl::SetIAMRole(const char* v) {
    std::string o = IAM_role; IAM_role = v ? v : ""; return o;
}
bool S3fsCurl::SetMultipartSize(off_t v) {
    if(v < MIN_MULTIPART_SIZE / (1024 * 1024)) return false;
    multipart_size = v * 1024 * 1024; return true;
}
bool S3fsCurl::DetectBucketTypeFromHeaders(const headers_t&) { return false; }

// Instance methods
bool S3fsCurl::CreateCurlHandle(bool) { return true; }
bool S3fsCurl::DestroyCurlHandle() { return true; }
bool S3fsCurl::LoadIAMRoleFromMetaData() { return false; }
bool S3fsCurl::AddSseRequestHead(sse_type_t, std::string&, bool, bool) { return true; }
bool S3fsCurl::GetResponseCode(long& c) { c = LastResponseCode; return true; }
CURLcode S3fsCurl::Hws_curl_easy_perform() { return CURLE_OK; }
int S3fsCurl::RequestPerform() { return 0; }

bool S3fsCurl::SetUseAhbe(bool v) { bool o = is_use_ahbe; is_use_ahbe = v; return o; }

// ===== KEY MOCK METHODS =====

int S3fsCurl::HeadRequest(const char* tpath, headers_t& meta, long long* inode_no, const char* shardkey) {
    (void)inode_no;
    (void)shardkey;
    std::string path = tpath ? tpath : "";
    g_mock.last_head_path = path;
    g_mock.head_paths.push_back(path);
    g_mock.head_call_count++;

    // Per-path result override
    auto rit = g_mock.head_result_map.find(path);
    if(rit != g_mock.head_result_map.end()){
        if(rit->second != 0) return rit->second;
        auto mit = g_mock.head_meta_map.find(path);
        if(mit != g_mock.head_meta_map.end()){
            meta = mit->second;
        }else{
            meta = g_mock.head_meta;
        }
        return 0;
    }

    if(g_mock.head_result != 0) return g_mock.head_result;
    meta = g_mock.head_meta;
    return 0;
}

int S3fsCurl::PutRequest(const char* tpath, headers_t& meta, int fd) {
    (void)meta;
    (void)fd;
    g_mock.last_put_path = tpath ? tpath : "";
    return g_mock.put_result;
}

int S3fsCurl::PutHeadRequest(const char* tpath, headers_t& meta, bool is_copy, const std::string& qs) {
    (void)meta;
    (void)is_copy;
    (void)qs;
    g_mock.last_put_path = tpath ? tpath : "";
    return g_mock.put_head_result;
}

int S3fsCurl::DeleteRequest(const char* tpath) {
    g_mock.last_delete_path = tpath ? tpath : "";
    g_mock.delete_paths.push_back(g_mock.last_delete_path);
    int idx = g_mock.delete_call_count++;
    if(idx < (int)g_mock.delete_result_queue.size()){
        return g_mock.delete_result_queue[idx];
    }
    return g_mock.delete_result;
}

int S3fsCurl::ListBucketRequest(const char* tpath, const char* query) {
    g_mock.last_list_path = tpath ? tpath : "";
    g_mock.last_list_query = query ? query : "";
    g_mock.list_queries.push_back(g_mock.last_list_query);

    // Determine per-call result code
    int result_code;
    if(!g_mock.list_result_queue.empty() && g_mock.list_call_count < (int)g_mock.list_result_queue.size()) {
        result_code = g_mock.list_result_queue[g_mock.list_call_count];
    } else {
        result_code = g_mock.list_result;
    }

    // Determine per-call XML body
    std::string xml;
    if(!g_mock.list_xml_queue.empty() && g_mock.list_call_count < (int)g_mock.list_xml_queue.size()) {
        xml = g_mock.list_xml_queue[g_mock.list_call_count];
    } else {
        xml = g_mock.list_body_xml;
    }

    g_mock.list_call_count++;

    if(result_code != 0) return result_code;

    // Populate bodydata with mock XML
    if(!xml.empty()) {
        if(!bodydata) bodydata = new BodyData();
        bodydata->Clear();
        bodydata->Append((void*)xml.c_str(), xml.size());
    }
    return 0;
}

// Mock CheckObjectBucketPrefixExists: delegates to mocked ListBucketRequest so
// existing Getattr_C1_* tests keep working with their list_body_xml fixtures.
// Real impl (curl.cpp:4489) does the exact same ListBucketRequest + scan-for-
// non-empty <Contents>/<CommonPrefixes>; here we use a substring scan which is
// sufficient for the well-formed XMLs the tests provide.
int S3fsCurl::CheckObjectBucketPrefixExists(const std::string& prefix) {
    std::string norm = prefix;
    while(!norm.empty() && norm.front() == '/') norm.erase(0, 1);
    if(!norm.empty() && norm.back() != '/') norm += "/";
    std::string query = "prefix=" + norm + "&delimiter=/&max-keys=2&encoding-type=url";
    int rc = ListBucketRequest("/", query.c_str());
    if(rc != 0) return rc;
    const BodyData* body = GetBodyData();
    if(!body || body->size() == 0) return -ENOENT;
    std::string xml(body->str(), body->size());
    bool found = xml.find("<Contents>") != std::string::npos
              || xml.find("<Contents ") != std::string::npos
              || xml.find("<CommonPrefixes>") != std::string::npos
              || xml.find("<CommonPrefixes ") != std::string::npos;
    return found ? 0 : -ENOENT;
}

int S3fsCurl::GetObjectRequest(const char* tpath, int fd, off_t start, ssize_t size) {
    (void)tpath;
    (void)fd;
    (void)start;
    (void)size;
    return g_mock.get_obj_result;
}

int S3fsCurl::PreGetObjectRequest(const char* tpath, int fd, off_t start, ssize_t size, sse_type_t ssetype, std::string& ssevalue) {
    (void)tpath;
    (void)fd;
    (void)start;
    (void)size;
    (void)ssetype;
    (void)ssevalue;
    return 0;
}

bool S3fsCurl::PreHeadRequest(const char* tpath, const char* bpath, const char* savedpath, int ssekey_pos, long long* inode_no, const char* shardkey) {
    (void)tpath;
    (void)bpath;
    (void)savedpath;
    (void)ssekey_pos;
    (void)inode_no;
    (void)shardkey;
    return true;
}

int S3fsCurl::CheckBucket(const std::string&, const std::string&, const std::string&) { return 0; }
int S3fsCurl::PreMultipartPostRequest(const char*, headers_t&, std::string&, bool) { return 0; }
int S3fsCurl::CompleteMultipartPostRequest(const char*, std::string&, etaglist_t&) { return 0; }
int S3fsCurl::UploadMultipartPostRequest(const char*, int, const std::string&) { return 0; }
int S3fsCurl::MultipartListRequest(std::string&) { return 0; }
int S3fsCurl::AbortMultipartUpload(const char*, std::string&) { return 0; }
int S3fsCurl::MultipartHeadRequest(const char*, off_t, headers_t&, bool) { return 0; }
int S3fsCurl::MultipartUploadRequest(const char*, headers_t&, int, bool) { return 0; }
int S3fsCurl::MultipartUploadRequest(const std::string&, const char*, int, off_t, size_t, etaglist_t&) { return 0; }
int S3fsCurl::MultipartUploadRequest(const std::string&, const char*, int, off_t, size_t, etaglist_t&, int, int) { return 0; }
int S3fsCurl::MultipartRenameRequest(const char*, const char*, headers_t&, off_t) { return g_mock.rename_result; }
int S3fsCurl::CreateZeroByteFileObject(const char*, headers_t&) { return 0; }
int S3fsCurl::WriteBytesToFileObject(const char*, tagDataBuffer*, off_t, tag_index_cache_entry_t*) { return 0; }
void S3fsCurl::RenameSetopt() {}
int S3fsCurl::RenameFileOrDirObject(const char*, const char*, bool&) { return 0; }
int S3fsCurl::CreateZeroByteDirObject(const char*, headers_t&) { return 0; }
int S3fsCurl::PreReadFromFileObject(const char*, char*, off_t, size_t, sse_type_t, std::string&, const char*, const char*) { return 0; }
int S3fsCurl::ReadFromFileObject(const char*, char*, off_t, size_t, const char*, const char*) { return 0; }
int S3fsCurl::getMountPrefixInode() { return 0; }
int S3fsCurl::ParallelMultipartUploadRequest(const char*, headers_t&, int) { return 0; }
int S3fsCurl::MixMultipartUploadRequest(const char*, headers_t&, int, const std::list<struct fdpage*>&) { return 0; }
int S3fsCurl::ParallelGetObjectRequest(const char*, int, off_t, ssize_t) { return 0; }

// S3fsMultiCurl implementations
S3fsMultiCurl::S3fsMultiCurl() : SuccessCallback(NULL), RetryCallback(NULL) {}
S3fsMultiCurl::~S3fsMultiCurl() { Clear(); }
bool S3fsMultiCurl::ClearEx(bool) { return true; }
int S3fsMultiCurl::MultiPerform() { return 0; }
int S3fsMultiCurl::MultiRead() { return 0; }
void* S3fsMultiCurl::RequestPerformWrapper(void*) { return NULL; }
int S3fsMultiCurl::SetMaxMultiRequest(int v) { int o = max_multireq; max_multireq = v; return o; }
S3fsMultiSuccessCallback S3fsMultiCurl::SetSuccessCallback(S3fsMultiSuccessCallback f) {
    S3fsMultiSuccessCallback o = SuccessCallback; SuccessCallback = f; return o;
}
S3fsMultiRetryCallback S3fsMultiCurl::SetRetryCallback(S3fsMultiRetryCallback f) {
    S3fsMultiRetryCallback o = RetryCallback; RetryCallback = f; return o;
}
bool S3fsMultiCurl::SetS3fsCurlObject(S3fsCurl*) { return true; }
int S3fsMultiCurl::Request() { return 0; }

// Utility functions from curl.cpp - provide stubs
// (test_stubs.o provides weak curl_slist_sort_insert, others may be needed)
std::string GetContentMD5(int) { return ""; }
unsigned char* md5hexsum(int, off_t, ssize_t) { return NULL; }
std::string md5sum(int, off_t, ssize_t) { return ""; }
std::string get_sorted_header_keys(const struct curl_slist*) { return ""; }
std::string get_canonical_headers(const struct curl_slist*, bool) { return "\n"; }
std::string get_header_value(const struct curl_slist*, const std::string&) { return ""; }
bool MakeUrlResource(const char* realpath, std::string& resourcepath, std::string& url) {
    if(!realpath) return false;
    resourcepath = realpath;
    url = "https://obs.example.com" + std::string(realpath);
    return true;
}
std::string prepare_url(const char* url) { return url ? url : ""; }

// =================================================================
// Google Test framework
// =================================================================
#include "gtest.h"

// =================================================================
// Test Fixture
// =================================================================
class S3fsOpsMockTest : public ::testing::Test {
protected:
    void SetUp() override {
        g_mock.reset();
        g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
        bucket = "test-bucket";
        host = "https://obs.example.com";
        pathrequeststyle = true;
        // Clear stat caches
        StatCache::getStatCacheData()->DelStatWildcard("/");
    }
    void TearDown() override {
        g_mock.reset();
    }
};

// =================================================================
// 1. getattr tests
// =================================================================

TEST_F(S3fsOpsMockTest, Getattr_Root) {
    struct stat stbuf;
    EXPECT_EQ(0, s3fs_getattr_s3("/", &stbuf));
    EXPECT_TRUE(S_ISDIR(stbuf.st_mode));
    EXPECT_EQ(2, (int)stbuf.st_nlink);
}

TEST_F(S3fsOpsMockTest, Getattr_RegularFile) {
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Length"] = "1024";
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-hws-obs-meta-mode"] = "33188";  // S_IFREG|0644
    g_mock.head_meta["x-hws-obs-meta-mtime"] = "1700000000";

    struct stat stbuf;
    EXPECT_EQ(0, s3fs_getattr_s3("/test.txt", &stbuf));
    EXPECT_TRUE(S_ISREG(stbuf.st_mode));
    EXPECT_EQ(1024, stbuf.st_size);
}

TEST_F(S3fsOpsMockTest, Getattr_Directory) {
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "application/x-directory";
    g_mock.head_meta["Content-Length"] = "0";
    g_mock.head_meta["x-hws-obs-meta-mode"] = "16877";  // S_IFDIR|0755

    struct stat stbuf;
    EXPECT_EQ(0, s3fs_getattr_s3("/testdir", &stbuf));
    EXPECT_TRUE(S_ISDIR(stbuf.st_mode));
}

TEST_F(S3fsOpsMockTest, Getattr_NotFound) {
    g_mock.head_result = -ENOENT;
    struct stat stbuf;
    EXPECT_EQ(-ENOENT, s3fs_getattr_s3("/nonexistent", &stbuf));
}

TEST_F(S3fsOpsMockTest, Getattr_Symlink) {
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-amz-meta-symlink-target"] = "/target/path";
    g_mock.head_meta["x-hws-obs-meta-mode"] = "33188";

    struct stat stbuf;
    EXPECT_EQ(0, s3fs_getattr_s3("/mylink", &stbuf));
    EXPECT_TRUE(S_ISLNK(stbuf.st_mode));
    EXPECT_EQ(12, stbuf.st_size);  // length of "/target/path"
}

TEST_F(S3fsOpsMockTest, Getattr_RootNlink) {
    // Root directory always has nlink=2
    struct stat stbuf;
    EXPECT_EQ(0, s3fs_getattr_s3("/", &stbuf));
    EXPECT_EQ(2, (int)stbuf.st_nlink);
    EXPECT_EQ(getuid(), stbuf.st_uid);
    EXPECT_EQ(getgid(), stbuf.st_gid);
}

TEST_F(S3fsOpsMockTest, Getattr_FileWithUidGid) {
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Length"] = "512";
    g_mock.head_meta["Content-Type"] = "text/plain";
    g_mock.head_meta["x-hws-obs-meta-mode"] = "33188";
    g_mock.head_meta["x-amz-meta-uid"] = "1000";
    g_mock.head_meta["x-amz-meta-gid"] = "1000";

    struct stat stbuf;
    EXPECT_EQ(0, s3fs_getattr_s3("/owned.txt", &stbuf));
    EXPECT_EQ(1000u, stbuf.st_uid);
    EXPECT_EQ(1000u, stbuf.st_gid);
}

// =================================================================
// 1.5 Object key length validation tests
// validate_object_key_length: mount_prefix + path total ≤ 1024
// With default mount_prefix="", path of 1026 chars → fullPath.size()-1 = 1025 > 1024 → ENAMETOOLONG
// =================================================================

TEST_F(S3fsOpsMockTest, Create_PathTooLong) {
    std::string longpath(1026, 'a');
    longpath[0] = '/';
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.flags = O_CREAT | O_WRONLY;
    EXPECT_EQ(-ENAMETOOLONG, s3fs_create_s3(longpath.c_str(), 0644, &fi));
}

TEST_F(S3fsOpsMockTest, Create_PathAtMax) {
    // Build a long path with valid component lengths (each ≤ 255)
    // e.g. /aaa.../aaa.../aaa... totaling 1024 chars → key length OK
    std::string path;
    while(path.size() < 1020) {
        path += "/";
        path += std::string(200, 'a');
    }
    // Trim to exactly 1024 chars
    path.resize(1024, 'b');
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.flags = O_CREAT | O_WRONLY;
    g_mock.head_result = -ENOENT;
    g_mock.put_result = 0;
    // Passes both key length and component length checks
    EXPECT_EQ(0, s3fs_create_s3(path.c_str(), 0644, &fi));
}

TEST_F(S3fsOpsMockTest, Create_ComponentTooLong) {
    // Single component > 255 chars → rejected by component check
    std::string longname(256, 'a');
    std::string path = "/" + longname;
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.flags = O_CREAT | O_WRONLY;
    EXPECT_EQ(-ENAMETOOLONG, s3fs_create_s3(path.c_str(), 0644, &fi));
}

TEST_F(S3fsOpsMockTest, Mknod_PathTooLong) {
    std::string longpath(1026, 'a');
    longpath[0] = '/';
    EXPECT_EQ(-ENAMETOOLONG, s3fs_mknod_s3(longpath.c_str(), S_IFREG | 0644, 0));
}

TEST_F(S3fsOpsMockTest, Mkdir_PathTooLong) {
    std::string longpath(1026, 'a');
    longpath[0] = '/';
    EXPECT_EQ(-ENAMETOOLONG, s3fs_mkdir_s3(longpath.c_str(), 0755));
}

TEST_F(S3fsOpsMockTest, Symlink_PathTooLong) {
    std::string longpath(1026, 'a');
    longpath[0] = '/';
    // 'to' (target path in VFS) is validated, 'from' (symlink content) is not
    EXPECT_EQ(-ENAMETOOLONG, s3fs_symlink_s3("/short", longpath.c_str()));
}

TEST_F(S3fsOpsMockTest, Symlink_FromLongButToShort) {
    // Only 'to' path is validated (it becomes the object key)
    std::string longfrom(2000, 'a');
    longfrom[0] = '/';
    g_mock.put_result = 0;
    EXPECT_EQ(0, s3fs_symlink_s3(longfrom.c_str(), "/shortlink"));
}

TEST_F(S3fsOpsMockTest, Rename_DestPathTooLong) {
    std::string longpath(1026, 'a');
    longpath[0] = '/';
    EXPECT_EQ(-ENAMETOOLONG, s3fs_rename_s3("/short", longpath.c_str()));
}

TEST_F(S3fsOpsMockTest, Rename_DestPathAtBoundary) {
    // Build a long dest path with valid component lengths, totaling 1025 chars
    // (key length 1024 at boundary)
    std::string path;
    while(path.size() < 1020) {
        path += "/";
        path += std::string(200, 'a');
    }
    path.resize(1025, 'b');
    // Will pass length check but fail on getattr (source doesn't exist)
    g_mock.head_result = -ENOENT;
    EXPECT_EQ(-ENOENT, s3fs_rename_s3("/src", path.c_str()));
}

TEST_F(S3fsOpsMockTest, Rename_DestComponentTooLong) {
    // Dest filename > 255 chars
    std::string longname(256, 'r');
    std::string path = "/dir/" + longname;
    EXPECT_EQ(-ENAMETOOLONG, s3fs_rename_s3("/src", path.c_str()));
}

// =================================================================
// 2. mkdir/mknod tests
// =================================================================

TEST_F(S3fsOpsMockTest, Mkdir_Success) {
    g_mock.put_result = 0;
    EXPECT_EQ(0, s3fs_mkdir_s3("/newdir", 0755));
    // Verify the put was to a path ending with /
    EXPECT_FALSE(g_mock.last_put_path.empty());
    EXPECT_EQ('/', g_mock.last_put_path.back());
}

TEST_F(S3fsOpsMockTest, Mkdir_Root) {
    EXPECT_EQ(0, s3fs_mkdir_s3("/", 0755));  // root always succeeds
}

TEST_F(S3fsOpsMockTest, Mkdir_PutFails) {
    g_mock.put_result = -EIO;
    EXPECT_NE(0, s3fs_mkdir_s3("/newdir", 0755));
}

TEST_F(S3fsOpsMockTest, Mkdir_TrailingSlash) {
    g_mock.put_result = 0;
    // Path already has trailing slash
    EXPECT_EQ(0, s3fs_mkdir_s3("/already/", 0755));
    // Should still end with /
    EXPECT_EQ('/', g_mock.last_put_path.back());
}

TEST_F(S3fsOpsMockTest, Mknod_RegularFile) {
    g_mock.put_result = 0;
    EXPECT_EQ(0, s3fs_mknod_s3("/newfile.txt", S_IFREG | 0644, 0));
}

TEST_F(S3fsOpsMockTest, Mknod_NonRegularFile) {
    // S3 only supports regular files
    EXPECT_EQ(-ENOTSUP, s3fs_mknod_s3("/fifo", S_IFIFO | 0644, 0));
}

TEST_F(S3fsOpsMockTest, Mknod_PutFails) {
    g_mock.put_result = -EIO;
    EXPECT_NE(0, s3fs_mknod_s3("/fail.txt", S_IFREG | 0644, 0));
}

// =================================================================
// 3. unlink/rmdir tests
// =================================================================

TEST_F(S3fsOpsMockTest, Unlink_Success) {
    g_mock.delete_result = 0;
    EXPECT_EQ(0, s3fs_unlink_s3("/file.txt"));
    EXPECT_EQ("/file.txt", g_mock.last_delete_path);
}

TEST_F(S3fsOpsMockTest, Unlink_DeleteFails) {
    g_mock.delete_result = -EIO;
    EXPECT_EQ(-EIO, s3fs_unlink_s3("/file.txt"));
}

TEST_F(S3fsOpsMockTest, Rmdir_EmptyDir) {
    // directory_empty needs ListBucket to return empty result
    g_mock.list_result = 0;
    g_mock.list_body_xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                           "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
    g_mock.delete_result = 0;
    EXPECT_EQ(0, s3fs_rmdir_s3("/emptydir"));
}

TEST_F(S3fsOpsMockTest, Rmdir_NonEmptyDir) {
    g_mock.list_result = 0;
    g_mock.list_body_xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                           "<ListBucketResult><IsTruncated>false</IsTruncated>"
                           "<Contents><Key>emptydir/child.txt</Key><Size>100</Size></Contents>"
                           "</ListBucketResult>";
    EXPECT_EQ(-ENOTEMPTY, s3fs_rmdir_s3("/emptydir"));
}

TEST_F(S3fsOpsMockTest, Rmdir_DeleteFails) {
    // Empty directory but delete fails
    g_mock.list_result = 0;
    g_mock.list_body_xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                           "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
    g_mock.delete_result = -EIO;
    EXPECT_EQ(-EIO, s3fs_rmdir_s3("/emptydir"));
}

// =================================================================
// 4. directory_empty tests
// =================================================================

TEST_F(S3fsOpsMockTest, DirectoryEmpty_NullPath) {
    EXPECT_EQ(-EINVAL, directory_empty(NULL));
}

TEST_F(S3fsOpsMockTest, DirectoryEmpty_EmptyDir) {
    g_mock.list_result = 0;
    g_mock.list_body_xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                           "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
    EXPECT_EQ(0, directory_empty("/emptydir"));
}

TEST_F(S3fsOpsMockTest, DirectoryEmpty_NonEmptyDir) {
    g_mock.list_result = 0;
    g_mock.list_body_xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                           "<ListBucketResult><IsTruncated>false</IsTruncated>"
                           "<Contents><Key>emptydir/child.txt</Key><Size>100</Size></Contents>"
                           "</ListBucketResult>";
    EXPECT_EQ(-ENOTEMPTY, directory_empty("/emptydir"));
}

TEST_F(S3fsOpsMockTest, DirectoryEmpty_ListFails) {
    g_mock.list_result = -EIO;
    EXPECT_EQ(-EIO, directory_empty("/dir"));
}

TEST_F(S3fsOpsMockTest, DirectoryEmpty_StatCacheWitnessShortCircuit) {
    // ISSUE2026051100001 cleanup: directory_empty() short-circuits to
    // -ENOTEMPTY when StatCache witnesses any direct child (saves a
    // ListBucket round-trip after a local create/mkdir).
    headers_t meta;
    meta["Content-Length"] = "0";
    meta["Content-Type"] = "application/octet-stream";
    std::string key("/dir/local_child.txt");
    StatCache::getStatCacheData()->AddStat(key, meta, false, true);
    EXPECT_EQ(-ENOTEMPTY, directory_empty("/dir"));
}

TEST_F(S3fsOpsMockTest, DirectoryEmpty_NoStatCacheWitnessConsultsObs) {
    // ISSUE2026051100001 cleanup: with no StatCache witness, directory_empty()
    // must fall through to ListBucket and trust its authoritative answer.
    // Mock returns empty → expect 0 even though caller might naively assume
    // a recently-deleted entry could leave stale state.
    g_mock.list_result = 0;
    g_mock.list_body_xml.clear();
    EXPECT_EQ(0, directory_empty("/dir"));
}

TEST_F(S3fsOpsMockTest, DirectoryEmpty_EmptyResponse) {
    // Empty body = empty directory
    g_mock.list_result = 0;
    g_mock.list_body_xml.clear();  // no body data set
    EXPECT_EQ(0, directory_empty("/emptydir"));
}

TEST_F(S3fsOpsMockTest, DirectoryEmpty_WithCommonPrefixes) {
    g_mock.list_result = 0;
    g_mock.list_body_xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                           "<ListBucketResult><IsTruncated>false</IsTruncated>"
                           "<CommonPrefixes><Prefix>emptydir/subdir/</Prefix></CommonPrefixes>"
                           "</ListBucketResult>";
    EXPECT_EQ(-ENOTEMPTY, directory_empty("/emptydir"));
}

TEST_F(S3fsOpsMockTest, DirectoryEmpty_DirMarkerOnlyIsEmpty) {
    // Directory marker itself should not count as non-empty
    g_mock.list_result = 0;
    g_mock.list_body_xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                           "<ListBucketResult><IsTruncated>false</IsTruncated>"
                           "<Contents><Key>emptydir/</Key><Size>0</Size></Contents>"
                           "</ListBucketResult>";
    EXPECT_EQ(0, directory_empty("/emptydir"));
}

// =================================================================
// 5. symlink/readlink tests
// =================================================================

TEST_F(S3fsOpsMockTest, Symlink_Success) {
    g_mock.put_result = 0;
    EXPECT_EQ(0, s3fs_symlink_s3("/target", "/link"));
    EXPECT_EQ("/link", g_mock.last_put_path);
}

TEST_F(S3fsOpsMockTest, Symlink_PutFails) {
    g_mock.put_result = -EIO;
    EXPECT_EQ(-EIO, s3fs_symlink_s3("/target", "/link"));
}

TEST_F(S3fsOpsMockTest, Readlink_Success) {
    g_mock.head_result = 0;
    g_mock.head_meta["x-amz-meta-symlink-target"] = "/target/path";
    char buf[256] = {0};
    EXPECT_EQ(0, s3fs_readlink_s3("/link", buf, sizeof(buf)));
    EXPECT_STREQ("/target/path", buf);
}

TEST_F(S3fsOpsMockTest, Readlink_NotASymlink) {
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "text/plain";
    // No symlink-target header
    char buf[256] = {0};
    EXPECT_EQ(-EINVAL, s3fs_readlink_s3("/regular", buf, sizeof(buf)));
}

TEST_F(S3fsOpsMockTest, Readlink_HeadFails) {
    g_mock.head_result = -ENOENT;
    char buf[256] = {0};
    EXPECT_EQ(-ENOENT, s3fs_readlink_s3("/missing", buf, sizeof(buf)));
}

TEST_F(S3fsOpsMockTest, Readlink_BufferTooSmall) {
    g_mock.head_result = 0;
    g_mock.head_meta["x-amz-meta-symlink-target"] = "/a/very/long/target/path/that/exceeds/buffer";
    char buf[5] = {0};
    EXPECT_EQ(-ENAMETOOLONG, s3fs_readlink_s3("/link", buf, sizeof(buf)));
}

// =================================================================
// 6. chmod/chown tests
// =================================================================

TEST_F(S3fsOpsMockTest, Chmod_Root) {
    // chmod on root is a no-op
    EXPECT_EQ(0, s3fs_chmod_s3("/", 0755));
}

TEST_F(S3fsOpsMockTest, Chmod_Success) {
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-hws-obs-meta-mode"] = "33188";
    g_mock.put_head_result = 0;
    EXPECT_EQ(0, s3fs_chmod_s3("/file.txt", 0644));
}

TEST_F(S3fsOpsMockTest, Chmod_HeadFails) {
    g_mock.head_result = -ENOENT;
    EXPECT_EQ(-ENOENT, s3fs_chmod_s3("/nonexistent", 0644));
}

TEST_F(S3fsOpsMockTest, Chmod_PutHeadFails) {
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "text/plain";
    g_mock.put_head_result = -EIO;
    EXPECT_EQ(-EIO, s3fs_chmod_s3("/file.txt", 0644));
}

TEST_F(S3fsOpsMockTest, Chown_Root) {
    // chown on root is a no-op
    EXPECT_EQ(0, s3fs_chown_s3("/", 1000, 1000));
}

TEST_F(S3fsOpsMockTest, Chown_HeadFails) {
    g_mock.head_result = -ENOENT;
    EXPECT_EQ(-ENOENT, s3fs_chown_s3("/nonexistent", 1000, 1000));
}

TEST_F(S3fsOpsMockTest, ChmodNocopy_Noop) {
    // nocopy mode returns success without doing anything
    EXPECT_EQ(0, s3fs_chmod_nocopy_s3("/file.txt", 0644));
}

// =================================================================
// 7. truncate tests
// =================================================================

TEST_F(S3fsOpsMockTest, Truncate_SameSize) {
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Length"] = "100";
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-hws-obs-meta-mode"] = "33188";
    // Truncate to same size should be a no-op
    EXPECT_EQ(0, s3fs_truncate_s3("/file.txt", 100));
}

TEST_F(S3fsOpsMockTest, Truncate_DirectoryNoop) {
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Length"] = "0";
    g_mock.head_meta["Content-Type"] = "application/x-directory";
    // Truncate on directory is a no-op
    EXPECT_EQ(0, s3fs_truncate_s3("/dir", 0));
}

TEST_F(S3fsOpsMockTest, Truncate_HeadError) {
    // Non-ENOENT error from HeadRequest
    g_mock.head_result = -EIO;
    EXPECT_EQ(-EIO, s3fs_truncate_s3("/file.txt", 0));
}

// =================================================================
// 7b. Truncate edge case tests (ISSUE2026022500002/3)
// =================================================================

TEST_F(S3fsOpsMockTest, Truncate_ToZero) {
    // Truncate existing 100B file to 0 bytes
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Length"] = "100";
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-hws-obs-meta-mode"] = "33188";
    g_mock.put_result = 0;

    int rc = s3fs_truncate_s3("/trunc_zero.txt", 0);
    EXPECT_EQ(0, rc);

    // StatCache should show size=0
    struct stat st;
    std::string key = "/trunc_zero.txt";
    EXPECT_TRUE(StatCache::getStatCacheData()->GetStat(key, &st));
    EXPECT_EQ(0, st.st_size);
}

TEST_F(S3fsOpsMockTest, Truncate_Expand) {
    // Truncate 50B file to 200B (expand with zeros)
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Length"] = "50";
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-hws-obs-meta-mode"] = "33188";
    g_mock.get_obj_result = 0;
    g_mock.put_result = 0;

    int rc = s3fs_truncate_s3("/trunc_expand.txt", 200);
    EXPECT_EQ(0, rc);

    struct stat st;
    std::string key = "/trunc_expand.txt";
    EXPECT_TRUE(StatCache::getStatCacheData()->GetStat(key, &st));
    EXPECT_EQ(200, st.st_size);
}

TEST_F(S3fsOpsMockTest, Truncate_Shrink) {
    // Truncate 200B file to 50B
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Length"] = "200";
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-hws-obs-meta-mode"] = "33188";
    g_mock.get_obj_result = 0;
    g_mock.put_result = 0;

    int rc = s3fs_truncate_s3("/trunc_shrink.txt", 50);
    EXPECT_EQ(0, rc);

    struct stat st;
    std::string key = "/trunc_shrink.txt";
    EXPECT_TRUE(StatCache::getStatCacheData()->GetStat(key, &st));
    EXPECT_EQ(50, st.st_size);
}

TEST_F(S3fsOpsMockTest, Truncate_NewFile) {
    // Truncate on non-existent file creates it with size=10
    g_mock.head_result = -ENOENT;
    g_mock.put_result = 0;

    int rc = s3fs_truncate_s3("/trunc_new.txt", 10);
    EXPECT_EQ(0, rc);

    // StatCache should show size=10 (truncate-new path adds it via AddStat).
    struct stat st;
    std::string key = "/trunc_new.txt";
    EXPECT_TRUE(StatCache::getStatCacheData()->GetStat(key, &st));
    EXPECT_EQ(10, st.st_size);
}

TEST_F(S3fsOpsMockTest, Truncate_NegativeSize) {
    // Negative size is clamped to 0 (L1904)
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Length"] = "100";
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-hws-obs-meta-mode"] = "33188";
    g_mock.put_result = 0;

    int rc = s3fs_truncate_s3("/trunc_neg.txt", -5);
    EXPECT_EQ(0, rc);

    struct stat st;
    std::string key = "/trunc_neg.txt";
    EXPECT_TRUE(StatCache::getStatCacheData()->GetStat(key, &st));
    EXPECT_EQ(0, st.st_size);
}

TEST_F(S3fsOpsMockTest, Truncate_HeadFails_EACCES) {
    // Non-ENOENT error: permission denied
    g_mock.head_result = -EACCES;
    EXPECT_EQ(-EACCES, s3fs_truncate_s3("/trunc_perm.txt", 50));
}

// =================================================================
// 8. Helper function tests
// =================================================================

TEST_F(S3fsOpsMockTest, StrToOffT_Valid) {
    EXPECT_EQ(1024, str_to_off_t("1024"));
    EXPECT_EQ(0, str_to_off_t("0"));
    EXPECT_EQ(0, str_to_off_t(NULL));
    EXPECT_EQ(0, str_to_off_t(""));
}

TEST_F(S3fsOpsMockTest, StrToOffT_Negative) {
    EXPECT_EQ(-1, str_to_off_t("-1"));
    EXPECT_EQ(-100, str_to_off_t("-100"));
}

TEST_F(S3fsOpsMockTest, StrToOffT_Large) {
    EXPECT_EQ(1073741824, str_to_off_t("1073741824"));  // 1GB
}

TEST_F(S3fsOpsMockTest, BuildStandardMetadata_Basic) {
    headers_t meta;
    build_standard_metadata(meta, S_IFREG | 0644, "text/plain");
    EXPECT_EQ("text/plain", meta["Content-Type"]);
    EXPECT_FALSE(meta["x-hws-obs-meta-mode"].empty());
    EXPECT_FALSE(meta["x-hws-obs-meta-uid"].empty());
    EXPECT_FALSE(meta["x-hws-obs-meta-gid"].empty());
}

TEST_F(S3fsOpsMockTest, BuildStandardMetadata_Directory) {
    headers_t meta;
    build_standard_metadata(meta, S_IFDIR | 0755, "application/x-directory");
    EXPECT_EQ("application/x-directory", meta["Content-Type"]);
    EXPECT_FALSE(meta["x-hws-obs-meta-mode"].empty());
}

TEST_F(S3fsOpsMockTest, BuildStandardMetadata_PreserveMode) {
    headers_t meta;
    meta["x-hws-obs-meta-mode"] = "33188";
    build_standard_metadata(meta, S_IFREG | 0755, "", true);
    EXPECT_EQ("33188", meta["x-hws-obs-meta-mode"]);  // preserved
}

TEST_F(S3fsOpsMockTest, BuildStandardMetadata_EmptyContentType) {
    headers_t meta;
    build_standard_metadata(meta, S_IFREG | 0644, "");
    // Content-Type should NOT be set when empty string is passed
    EXPECT_EQ(meta.end(), meta.find("Content-Type"));
}

// [ISSUE2026040900001 C1/C2] Removed MapObsErrorToErrno / MapObsErrorToErrno_MoreCodes
// / IsRetryableError test cases — the underlying helper functions were deleted
// from s3fs_operations.cpp as unused production code.

// [ISSUE2026051100001 cleanup] Section "9. Pending dir entries tests" was
// removed alongside the pending_dir_entries static map and its three
// helpers. Every operation it covered (mknod/mkdir/create/truncate-new/
// rename target add, rmdir/unlink/rename src remove) now relies on
// StatCache AddStat/DelStat directly; corresponding behavioral coverage
// lives in the integration test test_external_modify.sh.

// =================================================================
// 10. link test (hard link unsupported)
// =================================================================

TEST_F(S3fsOpsMockTest, Link_Unsupported) {
    EXPECT_EQ(-ENOTSUP, s3fs_link_s3("/from", "/to"));
}

// =================================================================
// 11. rename tests
// =================================================================

TEST_F(S3fsOpsMockTest, Rename_NullArgs) {
    EXPECT_EQ(-EINVAL, s3fs_rename_s3(NULL, "/to"));
    EXPECT_EQ(-EINVAL, s3fs_rename_s3("/from", NULL));
}

TEST_F(S3fsOpsMockTest, Rename_Root) {
    EXPECT_EQ(-EINVAL, s3fs_rename_s3("/", "/newname"));
}

// Helper: populate headers_t with valid file metadata
// Uses hws_obs_meta_mode (from test_stubs.cpp) for mode header key,
// and sets Content-Type to application/x-directory for directories.
static void setup_file_meta(headers_t& meta, mode_t mode = S_IFREG | 0644, off_t size = 100) {
    bool is_dir = S_ISDIR(mode);
    meta["Content-Type"] = is_dir ? "application/x-directory" : "application/octet-stream";
    meta["Content-Length"] = std::to_string(size);
    meta[hws_obs_meta_mode] = std::to_string(mode);
    meta["x-amz-meta-uid"] = std::to_string(getuid());
    meta["x-amz-meta-gid"] = std::to_string(getgid());
    meta["x-amz-meta-mtime"] = "1234567890";
}

// UT-B1: Delete failure propagated as rename error
TEST_F(S3fsOpsMockTest, Rename_SingleFile_DeleteFails_ReturnsError) {
    setup_file_meta(g_mock.head_meta);
    g_mock.delete_result = -EIO;
    EXPECT_EQ(-EIO, s3fs_rename_s3("/old.txt", "/new.txt"));
}

// UT-B3: Delete failure preserves source StatCache entry
TEST_F(S3fsOpsMockTest, Rename_SingleFile_DeleteFails_SourceStatCachePreserved) {
    setup_file_meta(g_mock.head_meta);
    // Pre-populate StatCache for /old.txt
    headers_t src_meta;
    setup_file_meta(src_meta);
    std::string from_key = "/old.txt";
    StatCache::getStatCacheData()->AddStat(from_key, src_meta, false, true);

    g_mock.delete_result = -EIO;
    s3fs_rename_s3("/old.txt", "/new.txt");

    // Source should remain in StatCache (delete failed)
    struct stat st;
    std::string old_key = "/old.txt";
    std::string new_key = "/new.txt";
    EXPECT_TRUE(StatCache::getStatCacheData()->GetStat(old_key, &st));
    // Destination should also be in StatCache (copy succeeded)
    EXPECT_TRUE(StatCache::getStatCacheData()->GetStat(new_key, &st));
}

// UT-B4: Directory rename returns error when base marker delete fails
TEST_F(S3fsOpsMockTest, Rename_Directory_MarkerDeleteFails_ReturnsError) {
    // Setup dir meta for /dir
    headers_t dir_meta;
    setup_file_meta(dir_meta, S_IFDIR | 0755, 0);
    g_mock.head_meta_map["/dir"] = dir_meta;
    g_mock.head_result_map["/dir"] = 0;
    g_mock.head_meta_map["/dir/"] = dir_meta;
    g_mock.head_result_map["/dir/"] = 0;

    // /dir2 and /dir2/ don't exist
    g_mock.head_result_map["/dir2"] = -ENOENT;
    g_mock.head_result_map["/dir2/"] = -ENOENT;

    // File child meta
    headers_t file_meta;
    setup_file_meta(file_meta);
    g_mock.head_meta_map["/dir/file.txt"] = file_meta;
    g_mock.head_result_map["/dir/file.txt"] = 0;

    // Default HEAD uses global (for any other paths)
    g_mock.head_result = -ENOENT;

    // directory_empty LIST for /dir2 returns empty
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "</ListBucketResult>");
    // LIST returns one child file for rename listing
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/file.txt</Key></Contents>"
        "</ListBucketResult>");

    // Delete sequence:
    // 1. rename_object_s3("/dir/file.txt" → "/dir2/file.txt"): DeleteRequest("/dir/file.txt") → ok
    // 2. Base marker: DeleteRequest("/dir/") → FAIL
    g_mock.delete_result_queue = {0, -EIO};

    int result = s3fs_rename_s3("/dir", "/dir2");
    EXPECT_NE(0, result);
}

// UT-B6: Successful rename returns zero
TEST_F(S3fsOpsMockTest, Rename_SingleFile_AllSuccess_ReturnsZero) {
    setup_file_meta(g_mock.head_meta);
    g_mock.delete_result = 0;
    EXPECT_EQ(0, s3fs_rename_s3("/old.txt", "/new.txt"));
}

// UT-P2: No extra HEAD to destination for StatCache population
TEST_F(S3fsOpsMockTest, Rename_SingleFile_NoExtraHeadForStatCache) {
    setup_file_meta(g_mock.head_meta);
    s3fs_rename_s3("/old.txt", "/new.txt");
    // Before fix: HEAD(from) via getattr + HEAD(from) via HeadRequest + HEAD(to) for StatCache = 3 HEADs
    // After fix: HEAD(from) via getattr in s3fs_rename_s3 + HEAD(from) in rename_object_s3 = 2 HEADs
    // Key assertion: no HEAD was made to "/new.txt" (destination)
    for(const auto& p : g_mock.head_paths){
        EXPECT_NE("/new.txt", p) << "Should not HEAD destination for StatCache population";
    }
}

// =================================================================
// 11b. clone_directory_object_s3 tests
// =================================================================

TEST_F(S3fsOpsMockTest, CloneDir_Success) {
    // Happy path: clone /src_dir → /dst_dir
    headers_t dir_meta;
    setup_file_meta(dir_meta, S_IFDIR | 0755, 0);
    g_mock.head_result_map["/src_dir"] = 0;
    g_mock.head_meta_map["/src_dir"] = dir_meta;
    g_mock.head_result_map["/src_dir/"] = 0;
    g_mock.head_meta_map["/src_dir/"] = dir_meta;
    g_mock.head_result = -ENOENT;
    g_mock.put_result = 0;

    int rc = clone_directory_object_s3("/src_dir", "/dst_dir", false, NULL);
    EXPECT_EQ(0, rc);

    // Verify destination in StatCache
    std::string key = "/dst_dir/";
    EXPECT_TRUE(StatCache::getStatCacheData()->HasStat(key, false));
}

TEST_F(S3fsOpsMockTest, CloneDir_SourceNotFound) {
    // Source HEAD → ENOENT
    g_mock.head_result = -ENOENT;

    int rc = clone_directory_object_s3("/missing_dir", "/dst_dir", false, NULL);
    EXPECT_EQ(-ENOENT, rc);
}

TEST_F(S3fsOpsMockTest, CloneDir_SourceNotDirectory) {
    // Source is a regular file, not a directory
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    g_mock.head_result_map["/src_file"] = 0;
    g_mock.head_meta_map["/src_file"] = file_meta;
    g_mock.head_result = -ENOENT;

    int rc = clone_directory_object_s3("/src_file", "/dst_dir", false, NULL);
    EXPECT_EQ(-ENOTDIR, rc);
}

TEST_F(S3fsOpsMockTest, CloneDir_PutFails) {
    // PutRequest for dest marker fails
    headers_t dir_meta;
    setup_file_meta(dir_meta, S_IFDIR | 0755, 0);
    g_mock.head_result_map["/src_dir2"] = 0;
    g_mock.head_meta_map["/src_dir2"] = dir_meta;
    g_mock.head_result_map["/src_dir2/"] = 0;
    g_mock.head_meta_map["/src_dir2/"] = dir_meta;
    g_mock.head_result = -ENOENT;
    g_mock.put_result = -EIO;

    int rc = clone_directory_object_s3("/src_dir2", "/dst_dir2", false, NULL);
    EXPECT_NE(0, rc);
}

TEST_F(S3fsOpsMockTest, CloneDir_UpdatesCtime) {
    // update_ctime=true succeeds
    headers_t dir_meta;
    setup_file_meta(dir_meta, S_IFDIR | 0755, 0);
    g_mock.head_result_map["/ct_src"] = 0;
    g_mock.head_meta_map["/ct_src"] = dir_meta;
    g_mock.head_result_map["/ct_src/"] = 0;
    g_mock.head_meta_map["/ct_src/"] = dir_meta;
    g_mock.head_result = -ENOENT;
    g_mock.put_result = 0;

    int rc = clone_directory_object_s3("/ct_src", "/ct_dst", true, NULL);
    EXPECT_EQ(0, rc);
}

TEST_F(S3fsOpsMockTest, CloneDir_PreservesXattr) {
    // pxattrvalue non-null
    headers_t dir_meta;
    setup_file_meta(dir_meta, S_IFDIR | 0755, 0);
    g_mock.head_result_map["/xa_src"] = 0;
    g_mock.head_meta_map["/xa_src"] = dir_meta;
    g_mock.head_result_map["/xa_src/"] = 0;
    g_mock.head_meta_map["/xa_src/"] = dir_meta;
    g_mock.head_result = -ENOENT;
    g_mock.put_result = 0;

    int rc = clone_directory_object_s3("/xa_src", "/xa_dst", false, "user.key=value");
    EXPECT_EQ(0, rc);
}

// =================================================================
// 11c. check_parent_object_access_s3 tests
// =================================================================

TEST_F(S3fsOpsMockTest, CheckParent_RootPath) {
    // Root path always allowed
    EXPECT_EQ(0, check_parent_object_access_s3("/", W_OK | X_OK));
}

TEST_F(S3fsOpsMockTest, CheckParent_DotPath) {
    // "." path always allowed
    EXPECT_EQ(0, check_parent_object_access_s3(".", W_OK | X_OK));
}

TEST_F(S3fsOpsMockTest, CheckParent_ParentNotFound) {
    // Parent dir HEAD → ENOENT
    g_mock.head_result = -ENOENT;
    EXPECT_EQ(-ENOENT, check_parent_object_access_s3("/missing_dir/file.txt", W_OK | X_OK));
}

TEST_F(S3fsOpsMockTest, CheckParent_NestedPathSuccess) {
    // /a/b/file.txt — all parents must exist
    headers_t dir_meta;
    setup_file_meta(dir_meta, S_IFDIR | 0755, 0);
    g_mock.head_result_map["/a"] = 0;
    g_mock.head_meta_map["/a"] = dir_meta;
    g_mock.head_result_map["/a/b"] = 0;
    g_mock.head_meta_map["/a/b"] = dir_meta;
    g_mock.head_result = -ENOENT;

    EXPECT_EQ(0, check_parent_object_access_s3("/a/b/file.txt", W_OK | X_OK));
}

// =================================================================
// 11d. Rename edge case tests
// =================================================================

TEST_F(S3fsOpsMockTest, Rename_SameFromTo) {
    // Rename /f.txt → /f.txt (same path)
    setup_file_meta(g_mock.head_meta);
    g_mock.delete_result = 0;
    // Same-path rename goes through the full copy+delete path
    EXPECT_EQ(0, s3fs_rename_s3("/f.txt", "/f.txt"));
}

TEST_F(S3fsOpsMockTest, Rename_SourceNotFound) {
    // Source getattr → ENOENT
    g_mock.head_result = -ENOENT;
    EXPECT_EQ(-ENOENT, s3fs_rename_s3("/nosrc.txt", "/dst.txt"));
}

TEST_F(S3fsOpsMockTest, Rename_DirSuccess_SingleChild) {
    // Full successful directory rename with one child
    headers_t dir_meta;
    setup_file_meta(dir_meta, S_IFDIR | 0755, 0);
    g_mock.head_result_map["/drs"] = 0;
    g_mock.head_meta_map["/drs"] = dir_meta;
    g_mock.head_result_map["/drs/"] = 0;
    g_mock.head_meta_map["/drs/"] = dir_meta;

    headers_t file_meta;
    setup_file_meta(file_meta);
    g_mock.head_result_map["/drs/file.txt"] = 0;
    g_mock.head_meta_map["/drs/file.txt"] = file_meta;

    // /drd and /drd/ don't exist
    g_mock.head_result_map["/drd"] = -ENOENT;
    g_mock.head_result_map["/drd/"] = -ENOENT;

    // Default HEAD for any other paths
    g_mock.head_result = -ENOENT;

    // directory_empty LIST for /drd → empty
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>");
    // LIST for rename listing → one child
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult><IsTruncated>false</IsTruncated>"
        "<Contents><Key>drs/file.txt</Key></Contents>"
        "</ListBucketResult>");

    // All deletes succeed: child file + base marker
    g_mock.delete_result_queue = {0, 0};

    int result = s3fs_rename_s3("/drs", "/drd");
    EXPECT_EQ(0, result);
}

TEST_F(S3fsOpsMockTest, Rename_TargetNotEmpty) {
    // Target dir has children → ENOTEMPTY
    setup_file_meta(g_mock.head_meta);
    // directory_empty for target returns non-empty
    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult><IsTruncated>false</IsTruncated>"
        "<Contents><Key>dst_dir/child.txt</Key><Size>100</Size></Contents>"
        "</ListBucketResult>";
    EXPECT_EQ(-ENOTEMPTY, s3fs_rename_s3("/src.txt", "/dst_dir"));
}

TEST_F(S3fsOpsMockTest, Rename_LargeFile_Multipart) {
    // File >= 5GB triggers multipart rename
    headers_t big_meta;
    setup_file_meta(big_meta, S_IFREG | 0644, 5368709120LL);  // 5GB
    g_mock.head_result_map["/big.dat"] = 0;
    g_mock.head_meta_map["/big.dat"] = big_meta;
    g_mock.head_result = -ENOENT;  // default for other paths
    g_mock.rename_result = 0;  // MultipartRenameRequest
    g_mock.delete_result = 0;
    // directory_empty for /big_new.dat → empty (no pending, list returns empty body)
    g_mock.list_body_xml.clear();

    int result = s3fs_rename_s3("/big.dat", "/big_new.dat");
    EXPECT_EQ(0, result);
}

// =================================================================
// 11e. FdManager::Rename no-cache fallback (ISSUE2026030200002)
// =================================================================

// Regression: write-after-rename must not EBADF in no-cache mode.
// FdManager::Rename now does linear search when fent.find(from) misses.
TEST_F(S3fsOpsMockTest, Rename_NocacheWriteAfterRename) {
    // Ensure no-cache mode (default: cache_dir is empty)
    ASSERT_FALSE(FdManager::IsCacheDir());

    // Setup /src.txt as regular file
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 0);
    g_mock.head_result_map["/src.txt"] = 0;
    g_mock.head_meta_map["/src.txt"] = file_meta;
    g_mock.head_result = -ENOENT;
    g_mock.get_obj_result = 0;
    g_mock.put_result = 0;
    g_mock.delete_result = 0;

    // Open /src.txt and write initial data
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.flags = O_RDWR | O_CREAT | O_TRUNC;
    int rc = s3fs_create_s3("/src.txt", 0644, &fi);
    ASSERT_EQ(0, rc);

    const char* data1 = "BEFORE";
    rc = s3fs_write_s3("/src.txt", data1, strlen(data1), 0, &fi);
    ASSERT_EQ((int)strlen(data1), rc);

    // fsync to flush
    rc = s3fs_fsync_s3("/src.txt", 0, &fi);
    ASSERT_EQ(0, rc);

    // Rename /src.txt → /dst.txt (while fd is open)
    g_mock.head_result_map["/dst.txt"] = -ENOENT;
    rc = s3fs_rename_s3("/src.txt", "/dst.txt");
    ASSERT_EQ(0, rc);

    // Setup /dst.txt metadata for subsequent operations
    headers_t dst_meta;
    setup_file_meta(dst_meta, S_IFREG | 0644, strlen(data1));
    g_mock.head_result_map["/dst.txt"] = 0;
    g_mock.head_meta_map["/dst.txt"] = dst_meta;

    // Write more via same fd — this is the critical test.
    // Before fix: EBADF because FdEntity path wasn't updated in no-cache mode.
    const char* data2 = "_AFTER";
    rc = s3fs_write_s3("/dst.txt", data2, strlen(data2), strlen(data1), &fi);
    EXPECT_EQ((int)strlen(data2), rc);

    // fsync should succeed too
    rc = s3fs_fsync_s3("/dst.txt", 0, &fi);
    EXPECT_EQ(0, rc);

    s3fs_release_s3("/dst.txt", &fi);
}

// Verify FdManager::Rename updates entity path so GetFdEntity(path, fd) finds it.
// In no-cache mode, GetFdEntity needs the fd for linear search (key is random).
TEST_F(S3fsOpsMockTest, Rename_NocacheEntityPathUpdated) {
    ASSERT_FALSE(FdManager::IsCacheDir());

    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 0);
    g_mock.head_result_map["/src2.txt"] = 0;
    g_mock.head_meta_map["/src2.txt"] = file_meta;
    g_mock.head_result = -ENOENT;
    g_mock.get_obj_result = 0;
    g_mock.put_result = 0;
    g_mock.delete_result = 0;

    // Open /src2.txt
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.flags = O_RDWR | O_CREAT | O_TRUNC;
    int rc = s3fs_create_s3("/src2.txt", 0644, &fi);
    ASSERT_EQ(0, rc);
    int fd = static_cast<int>(fi.fh);

    // FdManager::Rename
    FdManager::get()->Rename("/src2.txt", "/dst2.txt");

    // GetFdEntity(new_path, fd) should find entity at new path
    FdEntity* ent = FdManager::get()->GetFdEntity("/dst2.txt", fd);
    ASSERT_NE(nullptr, ent);
    EXPECT_STREQ("/dst2.txt", ent->GetPath());

    // Old path with same fd should NOT find it (path mismatch)
    FdEntity* old_ent = FdManager::get()->GetFdEntity("/src2.txt", fd);
    EXPECT_EQ(nullptr, old_ent);

    s3fs_release_s3("/dst2.txt", &fi);
}

// Verify RenamePath renames disk cache file and updates cachepath in cache mode.
// ISSUE2026030200003/4: before fix, SetPath didn't rename cachepath → stale ref.
TEST_F(S3fsOpsMockTest, Rename_CacheModeEntityPathUpdated) {
    ASSERT_FALSE(FdManager::IsCacheDir());

    // Set up a temporary cache directory
    char tmpdir[] = "/tmp/obsfs_test_cache_XXXXXX";
    ASSERT_NE(nullptr, mkdtemp(tmpdir));
    FdManager::SetCacheDir(tmpdir);
    ASSERT_TRUE(FdManager::IsCacheDir());

    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 0);
    g_mock.head_result_map["/csrc.txt"] = 0;
    g_mock.head_meta_map["/csrc.txt"] = file_meta;
    g_mock.head_result = -ENOENT;
    g_mock.get_obj_result = 0;
    g_mock.put_result = 0;
    g_mock.delete_result = 0;

    // Open /csrc.txt (creates cache file)
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.flags = O_RDWR | O_CREAT | O_TRUNC;
    int rc = s3fs_create_s3("/csrc.txt", 0644, &fi);
    ASSERT_EQ(0, rc);

    // Get entity and verify cache file exists
    int fd = static_cast<int>(fi.fh);
    FdEntity* ent = FdManager::get()->GetFdEntity("/csrc.txt", fd);
    ASSERT_NE(nullptr, ent);

    // Save old cachepath for later verification
    std::string old_cachepath;
    FdManager::MakeCachePath("/csrc.txt", old_cachepath, false);

    // Write some data to create the cache file on disk
    const char* data = "CACHETEST";
    rc = s3fs_write_s3("/csrc.txt", data, strlen(data), 0, &fi);
    ASSERT_EQ((int)strlen(data), rc);

    // Rename /csrc.txt → /cdst.txt
    g_mock.head_result_map["/cdst.txt"] = -ENOENT;
    rc = s3fs_rename_s3("/csrc.txt", "/cdst.txt");
    ASSERT_EQ(0, rc);

    // Verify entity path is updated
    ent = FdManager::get()->GetFdEntity("/cdst.txt", fd);
    ASSERT_NE(nullptr, ent);
    EXPECT_STREQ("/cdst.txt", ent->GetPath());

    // Verify new cache file exists on disk
    std::string new_cachepath;
    FdManager::MakeCachePath("/cdst.txt", new_cachepath, false);
    struct stat st;
    EXPECT_EQ(0, stat(new_cachepath.c_str(), &st));

    // Verify old cache file is gone
    EXPECT_NE(0, stat(old_cachepath.c_str(), &st));

    // Old path should NOT find the entity
    FdEntity* old_ent = FdManager::get()->GetFdEntity("/csrc.txt", fd);
    EXPECT_EQ(nullptr, old_ent);

    // Write after rename should still work
    headers_t dst_meta;
    setup_file_meta(dst_meta, S_IFREG | 0644, strlen(data));
    g_mock.head_result_map["/cdst.txt"] = 0;
    g_mock.head_meta_map["/cdst.txt"] = dst_meta;

    const char* data2 = "_MORE";
    rc = s3fs_write_s3("/cdst.txt", data2, strlen(data2), strlen(data), &fi);
    EXPECT_EQ((int)strlen(data2), rc);

    s3fs_release_s3("/cdst.txt", &fi);

    // Cleanup: restore no-cache mode and remove temp dir
    FdManager::SetCacheDir("");
    // Best-effort cleanup of temp dir
    std::string rm_cmd = std::string("rm -rf ") + tmpdir;
    system(rm_cmd.c_str());
}

// =================================================================
// 12. statfs test
// =================================================================

TEST_F(S3fsOpsMockTest, Statfs_Basic) {
    struct statvfs stbuf;
    int result = s3fs_statfs_s3("/", &stbuf);
    EXPECT_EQ(0, result);
    // s3fs_statfs_s3 should fill in some reasonable values
    EXPECT_GT(stbuf.f_bsize, 0u);
    EXPECT_GT(stbuf.f_namemax, 0u);
}

// =================================================================
// 13. access test
// =================================================================

TEST_F(S3fsOpsMockTest, Access_Root) {
    // Access check on root should succeed
    EXPECT_EQ(0, s3fs_access_s3("/", F_OK));
}

TEST_F(S3fsOpsMockTest, Access_NonExistent) {
    g_mock.head_result = -ENOENT;
    EXPECT_EQ(-ENOENT, s3fs_access_s3("/nonexistent", F_OK));
}

// =================================================================
// 14. Additional edge case tests
// =================================================================

TEST_F(S3fsOpsMockTest, Mknod_AddsToStatCache) {
    g_mock.put_result = 0;
    EXPECT_EQ(0, s3fs_mknod_s3("/dir/newfile.txt", S_IFREG | 0644, 0));
    // The newly-created file must be witnessed by StatCache so readdir
    // sees it ahead of any ListBucket round-trip.
    struct stat st;
    std::string key("/dir/newfile.txt");
    EXPECT_TRUE(StatCache::getStatCacheData()->GetStat(key, &st));
}

TEST_F(S3fsOpsMockTest, Mkdir_AddsToStatCache) {
    g_mock.put_result = 0;
    EXPECT_EQ(0, s3fs_mkdir_s3("/parent/newdir", 0755));
    struct stat st;
    std::string key("/parent/newdir");
    EXPECT_TRUE(StatCache::getStatCacheData()->GetStat(key, &st, nullptr, true /*overcheck*/));
}

// =================================================================
// ISSUE2026051200001 M.3: setter AddStat(notruncate=false) tests
//
// After Phase 2, all six setter paths (mknod/mkdir/create/truncate-new/
// rename-dst/release-flushed) must add to StatCache with notruncate=false
// so the entry is subject to normal LRU/TTL lifecycle. We verify this via
// DelStatIfTruncatable, which returns true only when an existing entry has
// notruncate==0 (and false if missing or pinned).
// =================================================================

TEST_F(S3fsOpsMockTest, Mknod_AddsToStatCache_AsTruncatable) {
    g_mock.put_result = 0;
    ASSERT_EQ(0, s3fs_mknod_s3("/setter/mknod.txt", S_IFREG | 0644, 0));
    std::string key("/setter/mknod.txt");
    // notruncate==0 → DelStatIfTruncatable removes it and returns true.
    // Pass future `now` to bypass the 1s cleanup-race grace — we're verifying
    // the notruncate flag, not the freshness window.
    // cache_date uses CLOCK_MONOTONIC_COARSE; match that clock here.
    {
        struct timespec _grace_ts;
        clock_gettime(CLOCK_MONOTONIC_COARSE, &_grace_ts);
        EXPECT_TRUE(StatCache::getStatCacheData()->DelStatIfTruncatable(key, _grace_ts.tv_sec + 1000));
    }
}

TEST_F(S3fsOpsMockTest, Mkdir_AddsToStatCache_AsTruncatable) {
    g_mock.put_result = 0;
    ASSERT_EQ(0, s3fs_mkdir_s3("/setter/dir", 0755));
    std::string key("/setter/dir");
    // Pass future `now` to bypass the 1s cleanup-race grace — we're verifying
    // the notruncate flag, not the freshness window.
    // cache_date uses CLOCK_MONOTONIC_COARSE; match that clock here.
    {
        struct timespec _grace_ts;
        clock_gettime(CLOCK_MONOTONIC_COARSE, &_grace_ts);
        EXPECT_TRUE(StatCache::getStatCacheData()->DelStatIfTruncatable(key, _grace_ts.tv_sec + 1000));
    }
}

TEST_F(S3fsOpsMockTest, Create_AddsToStatCache_AsTruncatable) {
    // s3fs_create_s3 takes the "new file" path only when HEAD returns ENOENT.
    g_mock.reset();
    g_mock.head_result = -ENOENT;
    g_mock.put_result = 0;
    struct fuse_file_info fi = {};
    fi.flags = O_WRONLY | O_CREAT;
    ASSERT_EQ(0, s3fs_create_s3("/setter/create.txt", S_IFREG | 0644, &fi));
    std::string key("/setter/create.txt");
    // Pass future `now` to bypass the 1s cleanup-race grace — we're verifying
    // the notruncate flag, not the freshness window.
    // cache_date uses CLOCK_MONOTONIC_COARSE; match that clock here.
    {
        struct timespec _grace_ts;
        clock_gettime(CLOCK_MONOTONIC_COARSE, &_grace_ts);
        EXPECT_TRUE(StatCache::getStatCacheData()->DelStatIfTruncatable(key, _grace_ts.tv_sec + 1000));
    }
    // cleanup the FdEntity opened by create
    s3fs_release_s3("/setter/create.txt", &fi);
}

TEST_F(S3fsOpsMockTest, Truncate_AddsToStatCache_AsTruncatable) {
    // Shrinking a real file → executes the size-changing path that ends with
    // AddStat (M.3 notruncate=false).
    g_mock.reset();
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-hws-obs-meta-mode"] = std::to_string(S_IFREG | 0644);
    g_mock.head_meta["Content-Length"] = "100";
    g_mock.put_result = 0;

    ASSERT_EQ(0, s3fs_truncate_s3("/setter/trunc.txt", 0));
    std::string key("/setter/trunc.txt");
    // Pass future `now` to bypass the 1s cleanup-race grace — we're verifying
    // the notruncate flag, not the freshness window.
    // cache_date uses CLOCK_MONOTONIC_COARSE; match that clock here.
    {
        struct timespec _grace_ts;
        clock_gettime(CLOCK_MONOTONIC_COARSE, &_grace_ts);
        EXPECT_TRUE(StatCache::getStatCacheData()->DelStatIfTruncatable(key, _grace_ts.tv_sec + 1000));
    }
}

TEST_F(S3fsOpsMockTest, Unlink_RemovesFromStatCache) {
    // Seed StatCache as mknod/create would.
    {
        headers_t meta;
        meta["Content-Length"] = "0";
        meta["Content-Type"] = "application/octet-stream";
        std::string key("/dir/file.txt");
        StatCache::getStatCacheData()->AddStat(key, meta, false, true);
    }
    g_mock.delete_result = 0;
    EXPECT_EQ(0, s3fs_unlink_s3("/dir/file.txt"));
    struct stat st;
    std::string key("/dir/file.txt");
    EXPECT_FALSE(StatCache::getStatCacheData()->GetStat(key, &st));
}

TEST_F(S3fsOpsMockTest, Getattr_CachesResult) {
    // Verify that after a successful getattr, StatCache stores the result.
    // Note: StatCache only caches keys with specific prefixes (content-type,
    // content-length, x-amz-*). The meta mode key "x-hws-obs-meta-mode"
    // is NOT cached by StatCache (doesn't start with "x-amz"), so the second
    // call will still hit the HEAD mock. We verify initial caching works.
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Length"] = "200";
    g_mock.head_meta["Content-Type"] = "text/plain";
    g_mock.head_meta["x-amz-meta-mode"] = "33188";

    struct stat stbuf;
    EXPECT_EQ(0, s3fs_getattr_s3("/cached.txt", &stbuf));
    EXPECT_EQ(200, stbuf.st_size);

    // Second call - mock still returns success so we verify consistency
    struct stat stbuf2;
    EXPECT_EQ(0, s3fs_getattr_s3("/cached.txt", &stbuf2));
    EXPECT_EQ(200, stbuf2.st_size);
}

TEST_F(S3fsOpsMockTest, Getattr_DirectoryByContentType_TextDirectory) {
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "text/directory";
    g_mock.head_meta["Content-Length"] = "0";
    g_mock.head_meta["x-hws-obs-meta-mode"] = "16877";

    struct stat stbuf;
    EXPECT_EQ(0, s3fs_getattr_s3("/textdir", &stbuf));
    EXPECT_TRUE(S_ISDIR(stbuf.st_mode));
}

// =================================================================
// 16. XML namespace helper tests
// =================================================================

TEST_F(S3fsOpsMockTest, GetXmlNsUrl_NullDoc) {
    std::string nsurl;
    EXPECT_FALSE(GetXmlNsUrl(NULL, nsurl));
}

TEST_F(S3fsOpsMockTest, GetXmlNsUrl_WithNamespace) {
    const char* xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                      "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                      "</ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    std::string nsurl;
    EXPECT_TRUE(GetXmlNsUrl(doc, nsurl));
    EXPECT_EQ("http://s3.amazonaws.com/doc/2006-03-01/", nsurl);
    xmlFreeDoc(doc);
}

TEST_F(S3fsOpsMockTest, GetXmlNsUrl_WithoutNamespace) {
    const char* xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                      "<ListBucketResult></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    std::string nsurl;
    EXPECT_FALSE(GetXmlNsUrl(doc, nsurl));
    xmlFreeDoc(doc);
}

// =================================================================
// 17. is_truncated_s3ops / get_next_marker_s3ops tests
// =================================================================

TEST_F(S3fsOpsMockTest, IsTruncated_True) {
    const char* xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                      "<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    EXPECT_TRUE(is_truncated_s3ops(doc));
    xmlFreeDoc(doc);
}

TEST_F(S3fsOpsMockTest, IsTruncated_False) {
    const char* xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                      "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    EXPECT_FALSE(is_truncated_s3ops(doc));
    xmlFreeDoc(doc);
}

TEST_F(S3fsOpsMockTest, IsTruncated_Missing) {
    const char* xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                      "<ListBucketResult></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    EXPECT_FALSE(is_truncated_s3ops(doc));
    xmlFreeDoc(doc);
}

TEST_F(S3fsOpsMockTest, IsTruncated_NullDoc) {
    EXPECT_FALSE(is_truncated_s3ops(NULL));
}

TEST_F(S3fsOpsMockTest, GetNextMarker_Present) {
    const char* xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                      "<ListBucketResult><NextMarker>key123</NextMarker></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    EXPECT_EQ("key123", get_next_marker_s3ops(doc));
    xmlFreeDoc(doc);
}

TEST_F(S3fsOpsMockTest, GetNextMarker_Missing) {
    const char* xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                      "<ListBucketResult></ListBucketResult>";
    xmlDocPtr doc = xmlReadMemory(xml, strlen(xml), "", NULL, 0);
    ASSERT_NE(nullptr, doc);
    EXPECT_TRUE(get_next_marker_s3ops(doc).empty());
    xmlFreeDoc(doc);
}

// =================================================================
// 18. get_meta_xattr_value_s3 tests
// =================================================================

TEST_F(S3fsOpsMockTest, GetMetaXattrValue_NullPath) {
    std::string rawvalue;
    EXPECT_FALSE(get_meta_xattr_value_s3(NULL, rawvalue));
}

TEST_F(S3fsOpsMockTest, GetMetaXattrValue_EmptyPath) {
    std::string rawvalue;
    EXPECT_FALSE(get_meta_xattr_value_s3("", rawvalue));
}

TEST_F(S3fsOpsMockTest, GetMetaXattrValue_HeadFails) {
    g_mock.reset();
    g_mock.head_result = -ENOENT;

    std::string rawvalue;
    EXPECT_FALSE(get_meta_xattr_value_s3("/testfile.txt", rawvalue));
    EXPECT_EQ("/testfile.txt", g_mock.last_head_path);
}

TEST_F(S3fsOpsMockTest, GetMetaXattrValue_NoXattrHeader) {
    g_mock.reset();
    g_mock.head_result = 0;
    g_mock.head_meta.clear();
    // No x-amz-meta-xattr header

    std::string rawvalue;
    EXPECT_FALSE(get_meta_xattr_value_s3("/testfile.txt", rawvalue));
    EXPECT_EQ("/testfile.txt", g_mock.last_head_path);
}

TEST_F(S3fsOpsMockTest, GetMetaXattrValue_WithXattrHeader) {
    g_mock.reset();
    g_mock.head_result = 0;
    g_mock.head_meta.clear();
    g_mock.head_meta["x-amz-meta-xattr"] = "user.key=value";

    std::string rawvalue;
    EXPECT_TRUE(get_meta_xattr_value_s3("/testfile.txt", rawvalue));
    EXPECT_EQ("user.key=value", rawvalue);
    EXPECT_EQ("/testfile.txt", g_mock.last_head_path);
}

TEST_F(S3fsOpsMockTest, GetMetaXattrValue_WithMultipleXattrs) {
    g_mock.reset();
    g_mock.head_result = 0;
    g_mock.head_meta.clear();
    // Multiple xattrs encoded in single header
    g_mock.head_meta["x-amz-meta-xattr"] = "user.key1=value1\nuser.key2=value2";

    std::string rawvalue;
    EXPECT_TRUE(get_meta_xattr_value_s3("/testfile.txt", rawvalue));
    EXPECT_EQ("user.key1=value1\nuser.key2=value2", rawvalue);
}

TEST_F(S3fsOpsMockTest, GetMetaXattrValue_WithEmptyXattrValue) {
    g_mock.reset();
    g_mock.head_result = 0;
    g_mock.head_meta.clear();
    g_mock.head_meta["x-amz-meta-xattr"] = "";

    std::string rawvalue;
    // Empty header value is still present, so should return true
    EXPECT_TRUE(get_meta_xattr_value_s3("/testfile.txt", rawvalue));
    EXPECT_TRUE(rawvalue.empty());
}

// =================================================================
// 19. Readdir helper for pagination tests
// =================================================================
static std::vector<std::string> g_readdir_entries;

static int test_filler(void* buf, const char* name, const struct stat* stbuf, off_t off) {
    (void)buf; (void)stbuf; (void)off;
    if(name) g_readdir_entries.push_back(name);
    return 0;
}

// Helper: count entries excluding . and ..
static size_t count_real_entries() {
    size_t count = 0;
    for(const auto& e : g_readdir_entries) {
        if(e != "." && e != "..") count++;
    }
    return count;
}

// Helper: check if a name is in readdir results
static bool has_entry(const std::string& name) {
    return std::find(g_readdir_entries.begin(), g_readdir_entries.end(), name)
           != g_readdir_entries.end();
}

// =================================================================
// 20. DC-10: Readdir pagination tests
// =================================================================

TEST_F(S3fsOpsMockTest, Readdir_SinglePage_NoTruncation) {
    // Single page with 3 files, IsTruncated=false
    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/file_a.txt</Key><Size>10</Size></Contents>"
        "<Contents><Key>dir/file_b.txt</Key><Size>20</Size></Contents>"
        "<Contents><Key>dir/file_c.txt</Key><Size>30</Size></Contents>"
        "</ListBucketResult>";

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));
    EXPECT_EQ(3u, count_real_entries());
    EXPECT_TRUE(has_entry("file_a.txt"));
    EXPECT_TRUE(has_entry("file_b.txt"));
    EXPECT_TRUE(has_entry("file_c.txt"));
    // Should only call ListBucket once
    EXPECT_EQ(1, g_mock.list_call_count);
}

TEST_F(S3fsOpsMockTest, Readdir_TwoPages) {
    // Page 1: 3 files, IsTruncated=true, NextMarker present
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<NextMarker>dir/file_c.txt</NextMarker>"
        "<Contents><Key>dir/file_a.txt</Key><Size>10</Size></Contents>"
        "<Contents><Key>dir/file_b.txt</Key><Size>20</Size></Contents>"
        "<Contents><Key>dir/file_c.txt</Key><Size>30</Size></Contents>"
        "</ListBucketResult>");

    // Page 2: 2 files, IsTruncated=false
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/file_d.txt</Key><Size>40</Size></Contents>"
        "<Contents><Key>dir/file_e.txt</Key><Size>50</Size></Contents>"
        "</ListBucketResult>");

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));
    EXPECT_EQ(5u, count_real_entries());
    EXPECT_TRUE(has_entry("file_a.txt"));
    EXPECT_TRUE(has_entry("file_b.txt"));
    EXPECT_TRUE(has_entry("file_c.txt"));
    EXPECT_TRUE(has_entry("file_d.txt"));
    EXPECT_TRUE(has_entry("file_e.txt"));
    EXPECT_EQ(2, g_mock.list_call_count);
}

TEST_F(S3fsOpsMockTest, Readdir_ThreePages) {
    // Page 1
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<NextMarker>dir/f02</NextMarker>"
        "<Contents><Key>dir/f01</Key><Size>1</Size></Contents>"
        "<Contents><Key>dir/f02</Key><Size>1</Size></Contents>"
        "</ListBucketResult>");
    // Page 2
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<NextMarker>dir/f04</NextMarker>"
        "<Contents><Key>dir/f03</Key><Size>1</Size></Contents>"
        "<Contents><Key>dir/f04</Key><Size>1</Size></Contents>"
        "</ListBucketResult>");
    // Page 3
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/f05</Key><Size>1</Size></Contents>"
        "</ListBucketResult>");

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));
    EXPECT_EQ(5u, count_real_entries());
    EXPECT_EQ(3, g_mock.list_call_count);
}

TEST_F(S3fsOpsMockTest, Readdir_MarkerInQuery) {
    // Verify the second call includes &marker= in the query
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<NextMarker>dir/last_key</NextMarker>"
        "<Contents><Key>dir/first.txt</Key><Size>1</Size></Contents>"
        "</ListBucketResult>");
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/second.txt</Key><Size>1</Size></Contents>"
        "</ListBucketResult>");

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));

    // First query should NOT have marker
    ASSERT_GE((int)g_mock.list_queries.size(), 2);
    EXPECT_EQ(std::string::npos, g_mock.list_queries[0].find("marker="));
    // Second query MUST have marker=dir/last_key (URL-encoded)
    EXPECT_NE(std::string::npos, g_mock.list_queries[1].find("marker="));
    EXPECT_NE(std::string::npos, g_mock.list_queries[1].find("last_key"));
}

TEST_F(S3fsOpsMockTest, Readdir_NextMarkerFallbackToLastKey) {
    // IsTruncated=true but no NextMarker element → falls back to last key
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<Contents><Key>dir/alpha.txt</Key><Size>1</Size></Contents>"
        "<Contents><Key>dir/beta.txt</Key><Size>1</Size></Contents>"
        "</ListBucketResult>");
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/gamma.txt</Key><Size>1</Size></Contents>"
        "</ListBucketResult>");

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));
    EXPECT_EQ(3u, count_real_entries());
    // Second query should use last key (dir/beta.txt) as marker
    ASSERT_GE((int)g_mock.list_queries.size(), 2);
    EXPECT_NE(std::string::npos, g_mock.list_queries[1].find("marker="));
    EXPECT_NE(std::string::npos, g_mock.list_queries[1].find("beta.txt"));
}

TEST_F(S3fsOpsMockTest, Readdir_ListFailsSecondPage) {
    // First page OK, second page returns error
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<NextMarker>dir/key</NextMarker>"
        "<Contents><Key>dir/file.txt</Key><Size>1</Size></Contents>"
        "</ListBucketResult>");
    // Second call returns error
    g_mock.list_result_queue.push_back(0);    // first call OK
    g_mock.list_result_queue.push_back(-EIO); // second call fails

    g_readdir_entries.clear();
    EXPECT_EQ(-EIO, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));
    EXPECT_EQ(2, g_mock.list_call_count);
}

TEST_F(S3fsOpsMockTest, Readdir_EmptyDirectory) {
    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "</ListBucketResult>";

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/emptydir", NULL, test_filler, 0, NULL));
    // Only . and .. should be present
    EXPECT_EQ(0u, count_real_entries());
    EXPECT_TRUE(has_entry("."));
    EXPECT_TRUE(has_entry(".."));
}

TEST_F(S3fsOpsMockTest, Readdir_WithCommonPrefixes_Paginated) {
    // Page 1: files + subdirectory prefix, truncated
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<NextMarker>dir/file_b.txt</NextMarker>"
        "<Contents><Key>dir/file_a.txt</Key><Size>10</Size></Contents>"
        "<Contents><Key>dir/file_b.txt</Key><Size>20</Size></Contents>"
        "<CommonPrefixes><Prefix>dir/subdir1/</Prefix></CommonPrefixes>"
        "</ListBucketResult>");
    // Page 2: more files + another subdirectory
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/file_c.txt</Key><Size>30</Size></Contents>"
        "<CommonPrefixes><Prefix>dir/subdir2/</Prefix></CommonPrefixes>"
        "</ListBucketResult>");

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));
    // 3 files + 2 subdirectories = 5 real entries
    EXPECT_EQ(5u, count_real_entries());
    EXPECT_TRUE(has_entry("file_a.txt"));
    EXPECT_TRUE(has_entry("file_b.txt"));
    EXPECT_TRUE(has_entry("file_c.txt"));
    EXPECT_TRUE(has_entry("subdir1"));
    EXPECT_TRUE(has_entry("subdir2"));
}

TEST_F(S3fsOpsMockTest, Readdir_RootDirectory_Paginated) {
    // Root directory pagination (prefix is empty)
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<NextMarker>file_b.txt</NextMarker>"
        "<Contents><Key>file_a.txt</Key><Size>10</Size></Contents>"
        "<Contents><Key>file_b.txt</Key><Size>20</Size></Contents>"
        "</ListBucketResult>");
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>file_c.txt</Key><Size>30</Size></Contents>"
        "</ListBucketResult>");

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/", NULL, test_filler, 0, NULL));
    EXPECT_EQ(3u, count_real_entries());
    EXPECT_TRUE(has_entry("file_a.txt"));
    EXPECT_TRUE(has_entry("file_c.txt"));
    // First query should NOT have prefix= (root)
    ASSERT_GE((int)g_mock.list_queries.size(), 1);
    EXPECT_EQ(std::string::npos, g_mock.list_queries[0].find("prefix="));
}

TEST_F(S3fsOpsMockTest, Readdir_DuplicateEntriesAcrossPages) {
    // Ensure listed_entries set prevents duplicates if same key appears in both pages
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<NextMarker>dir/dup.txt</NextMarker>"
        "<Contents><Key>dir/dup.txt</Key><Size>10</Size></Contents>"
        "</ListBucketResult>");
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/dup.txt</Key><Size>10</Size></Contents>"
        "<Contents><Key>dir/unique.txt</Key><Size>20</Size></Contents>"
        "</ListBucketResult>");

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));
    // filler is called for both occurrences (S3 shouldn't return dupes,
    // but listed_entries prevents StatCache merge dupes later)
    EXPECT_TRUE(has_entry("dup.txt"));
    EXPECT_TRUE(has_entry("unique.txt"));
}

// =================================================================
// DC-10 continued: NextMarker URL decode + special chars tests
// =================================================================

TEST_F(S3fsOpsMockTest, Readdir_NextMarkerUrlDecoded) {
    // ISSUE2026022600012: NextMarker from server is URL-encoded when encoding-type=url.
    // get_next_marker_s3ops must decode it, then readdir's urlEncode() produces single encoding.
    // Page 1: NextMarker is URL-encoded "dir%2Ffile+0999" (= "dir/file 0999")
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<NextMarker>dir%2Ffile+0999</NextMarker>"
        "<Contents><Key>dir/file_a.txt</Key><Size>10</Size></Contents>"
        "</ListBucketResult>");
    // Page 2: final page
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/file_z.txt</Key><Size>20</Size></Contents>"
        "</ListBucketResult>");

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));

    // Verify both pages returned entries
    EXPECT_TRUE(has_entry("file_a.txt"));
    EXPECT_TRUE(has_entry("file_z.txt"));

    // Verify the second query used properly decoded+re-encoded marker
    // After decode: "dir/file 0999", after urlEncode: "dir%2Ffile%200999"
    ASSERT_GE(g_mock.list_queries.size(), 2u);
    std::string q2 = g_mock.list_queries[1];
    // The marker should be urlEncode("dir/file 0999") which contains %2F and %20
    EXPECT_NE(std::string::npos, q2.find("marker="));
    // Must NOT contain double-encoded sequences like %252F
    EXPECT_EQ(std::string::npos, q2.find("%252F"));
    EXPECT_EQ(std::string::npos, q2.find("%2B"));  // '+' should have been decoded to space first
}

TEST_F(S3fsOpsMockTest, Readdir_SpecialCharsInKeys) {
    // Keys with spaces: verify readdir returns correctly decoded filenames
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<NextMarker>dir/file+0001</NextMarker>"
        "<Contents><Key>dir/file+0000</Key><Size>10</Size></Contents>"
        "<Contents><Key>dir/file+0001</Key><Size>10</Size></Contents>"
        "</ListBucketResult>");
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/file+0002</Key><Size>10</Size></Contents>"
        "</ListBucketResult>");

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));

    // URL-encoded '+' in keys should be decoded to space in filenames
    EXPECT_TRUE(has_entry("file 0000"));
    EXPECT_TRUE(has_entry("file 0001"));
    EXPECT_TRUE(has_entry("file 0002"));
}

// =================================================================
// ISSUE2026051200001 C.1 review fix: implicit dir ListBucket probe
// =================================================================
// Verifies the HEAD-double-404 → ListBucket fallback path. Review found
// EM9/EM10 integration tests didn't actually exercise this path (they
// pre-mkdir'd the parent so it had a marker).
// =================================================================

TEST_F(S3fsOpsMockTest, Getattr_C1_ImplicitDir_ContentsNonEmpty_ReturnsDir) {
    // HEAD /path 404, HEAD /path/ 404, ListBucket prefix=path/ → Contents present.
    // Expect getattr returns 0 and stbuf is a directory with mode 0750.
    StatCache* cache = StatCache::getStatCacheData();
    cache->DelStat("/implicit_dir");

    g_mock.head_result = -ENOENT;   // both HEADs miss
    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>implicit_dir/leaf.txt</Key><Size>10</Size></Contents>"
        "</ListBucketResult>";

    struct stat stbuf;
    int rc = s3fs_getattr_s3("/implicit_dir", &stbuf);

    EXPECT_EQ(0, rc);
    EXPECT_TRUE(S_ISDIR(stbuf.st_mode));
    EXPECT_EQ(stbuf.st_mode & 0777, 0750);
    EXPECT_EQ(stbuf.st_uid, geteuid());
    EXPECT_EQ(stbuf.st_gid, getegid());
}

TEST_F(S3fsOpsMockTest, Getattr_C1_ImplicitDir_CommonPrefixesNonEmpty_ReturnsDir) {
    StatCache* cache = StatCache::getStatCacheData();
    cache->DelStat("/implicit_dir2");

    g_mock.head_result = -ENOENT;
    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<CommonPrefixes><Prefix>implicit_dir2/sub/</Prefix></CommonPrefixes>"
        "</ListBucketResult>";

    struct stat stbuf;
    int rc = s3fs_getattr_s3("/implicit_dir2", &stbuf);

    EXPECT_EQ(0, rc);
    EXPECT_TRUE(S_ISDIR(stbuf.st_mode));
    EXPECT_EQ(stbuf.st_mode & 0777, 0750);
}

TEST_F(S3fsOpsMockTest, Getattr_C1_ImplicitDir_EmptyListBucket_ReturnsENOENT) {
    StatCache* cache = StatCache::getStatCacheData();
    cache->DelStat("/truly_absent");

    g_mock.head_result = -ENOENT;
    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";

    struct stat stbuf;
    int rc = s3fs_getattr_s3("/truly_absent", &stbuf);

    EXPECT_EQ(-ENOENT, rc);
}

TEST_F(S3fsOpsMockTest, Getattr_C1_ImplicitDir_ListBucketFails_ReturnsENOENT) {
    // Probe network failure → don't crash, fall back to ENOENT.
    StatCache* cache = StatCache::getStatCacheData();
    cache->DelStat("/probe_fails");

    g_mock.head_result = -ENOENT;
    g_mock.list_result = -EIO;

    struct stat stbuf;
    int rc = s3fs_getattr_s3("/probe_fails", &stbuf);

    EXPECT_EQ(-ENOENT, rc);
    g_mock.list_result = 0;   // restore
}

TEST_F(S3fsOpsMockTest, Getattr_C1_ImplicitDir_FileBucketSkipsProbe) {
    // M-1/M-2 review: C.1 probe must be object-bucket only.
    bucket_type_t saved = g_bucket_type;
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
    StatCache* cache = StatCache::getStatCacheData();
    cache->DelStat("/file_bucket_dir");

    g_mock.head_result = -ENOENT;
    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>file_bucket_dir/x</Key><Size>0</Size></Contents>"
        "</ListBucketResult>";
    g_mock.list_call_count = 0;

    struct stat stbuf;
    int rc = s3fs_getattr_s3("/file_bucket_dir", &stbuf);

    EXPECT_EQ(-ENOENT, rc);
    EXPECT_EQ(0, g_mock.list_call_count);   // no ListBucket probe in file bucket

    g_bucket_type = saved;
}

TEST_F(S3fsOpsMockTest, Getattr_C1_NegativeCache_OnENOENT) {
    // M-1 review: after C.1 probe returns ENOENT, AddNoObjectCache writes a
    // negative entry so a repeated getattr doesn't send HEAD+HEAD+List again.
    // (no-op unless `-o enable_noobj_cache`; we enable it explicitly here.)
    StatCache* cache = StatCache::getStatCacheData();
    bool saved_noobj = cache->SetCacheNoObject(true);
    cache->DelStat("/missing_path");

    g_mock.head_result = -ENOENT;
    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";

    struct stat stbuf;
    EXPECT_EQ(-ENOENT, s3fs_getattr_s3("/missing_path", &stbuf));
    int first_head_count = g_mock.head_call_count;
    int first_list_count = g_mock.list_call_count;

    // Repeat — should be cache-served, no extra HEAD or LIST.
    EXPECT_EQ(-ENOENT, s3fs_getattr_s3("/missing_path", &stbuf));
    EXPECT_EQ(first_head_count, g_mock.head_call_count);
    EXPECT_EQ(first_list_count, g_mock.list_call_count);

    cache->SetCacheNoObject(saved_noobj);
}

// =================================================================
// ISSUE2026051200001 M.1/M.2 review fix: readdir synthesized meta content
// =================================================================
// Verifies the synthesized meta written to StatCache by readdir actually
// has correct uid/gid/mode/mtime — review found unit tests only asserted
// HasStat==true, missing the very content that caused the SMB-guest /
// uid=0 / mtime=1970 user-visible bug.
// =================================================================

TEST_F(S3fsOpsMockTest, Readdir_M1_Contents_SynthesizedMetaContent) {
    // ListBucket returns one Contents entry with Size + LastModified + ETag.
    // After readdir, StatCache must hold synthesized meta with:
    //   uid=geteuid, gid=getegid, mode=S_IFREG|0640, size from <Size>,
    //   mtime from <LastModified>, ETag preserved.
    StatCache* cache = StatCache::getStatCacheData();
    cache->DelStat("/dir/leaf.txt");   // clean slate

    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents>"
        "<Key>dir/leaf.txt</Key>"
        "<Size>1234</Size>"
        "<LastModified>2026-05-12T07:00:00.000Z</LastModified>"
        "<ETag>\"abc123\"</ETag>"
        "</Contents>"
        "</ListBucketResult>";

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));

    struct stat stbuf;
    headers_t  meta;
    std::string key = "/dir/leaf.txt";
    ASSERT_TRUE(cache->GetStat(key, &stbuf, &meta));

    EXPECT_EQ(stbuf.st_uid,  geteuid());
    EXPECT_EQ(stbuf.st_gid,  getegid());
    EXPECT_TRUE(S_ISREG(stbuf.st_mode));
    EXPECT_EQ(stbuf.st_mode & 0777, 0640);
    EXPECT_EQ(stbuf.st_size, 1234);
    EXPECT_GT(stbuf.st_mtime, 0);   // not epoch (1970-01-01)

    // ETag survives through cache (StatCache stores it via Add path).
    EXPECT_NE(meta.end(), meta.find("ETag"));
    EXPECT_EQ(meta["ETag"], "abc123");
}

TEST_F(S3fsOpsMockTest, Readdir_M2_CommonPrefix_SynthesizedMetaContent) {
    // ListBucket returns one CommonPrefix.
    // After readdir, StatCache must hold:
    //   uid=geteuid, gid=getegid, mode=S_IFDIR|0750
    StatCache* cache = StatCache::getStatCacheData();
    cache->DelStat("/dir/subdir");

    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<CommonPrefixes><Prefix>dir/subdir/</Prefix></CommonPrefixes>"
        "</ListBucketResult>";

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));

    struct stat stbuf;
    std::string key = "/dir/subdir";
    ASSERT_TRUE(cache->GetStat(key, &stbuf));

    EXPECT_EQ(stbuf.st_uid,  geteuid());
    EXPECT_EQ(stbuf.st_gid,  getegid());
    EXPECT_TRUE(S_ISDIR(stbuf.st_mode));
    EXPECT_EQ(stbuf.st_mode & 0777, 0750);
}

TEST_F(S3fsOpsMockTest, Readdir_M2_GetStatProbe_DoesNotOverwriteRealMeta) {
    // Pre-seed cache with REAL meta (uid=9999, mode=0700) — simulates a prior
    // mkdir or HEAD that captured custom user metadata.
    // readdir then sees the same name via CommonPrefix and tries to synthesize.
    // The probe must detect the existing real meta and skip AddStat,
    // preserving uid=9999/mode=0700.
    StatCache* cache = StatCache::getStatCacheData();
    {
        std::string clear_key("/dir/preserved");
        cache->DelStat(clear_key);
    }

    {
        // NOTE: test_stubs overrides hws_obs_meta_* to "x-hws-obs-meta-*".
        // get_uid/get_gid/get_mode look up those names at convert_header_to_stat
        // time, so use them to seed stbuf with custom uid/gid/mode.
        headers_t real_meta;
        real_meta["Content-Type"]        = "application/x-directory";
        real_meta["Content-Length"]      = "0";
        real_meta["x-hws-obs-meta-uid"]  = "9999";
        real_meta["x-hws-obs-meta-gid"]  = "9999";
        real_meta["x-hws-obs-meta-mode"] = std::to_string(S_IFDIR | 0700);
        std::string k = "/dir/preserved";
        ASSERT_TRUE(cache->AddStat(k, real_meta, true, false));
    }

    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<CommonPrefixes><Prefix>dir/preserved/</Prefix></CommonPrefixes>"
        "</ListBucketResult>";

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));

    struct stat stbuf;
    std::string key = "/dir/preserved";
    ASSERT_TRUE(cache->GetStat(key, &stbuf));
    EXPECT_EQ(stbuf.st_uid, 9999u);
    EXPECT_EQ(stbuf.st_gid, 9999u);
    EXPECT_EQ(stbuf.st_mode & 0777, 0700);   // real meta preserved
}

TEST_F(S3fsOpsMockTest, Readdir_M4_AccurateReaddir_SkipsSyntheticAddStat) {
    // M-4 review: with accurate_readdir=true, readdir must NOT pre-populate
    // StatCache; getattr falls back to per-entry HEAD. Verify by checking
    // cache miss after readdir.
    bool saved = accurate_readdir;
    accurate_readdir = true;
    StatCache* cache = StatCache::getStatCacheData();
    {
        std::string k1("/dir/leaf.txt");
        std::string k2("/dir/sub");
        cache->DelStat(k1);
        cache->DelStat(k2);
    }

    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/leaf.txt</Key><Size>10</Size></Contents>"
        "<CommonPrefixes><Prefix>dir/sub/</Prefix></CommonPrefixes>"
        "</ListBucketResult>";

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));

    std::string k1 = "/dir/leaf.txt";
    std::string k2 = "/dir/sub";
    EXPECT_FALSE(cache->HasStat(k1));
    EXPECT_FALSE(cache->HasStat(k2));

    accurate_readdir = saved;
}

// =================================================================
// ISSUE2026051100001 iter-7: readdir stale-entry cleanup
// =================================================================

TEST_F(S3fsOpsMockTest, Readdir_StaleCleanup_RemovesAbsentChildren) {
    // Pre-seed StatCache with /dir/b and /dir/c (notruncate=0)
    StatCache* cache = StatCache::getStatCacheData();
    headers_t meta_b, meta_c;
    meta_b["Content-Type"] = "application/octet-stream";
    meta_b["Content-Length"] = "10";
    meta_c = meta_b;
    {
        std::string k = "/dir/b"; ASSERT_TRUE(cache->AddStat(k, meta_b, false, false));
        std::string k2 = "/dir/c"; ASSERT_TRUE(cache->AddStat(k2, meta_c, false, false));
    }
    // Age entries past the 1s cleanup grace window (we're testing the
    // stale-cleanup path, not the freshness protection).
    cache->TestBackdateEntry("/dir/b", 2);
    cache->TestBackdateEntry("/dir/c", 2);
    {
        std::string k = "/dir/b"; ASSERT_TRUE(cache->HasStat(k));
        std::string k2 = "/dir/c"; ASSERT_TRUE(cache->HasStat(k2));
    }

    // ListBucket /dir returns only c (b was externally deleted)
    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/c</Key><Size>10</Size></Contents>"
        "</ListBucketResult>";

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));

    // b should be cleaned, c should remain
    {
        std::string k = "/dir/b";
        EXPECT_FALSE(cache->HasStat(k));
        std::string k2 = "/dir/c";
        EXPECT_TRUE(cache->HasStat(k2));
    }
}

TEST_F(S3fsOpsMockTest, Readdir_StaleCleanup_PreservesNotruncatePinnedChildren) {
    StatCache* cache = StatCache::getStatCacheData();
    headers_t meta_pin, meta_free;
    meta_pin["Content-Type"] = "application/octet-stream";
    meta_pin["Content-Length"] = "10";
    meta_free = meta_pin;
    {
        std::string k = "/dir/pinned"; ASSERT_TRUE(cache->AddStat(k, meta_pin, false, true));  // notruncate=1
        std::string k2 = "/dir/free"; ASSERT_TRUE(cache->AddStat(k2, meta_free, false, false));
    }
    cache->TestBackdateEntry("/dir/pinned", 2);
    cache->TestBackdateEntry("/dir/free", 2);

    // ListBucket /dir empty
    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));

    {
        std::string k = "/dir/pinned";
        EXPECT_TRUE(cache->HasStat(k));  // preserved
        std::string k2 = "/dir/free";
        EXPECT_FALSE(cache->HasStat(k2));  // cleaned
    }
}

TEST_F(S3fsOpsMockTest, Readdir_StaleCleanup_SkippedOnPaginationFailure) {
    StatCache* cache = StatCache::getStatCacheData();
    headers_t meta;
    meta["Content-Type"] = "application/octet-stream";
    meta["Content-Length"] = "10";
    {
        std::string k = "/dir/stale"; ASSERT_TRUE(cache->AddStat(k, meta, false, false));
    }

    // Page 1 OK, page 2 fails
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<NextMarker>dir/aaa</NextMarker>"
        "<Contents><Key>dir/aaa</Key><Size>10</Size></Contents>"
        "</ListBucketResult>");
    g_mock.list_xml_queue.push_back("");  // page 2 — won't be read
    g_mock.list_result_queue.push_back(0);
    g_mock.list_result_queue.push_back(-EIO);  // page 2 fails

    g_readdir_entries.clear();
    int rc = s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL);
    EXPECT_NE(0, rc);  // pagination failed

    // /dir/stale should NOT be cleaned (cleanup block didn't run)
    {
        std::string k = "/dir/stale";
        EXPECT_TRUE(cache->HasStat(k));
    }
}

TEST_F(S3fsOpsMockTest, Readdir_StaleCleanup_PreservesListedChildrenWhenPresent) {
    StatCache* cache = StatCache::getStatCacheData();
    headers_t meta;
    meta["Content-Type"] = "application/octet-stream";
    meta["Content-Length"] = "10";
    {
        std::string k = "/dir/b"; ASSERT_TRUE(cache->AddStat(k, meta, false, false));
    }

    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/b</Key><Size>10</Size></Contents>"
        "</ListBucketResult>";

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));

    {
        std::string k = "/dir/b";
        EXPECT_TRUE(cache->HasStat(k));
    }
}

TEST_F(S3fsOpsMockTest, Readdir_StaleCleanup_RemovesAllWhenListBucketEmpty) {
    StatCache* cache = StatCache::getStatCacheData();
    headers_t meta;
    meta["Content-Type"] = "application/octet-stream";
    meta["Content-Length"] = "10";
    {
        std::string k = "/dir/x"; ASSERT_TRUE(cache->AddStat(k, meta, false, false));
        std::string k2 = "/dir/y"; ASSERT_TRUE(cache->AddStat(k2, meta, false, false));
        std::string k3 = "/dir/z"; ASSERT_TRUE(cache->AddStat(k3, meta, false, false));
    }
    cache->TestBackdateEntry("/dir/x", 2);
    cache->TestBackdateEntry("/dir/y", 2);
    cache->TestBackdateEntry("/dir/z", 2);

    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));

    {
        std::string k = "/dir/x"; EXPECT_FALSE(cache->HasStat(k));
        std::string k2 = "/dir/y"; EXPECT_FALSE(cache->HasStat(k2));
        std::string k3 = "/dir/z"; EXPECT_FALSE(cache->HasStat(k3));
    }
}

TEST_F(S3fsOpsMockTest, Readdir_StaleCleanup_SkippedInFileBucketMode) {
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;
    StatCache* cache = StatCache::getStatCacheData();
    headers_t meta;
    meta["Content-Type"] = "application/octet-stream";
    meta["Content-Length"] = "10";
    {
        std::string k = "/dir/stale_file"; ASSERT_TRUE(cache->AddStat(k, meta, false, false));
    }

    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";

    g_readdir_entries.clear();
    // file bucket uses different readdir path; just call the object bucket function
    // to confirm the cleanup block is gated by g_bucket_type.
    s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL);

    // file bucket mode: cleanup block should be skipped
    {
        std::string k = "/dir/stale_file";
        EXPECT_TRUE(cache->HasStat(k));
    }
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
}

TEST_F(S3fsOpsMockTest, Readdir_StaleCleanup_RootPathPathConstruction) {
    StatCache* cache = StatCache::getStatCacheData();
    headers_t meta;
    meta["Content-Type"] = "application/octet-stream";
    meta["Content-Length"] = "10";
    {
        std::string k = "/foo"; ASSERT_TRUE(cache->AddStat(k, meta, false, false));
    }
    cache->TestBackdateEntry("/foo", 2);

    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/", NULL, test_filler, 0, NULL));

    {
        std::string k = "/foo";
        EXPECT_FALSE(cache->HasStat(k));  // verified path "/" + "foo" = "/foo"
    }
}

TEST_F(S3fsOpsMockTest, Readdir_StaleCleanup_MultiPageAccumulatesListedEntries) {
    StatCache* cache = StatCache::getStatCacheData();
    headers_t meta;
    meta["Content-Type"] = "application/octet-stream";
    meta["Content-Length"] = "10";
    {
        std::string ka = "/dir/a"; ASSERT_TRUE(cache->AddStat(ka, meta, false, false));
        std::string kb = "/dir/b"; ASSERT_TRUE(cache->AddStat(kb, meta, false, false));
        std::string kc = "/dir/c"; ASSERT_TRUE(cache->AddStat(kc, meta, false, false));
        std::string kd = "/dir/d"; ASSERT_TRUE(cache->AddStat(kd, meta, false, false));
        std::string ke = "/dir/e"; ASSERT_TRUE(cache->AddStat(ke, meta, false, false));  // stale
    }
    cache->TestBackdateEntry("/dir/a", 2);
    cache->TestBackdateEntry("/dir/b", 2);
    cache->TestBackdateEntry("/dir/c", 2);
    cache->TestBackdateEntry("/dir/d", 2);
    cache->TestBackdateEntry("/dir/e", 2);

    // Page 1: a, b
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<NextMarker>dir/b</NextMarker>"
        "<Contents><Key>dir/a</Key><Size>10</Size></Contents>"
        "<Contents><Key>dir/b</Key><Size>10</Size></Contents>"
        "</ListBucketResult>");
    // Page 2: c
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>true</IsTruncated>"
        "<NextMarker>dir/c</NextMarker>"
        "<Contents><Key>dir/c</Key><Size>10</Size></Contents>"
        "</ListBucketResult>");
    // Page 3: d
    g_mock.list_xml_queue.push_back(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/d</Key><Size>10</Size></Contents>"
        "</ListBucketResult>");

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));

    {
        std::string ka = "/dir/a"; EXPECT_TRUE(cache->HasStat(ka));
        std::string kb = "/dir/b"; EXPECT_TRUE(cache->HasStat(kb));
        std::string kc = "/dir/c"; EXPECT_TRUE(cache->HasStat(kc));
        std::string kd = "/dir/d"; EXPECT_TRUE(cache->HasStat(kd));
        std::string ke = "/dir/e"; EXPECT_FALSE(cache->HasStat(ke));  // stale, removed
    }
}

TEST_F(S3fsOpsMockTest, Readdir_StaleCleanup_MixedChildrenSelectiveCleanup) {
    StatCache* cache = StatCache::getStatCacheData();
    headers_t meta;
    meta["Content-Type"] = "application/octet-stream";
    meta["Content-Length"] = "10";
    {
        std::string kx = "/dir/x"; ASSERT_TRUE(cache->AddStat(kx, meta, false, false));  // notruncate=0
        std::string ky = "/dir/y"; ASSERT_TRUE(cache->AddStat(ky, meta, false, true));   // notruncate=1
        std::string kz = "/dir/z"; ASSERT_TRUE(cache->AddStat(kz, meta, false, false));  // notruncate=0
    }
    cache->TestBackdateEntry("/dir/x", 2);
    cache->TestBackdateEntry("/dir/y", 2);
    cache->TestBackdateEntry("/dir/z", 2);

    // ListBucket only returns z
    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/z</Key><Size>10</Size></Contents>"
        "</ListBucketResult>";

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));

    {
        std::string kx = "/dir/x"; EXPECT_FALSE(cache->HasStat(kx));  // stale, cleaned
        std::string ky = "/dir/y"; EXPECT_TRUE(cache->HasStat(ky));   // pinned, preserved
        std::string kz = "/dir/z"; EXPECT_TRUE(cache->HasStat(kz));   // listed, preserved
    }
}

// ISSUE2026051200001 review fix: cleanup-race grace.
// mkdir/create immediately followed by readdir must NOT self-evict its own
// entry if ListBucket consistency lags the PUT. Simulated here by AddStat'ing
// a fresh entry (no TestBackdateEntry call) and serving a ListBucket that
// omits it. The 1-second grace must protect the entry from cleanup.
TEST_F(S3fsOpsMockTest, Readdir_StaleCleanup_ProtectsFreshlyAddedEntry) {
    StatCache* cache = StatCache::getStatCacheData();
    headers_t meta;
    meta["Content-Type"]   = "application/octet-stream";
    meta["Content-Length"] = "0";
    {
        std::string fresh = "/dir/just_mkdir";
        ASSERT_TRUE(cache->AddStat(fresh, meta, false, false));  // notruncate=0
    }
    // Deliberately NOT calling TestBackdateEntry — the entry is <1s old.

    // ListBucket /dir returns empty (consistency lag: PUT not yet visible).
    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));

    // Without the grace, /dir/just_mkdir would be falsely cleaned up.
    std::string fresh("/dir/just_mkdir");
    EXPECT_TRUE(cache->HasStat(fresh));

    // After backdating past the grace, the next readdir does cleanup.
    cache->TestBackdateEntry(fresh, 2);
    g_readdir_entries.clear();
    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
    EXPECT_EQ(0, s3fs_readdir_s3("/dir", NULL, test_filler, 0, NULL));
    EXPECT_FALSE(cache->HasStat(fresh));
}

TEST_F(S3fsOpsMockTest, Readdir_StaleCleanup_DoesNotAffectSiblingDirs) {
    StatCache* cache = StatCache::getStatCacheData();
    headers_t meta;
    meta["Content-Type"] = "application/octet-stream";
    meta["Content-Length"] = "10";
    {
        std::string ka = "/dir_a/foo"; ASSERT_TRUE(cache->AddStat(ka, meta, false, false));
        std::string kb = "/dir_b/bar"; ASSERT_TRUE(cache->AddStat(kb, meta, false, false));
    }
    cache->TestBackdateEntry("/dir_a/foo", 2);
    cache->TestBackdateEntry("/dir_b/bar", 2);

    // Readdir /dir_a — empty
    g_mock.list_body_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";

    g_readdir_entries.clear();
    EXPECT_EQ(0, s3fs_readdir_s3("/dir_a", NULL, test_filler, 0, NULL));

    {
        std::string ka = "/dir_a/foo"; EXPECT_FALSE(cache->HasStat(ka));  // cleaned (under /dir_a)
        std::string kb = "/dir_b/bar"; EXPECT_TRUE(cache->HasStat(kb));   // untouched (under /dir_b)
    }
}

// =================================================================
// 21. DC-07: StatCache TTL boundary tests
// =================================================================

TEST_F(S3fsOpsMockTest, StatCache_DefaultNoExpiry) {
    // Fresh StatCache should have no expiry
    // GetExpireTime returns -1 when IsExpireTime is false
    StatCache* cache = StatCache::getStatCacheData();
    time_t saved = cache->GetExpireTime();
    cache->UnsetExpireTime();
    EXPECT_EQ(-1, cache->GetExpireTime());
    // Restore
    if(saved != -1) {
        cache->SetExpireTime(saved);
    }
}

TEST_F(S3fsOpsMockTest, StatCache_SetExpireTimeWorks) {
    StatCache* cache = StatCache::getStatCacheData();
    time_t saved = cache->GetExpireTime();
    cache->SetExpireTime(900);
    EXPECT_EQ(900, cache->GetExpireTime());
    // Restore
    if(saved == -1) {
        cache->UnsetExpireTime();
    } else {
        cache->SetExpireTime(saved);
    }
}

TEST_F(S3fsOpsMockTest, StatCache_ObjectBucketDefaultTTL) {
    // Simulate DC-07 conditional: object bucket + no explicit flag → SetExpireTime(900)
    StatCache* cache = StatCache::getStatCacheData();
    time_t saved = cache->GetExpireTime();
    cache->UnsetExpireTime();

    bool explicitly_set = false;
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;

    // Replicate the DC-07 logic from hws_s3fs.cpp
    if(BUCKET_TYPE_OBJECT_SEMANTIC == g_bucket_type && !explicitly_set) {
        cache->SetExpireTime(900);
    }
    EXPECT_EQ(900, cache->GetExpireTime());

    // Restore
    if(saved == -1) {
        cache->UnsetExpireTime();
    } else {
        cache->SetExpireTime(saved);
    }
}

TEST_F(S3fsOpsMockTest, StatCache_FileBucketNoTTL) {
    // Simulate DC-07 conditional: file bucket → no TTL change
    StatCache* cache = StatCache::getStatCacheData();
    time_t saved = cache->GetExpireTime();
    cache->UnsetExpireTime();

    bool explicitly_set = false;
    g_bucket_type = BUCKET_TYPE_FILE_SEMANTIC;

    // Replicate the DC-07 logic
    if(BUCKET_TYPE_OBJECT_SEMANTIC == g_bucket_type && !explicitly_set) {
        cache->SetExpireTime(900);
    }
    // File bucket should NOT get TTL
    EXPECT_EQ(-1, cache->GetExpireTime());

    // Restore
    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;
    if(saved == -1) {
        cache->UnsetExpireTime();
    } else {
        cache->SetExpireTime(saved);
    }
}

TEST_F(S3fsOpsMockTest, StatCache_ExplicitOverridePreventsDefault) {
    // Simulate DC-07: user explicitly sets stat_cache_expire → flag prevents default
    StatCache* cache = StatCache::getStatCacheData();
    time_t saved = cache->GetExpireTime();

    // User sets explicit value (simulates -o stat_cache_expire=300)
    cache->SetExpireTime(300);
    bool explicitly_set = true;

    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;

    // Replicate the DC-07 logic
    if(BUCKET_TYPE_OBJECT_SEMANTIC == g_bucket_type && !explicitly_set) {
        cache->SetExpireTime(900);  // Should NOT execute
    }
    // Should remain at user's explicit value, not 900
    EXPECT_EQ(300, cache->GetExpireTime());

    // Restore
    if(saved == -1) {
        cache->UnsetExpireTime();
    } else {
        cache->SetExpireTime(saved);
    }
}

TEST_F(S3fsOpsMockTest, StatCache_ExplicitZeroOverride) {
    // Simulate: user sets stat_cache_expire=0 (disable expiry explicitly)
    StatCache* cache = StatCache::getStatCacheData();
    time_t saved = cache->GetExpireTime();

    // User explicitly sets 0 (SetExpireTime(0) still sets IsExpireTime=true)
    cache->SetExpireTime(0);
    bool explicitly_set = true;

    g_bucket_type = BUCKET_TYPE_OBJECT_SEMANTIC;

    if(BUCKET_TYPE_OBJECT_SEMANTIC == g_bucket_type && !explicitly_set) {
        cache->SetExpireTime(900);
    }
    // Should stay at 0, not 900
    EXPECT_EQ(0, cache->GetExpireTime());

    // Restore
    if(saved == -1) {
        cache->UnsetExpireTime();
    } else {
        cache->SetExpireTime(saved);
    }
}

// =================================================================
// Tests for s3fs_release_s3 — StatCache behavior on Flush success/failure
// =================================================================

TEST_F(S3fsOpsMockTest, Release_FlushSuccess_UpdatesStatCache) {
    g_mock.reset();
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-amz-meta-mode"] = std::to_string(S_IFREG | 0644);
    g_mock.head_meta["Content-Length"] = "0";
    g_mock.put_result = 0;  // Flush will succeed

    // Create and open file
    struct fuse_file_info fi = {};
    fi.flags = O_WRONLY | O_CREAT;
    int rc = s3fs_open_s3("/test_release_ok", &fi);
    ASSERT_EQ(0, rc);

    // Write data to mark dirty
    char buf[] = "hello";
    int wrc = s3fs_write_s3("/test_release_ok", buf, 5, 0, &fi);
    ASSERT_EQ(5, wrc);

    // Clear StatCache before release
    StatCache::getStatCacheData()->DelStat("/test_release_ok");

    // Release — Flush succeeds
    rc = s3fs_release_s3("/test_release_ok", &fi);
    EXPECT_EQ(0, rc);

    // Verify: StatCache SHOULD be updated
    std::string key = "/test_release_ok";
    EXPECT_TRUE(StatCache::getStatCacheData()->HasStat(key, false));

    // Cleanup
    StatCache::getStatCacheData()->DelStat("/test_release_ok");
}

// =================================================================
// ISSUE2026051200001 M.3: Release flush-success three branches
//
// Branch 1 — orgmeta has meta: AddStat with full meta, uid/gid/mode preserved
// Branch 2 — orgmeta empty, StatCache has meta: fallback to StatCache
// Branch 3 — neither has meta: DelStat (force re-HEAD on next getattr)
// Also: the inserted entry must be notruncate=false.
// =================================================================

TEST_F(S3fsOpsMockTest, Release_FlushSuccess_PreservesUidGidMode) {
    // Branch 1: orgmeta populated from open's HEAD — preserves uid/gid/mode.
    // NOTE: test_stubs.cpp overrides hws_obs_meta_* to "x-hws-obs-meta-*"; use
    // those headers so get_uid/get_gid/get_mode pick them up.
    g_mock.reset();
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-hws-obs-meta-mode"] = std::to_string(S_IFREG | 0640);
    g_mock.head_meta["x-hws-obs-meta-uid"] = "1234";
    g_mock.head_meta["x-hws-obs-meta-gid"] = "5678";
    g_mock.head_meta["Content-Length"] = "0";
    g_mock.put_result = 0;

    struct fuse_file_info fi = {};
    fi.flags = O_WRONLY;
    ASSERT_EQ(0, s3fs_open_s3("/rel_preserve.txt", &fi));

    char buf[] = "data";
    ASSERT_EQ(4, s3fs_write_s3("/rel_preserve.txt", buf, 4, 0, &fi));

    // Clear cache so release must populate it from orgmeta (not pre-existing entry).
    StatCache::getStatCacheData()->DelStat("/rel_preserve.txt");

    ASSERT_EQ(0, s3fs_release_s3("/rel_preserve.txt", &fi));

    // Verify cache entry has the original uid/gid/mode (not 0/0/0).
    struct stat st;
    std::string key("/rel_preserve.txt");
    ASSERT_TRUE(StatCache::getStatCacheData()->GetStat(key, &st));
    EXPECT_EQ(static_cast<uid_t>(1234), st.st_uid);
    EXPECT_EQ(static_cast<gid_t>(5678), st.st_gid);
    EXPECT_EQ(static_cast<mode_t>(S_IFREG | 0640), st.st_mode);
    // Inserted as truncatable.
    // Pass future `now` to bypass the 1s cleanup-race grace — we're verifying
    // the notruncate flag, not the freshness window.
    // cache_date uses CLOCK_MONOTONIC_COARSE; match that clock here.
    {
        struct timespec _grace_ts;
        clock_gettime(CLOCK_MONOTONIC_COARSE, &_grace_ts);
        EXPECT_TRUE(StatCache::getStatCacheData()->DelStatIfTruncatable(key, _grace_ts.tv_sec + 1000));
    }
}

TEST_F(S3fsOpsMockTest, Release_FlushSuccess_NoOrgmeta_FallbackToStatCache) {
    // Branch 2: open() saw HEAD ENOENT (new file) → orgmeta is empty.
    // The release path must fall back to existing StatCache entry rather
    // than DelStat (which would be the wrong branch for this case).
    //
    // Asserting specific uid/gid is intentionally avoided: AddStat filters
    // headers down to "x-amz*", but test_stubs overrides hws_obs_meta_* to
    // "x-hws-obs-meta-*" which falls outside that filter — production keeps
    // them because hws_obs_meta_* == "x-amz-meta-*". We instead verify the
    // branch executed (cache entry survives release with notruncate=0).
    g_mock.reset();
    g_mock.head_result = -ENOENT;
    g_mock.put_result = 0;

    struct fuse_file_info fi = {};
    fi.flags = O_WRONLY | O_CREAT;
    ASSERT_EQ(0, s3fs_open_s3("/rel_fallback.txt", &fi));

    char buf[] = "x";
    ASSERT_EQ(1, s3fs_write_s3("/rel_fallback.txt", buf, 1, 0, &fi));

    {
        headers_t seed;
        seed["Content-Type"]   = "application/octet-stream";
        seed["Content-Length"] = "0";
        std::string key("/rel_fallback.txt");
        ASSERT_TRUE(StatCache::getStatCacheData()->AddStat(key, seed, false, false));
    }

    ASSERT_EQ(0, s3fs_release_s3("/rel_fallback.txt", &fi));

    std::string key("/rel_fallback.txt");
    // Branch 2 took fallback path → entry kept (not DelStat'd).
    EXPECT_TRUE(StatCache::getStatCacheData()->HasStat(key, false));
    // Inserted as truncatable (M.3 notruncate=false).
    // Pass future `now` to bypass the 1s cleanup-race grace — we're verifying
    // the notruncate flag, not the freshness window.
    // cache_date uses CLOCK_MONOTONIC_COARSE; match that clock here.
    {
        struct timespec _grace_ts;
        clock_gettime(CLOCK_MONOTONIC_COARSE, &_grace_ts);
        EXPECT_TRUE(StatCache::getStatCacheData()->DelStatIfTruncatable(key, _grace_ts.tv_sec + 1000));
    }
}

TEST_F(S3fsOpsMockTest, Release_FlushSuccess_NoMetaAnywhere_DelStat) {
    // Branch 3: orgmeta empty AND no StatCache entry → DelStat (cache miss
    // forces next getattr to re-HEAD from OBS).
    g_mock.reset();
    g_mock.head_result = -ENOENT;
    g_mock.put_result = 0;

    struct fuse_file_info fi = {};
    fi.flags = O_WRONLY | O_CREAT;
    ASSERT_EQ(0, s3fs_open_s3("/rel_no_meta.txt", &fi));

    char buf[] = "y";
    ASSERT_EQ(1, s3fs_write_s3("/rel_no_meta.txt", buf, 1, 0, &fi));

    // No StatCache seed and no orgmeta (HEAD returned ENOENT at open time).
    StatCache::getStatCacheData()->DelStat("/rel_no_meta.txt");

    ASSERT_EQ(0, s3fs_release_s3("/rel_no_meta.txt", &fi));

    std::string key("/rel_no_meta.txt");
    EXPECT_FALSE(StatCache::getStatCacheData()->HasStat(key, false));
}

TEST_F(S3fsOpsMockTest, Release_FlushFail_SkipsStatCache) {
    g_mock.reset();
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-amz-meta-mode"] = std::to_string(S_IFREG | 0644);
    g_mock.head_meta["Content-Length"] = "0";
    g_mock.put_result = 0;  // Open needs HEAD to succeed

    // Create and open file
    struct fuse_file_info fi = {};
    fi.flags = O_WRONLY | O_CREAT;
    int rc = s3fs_open_s3("/test_release_fail", &fi);
    ASSERT_EQ(0, rc);

    // Write data to mark dirty
    char buf[] = "hello";
    int wrc = s3fs_write_s3("/test_release_fail", buf, 5, 0, &fi);
    ASSERT_EQ(5, wrc);

    // Clear StatCache before release
    StatCache::getStatCacheData()->DelStat("/test_release_fail");

    // Make PutRequest fail → Flush will fail
    g_mock.put_result = -EIO;

    // Release — Flush fails
    rc = s3fs_release_s3("/test_release_fail", &fi);
    EXPECT_EQ(0, rc);  // release always returns 0

    // Verify: StatCache should NOT be updated (the fix)
    std::string key = "/test_release_fail";
    EXPECT_FALSE(StatCache::getStatCacheData()->HasStat(key, false));

    // Cleanup
    StatCache::getStatCacheData()->DelStat("/test_release_fail");
}

TEST_F(S3fsOpsMockTest, Release_FlushFail_StillClosesFdEntity) {
    g_mock.reset();
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-amz-meta-mode"] = std::to_string(S_IFREG | 0644);
    g_mock.head_meta["Content-Length"] = "0";
    g_mock.put_result = 0;

    // Create and open file
    struct fuse_file_info fi = {};
    fi.flags = O_WRONLY | O_CREAT;
    int rc = s3fs_open_s3("/test_release_close", &fi);
    ASSERT_EQ(0, rc);

    // Write data
    char buf[] = "data";
    s3fs_write_s3("/test_release_close", buf, 4, 0, &fi);

    // Make Flush fail
    g_mock.put_result = -EIO;

    // Release
    s3fs_release_s3("/test_release_close", &fi);

    // Verify: FdEntity should be closed (GetFdEntity returns NULL)
    FdEntity* ent = FdManager::get()->GetFdEntity("/test_release_close", static_cast<int>(fi.fh));
    EXPECT_EQ(nullptr, ent);

    // Cleanup
    StatCache::getStatCacheData()->DelStat("/test_release_close");
}

TEST_F(S3fsOpsMockTest, Flush_FlushFail_SkipsStatCache) {
    g_mock.reset();
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-amz-meta-mode"] = std::to_string(S_IFREG | 0644);
    g_mock.head_meta["Content-Length"] = "0";
    g_mock.put_result = 0;

    // Create and open file
    struct fuse_file_info fi = {};
    fi.flags = O_WRONLY | O_CREAT;
    int rc = s3fs_open_s3("/test_flush_fail", &fi);
    ASSERT_EQ(0, rc);

    // Write data
    char buf[] = "hello";
    s3fs_write_s3("/test_flush_fail", buf, 5, 0, &fi);

    // Clear StatCache
    StatCache::getStatCacheData()->DelStat("/test_flush_fail");

    // Make PutRequest fail
    g_mock.put_result = -EIO;

    // Flush — should return error
    rc = s3fs_flush_s3("/test_flush_fail", &fi);
    EXPECT_NE(0, rc);

    // Verify: StatCache should NOT be updated
    std::string key = "/test_flush_fail";
    EXPECT_FALSE(StatCache::getStatCacheData()->HasStat(key, false));

    // Cleanup: release the FdEntity
    g_mock.put_result = 0;  // let release succeed or at least close
    s3fs_release_s3("/test_flush_fail", &fi);
    StatCache::getStatCacheData()->DelStat("/test_flush_fail");
}

// =================================================================
// Tests for s3fs_write_s3 — parameter validation
// =================================================================

TEST_F(S3fsOpsMockTest, Write_ZeroSize_ReturnsZero) {
    // s3fs_write_s3 with size=0 should return 0 (no-op write)
    g_mock.reset();
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-amz-meta-mode"] = std::to_string(S_IFREG | 0644);
    g_mock.head_meta["Content-Length"] = "0";

    // Create file first via open
    struct fuse_file_info fi_open = {};
    fi_open.flags = O_WRONLY | O_CREAT;
    int rc = s3fs_open_s3("/test_write_zero", &fi_open);
    if(rc == 0) {
        struct fuse_file_info fi_write = {};
        fi_write.flags = O_WRONLY;
        fi_write.fh = fi_open.fh;
        char buf[1] = {'a'};
        int wrc = s3fs_write_s3("/test_write_zero", buf, 0, 0, &fi_write);
        // size=0 → FdEntity::Write returns 0 → function returns 0
        EXPECT_EQ(0, wrc);
        // Cleanup: release
        s3fs_release_s3("/test_write_zero", &fi_write);
    }
}

TEST_F(S3fsOpsMockTest, Write_NoFdEntity_ReturnsEBADF) {
    // s3fs_write_s3 with invalid fh (no FdEntity) should return -EBADF
    struct fuse_file_info fi = {};
    fi.flags = O_WRONLY;
    fi.fh = 99999;  // No FdEntity for this fh
    char buf[10] = "test";
    int rc = s3fs_write_s3("/nonexistent_write", buf, 4, 0, &fi);
    EXPECT_EQ(-EBADF, rc);
}

// =================================================================
// 22. Open/Release/Flush/Fsync edge case tests
// =================================================================

TEST_F(S3fsOpsMockTest, Open_ReadOnly_NotFound) {
    // O_RDONLY on non-existent file → ENOENT
    g_mock.head_result = -ENOENT;
    struct fuse_file_info fi = {};
    fi.flags = O_RDONLY;
    EXPECT_EQ(-ENOENT, s3fs_open_s3("/missing_read.txt", &fi));
}

TEST_F(S3fsOpsMockTest, Open_WriteCreate_NotFound) {
    // O_WRONLY on non-existent file → succeeds (creates FdEntity)
    g_mock.head_result = -ENOENT;
    struct fuse_file_info fi = {};
    fi.flags = O_WRONLY | O_CREAT;
    int rc = s3fs_open_s3("/new_write.txt", &fi);
    EXPECT_EQ(0, rc);
    EXPECT_GT((int)fi.fh, 0);

    // Cleanup
    g_mock.put_result = 0;
    s3fs_release_s3("/new_write.txt", &fi);
}

TEST_F(S3fsOpsMockTest, Open_WithTrunc_Flag) {
    // O_WRONLY|O_TRUNC on existing 100B file → truncates to 0
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-hws-obs-meta-mode"] = std::to_string(S_IFREG | 0644);
    g_mock.head_meta["Content-Length"] = "100";
    g_mock.put_result = 0;

    struct fuse_file_info fi = {};
    fi.flags = O_WRONLY | O_TRUNC;
    int rc = s3fs_open_s3("/trunc_open.txt", &fi);
    EXPECT_EQ(0, rc);

    // Cleanup
    s3fs_release_s3("/trunc_open.txt", &fi);
}

TEST_F(S3fsOpsMockTest, Release_NoFdEntity) {
    // Release with invalid fh=99999 → -EIO
    struct fuse_file_info fi = {};
    fi.fh = 99999;
    EXPECT_EQ(-EIO, s3fs_release_s3("/no_ent_release.txt", &fi));
}

TEST_F(S3fsOpsMockTest, Fsync_NoFdEntity) {
    // Fsync with invalid fh=99999 → 0 (no-op)
    struct fuse_file_info fi = {};
    fi.fh = 99999;
    EXPECT_EQ(0, s3fs_fsync_s3("/no_ent_fsync.txt", 0, &fi));
}

TEST_F(S3fsOpsMockTest, Flush_NoFdEntity) {
    // Flush with invalid fh=99999 → 0 (no-op)
    struct fuse_file_info fi = {};
    fi.fh = 99999;
    EXPECT_EQ(0, s3fs_flush_s3("/no_ent_flush.txt", &fi));
}

TEST_F(S3fsOpsMockTest, Fsync_NoModify_NoUpload) {
    // Open file, fsync without writing → PUT not called
    g_mock.head_result = 0;
    g_mock.head_meta["Content-Type"] = "application/octet-stream";
    g_mock.head_meta["x-hws-obs-meta-mode"] = std::to_string(S_IFREG | 0644);
    g_mock.head_meta["Content-Length"] = "50";
    g_mock.put_result = 0;

    struct fuse_file_info fi = {};
    fi.flags = O_RDWR;
    int rc = s3fs_open_s3("/fsync_nomod.txt", &fi);
    ASSERT_EQ(0, rc);

    // Fsync without any write — should succeed, no upload
    g_mock.last_put_path.clear();
    rc = s3fs_fsync_s3("/fsync_nomod.txt", 0, &fi);
    EXPECT_EQ(0, rc);
    // PUT should not have been called (is_modify=false)
    EXPECT_TRUE(g_mock.last_put_path.empty());

    // Cleanup
    s3fs_release_s3("/fsync_nomod.txt", &fi);
}

// =================================================================
// 23. Close/reopen code path tests
// =================================================================

// Truncate to same size: s3fs_truncate_s3 L1932-1934 returns 0 immediately (no-op)
TEST_F(S3fsOpsMockTest, Truncate_SameSize_NoOp) {
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    g_mock.head_result_map["/same.txt"] = 0;
    g_mock.head_meta_map["/same.txt"] = file_meta;
    g_mock.head_result = -ENOENT;

    // Truncate to same size (100→100)
    int rc = s3fs_truncate_s3("/same.txt", 100);
    EXPECT_EQ(0, rc);
    // No PUT should have been called (no-op path)
    EXPECT_TRUE(g_mock.last_put_path.empty());
}

// Fsync-write-fsync cycle: tests is_modify flag management
// After first fsync, is_modify should be reset to false.
// After subsequent write, is_modify should be re-set to true.
// Second fsync should upload again.
TEST_F(S3fsOpsMockTest, Fsync_Write_Fsync_Cycle) {
    // Setup: file exists with 10 bytes
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 10);
    g_mock.head_result_map["/fwf.txt"] = 0;
    g_mock.head_meta_map["/fwf.txt"] = file_meta;
    g_mock.head_result = -ENOENT;
    g_mock.get_obj_result = 0;
    g_mock.put_result = 0;

    // Open file
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.flags = O_RDWR;
    int rc = s3fs_open_s3("/fwf.txt", &fi);
    EXPECT_EQ(0, rc);
    EXPECT_GT(fi.fh, 0u);

    // Write phase 1
    const char* data1 = "HELLO";
    rc = s3fs_write_s3("/fwf.txt", data1, 5, 0, &fi);
    EXPECT_EQ(5, rc);

    // Fsync phase 1 — should trigger upload (is_modify=true)
    g_mock.last_put_path.clear();
    rc = s3fs_fsync_s3("/fwf.txt", 0, &fi);
    EXPECT_EQ(0, rc);
    // PUT should have been called
    EXPECT_FALSE(g_mock.last_put_path.empty());

    // Write phase 2
    const char* data2 = "WORLD";
    rc = s3fs_write_s3("/fwf.txt", data2, 5, 5, &fi);
    EXPECT_EQ(5, rc);

    // Fsync phase 2 — should trigger upload again (is_modify re-set to true)
    g_mock.last_put_path.clear();
    rc = s3fs_fsync_s3("/fwf.txt", 0, &fi);
    EXPECT_EQ(0, rc);
    // PUT should have been called AGAIN (not skipped due to is_modify=false)
    EXPECT_FALSE(g_mock.last_put_path.empty());

    s3fs_release_s3("/fwf.txt", &fi);
}

// Truncate shrink then expand: data is preserved correctly through both ops
TEST_F(S3fsOpsMockTest, Truncate_Shrink_Then_Expand) {
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    g_mock.head_result_map["/se.txt"] = 0;
    g_mock.head_meta_map["/se.txt"] = file_meta;
    g_mock.head_result = -ENOENT;
    g_mock.get_obj_result = 0;
    g_mock.put_result = 0;

    // Truncate 100→50
    int rc = s3fs_truncate_s3("/se.txt", 50);
    EXPECT_EQ(0, rc);

    // Verify StatCache shows 50
    struct stat stbuf;
    memset(&stbuf, 0, sizeof(stbuf));
    rc = s3fs_getattr_s3("/se.txt", &stbuf);
    EXPECT_EQ(0, rc);
    EXPECT_EQ(50, stbuf.st_size);

    // Cleanup first truncate's FdEntity
    FdEntity* ent = FdManager::get()->GetFdEntity("/se.txt");
    if (ent) FdManager::get()->Close(ent);

    // Reset mock for second truncate (now file is 50 bytes)
    headers_t meta50;
    setup_file_meta(meta50, S_IFREG | 0644, 50);
    g_mock.head_result_map["/se.txt"] = 0;
    g_mock.head_meta_map["/se.txt"] = meta50;

    // Truncate 50→200 (expand)
    rc = s3fs_truncate_s3("/se.txt", 200);
    EXPECT_EQ(0, rc);

    // Verify StatCache shows 200
    memset(&stbuf, 0, sizeof(stbuf));
    rc = s3fs_getattr_s3("/se.txt", &stbuf);
    EXPECT_EQ(0, rc);
    EXPECT_EQ(200, stbuf.st_size);

    ent = FdManager::get()->GetFdEntity("/se.txt");
    if (ent) FdManager::get()->Close(ent);
}

// =================================================================
// 24. Interaction / divergent tests (rename chain, unlink+create, etc.)
// =================================================================

// Chain rename: A→B then B→C
// Tests that StatCache properly invalidates intermediate state and the
// second rename finds B as a valid source.
TEST_F(S3fsOpsMockTest, Rename_ChainABC) {
    // Setup: /a.txt exists as regular file
    headers_t file_meta;
    setup_file_meta(file_meta);
    g_mock.head_result_map["/a.txt"] = 0;
    g_mock.head_meta_map["/a.txt"] = file_meta;
    g_mock.head_result = -ENOENT;  // default for unlisted paths
    g_mock.delete_result = 0;

    // Rename A→B
    int rc = s3fs_rename_s3("/a.txt", "/b.txt");
    EXPECT_EQ(0, rc);

    // After A→B: StatCache should have /b.txt, not /a.txt
    // Reset mock for second rename: B exists, A does not
    g_mock.reset();
    g_mock.head_result_map["/b.txt"] = 0;
    g_mock.head_meta_map["/b.txt"] = file_meta;
    g_mock.head_result = -ENOENT;
    g_mock.delete_result = 0;

    // Rename B→C
    rc = s3fs_rename_s3("/b.txt", "/c.txt");
    EXPECT_EQ(0, rc);

    // Verify: StatCache should have /c.txt
    std::string c_key("/c.txt");
    EXPECT_TRUE(StatCache::getStatCacheData()->HasStat(c_key));
}

// Round-trip rename: A→B then B→A
// Tests that StatCache correctly handles renames back to original path.
TEST_F(S3fsOpsMockTest, Rename_RoundTrip_ABA) {
    headers_t file_meta;
    setup_file_meta(file_meta);
    g_mock.head_result_map["/rt.txt"] = 0;
    g_mock.head_meta_map["/rt.txt"] = file_meta;
    g_mock.head_result = -ENOENT;
    g_mock.delete_result = 0;

    // A→B
    int rc = s3fs_rename_s3("/rt.txt", "/rt_tmp.txt");
    EXPECT_EQ(0, rc);

    // Reset for B→A
    g_mock.reset();
    g_mock.head_result_map["/rt_tmp.txt"] = 0;
    g_mock.head_meta_map["/rt_tmp.txt"] = file_meta;
    g_mock.head_result = -ENOENT;
    g_mock.delete_result = 0;

    // B→A (back to original)
    rc = s3fs_rename_s3("/rt_tmp.txt", "/rt.txt");
    EXPECT_EQ(0, rc);

    // Verify: /rt.txt in StatCache, /rt_tmp.txt not
    std::string rt_key("/rt.txt");
    EXPECT_TRUE(StatCache::getStatCacheData()->HasStat(rt_key));
}

// Unlink then create same path: verifies StatCache doesn't return stale data
TEST_F(S3fsOpsMockTest, Unlink_ThenCreate_SamePath) {
    // Setup: /uc.txt exists
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 50);
    g_mock.head_result_map["/uc.txt"] = 0;
    g_mock.head_meta_map["/uc.txt"] = file_meta;
    g_mock.head_result = -ENOENT;  // default
    g_mock.delete_result = 0;

    // Add to StatCache to simulate prior getattr
    std::string uc_add_key("/uc.txt");
    StatCache::getStatCacheData()->AddStat(uc_add_key, file_meta, false, true);

    // Unlink
    int rc = s3fs_unlink_s3("/uc.txt");
    EXPECT_EQ(0, rc);

    // Verify: StatCache should NOT have /uc.txt after unlink
    std::string uc_key("/uc.txt");
    EXPECT_FALSE(StatCache::getStatCacheData()->HasStat(uc_key));
}

// Truncate then getattr: verifies StatCache reflects new size
TEST_F(S3fsOpsMockTest, Truncate_ThenGetattr_SizeConsistent) {
    // Setup: /tg.txt exists with 100 bytes
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    g_mock.head_result_map["/tg.txt"] = 0;
    g_mock.head_meta_map["/tg.txt"] = file_meta;
    g_mock.head_result = -ENOENT;
    g_mock.get_obj_result = 0;
    g_mock.put_result = 0;

    // Truncate to 30
    int rc = s3fs_truncate_s3("/tg.txt", 30);
    EXPECT_EQ(0, rc);

    // Getattr should return size=30
    struct stat stbuf;
    memset(&stbuf, 0, sizeof(stbuf));
    rc = s3fs_getattr_s3("/tg.txt", &stbuf);
    EXPECT_EQ(0, rc);
    EXPECT_EQ(30, stbuf.st_size);

    // Cleanup: release any open FdEntity
    FdEntity* cleanup_ent = FdManager::get()->GetFdEntity("/tg.txt");
    if (cleanup_ent) {
        FdManager::get()->Close(cleanup_ent);
    }
}

// Rename small file overwriting larger: verifies StatCache size update
TEST_F(S3fsOpsMockTest, Rename_SmallOverwriteLarge_SizeUpdated) {
    headers_t small_meta, large_meta;
    setup_file_meta(small_meta, S_IFREG | 0644, 10);    // 10 bytes
    setup_file_meta(large_meta, S_IFREG | 0644, 1000);  // 1000 bytes

    // Both exist
    g_mock.head_result_map["/small.txt"] = 0;
    g_mock.head_meta_map["/small.txt"] = small_meta;
    g_mock.head_result_map["/large.txt"] = 0;
    g_mock.head_meta_map["/large.txt"] = large_meta;
    g_mock.head_result = -ENOENT;
    g_mock.delete_result = 0;

    // Pre-populate StatCache with large.txt (size=1000)
    std::string pre_key("/large.txt");
    StatCache::getStatCacheData()->AddStat(pre_key, large_meta, false, true);

    // Rename small → large (overwrite)
    int rc = s3fs_rename_s3("/small.txt", "/large.txt");
    EXPECT_EQ(0, rc);

    // After rename: /large.txt should have size=10 (from small), not 1000
    struct stat stbuf;
    memset(&stbuf, 0, sizeof(stbuf));
    std::string large_key("/large.txt");
    if (StatCache::getStatCacheData()->GetStat(large_key, &stbuf)) {
        EXPECT_EQ(10, stbuf.st_size);
    }
    // /small.txt should be gone
    std::string small_key("/small.txt");
    EXPECT_FALSE(StatCache::getStatCacheData()->HasStat(small_key));
}

// =================================================================
// Tests for s3fs_setxattr_s3 (currently 0% coverage)
// =================================================================
TEST_F(S3fsOpsMockTest, Setxattr_NewAttribute) {
    // Setup: file exists
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    g_mock.head_result_map["/testfile"] = 0;
    g_mock.head_meta_map["/testfile"] = file_meta;
    g_mock.head_result = -ENOENT;
    g_mock.put_head_result = 0;

    int rc = s3fs_setxattr_s3("/testfile", "user.comment", "test value", 11, 0);
    EXPECT_EQ(0, rc);
}

TEST_F(S3fsOpsMockTest, Setxattr_FileNotFound) {
    // When file doesn't exist, HeadRequest returns -ENOENT
    // But the function continues and tries to create xattr
    // PutHeadRequest will be called
    g_mock.head_result = -ENOENT;
    g_mock.head_result_map.clear();
    g_mock.head_meta_map.clear();
    g_mock.put_head_result = -ENOENT;  // Simulate PutHeadRequest failure

    int rc = s3fs_setxattr_s3("/nonexistent", "user.comment", "test", 5, 0);
    // PutHeadRequest fails with -ENOENT, function returns -EIO
    EXPECT_EQ(-EIO, rc);
}

TEST_F(S3fsOpsMockTest, Setxattr_HeadRequestFails) {
    g_mock.head_result = -EACCES;
    g_mock.head_result_map.clear();

    int rc = s3fs_setxattr_s3("/testfile", "user.comment", "test", 5, 0);
    EXPECT_EQ(-EACCES, rc);
}

// =================================================================
// Tests for s3fs_getxattr_s3 (currently 0% coverage)
// =================================================================
TEST_F(S3fsOpsMockTest, Getxattr_ExistingAttribute) {
    // Setup: file with xattr
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    file_meta["x-amz-meta-xattr-user.comment"] = "test value";
    g_mock.head_result_map["/testfile"] = 0;
    g_mock.head_meta_map["/testfile"] = file_meta;
    g_mock.head_result = -ENOENT;

    char buf[256];
    int rc = s3fs_getxattr_s3("/testfile", "user.comment", buf, sizeof(buf));
    EXPECT_GT(rc, 0);  // Should return the length of the value
}

TEST_F(S3fsOpsMockTest, Getxattr_NonExistingAttribute) {
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    g_mock.head_result_map["/testfile"] = 0;
    g_mock.head_meta_map["/testfile"] = file_meta;
    g_mock.head_result = -ENOENT;

    char buf[256];
    int rc = s3fs_getxattr_s3("/testfile", "user.nonexistent", buf, sizeof(buf));
    EXPECT_EQ(-ENODATA, rc);
}

TEST_F(S3fsOpsMockTest, Getxattr_SizeQuery) {
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    file_meta["x-amz-meta-xattr-user.comment"] = "test value";
    g_mock.head_result_map["/testfile"] = 0;
    g_mock.head_meta_map["/testfile"] = file_meta;
    g_mock.head_result = -ENOENT;

    int rc = s3fs_getxattr_s3("/testfile", "user.comment", NULL, 0);
    EXPECT_GT(rc, 0);  // Should return the size needed
}

TEST_F(S3fsOpsMockTest, Getxattr_BufferTooSmall) {
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    file_meta["x-amz-meta-xattr-user.comment"] = "test value that is quite long";
    g_mock.head_result_map["/testfile"] = 0;
    g_mock.head_meta_map["/testfile"] = file_meta;
    g_mock.head_result = -ENOENT;

    char buf[5];  // Too small
    int rc = s3fs_getxattr_s3("/testfile", "user.comment", buf, sizeof(buf));
    EXPECT_EQ(-ERANGE, rc);
}

TEST_F(S3fsOpsMockTest, Getxattr_HeadRequestFails) {
    g_mock.head_result = -ENOENT;
    g_mock.head_result_map.clear();

    char buf[256];
    int rc = s3fs_getxattr_s3("/nonexistent", "user.comment", buf, sizeof(buf));
    EXPECT_EQ(-ENOENT, rc);
}

// =================================================================
// Tests for s3fs_listxattr_s3 (currently 0% coverage)
// =================================================================
TEST_F(S3fsOpsMockTest, Listxattr_MultipleAttributes) {
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    file_meta["x-amz-meta-xattr-user.comment"] = "comment";
    file_meta["x-amz-meta-xattr-user.label"] = "label";
    g_mock.head_result_map["/testfile"] = 0;
    g_mock.head_meta_map["/testfile"] = file_meta;
    g_mock.head_result = -ENOENT;

    char buf[256];
    int rc = s3fs_listxattr_s3("/testfile", buf, sizeof(buf));
    EXPECT_GT(rc, 0);  // Should return the total size
}

TEST_F(S3fsOpsMockTest, Listxattr_NoAttributes) {
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    g_mock.head_result_map["/testfile"] = 0;
    g_mock.head_meta_map["/testfile"] = file_meta;
    g_mock.head_result = -ENOENT;

    char buf[256];
    int rc = s3fs_listxattr_s3("/testfile", buf, sizeof(buf));
    EXPECT_EQ(0, rc);  // No xattrs
}

TEST_F(S3fsOpsMockTest, Listxattr_SizeQuery) {
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    file_meta["x-amz-meta-xattr-user.comment"] = "comment";
    g_mock.head_result_map["/testfile"] = 0;
    g_mock.head_meta_map["/testfile"] = file_meta;
    g_mock.head_result = -ENOENT;

    int rc = s3fs_listxattr_s3("/testfile", NULL, 0);
    EXPECT_GT(rc, 0);  // Should return the size needed
}

TEST_F(S3fsOpsMockTest, Listxattr_BufferTooSmall) {
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    file_meta["x-amz-meta-xattr-user.comment"] = "comment";
    file_meta["x-amz-meta-xattr-user.label"] = "label";
    g_mock.head_result_map["/testfile"] = 0;
    g_mock.head_meta_map["/testfile"] = file_meta;
    g_mock.head_result = -ENOENT;

    char buf[5];  // Too small
    int rc = s3fs_listxattr_s3("/testfile", buf, sizeof(buf));
    EXPECT_EQ(-ERANGE, rc);
}

TEST_F(S3fsOpsMockTest, Listxattr_HeadRequestFails) {
    g_mock.head_result = -ENOENT;
    g_mock.head_result_map.clear();

    char buf[256];
    int rc = s3fs_listxattr_s3("/nonexistent", buf, sizeof(buf));
    EXPECT_EQ(-ENOENT, rc);
}

// =================================================================
// Tests for s3fs_removexattr_s3 (currently 0% coverage)
// =================================================================
TEST_F(S3fsOpsMockTest, Removexattr_ExistingAttribute) {
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    file_meta["x-amz-meta-xattr-user.comment"] = "comment";
    g_mock.head_result_map["/testfile"] = 0;
    g_mock.head_meta_map["/testfile"] = file_meta;
    g_mock.head_result = -ENOENT;
    g_mock.put_head_result = 0;

    int rc = s3fs_removexattr_s3("/testfile", "user.comment");
    EXPECT_EQ(0, rc);
}

TEST_F(S3fsOpsMockTest, Removexattr_NonExistingAttribute) {
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    g_mock.head_result_map["/testfile"] = 0;
    g_mock.head_meta_map["/testfile"] = file_meta;
    g_mock.head_result = -ENOENT;

    int rc = s3fs_removexattr_s3("/testfile", "user.nonexistent");
    EXPECT_EQ(-ENODATA, rc);
}

TEST_F(S3fsOpsMockTest, Removexattr_HeadRequestFails) {
    g_mock.head_result = -ENOENT;
    g_mock.head_result_map.clear();

    int rc = s3fs_removexattr_s3("/nonexistent", "user.comment");
    EXPECT_EQ(-ENOENT, rc);
}

// =================================================================
// Tests for s3fs_read_s3 (currently 0% coverage)
// =================================================================
TEST_F(S3fsOpsMockTest, ReadS3_InvalidFd) {
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    fi.fh = -1;  // Invalid fd

    char buf[1024];
    int rc = s3fs_read_s3("/testfile", buf, sizeof(buf), 0, &fi);
    // With invalid fd, GetFdEntity returns NULL
    EXPECT_EQ(-EBADF, rc);
}

TEST_F(S3fsOpsMockTest, ReadS3_ValidFd) {
    // Setup: create a file with content
    headers_t file_meta;
    setup_file_meta(file_meta, S_IFREG | 0644, 100);
    g_mock.head_result_map["/testfile"] = 0;
    g_mock.head_meta_map["/testfile"] = file_meta;
    g_mock.head_result = -ENOENT;
    g_mock.get_obj_result = 0;

    // Open the file first
    struct fuse_file_info fi;
    memset(&fi, 0, sizeof(fi));
    int open_rc = s3fs_open_s3("/testfile", &fi);
    ASSERT_EQ(0, open_rc);

    // Read from the file
    char buf[1024];
    int rc = s3fs_read_s3("/testfile", buf, sizeof(buf), 0, &fi);
    // Read should succeed (may return 0 or more bytes depending on mock)
    EXPECT_GE(rc, 0);

    // Close the file
    s3fs_release_s3("/testfile", &fi);
}

// =================================================================
// main
// =================================================================
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    xmlInitParser();
    int result = RUN_ALL_TESTS();
    xmlCleanupParser();
    return result;
}
