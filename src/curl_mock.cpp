// ==========================================================================
// curl_mock.cpp
//
// Implementation of libcurl mock functions using GNU ld --wrap.
// These functions replace the real libcurl functions at link time.
//
// Link with: -Wl,--wrap=curl_easy_perform,--wrap=curl_easy_getinfo,
//            --wrap=curl_easy_setopt,--wrap=sleep
// ==========================================================================

#include "curl_mock.h"
#include "common.h"  // for headers_t (header_nocase_cmp)
#include <cstdarg>
#include <cstring>
#include <mutex>

// ==========================================================================
// CurlMockController Implementation
// ==========================================================================

CurlMockController& CurlMockController::Instance() {
    static CurlMockController instance;
    return instance;
}

void CurlMockController::Reset() {
    while (!perform_results_.empty()) {
        perform_results_.pop();
    }
    perform_call_count_ = 0;
    response_code_ = 200;
    effective_url_.clear();
    content_type_.clear();
    setopt_records_.clear();
    response_body_.clear();
    error_message_.clear();
    perform_callback_ = nullptr;
    mock_response_headers_.clear();
    captured_header_data_ = nullptr;
    captured_write_data_ = nullptr;
    captured_write_func_ = nullptr;
    captured_request_headers_.clear();
    last_url_.clear();
}

void CurlMockController::QueuePerformResult(CURLcode result) {
    perform_results_.push(result);
}

void CurlMockController::QueuePerformResults(std::initializer_list<CURLcode> results) {
    for (CURLcode result : results) {
        perform_results_.push(result);
    }
}

void CurlMockController::SetPerformResult(CURLcode result) {
    while (!perform_results_.empty()) {
        perform_results_.pop();
    }
    perform_results_.push(result);
}

CURLcode CurlMockController::GetNextPerformResult() {
    std::lock_guard<std::mutex> lock(perform_mutex_);
    perform_call_count_++;
    if (perform_callback_) {
        return perform_callback_(nullptr);
    }
    if (perform_results_.empty()) {
        return CURLE_OK;
    }
    CURLcode result = perform_results_.front();
    perform_results_.pop();
    return result;
}

void CurlMockController::RecordSetopt(CURLoption option, const std::string& value) {
    setopt_records_[option] = value;
}

bool CurlMockController::WasSetoptCalled(CURLoption option) const {
    return setopt_records_.find(option) != setopt_records_.end();
}

void CurlMockController::ClearSetoptRecords() {
    setopt_records_.clear();
}

// ==========================================================================
// Wrapped libcurl Functions
// These replace the real functions when linked with --wrap
// ==========================================================================

extern "C" {

// Declaration of real functions (for forwarding if needed)
extern CURLcode __real_curl_easy_perform(CURL* handle);
extern CURLcode __real_curl_easy_getinfo(CURL* handle, CURLINFO info, ...);
extern CURLcode __real_curl_easy_setopt(CURL* handle, CURLoption option, ...);
extern unsigned int __real_sleep(unsigned int seconds);

// Mock implementation of curl_easy_setopt
// Captures HEADERDATA, WRITEDATA, and WRITEFUNCTION for response injection.
CURLcode __wrap_curl_easy_setopt(CURL* handle, CURLoption option, ...) {
    (void)handle;

    va_list args;
    va_start(args, option);

    CurlMockController& mock = CurlMockController::Instance();

    switch (option) {
        case CURLOPT_URL: {
            // New request setup — clear stale captured pointers from previous requests
            // so header/body injection only applies to requests that explicitly set them.
            mock.SetCapturedHeaderData(nullptr);
            mock.SetCapturedWriteData(nullptr);
            mock.SetCapturedWriteFunc(nullptr);
            // Reset per-request HTTPHEADER capture so each request's headers
            // are isolated for assertion.
            mock.ClearCapturedRequestHeaders();
            const char* url = va_arg(args, char*);
            if (url) {
                mock.SetLastUrl(url);
            } else {
                mock.SetLastUrl("");
            }
            break;
        }
        case CURLOPT_HEADERDATA:
            mock.SetCapturedHeaderData(va_arg(args, void*));
            break;
        case CURLOPT_WRITEDATA:
            mock.SetCapturedWriteData(va_arg(args, void*));
            break;
        case CURLOPT_WRITEFUNCTION:
            mock.SetCapturedWriteFunc(va_arg(args, curl_write_callback));
            break;
        case CURLOPT_HTTPHEADER: {
            // Capture each header line in the curl_slist for assertion.
            // libcurl accepts NULL to clear; treat as no-op (already cleared
            // on CURLOPT_URL).
            curl_slist* slist = va_arg(args, curl_slist*);
            for (curl_slist* node = slist; node != nullptr; node = node->next) {
                if (node->data) {
                    mock.AddCapturedRequestHeader(node->data);
                }
            }
            break;
        }
        default:
            break;
    }

    va_end(args);
    return CURLE_OK;
}

// Mock implementation of curl_easy_perform
// Injects response headers and body if configured.
CURLcode __wrap_curl_easy_perform(CURL* handle) {
    (void)handle;
    CurlMockController& mock = CurlMockController::Instance();
    CURLcode result = mock.GetNextPerformResult();

    // Inject response headers if configured.
    // CURLOPT_HEADERDATA points to S3fsCurl::responseHeaders (headers_t*).
    const auto& mock_headers = mock.GetMockResponseHeaders();
    if (!mock_headers.empty()) {
        void* header_data = mock.GetCapturedHeaderData();
        if (header_data) {
            headers_t* resp = static_cast<headers_t*>(header_data);
            for (const auto& kv : mock_headers) {
                (*resp)[kv.first] = kv.second;
            }
        }
    }

    // Inject response body via WRITEFUNCTION callback
    const std::string& body = mock.GetResponseBody();
    if (!body.empty()) {
        curl_write_callback write_func = mock.GetCapturedWriteFunc();
        void* write_data = mock.GetCapturedWriteData();
        if (write_func && write_data) {
            write_func(const_cast<char*>(body.data()), 1, body.size(), write_data);
        }
    }

    return result;
}

// Mock implementation of curl_easy_getinfo
CURLcode __wrap_curl_easy_getinfo(CURL* handle, CURLINFO info, ...) {
    (void)handle;

    va_list args;
    va_start(args, info);

    CURLcode result = CURLE_OK;
    CurlMockController& mock = CurlMockController::Instance();

    switch (info) {
        case CURLINFO_RESPONSE_CODE: {
            long* code = va_arg(args, long*);
            if (code) {
                *code = mock.GetResponseCode();
            }
            break;
        }
        case CURLINFO_EFFECTIVE_URL: {
            char** url = va_arg(args, char**);
            if (url) {
                static thread_local std::string url_storage;
                url_storage = mock.GetEffectiveUrl();
                *url = const_cast<char*>(url_storage.c_str());
            }
            break;
        }
        case CURLINFO_CONTENT_TYPE: {
            char** type = va_arg(args, char**);
            if (type) {
                static thread_local std::string type_storage;
                type_storage = mock.GetContentType();
                *type = type_storage.empty() ? nullptr : const_cast<char*>(type_storage.c_str());
            }
            break;
        }
        case CURLINFO_SIZE_DOWNLOAD: {
            double* size = va_arg(args, double*);
            if (size) {
                *size = static_cast<double>(mock.GetResponseBody().size());
            }
            break;
        }
        default:
            break;
    }

    va_end(args);
    return result;
}

// Mock implementation of sleep - returns immediately in tests
unsigned int __wrap_sleep(unsigned int seconds) {
    (void)seconds;
    return 0;
}

} // extern "C"
