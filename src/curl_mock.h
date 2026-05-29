// ==========================================================================
// curl_mock.h
//
// Mock infrastructure for libcurl API functions using GNU ld --wrap.
// Provides controllable, deterministic behavior for unit testing.
//
// Usage:
//   1. Link test binary with -Wl,--wrap=curl_easy_perform,--wrap=curl_easy_getinfo,
//      --wrap=curl_easy_setopt,--wrap=sleep
//   2. Include this header in test file
//   3. Use CurlMockController to set up expected behavior
// ==========================================================================

#ifndef CURL_MOCK_H_
#define CURL_MOCK_H_

#include <curl/curl.h>
#include <functional>
#include <queue>
#include <string>
#include <vector>
#include <map>
#include <mutex>

// ==========================================================================
// CurlMockController - Global mock state controller
// ==========================================================================
class CurlMockController {
public:
    // Singleton access
    static CurlMockController& Instance();

    // Reset all mock state (call in test SetUp/TearDown)
    void Reset();

    // ---- curl_easy_perform mock control ----
    // Queue multiple return values for sequential calls
    void QueuePerformResult(CURLcode result);
    void QueuePerformResults(std::initializer_list<CURLcode> results);
    // Set single return value (clears queue, then pushes)
    void SetPerformResult(CURLcode result);
    // Get next queued result (returns CURLE_OK if queue empty)
    CURLcode GetNextPerformResult();
    // Get call count
    int GetPerformCallCount() const { return perform_call_count_; }

    // ---- curl_easy_getinfo mock control ----
    // Set response code to return for CURLINFO_RESPONSE_CODE
    void SetResponseCode(long code) { response_code_ = code; }
    long GetResponseCode() const { return response_code_; }
    // Set effective URL to return for CURLINFO_EFFECTIVE_URL
    void SetEffectiveUrl(const std::string& url) { effective_url_ = url; }
    const std::string& GetEffectiveUrl() const { return effective_url_; }
    // Set content type to return for CURLINFO_CONTENT_TYPE
    void SetContentType(const std::string& type) { content_type_ = type; }
    const std::string& GetContentType() const { return content_type_; }

    // ---- curl_easy_setopt mock control ----
    // Track what options were set
    void RecordSetopt(CURLoption option, const std::string& value);
    bool WasSetoptCalled(CURLoption option) const;
    void ClearSetoptRecords();

    // ---- Custom callbacks for advanced scenarios ----
    // Set custom perform callback (overrides queue)
    using PerformCallback = std::function<CURLcode(CURL*)>;
    void SetPerformCallback(PerformCallback cb) { perform_callback_ = cb; }
    PerformCallback GetPerformCallback() const { return perform_callback_; }

    // ---- Response body simulation ----
    // Set body data to be "received" by WriteMemoryCallback
    void SetResponseBody(const std::string& body) { response_body_ = body; }
    const std::string& GetResponseBody() const { return response_body_; }

    // ---- Error message simulation ----
    void SetErrorMessage(const std::string& msg) { error_message_ = msg; }
    const std::string& GetErrorMessage() const { return error_message_; }

    // ---- Response header injection ----
    // Set headers to be injected directly into S3fsCurl's responseHeaders map
    // during __wrap_curl_easy_perform. Keys should match HTTP header names.
    void SetMockResponseHeaders(const std::map<std::string, std::string>& h) {
        mock_response_headers_ = h;
    }
    void AddMockResponseHeader(const std::string& key, const std::string& value) {
        mock_response_headers_[key] = value;
    }
    void ClearMockResponseHeaders() { mock_response_headers_.clear(); }
    const std::map<std::string, std::string>& GetMockResponseHeaders() const {
        return mock_response_headers_;
    }

    // ---- Captured setopt state (for response injection) ----
    void SetCapturedHeaderData(void* ptr) { captured_header_data_ = ptr; }
    void* GetCapturedHeaderData() const { return captured_header_data_; }
    void SetCapturedWriteData(void* ptr) { captured_write_data_ = ptr; }
    void* GetCapturedWriteData() const { return captured_write_data_; }
    void SetCapturedWriteFunc(curl_write_callback cb) { captured_write_func_ = cb; }
    curl_write_callback GetCapturedWriteFunc() const { return captured_write_func_; }

    // ---- Captured request headers (CURLOPT_HTTPHEADER) ----
    // Each string in the vector is one HTTP header line ("Name: Value").
    // Cleared on Reset() and at the start of every new request (CURLOPT_URL).
    // Tests assert presence/absence via HasRequestHeaderPrefix.
    void AddCapturedRequestHeader(const std::string& line) {
        captured_request_headers_.push_back(line);
    }
    void ClearCapturedRequestHeaders() { captured_request_headers_.clear(); }
    bool HasRequestHeaderPrefix(const std::string& prefix) const {
        for (const auto& h : captured_request_headers_) {
            if (h.size() >= prefix.size() &&
                0 == h.compare(0, prefix.size(), prefix)) {
                return true;
            }
        }
        return false;
    }

    // ---- Captured request URL (CURLOPT_URL) ----
    const std::string& GetLastUrl() const { return last_url_; }
    void SetLastUrl(const std::string& u) { last_url_ = u; }

private:
    CurlMockController() = default;
    ~CurlMockController() = default;
    CurlMockController(const CurlMockController&) = delete;
    CurlMockController& operator=(const CurlMockController&) = delete;

    // Perform mock state
    std::queue<CURLcode> perform_results_;
    int perform_call_count_ = 0;
    PerformCallback perform_callback_;
    std::mutex perform_mutex_;

    // Getinfo mock state
    long response_code_ = 200;
    std::string effective_url_;
    std::string content_type_;

    // Setopt tracking
    std::map<CURLoption, std::string> setopt_records_;

    // Response simulation
    std::string response_body_;
    std::string error_message_;

    // Response header injection
    std::map<std::string, std::string> mock_response_headers_;

    // Captured setopt state for response injection
    void* captured_header_data_ = nullptr;
    void* captured_write_data_ = nullptr;
    curl_write_callback captured_write_func_ = nullptr;

    // Captured request headers (CURLOPT_HTTPHEADER) for assertion
    std::vector<std::string> captured_request_headers_;

    // Captured request URL (CURLOPT_URL) for assertion
    std::string last_url_;
};

// ==========================================================================
// ScopedMock - RAII helper for automatic cleanup
// ==========================================================================
class ScopedCurlMock {
public:
    ScopedCurlMock() { CurlMockController::Instance().Reset(); }
    ~ScopedCurlMock() { CurlMockController::Instance().Reset(); }
};

// ==========================================================================
// Convenience macros for test assertions
// ==========================================================================
#define EXPECT_PERFORM_CALLED_TIMES(n) \
    EXPECT_EQ(CurlMockController::Instance().GetPerformCallCount(), n)

#define EXPECT_RESPONSE_CODE(code) \
    EXPECT_EQ(CurlMockController::Instance().GetResponseCode(), code)

#endif // CURL_MOCK_H_
