/*
 * hws_s3fs_xattrs_test_helper.h
 *
 * Test helper for accessing static functions from hws_s3fs.cpp
 * This header is only included in test builds to enable unit testing
 * of otherwise internal static functions.
 */

#ifndef HWS_S3FS_XATTRS_TEST_HELPER_H_
#define HWS_S3FS_XATTRS_TEST_HELPER_H_

#include <string>
#include <map>
#include "common.h"

// Forward declarations for testing static functions
// These functions are normally static in hws_s3fs.cpp
// but we make them available for testing via this helper

#ifdef HWS_S3FS_TESTING

// Test helper function to parse xattr key-value pair
// Returns true on success, false on failure
bool test_parse_xattr_keyval(const std::string& xattrpair,
                              std::string& key,
                              PXATTRVAL& pval);

// Test helper function to parse xattrs from string
// Returns the number of xattrs parsed
size_t test_parse_xattrs(const std::string& strxattrs, xattrs_t& xattrs);

// Test helper function to build xattrs string from map
std::string test_build_xattrs(const xattrs_t& xattrs);

// Test helper function to free xattrs memory
void test_free_xattrs(xattrs_t& xattrs);

#endif // HWS_S3FS_TESTING

#endif // HWS_S3FS_XATTRS_TEST_HELPER_H_
