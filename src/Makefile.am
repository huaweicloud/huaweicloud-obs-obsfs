######################################################################
# s3fs - FUSE-based file system backed by Amazon S3
#
# Copyright 2007-2008 Randy Rizun <rrizun@gmail.com>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
######################################################################
bin_PROGRAMS=obsfs

AM_CPPFLAGS = $(DEPS_CFLAGS)

obsfs_SOURCES = s3fs.h curl.cpp curl.h cache.cpp cache.h string_util.cpp string_util.h s3fs_util.cpp s3fs_util.h fdcache.cpp fdcache.h fdcache_untreated.cpp fdcache_untreated.h \
     readdir_marker_map.h readdir_marker_map.cpp common_auth.cpp s3fs_auth.h addhead.cpp addhead.h hws_index_cache.h hws_index_cache.cpp common.h\
     hws_s3fs.cpp hws_s3fs_statis_api.h hws_s3fs_statis_api.cpp hws_fd_cache.cpp hws_fd_cache.h\
     crc32c.cpp crc32c.h hws_configure.cpp hws_configure.h obsfs_log.cpp obsfs_log.h hws_fs_util.h hws_fs_util.cpp hws_cipher_key_check.h hws_cipher_key_check.cpp\
     s3fs_operations.h s3fs_operations.cpp \
     hws_encrypt_util.h hws_encrypt_util.cpp \
     openssl_auth.cpp

obsfs_LDADD = $(DEPS_LIBS)

#
# NOTE: CLEANFILES with wildcards doesn't work properly with autotools
# because `test -z` checks if the string is empty, not if files exist.
# We use clean-local target instead.
# CLEANFILES = *.gcda *.gcno *.gcov coverage.info ../coverage_html

#
# Unit Tests
#

# Google Test test programs
# Note: Some tests have complex linking dependencies due to tight code coupling.
# These are tested via integration tests instead:
# - bucket_detection_test (needs curl.o, s3fs_util.o, and many dependencies)
# - virtual_inode_test (needs s3fs_util.o which has many dependencies)
#
# Current unit tests focus on isolated utility functions that can be tested independently.
# Note: hws_fd_cache_test and hws_fd_manager_test are temporarily disabled due to
# complex linking dependencies (fdcache.o requires curl.o which has many dependencies).
# These should be tested via integration tests instead.
check_PROGRAMS = string_util_test s3fs_util_test s3fs_util_path_test cache_test hws_index_cache_test crc32c_test hws_cipher_key_check_test hws_fs_util_test readdir_marker_map_test hws_configure_test common_auth_test obsfs_log_test hws_s3fs_statis_api_test addhead_test fdpage_test hws_fd_cache_test openssl_auth_test fdcache_linkage_test snas_list_test hws_s3fs_xattrs_test s3fs_test curl_core_test curl_coverage_test curl_include_test curl_util_test curl_extended_test curl_request_mock_test curl_callback_mock_test curl_auth_mock_test curl_network_mock_test fdcache_coverage_test fdcache_network_mock_test fdcache_fs_mock_test fdcache_combined_mock_test s3fs_operations_include_test s3fs_operations_coverage_test s3fs_operations_comprehensive_test hws_s3fs_include_test s3fs_ops_linkage_test hws_fd_cache_linkage_test s3fs_operations_mock_test hws_configure_linkage_test fdcache_untreated_test hws_s3fs_fuse_mock_test hws_s3fs_linkage_test hws_s3fs_coverage_test
TESTS = string_util_test s3fs_util_test s3fs_util_path_test cache_test hws_index_cache_test crc32c_test hws_cipher_key_check_test hws_fs_util_test readdir_marker_map_test hws_configure_test common_auth_test obsfs_log_test hws_s3fs_statis_api_test addhead_test fdpage_test hws_fd_cache_test openssl_auth_test fdcache_linkage_test snas_list_test hws_s3fs_xattrs_test s3fs_test curl_core_test curl_coverage_test curl_include_test curl_util_test curl_extended_test curl_request_mock_test curl_callback_mock_test curl_auth_mock_test curl_network_mock_test fdcache_coverage_test fdcache_network_mock_test fdcache_fs_mock_test fdcache_combined_mock_test s3fs_operations_include_test s3fs_operations_coverage_test s3fs_operations_comprehensive_test hws_s3fs_include_test s3fs_ops_linkage_test hws_fd_cache_linkage_test s3fs_operations_mock_test hws_configure_linkage_test fdcache_untreated_test hws_s3fs_fuse_mock_test hws_s3fs_linkage_test hws_s3fs_coverage_test

#
# Mock Infrastructure (for testing S3 operations)
#
# Note: Mock files are built manually for integration tests.
# They are not part of the standard test suite to avoid
# complex linking dependencies.
#
# To build mock infrastructure:
#   g++ -c src/test/mock/mock_s3fs_curl.cpp -Isrc -I/usr/include/libxml2
#   g++ -c src/test/s3fs_operations_test_base.cpp -Isrc -Isrc/test -I/usr/include/libxml2
#

string_util_test_SOURCES = string_util_test.cpp gtest-all.cc
string_util_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
string_util_test_LDADD = string_util.$(OBJEXT) $(DEPS_LIBS) -lcrypt -lxml2 -lcurl

s3fs_util_test_SOURCES = s3fs_util_test.cpp test_stubs.cpp gtest-all.cc
s3fs_util_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
s3fs_util_test_LDADD = s3fs_util.$(OBJEXT) string_util.$(OBJEXT) $(DEPS_LIBS) -lcrypt -lxml2 -lcurl

# Unit tests for s3fs_util path-related functions (get_basename, etc.)
s3fs_util_path_test_SOURCES = s3fs_util_path_test.cpp test_stubs.cpp gtest-all.cc
s3fs_util_path_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
s3fs_util_path_test_LDADD = s3fs_util.$(OBJEXT) string_util.$(OBJEXT) $(DEPS_LIBS) -lcrypt -lxml2 -lcurl

# Unit tests for cache functionality (DelStatWildcard, etc.)
cache_test_SOURCES = cache_test.cpp test_stubs.cpp gtest-all.cc
cache_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
cache_test_LDADD = cache.$(OBJEXT) string_util.$(OBJEXT) s3fs_util.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl

# Unit tests for IndexCache (DeleteIndexWildcard, etc.)
hws_index_cache_test_SOURCES = hws_index_cache_test.cpp test_stubs.cpp gtest-all.cc
hws_index_cache_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
hws_index_cache_test_LDADD = hws_index_cache.$(OBJEXT) string_util.$(OBJEXT) s3fs_util.$(OBJEXT) $(DEPS_LIBS) -lcrypt -lxml2 -lcurl

# Unit tests for CRC32C checksum functions
crc32c_test_SOURCES = crc32c_test.cpp gtest-all.cc
crc32c_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
crc32c_test_LDADD = crc32c.$(OBJEXT) $(DEPS_LIBS)

# Unit tests for HwsCipherKeyCheck
hws_cipher_key_check_test_SOURCES = hws_cipher_key_check_test.cpp gtest-all.cc
hws_cipher_key_check_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
hws_cipher_key_check_test_LDADD = hws_cipher_key_check.$(OBJEXT) $(DEPS_LIBS)

# Unit tests for HwsFsUtil (verifyPath function)
hws_fs_util_test_SOURCES = hws_fs_util_test.cpp gtest-all.cc
hws_fs_util_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
hws_fs_util_test_LDADD = hws_fs_util.$(OBJEXT) $(DEPS_LIBS)

# Unit tests for readdir_marker_map (marker management for readdir operations)
readdir_marker_map_test_SOURCES = readdir_marker_map_test.cpp gtest-all.cc
readdir_marker_map_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
readdir_marker_map_test_LDADD = readdir_marker_map.$(OBJEXT) string_util.$(OBJEXT) obsfs_log.$(OBJEXT) hws_fs_util.$(OBJEXT) $(DEPS_LIBS) -lpthread

# Unit tests for HwsFdCache (file descriptor cache with OBS-specific optimizations)
# Tests write pages, read pages, CRC checking, and cache management
# Uses test_stubs.cpp for stub implementations of hws_s3fs.cpp functions
# NOTE: hws_fd_cache.cpp is included directly to compile with -DHWS_FD_CACHE_TEST
hws_fd_cache_test_SOURCES = hws_fd_cache_test.cpp test_stubs.cpp hws_fd_cache.cpp gtest-all.cc
hws_fd_cache_test_CPPFLAGS = $(AM_CPPFLAGS) -I. -DHWS_FD_CACHE_TEST
hws_fd_cache_test_LDADD = hws_index_cache.$(OBJEXT) string_util.$(OBJEXT) s3fs_util.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) cache.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Linkage test for hws_fd_cache.cpp
# This test LINKS against hws_fd_cache.o (not #include) to ensure
# the coverage data goes to the main source file's .gcda file.
# Tests print_err_type() and verifies global variable linkage.
hws_fd_cache_linkage_test_SOURCES = hws_fd_cache_linkage_test.cpp gtest-all.cc
hws_fd_cache_linkage_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
hws_fd_cache_linkage_test_LDADD = hws_fd_cache.$(OBJEXT) hws_index_cache.$(OBJEXT) string_util.$(OBJEXT) s3fs_util.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) cache.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) test_stubs.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Unit tests for HwsFdManager (file descriptor manager)
# Tests GetAllEntities, GetAllEntitiesUnderPath, and entity management
# Uses test_stubs_fd_cache.cpp for stub implementations of hws_s3fs.cpp functions
# NOTE: Temporarily disabled due to complex linking dependencies (fdcache.o requires curl.o)
#hws_fd_manager_test_SOURCES = hws_fd_manager_test.cpp test_stubs_fd_cache.cpp gtest-all.cc
#hws_fd_manager_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
#hws_fd_manager_test_LDADD = hws_fd_cache.$(OBJEXT) hws_index_cache.$(OBJEXT) string_util.$(OBJEXT) s3fs_util.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) cache.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Core unit tests for curl (S3fsCurl class methods)
# Tests SetStorageClass, SetSseType, SetRetries, SetConnectTimeout, etc.
curl_core_test_SOURCES = curl_core_test.cpp gtest-all.cc
curl_core_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
curl_core_test_LDADD = curl.$(OBJEXT) string_util.$(OBJEXT) test_stubs.$(OBJEXT) s3fs_util.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) $(DEPS_LIBS) -lcrypt -lxml2 -lcurl

# Utility function tests for curl.cpp
# Tests etag_equals, adjust_block, url_to_host, get_bucket_host,
# case_insensitive_compare_func, tolower_header_name, curl_slist_append,
# and all compile-time constants

# Comprehensive coverage tests for curl.cpp
# Tests utility functions, SSE configuration, IAM credential parsing,
# curl handler pool, header manipulation, URL construction, and more.
curl_coverage_test_SOURCES = curl_coverage_test.cpp gtest-all.cc
curl_coverage_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
curl_coverage_test_LDADD = curl.$(OBJEXT) string_util.$(OBJEXT) test_stubs.$(OBJEXT) s3fs_util.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) $(DEPS_LIBS) -lcrypt -lxml2 -lcurl

# Unit tests for curl.cpp utility functions
# Tests etag_equals, adjust_block, url_to_host, MakeUrlResource, prepare_url
# Includes curl.cpp directly using #define main to avoid conflict
# NOTE: curl_util_test.cpp provides its own stubs for hws_s3fs.cpp globals
# NOTE: Does NOT link curl.o, openssl_auth.o, common_auth.o because it includes curl.cpp directly
# and test_stubs provides stubs for auth functions
curl_util_test_SOURCES = curl_util_test.cpp gtest-all.cc
curl_util_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
curl_util_test_LDADD = string_util.$(OBJEXT) s3fs_util.$(OBJEXT) addhead.$(OBJEXT) test_stubs.$(OBJEXT) obsfs_log.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) s3fs_operations.$(OBJEXT) readdir_marker_map.$(OBJEXT) hws_encrypt_util.$(OBJEXT) hws_cipher_key_check.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Unit tests for hws_s3fs.cpp xattrs functionality
# Tests parse_xattr_keyval, parse_xattrs, build_xattrs, free_xattrs
# Includes hws_s3fs.cpp directly to access static functions
# NOTE: hws_s3fs_xattrs_test.cpp provides its own crypto/auth stubs, so we do NOT
# link test_stubs.o, openssl_auth.o, or common_auth.o to avoid multiple definitions
hws_s3fs_xattrs_test_SOURCES = hws_s3fs_xattrs_test.cpp gtest-all.cc
hws_s3fs_xattrs_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
hws_s3fs_xattrs_test_LDADD = string_util.$(OBJEXT) obsfs_log.$(OBJEXT) hws_fs_util.$(OBJEXT) \
	curl.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) \
	hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) \
	cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) s3fs_util.$(OBJEXT) s3fs_operations.$(OBJEXT) \
	readdir_marker_map.$(OBJEXT) hws_encrypt_util.$(OBJEXT) hws_cipher_key_check.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Unit tests for hws_configure (configuration parser) functionality
# Tests config value parsing, range validation, string/integer config handling,
# config file parsing, log level setting, and config apply logic.
# Compiles hws_configure.cpp directly with -DHWS_CONFIGURE_TEST_PATH to override
# the config file path for testing file I/O.
hws_configure_test_SOURCES = hws_configure_test.cpp hws_configure.cpp gtest-all.cc
hws_configure_test_CPPFLAGS = $(AM_CPPFLAGS) -I. -DHWS_CONFIGURE_TEST_PATH='"/tmp/hws_configure_test_obsfsconfig"'
hws_configure_test_LDADD = hws_index_cache.$(OBJEXT) string_util.$(OBJEXT) s3fs_util.$(OBJEXT) test_stubs.$(OBJEXT) obsfs_log.$(OBJEXT) hws_fs_util.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl

# Unit tests for common_auth (authentication utilities) functionality
# Tests MD5 and SHA256 checksum functions for file content and buffers
# Links actual source code to get proper coverage
common_auth_test_SOURCES = common_auth_test.cpp gtest-all.cc
common_auth_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
common_auth_test_LDADD = common_auth.$(OBJEXT) openssl_auth.$(OBJEXT) string_util.$(OBJEXT) $(DEPS_LIBS) -lcrypt -lcrypto

# Unit tests for obsfs_log (logging system) functionality
# Tests log buffer management, log level filtering, and file operations
# Links actual obsfs_log.cpp source code for proper coverage
# Uses test_stubs.o to break circular dependency (obsfs_log depends on hws_fs_util)
obsfs_log_test_SOURCES = obsfs_log_test.cpp gtest-all.cc
obsfs_log_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
obsfs_log_test_LDADD = obsfs_log.$(OBJEXT) test_stubs.$(OBJEXT) hws_fs_util.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl

# Unit tests for hws_s3fs_statis_api (statistics API) functionality
# Tests time difference calculation, statistics tracking, and display
# Links actual hws_s3fs_statis_api.cpp source code for proper coverage
hws_s3fs_statis_api_test_SOURCES = hws_s3fs_statis_api_test.cpp gtest-all.cc
hws_s3fs_statis_api_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
hws_s3fs_statis_api_test_LDADD = hws_s3fs_statis_api.$(OBJEXT) hws_configure.$(OBJEXT) obsfs_log.$(OBJEXT) test_stubs.$(OBJEXT) string_util.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_index_cache.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl

# Unit tests for addhead (additional HTTP header) functionality
# Tests regex compilation, suffix matching, and header management
# Links actual addhead.cpp source code for proper coverage
addhead_test_SOURCES = addhead_test.cpp gtest-all.cc
addhead_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
addhead_test_LDADD = addhead.$(OBJEXT) test_stubs.$(OBJEXT) string_util.$(OBJEXT) hws_fs_util.$(OBJEXT) obsfs_log.$(OBJEXT) $(DEPS_LIBS) -lcrypt -lxml2 -lcurl


# Unit tests for fdpage struct (file descriptor page structure)
# Tests fdpage offset, bytes, loaded flag, and next()/end() methods
# Links actual fdcache.cpp source code to get proper coverage
fdpage_test_SOURCES = fdpage_test.cpp gtest-all.cc
fdpage_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
fdpage_test_LDADD = $(DEPS_LIBS)

# Unit tests for snas_list (SNAS linked list and hash table) functionality
# Tests double linked list operations, hash list operations, and list traversal
# NOTE: This is a standalone test that includes header-only implementations
# to avoid complex linking dependencies
snas_list_test_SOURCES = snas_list_test.cpp gtest-all.cc
snas_list_test_CPPFLAGS = $(AM_CPPFLAGS) -I. -Wno-dangling-pointer
snas_list_test_LDADD = $(DEPS_LIBS)


# Unit tests for fdcache linkage functionality
# Tests fdcache.cpp utility functions (CacheFileStat, fdpage) with actual linkage
# Links against the actual fdcache.o to get proper coverage
fdcache_linkage_test_SOURCES = fdcache_linkage_test.cpp gtest-all.cc
fdcache_linkage_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
fdcache_linkage_test_LDADD = fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) obsfs_log.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) cache.$(OBJEXT) s3fs_util.$(OBJEXT) string_util.$(OBJEXT) crc32c.$(OBJEXT) test_stubs.$(OBJEXT) $(DEPS_LIBS) -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Comprehensive unit tests for fdcache.cpp coverage
# Tests CacheFileStat, PageList (all methods + serialization), FdEntity (open/read/write/close/stats/metadata),
# and FdManager (singleton, cache dir, entity lifecycle, rename, close)
# Links against actual fdcache.o to contribute to fdcache.cpp code coverage
fdcache_coverage_test_SOURCES = fdcache_coverage_test.cpp gtest-all.cc
fdcache_coverage_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
fdcache_coverage_test_LDADD = fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) obsfs_log.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) cache.$(OBJEXT) s3fs_util.$(OBJEXT) string_util.$(OBJEXT) test_stubs.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# fdcache_network_mock_test - Tests NoCache* functions with curl mock
# Uses curl_mock infrastructure to intercept network calls and test NoCache* code paths
fdcache_network_mock_test_SOURCES = fdcache_network_mock_test.cpp curl_mock.cpp gtest-all.cc
fdcache_network_mock_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
fdcache_network_mock_test_LDADD = fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) obsfs_log.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) cache.$(OBJEXT) s3fs_util.$(OBJEXT) string_util.$(OBJEXT) test_stubs.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto
fdcache_network_mock_test_LDFLAGS = -Wl,--wrap=curl_easy_perform -Wl,--wrap=curl_easy_getinfo -Wl,--wrap=curl_easy_setopt -Wl,--wrap=sleep

# fdcache_fs_mock_test - Tests file system error paths with fs_mock
# Uses fs_mock infrastructure to intercept file system calls and test error handling paths
fdcache_fs_mock_test_SOURCES = fdcache_fs_mock_test.cpp fs_mock.cpp gtest-all.cc
fdcache_fs_mock_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
fdcache_fs_mock_test_LDADD = fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) obsfs_log.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) cache.$(OBJEXT) s3fs_util.$(OBJEXT) string_util.$(OBJEXT) test_stubs.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto
fdcache_fs_mock_test_LDFLAGS = -Wl,--wrap=open,--wrap=open64,--wrap=unlink,--wrap=link,--wrap=ftruncate,--wrap=ftruncate64,--wrap=fstat,--wrap=pwrite,--wrap=pread,--wrap=stat,--wrap=__xstat,--wrap=__lxstat,--wrap=close

# fdcache_combined_mock_test - Tests requiring both curl mock AND fs mock
# Covers ISSUE2026031000007: WriteMultipart ftruncate/ClearParts ordering
fdcache_combined_mock_test_SOURCES = fdcache_combined_mock_test.cpp curl_mock.cpp fs_mock.cpp gtest-all.cc
fdcache_combined_mock_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
fdcache_combined_mock_test_LDADD = fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) obsfs_log.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) cache.$(OBJEXT) s3fs_util.$(OBJEXT) string_util.$(OBJEXT) test_stubs.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto
fdcache_combined_mock_test_LDFLAGS = -Wl,--wrap=curl_easy_perform,--wrap=curl_easy_getinfo,--wrap=curl_easy_setopt,--wrap=sleep,--wrap=open,--wrap=open64,--wrap=unlink,--wrap=link,--wrap=ftruncate,--wrap=ftruncate64,--wrap=fstat,--wrap=pwrite,--wrap=pread,--wrap=stat,--wrap=__xstat,--wrap=__lxstat,--wrap=close

# Unit tests for openssl_auth (OpenSSL-based authentication) functionality
# Tests HMAC (SHA1/SHA256), MD5, SHA256 hash functions, and crypt mutex management
# Links actual openssl_auth.cpp source code for proper coverage
openssl_auth_test_SOURCES = openssl_auth_test.cpp gtest-all.cc
openssl_auth_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
openssl_auth_test_LDADD = openssl_auth.$(OBJEXT) $(DEPS_LIBS) -lcrypto -lssl


# Comprehensive unit tests for curl.cpp using #include approach
# Includes curl.cpp directly to test static functions with real coverage
# Links against real .o files for all dependencies (string_util, addhead, etc.)
# Does NOT link curl.o (included via #include) or openssl_auth.o/common_auth.o (stubs in test file)
curl_include_test_SOURCES = curl_include_test.cpp gtest-all.cc
curl_include_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
curl_include_test_LDADD = string_util.$(OBJEXT) s3fs_util.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) s3fs_operations.$(OBJEXT) readdir_marker_map.$(OBJEXT) hws_encrypt_util.$(OBJEXT) hws_cipher_key_check.$(OBJEXT) test_stubs.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Extended unit tests for curl.cpp using #include approach
# Tests IAM credential parsing, signature computation, SSE key management,
# XML parsing (GetUploadId), request setup (PreHeadRequest), multi-curl, and error/debug
# Includes curl.cpp directly to test private/static functions with real coverage
# Does NOT link curl.o (included via #include) or openssl_auth.o/common_auth.o (stubs in test_stubs.o)
curl_extended_test_SOURCES = curl_extended_test.cpp gtest-all.cc
curl_extended_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
curl_extended_test_LDADD = string_util.$(OBJEXT) s3fs_util.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) s3fs_operations.$(OBJEXT) readdir_marker_map.$(OBJEXT) hws_encrypt_util.$(OBJEXT) hws_cipher_key_check.$(OBJEXT) test_stubs.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Mock-based coverage tests for curl.cpp request methods (RemakeHandle, DeleteRequest, etc.)
curl_request_mock_test_SOURCES = curl_request_mock_test.cpp gtest-all.cc
curl_request_mock_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
curl_request_mock_test_LDADD = string_util.$(OBJEXT) s3fs_util.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) s3fs_operations.$(OBJEXT) readdir_marker_map.$(OBJEXT) hws_encrypt_util.$(OBJEXT) hws_cipher_key_check.$(OBJEXT) test_stubs.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Mock tests for curl.cpp IAM auth, signature, SSE, CurlHandlerPool, RemakeHandle
curl_auth_mock_test_SOURCES = curl_auth_mock_test.cpp gtest-all.cc
curl_auth_mock_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
curl_auth_mock_test_LDADD = string_util.$(OBJEXT) s3fs_util.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) s3fs_operations.$(OBJEXT) readdir_marker_map.$(OBJEXT) hws_encrypt_util.$(OBJEXT) hws_cipher_key_check.$(OBJEXT) test_stubs.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Mock-based coverage tests for curl.cpp callbacks (WriteMemoryCallback, ReadCallback, etc.)
curl_callback_mock_test_SOURCES = curl_callback_mock_test.cpp gtest-all.cc
curl_callback_mock_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
curl_callback_mock_test_LDADD = string_util.$(OBJEXT) s3fs_util.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) s3fs_operations.$(OBJEXT) readdir_marker_map.$(OBJEXT) hws_encrypt_util.$(OBJEXT) hws_cipher_key_check.$(OBJEXT) test_stubs.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Mock-based coverage tests for curl.cpp network request functions (RequestPerform, etc.)
# Uses GNU ld --wrap to intercept curl_easy_perform and curl_easy_getinfo
# Links with curl_mock.cpp which provides wrapped implementations
curl_network_mock_test_SOURCES = curl_network_mock_test.cpp curl_mock.cpp gtest-all.cc
curl_network_mock_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
curl_network_mock_test_LDADD = string_util.$(OBJEXT) s3fs_util.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) s3fs_operations.$(OBJEXT) readdir_marker_map.$(OBJEXT) hws_encrypt_util.$(OBJEXT) hws_cipher_key_check.$(OBJEXT) test_stubs.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto
# Link with --wrap flags to intercept curl functions and sleep (eliminates retry delays)
curl_network_mock_test_LDFLAGS = -Wl,--wrap=curl_easy_perform -Wl,--wrap=curl_easy_getinfo -Wl,--wrap=curl_easy_setopt -Wl,--wrap=sleep

# Comprehensive unit tests for hws_s3fs.cpp using #include approach
# Includes hws_s3fs.cpp directly to test static functions with real coverage
# Uses #define main to avoid main() conflict
# Links against real .o files for all dependencies (s3fs_operations, cache, curl, etc.)
# Does NOT link openssl_auth.o/common_auth.o (stubs provided in test file)
hws_s3fs_include_test_SOURCES = hws_s3fs_include_test.cpp gtest-all.cc
hws_s3fs_include_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
hws_s3fs_include_test_LDADD = s3fs_operations.$(OBJEXT) string_util.$(OBJEXT) s3fs_util.$(OBJEXT) cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) readdir_marker_map.$(OBJEXT) hws_encrypt_util.$(OBJEXT) hws_cipher_key_check.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto
# [ISSUE2026040900001 C1/C2] Removed s3fs_operations_helper_test — the helper
# functions it tested (is_retryable_error / map_obs_error_to_errno) were
# unused dead code in production and have been deleted from s3fs_operations.cpp.


# Comprehensive unit tests for s3fs_operations.cpp using #include approach
# Includes s3fs_operations.cpp directly to test static functions with real coverage
# Links against real .o files for all dependencies (cache, fdcache, curl, etc.)
# Does NOT link s3fs_operations.o (included via #include) or openssl_auth.o/common_auth.o (stubs in test_stubs.o)
s3fs_operations_include_test_SOURCES = s3fs_operations_include_test.cpp gtest-all.cc
s3fs_operations_include_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
s3fs_operations_include_test_LDADD = string_util.$(OBJEXT) s3fs_util.$(OBJEXT) cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) test_stubs.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Unit tests for s3fs.h core constants and macros
# Tests FUSE version, FIVE_GB constant, memory management macros
# NOTE: This test is standalone and includes constant definitions directly
s3fs_test_SOURCES = s3fs_test.cpp gtest-all.cc
s3fs_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
s3fs_test_LDADD = string_util.$(OBJEXT) s3fs_util.$(OBJEXT) test_stubs.$(OBJEXT) obsfs_log.$(OBJEXT) hws_fs_util.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl

# Comprehensive unit tests for s3fs_operations.cpp with improved coverage
# Includes s3fs_operations.cpp directly to test static functions with real coverage
# Tests error paths, edge cases, and parameter validation
# Links against real .o files for all dependencies (cache, fdcache, curl, etc.)
# Does NOT link s3fs_operations.o (included via #include) or openssl_auth.o (stubs provided)
s3fs_operations_coverage_test_SOURCES = s3fs_operations_coverage_test.cpp gtest-all.cc
s3fs_operations_coverage_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
s3fs_operations_coverage_test_LDADD = string_util.$(OBJEXT) s3fs_util.$(OBJEXT) cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) test_stubs.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl

# Comprehensive unit tests for s3fs_operations.cpp with extensive coverage
# Includes s3fs_operations.cpp directly to test static functions with real coverage
# Tests additional error paths, edge cases, and complex scenarios
# Links against real .o files for all dependencies (cache, fdcache, curl, etc.)
# Does NOT link s3fs_operations.o (included via #include) or openssl_auth.o (stubs provided)
s3fs_operations_comprehensive_test_SOURCES = s3fs_operations_comprehensive_test.cpp gtest-all.cc
s3fs_operations_comprehensive_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
s3fs_operations_comprehensive_test_LDADD = string_util.$(OBJEXT) s3fs_util.$(OBJEXT) cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) test_stubs.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl

# Linkage test for s3fs_operations.cpp
# This test LINKS s3fs_operations.o (not #include) to ensure coverage goes to main source file
# Tests exported functions: str_to_off_t, build_standard_metadata
# Links against s3fs_operations.o and all its dependencies
s3fs_ops_linkage_test_SOURCES = s3fs_ops_linkage_test.cpp gtest-all.cc
s3fs_ops_linkage_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
s3fs_ops_linkage_test_LDADD = s3fs_operations.$(OBJEXT) string_util.$(OBJEXT) s3fs_util.$(OBJEXT) cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) test_stubs.$(OBJEXT) readdir_marker_map.$(OBJEXT) hws_encrypt_util.$(OBJEXT) hws_cipher_key_check.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Mock-based unit tests for s3fs_operations.cpp FUSE operations
# Uses link-seam mock: #include "s3fs_operations.cpp" with mocked S3fsCurl
# Does NOT link curl.o - all S3fsCurl symbols are defined in the test file
# Does NOT link s3fs_operations.o (included via #include)
# Tests getattr, mkdir, mknod, unlink, rmdir, symlink, readlink, chmod, truncate,
# directory_empty, pending entries, and helper functions with controllable mock state
s3fs_operations_mock_test_SOURCES = s3fs_operations_mock_test.cpp gtest-all.cc
s3fs_operations_mock_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
s3fs_operations_mock_test_LDADD = fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) cache.$(OBJEXT) string_util.$(OBJEXT) s3fs_util.$(OBJEXT) addhead.$(OBJEXT) obsfs_log.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) readdir_marker_map.$(OBJEXT) hws_encrypt_util.$(OBJEXT) hws_cipher_key_check.$(OBJEXT) test_stubs.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Linkage test for hws_configure.cpp
# This test LINKS against hws_configure.o (not #include) to ensure
# the coverage data goes to the main source file's .gcda file.
# Tests exported functions: HwsGetIntConfigValue, HwsConfigure class methods
hws_configure_linkage_test_SOURCES = hws_configure_linkage_test.cpp gtest-all.cc
hws_configure_linkage_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
hws_configure_linkage_test_LDADD = hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) string_util.$(OBJEXT) s3fs_util.$(OBJEXT) test_stubs.$(OBJEXT) obsfs_log.$(OBJEXT) hws_fs_util.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl

# Mock-based unit tests for hws_s3fs.cpp HTTP-dependent functions
# Uses #include approach (like hws_s3fs_include_test) + curl_mock.cpp + --wrap flags
# This enables testing functions that make HTTP calls via S3fsCurl
# (s3fs_check_service, create_directory_object, s3fs_truncate, xattr functions, etc.)
hws_s3fs_fuse_mock_test_SOURCES = hws_s3fs_fuse_mock_test.cpp curl_mock.cpp gtest-all.cc
hws_s3fs_fuse_mock_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
hws_s3fs_fuse_mock_test_LDADD = s3fs_operations.$(OBJEXT) string_util.$(OBJEXT) s3fs_util.$(OBJEXT) \
    cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) \
    hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) \
    hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) \
    readdir_marker_map.$(OBJEXT) hws_encrypt_util.$(OBJEXT) hws_cipher_key_check.$(OBJEXT) \
    $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto
hws_s3fs_fuse_mock_test_LDFLAGS = -Wl,--wrap=curl_easy_perform -Wl,--wrap=curl_easy_getinfo -Wl,--wrap=curl_easy_setopt -Wl,--wrap=sleep

# Linkage test for hws_s3fs.cpp
# This test LINKS against hws_s3fs.o (not #include) to ensure
# the coverage data goes to the main source file's .gcda file.
# Uses --wrap=main to avoid main() conflict with gtest.
# Tests exported global variables and verifies linkage.
# Links against openssl_auth.o for crypto function implementations.
hws_s3fs_linkage_test_SOURCES = hws_s3fs_linkage_test.cpp gtest-all.cc
hws_s3fs_linkage_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
hws_s3fs_linkage_test_LDFLAGS = -Wl,--wrap=main
hws_s3fs_linkage_test_LDADD = hws_s3fs.$(OBJEXT) openssl_auth.$(OBJEXT) common_auth.$(OBJEXT) s3fs_operations.$(OBJEXT) string_util.$(OBJEXT) s3fs_util.$(OBJEXT) cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) readdir_marker_map.$(OBJEXT) hws_encrypt_util.$(OBJEXT) hws_cipher_key_check.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Coverage test for hws_s3fs.cpp
# This test LINKS against hws_s3fs.o (not #include) to ensure
# the coverage data goes to the main source file's .gcda file.
# Uses --wrap=main to avoid main() conflict with gtest.
# Tests global variable access, SSE type retrieval, and diff_in_ms function.
hws_s3fs_coverage_test_SOURCES = hws_s3fs_coverage_test.cpp gtest-all.cc
hws_s3fs_coverage_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
hws_s3fs_coverage_test_LDFLAGS = -Wl,--wrap=main
hws_s3fs_coverage_test_LDADD = hws_s3fs.$(OBJEXT) openssl_auth.$(OBJEXT) common_auth.$(OBJEXT) s3fs_operations.$(OBJEXT) string_util.$(OBJEXT) s3fs_util.$(OBJEXT) cache.$(OBJEXT) fdcache.$(OBJEXT) fdcache_untreated.$(OBJEXT) curl.$(OBJEXT) addhead.$(OBJEXT) hws_s3fs_statis_api.$(OBJEXT) hws_fs_util.$(OBJEXT) hws_configure.$(OBJEXT) hws_index_cache.$(OBJEXT) hws_fd_cache.$(OBJEXT) crc32c.$(OBJEXT) obsfs_log.$(OBJEXT) readdir_marker_map.$(OBJEXT) hws_encrypt_util.$(OBJEXT) hws_cipher_key_check.$(OBJEXT) $(DEPS_LIBS) -lpthread -lcrypt -lxml2 -lcurl -lssl -lcrypto

# Unit tests for UntreatedParts (interval list for no-cache multipart write tracking)
# Tests all methods: AddPart, ClearParts, GetLastUpdatedPart, merge/split logic, etc.
# Standalone test - only needs fdcache_untreated.o (no fdcache.o or curl.o dependencies)
fdcache_untreated_test_SOURCES = fdcache_untreated_test.cpp gtest-all.cc
fdcache_untreated_test_CPPFLAGS = $(AM_CPPFLAGS) -I.
fdcache_untreated_test_LDADD = fdcache_untreated.$(OBJEXT) $(DEPS_LIBS) -lpthread

# Custom target to run unit tests
.PHONY: test-unit
test-unit:
	for testprog in $(check_PROGRAMS); do \
		echo "Running $$testprog..."; \
		./$$testprog || exit 1; \
	done

#
# IMPORTANT: Build with -j1 to avoid autotools .Tpo race conditions
# When using parallel make, autotools may generate corrupted .Tpo files
# causing build failures. Always use single-threaded make for this project.
#
# Examples:
#   make -j1              # Build with single thread
#   make -j1 check        # Run tests with single-thread build
#   make -j1 coverage     # Generate coverage with single-thread build
#

#
# Code Coverage Targets
#

# Check if lcov is available
HAVE_LCOV := $(shell command -v lcov 2> /dev/null)

# Initialize coverage data
.PHONY: coverage-init
coverage-init:
	@echo "Initializing coverage data..."
	@find . -name "*.gcda" -delete
	@if [ -n "$(HAVE_LCOV)" ]; then \
		lcov --directory . --zerocounters; \
	fi

# Capture coverage data and generate info file
.PHONY: coverage-capture
coverage-capture: coverage-init
	@echo "Running tests to generate coverage data..."
	@$(MAKE) -j1 check
	@if [ -n "$(HAVE_LCOV)" ]; then \
		echo "Capturing coverage data..."; \
		lcov --directory . --base-directory . --capture --output-file coverage.info; \
		echo "Filtering coverage data..."; \
		lcov --remove coverage.info '/usr/*' --output-file coverage.info; \
		lcov --remove coverage.info '*/gtest*.cc' --output-file coverage.info; \
		lcov --remove coverage.info '*/test*.cpp' --output-file coverage.info; \
		lcov --remove coverage.info '*/test*.cc' --output-file coverage.info; \
		lcov --remove coverage.info '*/tests/*.cpp' --output-file coverage.info; \
	else \
		echo "Note: lcov not available. Install lcov for detailed reports."; \
		echo "Coverage data (.gcda files) generated in current directory."; \
	fi

# Generate coverage summary report (terminal)
.PHONY: coverage-summary
coverage-summary: coverage-capture
	@if [ -n "$(HAVE_LCOV)" ]; then \
		echo ""; \
		echo "=========================================="; \
		echo "Code Coverage Summary"; \
		echo "=========================================="; \
		lcov --list coverage.info; \
		echo ""; \
		echo "Coverage info file: coverage.info"; \
		echo ""; \
	else \
		echo ""; \
		echo "=========================================="; \
		echo "Basic Coverage Summary"; \
		echo "=========================================="; \
		echo "lcov not available. Raw .gcda files generated."; \
		echo "Use 'gcov <source_file>' to view individual file coverage."; \
		echo ""; \
	fi

# Generate HTML coverage report
.PHONY: coverage-html
coverage-html: coverage-capture
	@if [ -n "$(HAVE_LCOV)" ]; then \
		if command -v genhtml >/dev/null 2>&1; then \
			echo "Generating HTML coverage report..."; \
			genhtml coverage.info --output-directory ../coverage_html --legend --show-details; \
			echo ""; \
			echo "=========================================="; \
			echo "HTML Coverage Report Generated"; \
			echo "=========================================="; \
			echo "Open: ../coverage_html/index.html"; \
			echo ""; \
		else \
			echo "Error: genhtml not found. Install lcov package."; \
			exit 1; \
		fi \
	else \
		echo "Error: lcov not found. Install lcov package for HTML reports."; \
		echo "  sudo apt-get install lcov"; \
		exit 1; \
	fi

# Main coverage target (generates both summary and HTML)
.PHONY: coverage
coverage: coverage-summary
	@if [ -n "$(HAVE_LCOV)" ]; then \
		$(MAKE) coverage-html; \
	fi

# Generate coverage for specific source file using gcov
.PHONY: coverage-gcov
coverage-gcov:
	@echo "Generating gcov reports..."
	@for src in $(obsfs_SOURCES); do \
		if [ -f "$$src" ]; then \
			basename="$$(basename $$src .cpp)"; \
			gcov="$$basename.gcov"; \
			if [ -f "$$basename.o" ]; then \
				gcov $$basename.o; \
			fi; \
		fi; \
	done

# Clean coverage data
.PHONY: coverage-clean
coverage-clean:
	@echo "Cleaning coverage data..."
	@find . -name "*.gcda" -delete
	@find . -name "*.gcno" -delete
	@find . -name "*.gcov" -delete
	@find . -name "*.gcov.json.gz" -delete
	@find .. -maxdepth 1 -name "*.gcov" -delete
	@find .. -maxdepth 1 -name "*.gcov.json.gz" -delete
	@rm -f coverage.info
	@rm -f ../coverage.info
	@rm -rf ../coverage_html
	@echo "Cleaning test credential files..."
	@find ../test -name "passwd-*" -delete
	@find ../test -name "*.lastuse" -delete

# Show coverage help
.PHONY: coverage-help
coverage-help:
	@echo "Code Coverage Targets:"
	@echo ""
	@echo "  make coverage          - Generate full coverage report (summary + HTML)"
	@echo "  make coverage-init     - Initialize coverage data"
	@echo "  make coverage-capture  - Run tests and capture coverage"
	@echo "  make coverage-summary  - Generate terminal summary"
	@echo "  make coverage-html     - Generate HTML report only"
	@echo "  make coverage-gcov     - Generate individual gcov files"
	@echo "  make coverage-clean    - Clean all coverage data"
	@echo "  make coverage-help     - Show this help message"
	@echo ""
	@echo "Requirements:"
	@echo "  - gcov (required): Usually installed with gcc/g++"
	@echo "  - lcov (recommended): sudo apt-get install lcov"
	@echo ""
	@echo "Example usage:"
	@echo "  ./configure --enable-coverage"
	@echo "  make clean && make"
	@echo "  cd src && make coverage"

# Clean build artifacts not tracked by automake
# Automake's generated clean only removes current check_PROGRAMS/TESTS entries.
# Orphaned test binaries from previous Makefile.am versions are missed.
clean-local:
	@echo "Cleaning orphaned test binaries..."
	-find . -maxdepth 1 -name "*_test" -type f -perm /111 -delete
	@echo "Cleaning test logs and results..."
	-rm -f *.log *.trs test-suite.log
	@echo "Cleaning coverage data files..."
	-find . -name "*.gcda" -delete
	-find . -name "*.gcno" -delete
	-find . -name "*.gcov" -delete
	-find . -name "*.gcov.*" -delete
	-find .. -maxdepth 1 -name "*.gcov" -delete
	-find .. -maxdepth 1 -name "*.gcov.*" -delete
	-rm -f coverage.info
	-rm -f ../coverage.info
	-rm -rf ../coverage_html
	@echo "Cleaning other build artifacts..."
	-rm -f default_commit_hash
	@echo "Cleaning test credential files..."
	-find ../test -name "passwd-*" -delete
	-find ../test -name "*.lastuse" -delete
