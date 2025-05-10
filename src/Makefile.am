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
if USE_GNUTLS_NETTLE
  AM_CPPFLAGS += -DUSE_GNUTLS_NETTLE
endif

obsfs_SOURCES = s3fs.h curl.cpp curl.h cache.cpp cache.h string_util.cpp string_util.h s3fs_util.cpp s3fs_util.h fdcache.cpp fdcache.h \
     readdir_marker_map.h readdir_marker_map.cpp common_auth.cpp s3fs_auth.h addhead.cpp addhead.h hws_index_cache.h hws_index_cache.cpp common.h\
     hws_s3fs.cpp hws_s3fs_statis_api.h hws_s3fs_statis_api.cpp hws_fd_cache.cpp hws_fd_cache.h\
     crc32c.cpp crc32c.h hws_configure.cpp hws_configure.h obsfs_log.cpp obsfs_log.h hws_fs_util.h hws_fs_util.cpp hws_cipher_key_check.h hws_cipher_key_check.cpp
if USE_SSL_OPENSSL
  obsfs_SOURCES += openssl_auth.cpp
endif

obsfs_LDADD = $(DEPS_LIBS)
