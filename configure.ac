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
dnl Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(s3fs, 1.83)
AC_CONFIG_HEADER([config.h])

AC_CANONICAL_SYSTEM
AM_INIT_AUTOMAKE([foreign])

AC_PROG_CXX
AC_PROG_CC

AC_CHECK_HEADERS([sys/xattr.h])
AC_CHECK_HEADERS([attr/xattr.h])
AC_CHECK_HEADERS([sys/extattr.h])

CXXFLAGS="$CXXFLAGS -Wall -D_FILE_OFFSET_BITS=64 -std=c++11 -fstack-protector-all -fPIE -pie -Wl,-z,now -D_FORTIFY_SOURCE=2 -O2"

dnl ----------------------------------------------
dnl CRC32C hardware acceleration (SSE4.2 + PCLMULQDQ)
dnl ----------------------------------------------
AC_MSG_CHECKING([for SSE4.2 and PCLMULQDQ support])
save_CXXFLAGS="$CXXFLAGS"
CXXFLAGS="$CXXFLAGS -msse4.2 -mpclmul"
AC_LANG_PUSH([C++])
AC_COMPILE_IFELSE(
  [AC_LANG_PROGRAM([[#include <nmmintrin.h>
                     #include <wmmintrin.h>]],
                   [[__m128i a = _mm_set_epi64x(0, 0);
                     (void)_mm_crc32_u64(0, 0);
                     (void)_mm_clmulepi64_si128(a, a, 0);]])],
  [AC_MSG_RESULT([yes])
   AC_DEFINE([HAVE_SSE42], [1], [SSE4.2 CRC32 intrinsics available])
   AC_DEFINE([HAVE_PCLMUL], [1], [PCLMULQDQ intrinsics available])
   dnl Keep -msse4.2 -mpclmul in CXXFLAGS
  ],
  [AC_MSG_RESULT([no])
   CXXFLAGS="$save_CXXFLAGS"
   AC_MSG_WARN([SSE4.2/PCLMULQDQ not available, CRC32C will use software fallback (~10x slower)])
  ]
)
AC_LANG_POP([C++])

dnl ----------------------------------------------
dnl For OSX
dnl ----------------------------------------------
case "$target" in
   *-darwin* )
      # Do something specific for mac
      min_fuse_version=2.7.3
      ;;
   *)
      # Default Case
      # assume other supported linux system
      min_fuse_version=2.8.4
      ;;
esac

dnl ----------------------------------------------
dnl SSL library (OpenSSL only)
dnl ----------------------------------------------
auth_lib=openssl

PKG_CHECK_MODULES([DEPS], [fuse >= ${min_fuse_version} libcurl >= 7.0 libxml-2.0 >= 2.6 libcrypto >= 0.9 ])

dnl ----------------------------------------------
dnl check functions
dnl ----------------------------------------------
dnl malloc_trim function
AC_CHECK_FUNCS([malloc_trim])

dnl clock_gettime function(osx)
AC_SEARCH_LIBS([clock_gettime],[rt posix4]) 
AC_CHECK_FUNCS([clock_gettime])

dnl ----------------------------------------------
dnl check symbols/macros/enums
dnl ----------------------------------------------
dnl PTHREAD_MUTEX_RECURSIVE
AC_MSG_CHECKING([pthread mutex recursive])
AC_COMPILE_IFELSE(
  [AC_LANG_PROGRAM([[#include <pthread.h>]],
                   [[int i = PTHREAD_MUTEX_RECURSIVE;]])
  ],
  [AC_DEFINE(S3FS_MUTEX_RECURSIVE, PTHREAD_MUTEX_RECURSIVE, [Define if you have PTHREAD_MUTEX_RECURSIVE])
   AC_MSG_RESULT(PTHREAD_MUTEX_RECURSIVE)
  ],
  [AC_COMPILE_IFELSE(
    [AC_LANG_PROGRAM([[#include <pthread.h>]],
                     [[int i = PTHREAD_MUTEX_RECURSIVE_NP;]])
    ],
    [AC_DEFINE(S3FS_MUTEX_RECURSIVE, PTHREAD_MUTEX_RECURSIVE_NP, [Define if you have PTHREAD_MUTEX_RECURSIVE_NP])
     AC_MSG_RESULT(PTHREAD_MUTEX_RECURSIVE_NP)
    ],
    [AC_MSG_ERROR([do not have PTHREAD_MUTEX_RECURSIVE symbol])])
  ]
)

dnl ----------------------------------------------
dnl output files
dnl ----------------------------------------------
AC_CONFIG_FILES(Makefile src/Makefile doc/Makefile)

dnl ----------------------------------------------
dnl Code coverage support
dnl ----------------------------------------------
AC_MSG_CHECKING([whether to enable code coverage])
AC_ARG_ENABLE(
  [coverage],
  [AS_HELP_STRING([--enable-coverage], [enable code coverage support with gcov (lcov recommended)])],
  [
    case "${enableval}" in
    yes)
      AC_MSG_RESULT(yes)
      AC_CHECK_PROG([GCOV], [gcov], [yes], [no])

      AS_IF([test "x$GCOV" = "xyes"], [
        AC_SUBST([GCOV], [gcov])

        # Check for lcov (optional but recommended)
        AC_CHECK_PROG([LCOV], [lcov], [yes], [no])
        AC_CHECK_PROG([GENHTML], [genhtml], [yes], [no])

        AS_IF([test "x$LCOV" = "xyes"], [
          AC_SUBST([LCOV], [lcov])
          AC_SUBST([GENHTML], [genhtml])
          AC_MSG_NOTICE([lcov found - HTML coverage reports will be available])
        ], [
          AC_MSG_WARN([lcov not found - only basic gcov coverage will be available])
          AC_MSG_WARN([Install lcov for HTML reports: sudo apt-get install lcov])
          AC_SUBST([LCOV], [""])
          AC_SUBST([GENHTML], [""])
        ])

        # Add coverage flags to CXXFLAGS
        CXXFLAGS="$CXXFLAGS --coverage"
        LDFLAGS="$LDFLAGS --coverage"

        AC_DEFINE([ENABLE_COVERAGE], [1], [Define if code coverage is enabled])
        AC_MSG_NOTICE([Code coverage enabled: CXXFLAGS=$CXXFLAGS])
      ], [
        AC_MSG_ERROR([gcov not found. Install gcc/g++ package which includes gcov.])
      ])
      ;;
    *)
      AC_MSG_RESULT(no)
      ;;
    esac
  ],
  [
    AC_MSG_RESULT(no)
  ])
AM_CONDITIONAL([ENABLE_COVERAGE], [test "x$enable_coverage" = "xyes"])
AM_CONDITIONAL([HAVE_LCOV], [test "x$LCOV" = "xyes"])

dnl ----------------------------------------------
dnl AddressSanitizer support
dnl ----------------------------------------------
AC_MSG_CHECKING([whether to enable AddressSanitizer])
AC_ARG_ENABLE(
  [asan],
  [AS_HELP_STRING([--enable-asan], [enable AddressSanitizer for detecting memory errors])],
  [
    case "${enableval}" in
    yes)
      AC_MSG_RESULT(yes)
      CXXFLAGS="$CXXFLAGS -fsanitize=address -fno-omit-frame-pointer"
      LDFLAGS="$LDFLAGS -fsanitize=address"
      AC_DEFINE([ENABLE_ASAN], [1], [Define if AddressSanitizer is enabled])
      AC_MSG_NOTICE([AddressSanitizer enabled: CXXFLAGS=$CXXFLAGS])
      ;;
    *)
      AC_MSG_RESULT(no)
      ;;
    esac
  ],
  [
    AC_MSG_RESULT(no)
  ])

dnl ----------------------------------------------
dnl short commit hash
dnl ----------------------------------------------
AC_CHECK_PROG([GITCMD], [git —version], [yes], [no])
AC_CHECK_FILE([.git], [DOTGITDIR=yes], [DOTGITDIR=no])

AC_MSG_CHECKING([github short commit hash])
if test “x${GITCMD}” = “xyes” -a “x${DOTGITDIR}” = “xyes”; then
    GITCOMMITHASH=`git rev-parse --short HEAD`
elif test -f default_commit_hash; then
    GITCOMMITHASH=`cat default_commit_hash`
else
    GITCOMMITHASH="unknown"
fi
AC_MSG_RESULT([${GITCOMMITHASH}])

AC_DEFINE_UNQUOTED([COMMIT_HASH_VAL], ["${GITCOMMITHASH}"], [short commit hash value on github])

dnl ----------------------------------------------
dnl put
dnl ----------------------------------------------
AC_OUTPUT

dnl ----------------------------------------------
dnl end configuration
dnl ----------------------------------------------

