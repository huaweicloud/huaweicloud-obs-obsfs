/*
 * s3fs - FUSE-based file system backed by Amazon S3
 *
 * Copyright(C) 2007 Randy Rizun <rrizun@gmail.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
#ifndef S3FS_STRING_UTIL_H_
#define S3FS_STRING_UTIL_H_

/*
 * A collection of string utilities for manipulating URLs and HTTP responses.
 */
#include <string.h>
#include <sys/types.h>

#include <string>

static const std::string SPACES = " \t\r\n";

static inline int STR2NCMP(const char *str1, const char *str2) { return strncmp(str1, str2, strlen(str2)); }

template <class T> std::string str(T value);

off_t s3fs_strtoofft(const char* str, bool is_base_16 = false);

std::string trim_left(const std::string &s, const std::string &t = SPACES);
std::string trim_right(const std::string &s, const std::string &t = SPACES);
std::string trim(const std::string &s, const std::string &t = SPACES);
std::string lower(std::string s);
std::string get_date_rfc850(void);
void get_date_sigv3(std::string& date, std::string& date8601);
std::string get_date_string(time_t tm);
std::string get_date_iso8601(time_t tm);
std::string urlEncode(const std::string &s);
std::string urlEncode2(const std::string &s);
std::string urlDecode(const std::string& s);
//do special decode for "+"(space)
std::string urlDecodeSpecial(std::string str);
bool takeout_str_dquart(std::string& str);
bool get_keyword_value(std::string& target, const char* keyword, std::string& value);

std::string s3fs_hex(const unsigned char* input, size_t length);
char* s3fs_base64(const unsigned char* input, size_t length);
unsigned char* s3fs_decode64(const char* input, size_t* plength);

void getMessageFromBodydata(const char *bodydata, char **message);

#endif // S3FS_STRING_UTIL_H_

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: noet sw=4 ts=4 fdm=marker
* vim<600: noet sw=4 ts=4
*/
