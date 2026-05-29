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

#include <string>
#include <cstring>

#include "gtest.h"
#include "crc32c.h"
#include "common.h"

// Stub variables and functions required for linking
s3fs_log_level debug_level = S3FS_LOG_INFO;
const char* s3fs_log_nest[S3FS_LOG_NEST_MAX] = {"", "  ", "    ", "      "};
s3fs_log_mode debug_log_mode = LOG_MODE_FOREGROUND;

void IndexLogEntry(const uint64_t module, const char* chunk_id, const uint32_t log_level,
                   const char* log_file_name, const char* log_func_name, const uint32_t log_line,
                   const char* pszFormat, ...) {
  // Stub implementation
}

void s3fsStatisLogModeNotObsfs() {
  // Stub implementation
}

using namespace std;
using namespace rocksdb::crc32c;

namespace {

class Crc32CTest : public ::testing::Test {
 protected:
  Crc32CTest() {
  }

  ~Crc32CTest() override {
  }

  void SetUp() override {
  }

  void TearDown() override {
  }
};

// Test IsFastCrc32Supported function
TEST_F(Crc32CTest, IsFastCrc32Supported_ReturnsValidString) {
  std::string result = IsFastCrc32Supported();
  EXPECT_FALSE(result.empty());
}

TEST_F(Crc32CTest, IsFastCrc32Supported_ContainsSupportedOrNotSupported) {
  std::string result = IsFastCrc32Supported();
  bool has_supported = result.find("Supported") != string::npos;
  bool has_not_supported = result.find("Not supported") != string::npos;
  EXPECT_TRUE(has_supported || has_not_supported);
}

// Test Mask and Unmask functions
TEST_F(Crc32CTest, MaskUnmask_RoundTrip) {
  uint32_t original = 0x12345678;
  uint32_t masked = Mask(original);
  uint32_t unmasked = Unmask(masked);
  EXPECT_EQ(original, unmasked);
}

TEST_F(Crc32CTest, MaskUnmask_Zero) {
  uint32_t original = 0;
  uint32_t masked = Mask(original);
  uint32_t unmasked = Unmask(masked);
  EXPECT_EQ(original, unmasked);
}

TEST_F(Crc32CTest, MaskUnmask_MaxValue) {
  uint32_t original = 0xFFFFFFFF;
  uint32_t masked = Mask(original);
  uint32_t unmasked = Unmask(masked);
  EXPECT_EQ(original, unmasked);
}

TEST_F(Crc32CTest, MaskUnmask_MultipleValues) {
  for (uint32_t i = 0; i < 1000; ++i) {
    uint32_t original = i * 0x9e3779b9;  // Golden ratio-like multiplier
    uint32_t masked = Mask(original);
    uint32_t unmasked = Unmask(masked);
    EXPECT_EQ(original, unmasked);
  }
}

// Test DecodeFixed32 function
TEST_F(Crc32CTest, DecodeFixed32_LittleEndian) {
  // Test little-endian decoding
  char le_bytes[] = "\x78\x56\x34\x12";
  uint32_t result = DecodeFixed32(le_bytes);
  EXPECT_EQ(result, 0x12345678);
}

TEST_F(Crc32CTest, DecodeFixed32_AllZeros) {
  char bytes[] = "\x00\x00\x00\x00";
  uint32_t result = DecodeFixed32(bytes);
  EXPECT_EQ(result, 0);
}

TEST_F(Crc32CTest, DecodeFixed32_AllOnes) {
  char bytes[] = "\xFF\xFF\xFF\xFF";
  uint32_t result = DecodeFixed32(bytes);
  EXPECT_EQ(result, 0xFFFFFFFF);
}

TEST_F(Crc32CTest, DecodeFixed32_BytePattern) {
  char bytes[] = "\x01\x02\x03\x04";
  uint32_t result = DecodeFixed32(bytes);
  EXPECT_EQ(result, 0x04030201);
}

// Test DecodeFixed64 function
TEST_F(Crc32CTest, DecodeFixed64_LittleEndian) {
  // Test little-endian decoding
  char le_bytes[] = "\x78\x56\x34\x12\x34\x56\x78\x9A";
  uint64_t result = DecodeFixed64(le_bytes);
  EXPECT_EQ(result, 0x9A78563412345678ULL);
}

TEST_F(Crc32CTest, DecodeFixed64_AllZeros) {
  char bytes[] = "\x00\x00\x00\x00\x00\x00\x00\x00";
  uint64_t result = DecodeFixed64(bytes);
  EXPECT_EQ(result, 0);
}

TEST_F(Crc32CTest, DecodeFixed64_AllOnes) {
  char bytes[] = "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF";
  uint64_t result = DecodeFixed64(bytes);
  EXPECT_EQ(result, 0xFFFFFFFFFFFFFFFFULL);
}

TEST_F(Crc32CTest, DecodeFixed64_BytePattern) {
  char bytes[] = "\x01\x02\x03\x04\x05\x06\x07\x08";
  uint64_t result = DecodeFixed64(bytes);
  EXPECT_EQ(result, 0x0807060504030201ULL);
}

// Test Value function (calculates CRC of data)
TEST_F(Crc32CTest, Value_EmptyString) {
  const char* data = "";
  uint32_t crc = Value(data, 0);
  EXPECT_EQ(crc, 0);
}

TEST_F(Crc32CTest, Value_SimpleString) {
  const char* data = "hello";
  uint32_t crc = Value(data, 5);
  // CRC32C of "hello" should be consistent
  EXPECT_NE(crc, 0);
}

TEST_F(Crc32CTest, Value_SameDataSameCrc) {
  const char* data1 = "test data";
  uint32_t crc1 = Value(data1, 9);
  const char* data2 = "test data";
  uint32_t crc2 = Value(data2, 9);
  EXPECT_EQ(crc1, crc2);
}

TEST_F(Crc32CTest, Value_DifferentDataDifferentCrc) {
  const char* data1 = "data A";
  const char* data2 = "data B";
  uint32_t crc1 = Value(data1, 6);
  uint32_t crc2 = Value(data2, 6);
  EXPECT_NE(crc1, crc2);
}

// Test Extend function (extends an existing CRC)
TEST_F(Crc32CTest, Extend_WithZeroInit) {
  const char* data = "hello";
  uint32_t crc = Extend(0, data, 5);
  uint32_t value_crc = Value(data, 5);
  EXPECT_EQ(crc, value_crc);
}

TEST_F(Crc32CTest, Extend_EmptyData) {
  const char* data = "test";
  uint32_t crc1 = Value(data, 4);
  uint32_t crc2 = Extend(crc1, "", 0);
  EXPECT_EQ(crc1, crc2);
}

TEST_F(Crc32CTest, Extend_ChunkedData) {
  const char* data1 = "hello ";
  const char* data2 = "world";

  // Calculate CRC for complete string
  const char* full = "hello world";
  uint32_t full_crc = Value(full, 11);

  // Calculate CRC incrementally
  uint32_t crc1 = Value(data1, 6);
  uint32_t crc2 = Extend(crc1, data2, 5);

  EXPECT_EQ(full_crc, crc2);
}

TEST_F(Crc32CTest, Extend_MultipleChunks) {
  const char* chunk1 = "AAAA";
  const char* chunk2 = "BBBB";
  const char* chunk3 = "CCCC";

  const char* full = "AAAABBBBCCCC";
  uint32_t full_crc = Value(full, 12);

  uint32_t crc = Value(chunk1, 4);
  crc = Extend(crc, chunk2, 4);
  crc = Extend(crc, chunk3, 4);

  EXPECT_EQ(full_crc, crc);
}

TEST_F(Crc32CTest, Extend_LargeData) {
  // Test with larger data to exercise different code paths
  const size_t size = 1000;
  char* data = new char[size];
  for (size_t i = 0; i < size; ++i) {
    data[i] = static_cast<char>(i & 0xFF);
  }

  uint32_t crc1 = Value(data, size);
  uint32_t crc2 = Value(data, size);
  EXPECT_EQ(crc1, crc2);

  delete[] data;
}

TEST_F(Crc32CTest, Extend_ByteDifference) {
  const char* data1 = "test";
  const char* data2 = "tesu";  // Only last byte different

  uint32_t crc1 = Value(data1, 4);
  uint32_t crc2 = Value(data2, 4);

  // Different data should produce different CRCs (very high probability)
  EXPECT_NE(crc1, crc2);
}

// Additional tests for boundary cases and edge conditions

TEST_F(Crc32CTest, Value_SingleByte) {
  const char* data = "A";
  uint32_t crc = Value(data, 1);
  EXPECT_NE(crc, 0);
}

TEST_F(Crc32CTest, Value_OneByteData) {
  const char* data = "\x01";
  uint32_t crc = Value(data, 1);
  EXPECT_NE(crc, 0);
}

TEST_F(Crc32CTest, Value_RepeatedPattern) {
  const char* data = "AAAAAAAAAA";  // 10 bytes of 'A'
  uint32_t crc = Value(data, 10);
  EXPECT_NE(crc, 0);
}

TEST_F(Crc32CTest, Value_AllZeros) {
  const char data[100] = {0};
  uint32_t crc = Value(data, 100);
  EXPECT_NE(crc, 0);
}

TEST_F(Crc32CTest, Value_AllOnes) {
  char data[100];
  memset(data, 0xFF, 100);
  uint32_t crc = Value(data, 100);
  EXPECT_NE(crc, 0);
}

TEST_F(Crc32CTest, Value_AlternatingPattern) {
  const char* data = "\x01\xFF\x01\xFF\x01\xFF\x01\xFF";
  uint32_t crc = Value(data, 8);
  EXPECT_NE(crc, 0);
}

TEST_F(Crc32CTest, Extend_WithInitialCRC) {
  const char* data1 = "first";
  const char* data2 = "second";

  uint32_t crc1 = Value(data1, 5);
  uint32_t crc2 = Extend(crc1, data2, 6);

  // Complete string should match
  const char* full = "firstsecond";
  uint32_t full_crc = Value(full, 11);
  EXPECT_EQ(crc2, full_crc);
}

TEST_F(Crc32CTest, Extend_ThreeWayIncremental) {
  const char* chunk1 = "AAAA";
  const char* chunk2 = "BBBB";
  const char* chunk3 = "CCCC";

  const char* full = "AAAABBBBCCCC";
  uint32_t full_crc = Value(full, 12);

  uint32_t crc = Value(chunk1, 4);
  crc = Extend(crc, chunk2, 4);
  crc = Extend(crc, chunk3, 4);

  EXPECT_EQ(full_crc, crc);
}

TEST_F(Crc32CTest, Extend_VerySmallIncremental) {
  const char* data = "ABCD";

  uint32_t full_crc = Value(data, 4);

  uint32_t crc = Value("", 0);
  crc = Extend(crc, "A", 1);
  crc = Extend(crc, "B", 1);
  crc = Extend(crc, "C", 1);
  crc = Extend(crc, "D", 1);

  EXPECT_EQ(full_crc, crc);
}

TEST_F(Crc32CTest, Value_LargeBufferRandomData) {
  const size_t size = 10000;
  char* data = new char[size];
  for (size_t i = 0; i < size; ++i) {
    data[i] = static_cast<char>((i * 7 + 13) & 0xFF);
  }

  uint32_t crc1 = Value(data, size);
  uint32_t crc2 = Value(data, size);
  EXPECT_EQ(crc1, crc2);
  EXPECT_NE(crc1, 0);

  delete[] data;
}

TEST_F(Crc32CTest, Value_DifferentSizes) {
  const char* data = "test data for crc calculation";

  // Calculate CRC for different prefix lengths
  uint32_t crc1 = Value(data, 5);
  uint32_t crc5 = Value(data, 5);
  uint32_t crc10 = Value(data, 10);
  uint32_t crc20 = Value(data, 20);

  // Same size should produce same CRC
  EXPECT_EQ(crc1, crc5);

  // Different sizes should produce different CRCs (high probability)
  bool all_different = (crc1 != crc10) && (crc1 != crc20) && (crc10 != crc20);
  EXPECT_TRUE(all_different);
}

TEST_F(Crc32CTest, Extend_PreservesUniqueness) {
  const char* base = "base";
  const char* suffix1 = "suffix1";
  const char* suffix2 = "suffix2";

  uint32_t base_crc = Value(base, 4);
  uint32_t crc1 = Extend(base_crc, suffix1, 7);
  uint32_t crc2 = Extend(base_crc, suffix2, 7);

  EXPECT_NE(crc1, crc2);
}

TEST_F(Crc32CTest, Value_NullTerminatedString) {
  const char* data = "test";
  uint32_t crc_with_null = Value(data, 5);  // includes null terminator
  uint32_t crc_without_null = Value(data, 4);

  EXPECT_NE(crc_with_null, crc_without_null);
}

TEST_F(Crc32CTest, Extend_MediumSizedData) {
  // Test with 150 bytes to exercise different code paths
  const size_t size = 150;
  char* data = new char[size];
  for (size_t i = 0; i < size; ++i) {
    data[i] = static_cast<char>(i % 256);
  }

  uint32_t crc1 = Value(data, size);
  uint32_t crc2 = Value(data, size);
  EXPECT_EQ(crc1, crc2);

  delete[] data;
}

TEST_F(Crc32CTest, Mask_DifferentInputsProduceDifferentOutputs) {
  uint32_t input1 = 0x12345678;
  uint32_t input2 = 0x87654321;

  uint32_t mask1 = Mask(input1);
  uint32_t mask2 = Mask(input2);

  EXPECT_NE(mask1, mask2);
}

TEST_F(Crc32CTest, Unmask_PreservesDifference) {
  uint32_t input1 = 0x12345678;
  uint32_t input2 = 0x87654321;

  uint32_t unmasked1 = Unmask(input1);
  uint32_t unmasked2 = Unmask(input2);

  EXPECT_NE(unmasked1, unmasked2);
}

// Additional tests for improved coverage

// Test Extend with aligned and unaligned buffers
TEST_F(Crc32CTest, Extend_AlignedVsUnaligned) {
  const char* data = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  size_t len = strlen(data);

  // CRC from aligned buffer
  uint32_t crc_aligned = Extend(0, data, len);

  // CRC with offset (unaligned start)
  uint32_t crc_offset = Extend(0, data + 1, len - 1);

  // Results should be different
  EXPECT_NE(crc_aligned, crc_offset);
}

// Test Extend with power-of-2 sized data
TEST_F(Crc32CTest, Extend_PowerOfTwoSizes) {
  char buffer[256];
  for (int i = 0; i < 256; i++) {
    buffer[i] = static_cast<char>(i);
  }

  for (int size : {8, 16, 32, 64, 128, 256}) {
    uint32_t crc = Extend(0, buffer, size);
    EXPECT_NE(0U, crc) << "CRC for size " << size << " should be non-zero";
  }
}

// Test Extend with all byte values
TEST_F(Crc32CTest, Extend_AllByteValues) {
  unsigned char buffer[256];
  for (int i = 0; i < 256; i++) {
    buffer[i] = static_cast<unsigned char>(i);
  }

  uint32_t crc = Extend(0, reinterpret_cast<char*>(buffer), 256);
  EXPECT_NE(0U, crc);

  // Verify consistency - same input gives same output
  uint32_t crc2 = Extend(0, reinterpret_cast<char*>(buffer), 256);
  EXPECT_EQ(crc, crc2);
}

// Test Value with empty string variations
TEST_F(Crc32CTest, Value_EmptyVariations) {
  uint32_t crc1 = Value("", 0);
  uint32_t crc2 = Value(nullptr, 0);
  EXPECT_EQ(crc1, crc2);
  EXPECT_EQ(0U, crc1);  // CRC of empty data should be 0
}

// Test Extend with maximum chunking
TEST_F(Crc32CTest, Extend_ByteByByte) {
  const char* data = "Hello, World!";
  size_t len = strlen(data);

  // Compute CRC all at once
  uint32_t crc_all = Extend(0, data, len);

  // Compute CRC byte by byte
  uint32_t crc_byte = 0;
  for (size_t i = 0; i < len; i++) {
    crc_byte = Extend(crc_byte, data + i, 1);
  }

  // Results should be identical
  EXPECT_EQ(crc_all, crc_byte);
}

// Test Extend with large initial CRC
TEST_F(Crc32CTest, Extend_LargeInitialCRC) {
  const char* data = "test data";
  uint32_t large_crc = 0xFFFFFFFF;

  uint32_t result = Extend(large_crc, data, strlen(data));
  EXPECT_NE(large_crc, result);
}

// Test Extend with data that spans multiple cache lines
TEST_F(Crc32CTest, Extend_CacheLineSpanning) {
  // Create data larger than typical cache line (64 bytes)
  const size_t size = 512;
  char* buffer = new char[size];
  for (size_t i = 0; i < size; i++) {
    buffer[i] = static_cast<char>(i % 256);
  }

  uint32_t crc = Extend(0, buffer, size);
  EXPECT_NE(0U, crc);

  // Verify chunked CRC gives same result
  uint32_t crc_chunked = 0;
  crc_chunked = Extend(crc_chunked, buffer, 256);
  crc_chunked = Extend(crc_chunked, buffer + 256, 256);

  EXPECT_EQ(crc, crc_chunked);

  delete[] buffer;
}

// Test Mask with CRC values
TEST_F(Crc32CTest, Mask_WithCRCValues) {
  const char* data = "test string for masking";
  uint32_t crc = Value(data, strlen(data));

  uint32_t masked = Mask(crc);
  uint32_t unmasked = Unmask(masked);

  EXPECT_EQ(crc, unmasked);
  EXPECT_NE(crc, masked);  // Mask should change the value
}

// Test Extend with binary data containing null bytes
TEST_F(Crc32CTest, Extend_BinaryDataWithNulls) {
  char buffer[64];
  memset(buffer, 0, sizeof(buffer));

  // Put some non-null values at various positions
  buffer[0] = 'A';
  buffer[31] = 'B';
  buffer[63] = 'C';

  uint32_t crc = Extend(0, buffer, sizeof(buffer));
  EXPECT_NE(0U, crc);
}

// Test Extend consistency with repeated calls
TEST_F(Crc32CTest, Extend_RepeatedConsistency) {
  const char* data = "Consistency test data";
  size_t len = strlen(data);

  uint32_t crc1 = Extend(0, data, len);
  uint32_t crc2 = Extend(0, data, len);
  uint32_t crc3 = Extend(0, data, len);

  EXPECT_EQ(crc1, crc2);
  EXPECT_EQ(crc2, crc3);
}

// Test Value with specific known patterns
TEST_F(Crc32CTest, Value_SpecificPatterns) {
  // All zeros
  char zeros[32] = {0};
  uint32_t crc_zeros = Value(zeros, sizeof(zeros));

  // All 0xFF
  char ones[32];
  memset(ones, 0xFF, sizeof(ones));
  uint32_t crc_ones = Value(ones, sizeof(ones));

  // Results should be different
  EXPECT_NE(crc_zeros, crc_ones);
}

// Test Extend with odd-sized chunks
TEST_F(Crc32CTest, Extend_OddSizedChunks) {
  const char* data = "0123456789ABCDEFGHIJ";
  size_t len = strlen(data);

  // Process in odd-sized chunks (3, 5, 7, etc.)
  uint32_t crc_chunked = 0;
  size_t pos = 0;

  crc_chunked = Extend(crc_chunked, data + pos, 3); pos += 3;
  crc_chunked = Extend(crc_chunked, data + pos, 5); pos += 5;
  crc_chunked = Extend(crc_chunked, data + pos, 7); pos += 7;
  crc_chunked = Extend(crc_chunked, data + pos, len - pos);

  uint32_t crc_all = Extend(0, data, len);
  EXPECT_EQ(crc_all, crc_chunked);
}

// Verify that crc32c.cpp includes config.h so HAVE_SSE42/HAVE_PCLMUL
// macros reach the CRC32 implementation. Without config.h, isSSE42()
// returns false and ChosenExtend is permanently set to Slow_CRC32.
TEST_F(Crc32CTest, HardwareCrc32PathEnabled) {
  std::string result = IsFastCrc32Supported();
  // On x86 with SSE4.2 (which this build requires via configure.ac),
  // the result MUST be "Supported on x86", not "Not supported on x86".
  // If this fails, crc32c.cpp is missing #include "../config.h".
  EXPECT_TRUE(result.find("Supported") != string::npos &&
              result.find("Not supported") == string::npos)
      << "Hardware CRC32 not enabled. IsFastCrc32Supported() = \"" << result
      << "\". Check that crc32c.cpp includes config.h";
}

// Verify CRC32C produces a known value.
// This catches implementation bugs and wrong-path selection.
TEST_F(Crc32CTest, KnownValue_Hello) {
  // Extend(0, "hello", 5) returns 0x9A71BB4C with this implementation
  uint32_t crc = Value("hello", 5);
  EXPECT_EQ(0x9A71BB4CU, crc);
}

// Large block test: crc32c_3way kicks in at >24 bytes with three-way
// parallel processing. Verify correctness at sizes that exercise
// the 3-way, 2-way, and tail code paths.
TEST_F(Crc32CTest, LargeBlock_ThreeWayPathCorrectness) {
  // Build a deterministic 8KB buffer
  const size_t size = 8192;
  char* buf = new char[size];
  for (size_t i = 0; i < size; i++) {
    buf[i] = static_cast<char>((i * 31 + 17) & 0xFF);
  }

  // Compute full-block CRC
  uint32_t crc_full = Value(buf, size);

  // Compute same CRC in 3 chunks (exercises Extend path)
  uint32_t crc_chunked = 0;
  crc_chunked = Extend(crc_chunked, buf, 3000);
  crc_chunked = Extend(crc_chunked, buf + 3000, 3000);
  crc_chunked = Extend(crc_chunked, buf + 6000, size - 6000);
  EXPECT_EQ(crc_full, crc_chunked);

  // Byte-by-byte must also match (reference path)
  uint32_t crc_ref = 0;
  for (size_t i = 0; i < size; i++) {
    crc_ref = Extend(crc_ref, buf + i, 1);
  }
  EXPECT_EQ(crc_full, crc_ref);

  delete[] buf;
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
