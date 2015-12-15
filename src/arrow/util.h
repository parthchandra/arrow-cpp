// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ARROW_UTIL_H
#define ARROW_UTIL_H

#include <cstdlib>

namespace arrow {

namespace util {

inline size_t ceil_byte(size_t size) {
  return (size + 7) & ~7;
}

inline size_t ceil_2bytes(size_t size) {
  return (size + 15) & ~15;
}

inline bool get_bit(const uint8_t* bits, size_t i) {
  return bits[i / 8] & (1 << (i % 8));
}

inline void set_bit(uint8_t* bits, size_t i, bool is_set) {
  bits[i / 8] |= (1 << (i % 8)) * is_set;
}


size_t next_power2(size_t n) {
  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  if (sizeof(size_t) == 8) {
    n |= n >> 32;
  }
  n++;
  return n;
}

void bytes_to_bits(uint8_t* bytes, size_t length, uint8_t* bits) {
  for (size_t i = 0; i < length; ++i) {
    set_bit(bits, i, static_cast<bool>(bytes[i]));
  }
}

uint8_t* bytes_to_bits(uint8_t* bytes, size_t length, size_t* out_length) {
  if (!length) {
    return nullptr;
  }
  size_t bit_length = *out_length = ceil_byte(length) / 8;
  uint8_t* result = reinterpret_cast<uint8_t*>(malloc(bit_length));
  if (result == nullptr) {
    // malloc failed
    return result;
  }

  memset(result, 0, bit_length);
  bytes_to_bits(bytes, length, result);
  return result;
}

} // namespace util

} // namespace arrow

#endif // ARROW_UTIL_H
