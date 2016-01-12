// Copyright 2016 Cloudera Inc.
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

#include "arrow/util/bit-util.h"

#include <cstring>

namespace arrow {

void util::bytes_to_bits(uint8_t* bytes, size_t length, uint8_t* bits) {
  for (size_t i = 0; i < length; ++i) {
    set_bit(bits, i, static_cast<bool>(bytes[i]));
  }
}

uint8_t* util::bytes_to_bits(uint8_t* bytes, size_t length, size_t* out_length) {
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

} // namespace arrow
