// Copyright 2015 Cloudera, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ARROW_TEST_UTIL_H_
#define ARROW_TEST_UTIL_H_

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/memory.h"

#include "arrow/util/bit-util.h"
#include "arrow/util/random.h"
#include "arrow/util/status.h"

#define ASSERT_RAISES(ENUM, expr)               \
  do {                                          \
    Status s = (expr);                          \
    ASSERT_TRUE(s.Is##ENUM());                  \
  } while (0)


#define ASSERT_OK(expr)                         \
  do {                                          \
    Status s = (expr);                          \
    ASSERT_TRUE(s.ok());                        \
  } while (0)

#define GC(buf)                                 \
  do {                                          \
    (buf)->Decref();                            \
  } while (0)


#define EXPECT_OK(expr)                         \
  do {                                          \
    Status s = (expr);                          \
    EXPECT_TRUE(s.ok());                        \
  } while (0)


namespace arrow {

class TestBase : public ::testing::Test {
 public:
  void SetUp() {
    pool_.reset(new MemoryPool());
  }

 protected:
  std::unique_ptr<MemoryPool> pool_;
};


template <typename T>
void randint(size_t N, T lower, T upper, std::vector<T>& out) {
  Random rng(random_seed());
  uint64_t draw;
  uint64_t span = upper - lower;
  T val;
  for (size_t i = 0; i < N; ++i) {
    draw = rng.Uniform64(span);
    val = lower + static_cast<T>(draw);
    out.push_back(val);
  }
}


template <typename T>
Buffer* to_buffer(const std::vector<T>& values) {
  return new Buffer(reinterpret_cast<uint8_t*>(const_cast<T*>(values.data())),
      values.size() * sizeof(T), false);
}

void random_nulls(size_t n, double pct_null, std::vector<uint8_t>& nulls) {
  Random rng(random_seed());
  for (int i = 0; i < n; ++i) {
    nulls.push_back(static_cast<uint8_t>(rng.NextDoubleFraction() > pct_null));
  }
}

void random_nulls(size_t n, double pct_null, std::vector<bool>& nulls) {
  Random rng(random_seed());
  for (int i = 0; i < n; ++i) {
    nulls.push_back(rng.NextDoubleFraction() > pct_null);
  }
}

Buffer* bytes_to_null_buffer(uint8_t* bytes, size_t length) {
  size_t out_length = 0;
  uint8_t* bits = util::bytes_to_bits(bytes, length, &out_length);
  return new Buffer(bits, out_length);
}

} // namespace arrow

#endif // ARROW_TEST_UTIL_H_
