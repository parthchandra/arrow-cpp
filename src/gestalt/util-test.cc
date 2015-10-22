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

#include <cstdlib>
#include <vector>

#include <gtest/gtest.h>

#include "gestalt/util.h"

namespace gestalt {

TEST(UtilTests, TestNextPower2) {
  using util::next_power2;

  ASSERT_EQ(8, next_power2(6));
  ASSERT_EQ(8, next_power2(8));

  ASSERT_EQ(1, next_power2(1));
  ASSERT_EQ(256, next_power2(131));

  ASSERT_EQ(1024, next_power2(1000));

  ASSERT_EQ(4096, next_power2(4000));

  ASSERT_EQ(65536, next_power2(64000));

  ASSERT_EQ(1ULL << 32, next_power2((1ULL << 32) - 1));
  ASSERT_EQ(1ULL << 31, next_power2((1ULL << 31) - 1));
  ASSERT_EQ(1ULL << 63, next_power2((1ULL << 63) - 1));
}

} // namespace gestalt
