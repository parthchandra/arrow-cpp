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

#include <auto_ptr.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "hello_world.h"

DECLARE_bool(use_long_form);

namespace test {

class HelloWorldTest : public ::testing::Test {
 public:
  void SetUp() {
    hello_world_.reset(new HelloWorld());
  }

 protected:
  std::auto_ptr<HelloWorld> hello_world_;
};

TEST_F(HelloWorldTest, TestShortForm) {
  ASSERT_EQ(hello_world_->SayHello(), "Hi!");
}

TEST_F(HelloWorldTest, TestLongForm) {
  FLAGS_use_long_form = true;
  ASSERT_EQ(hello_world_->SayHello(), "Hello world!");
}
} // namespace test
