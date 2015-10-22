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
#include <memory>
#include <string>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "gestalt/array.h"
#include "gestalt/memory.h"
#include "gestalt/test-util.h"

using std::string;
using std::vector;

namespace gestalt {

static TypePtr int32 = TypePtr(new Int32Type());
static TypePtr int32_nn = TypePtr(new Int32Type(false));


class TestArray : public TestBase {
 public:
  void SetUp() {
    TestBase::SetUp();

    Buffer* data;
    Buffer* nulls;

    ASSERT_OK(pool_->NewBuffer(400, &data));
    ASSERT_OK(pool_->NewBuffer(128, &nulls));

    arr_.reset(new Int32Array(100, data, nulls));
  }

 protected:
  std::unique_ptr<Int32Array> arr_;
};


TEST_F(TestArray, TestNullable) {
  Buffer* tmp = arr_->data();
  tmp->Incref();
  std::unique_ptr<Int32Array> arr_nn(new Int32Array(100, tmp));

  ASSERT_TRUE(arr_->nullable());
  ASSERT_FALSE(arr_nn->nullable());
}


TEST_F(TestArray, TestLength) {
  ASSERT_EQ(arr_->length(), 100);
}


TEST_F(TestArray, TestDestructor) {
  // Array destructor does not alter buffer reference counts
  Buffer* data;
  Buffer* nulls;

  ASSERT_OK(pool_->NewBuffer(400, &data));
  ASSERT_OK(pool_->NewBuffer(128, &nulls));

  Int32Array* arr = new Int32Array(100, data, nulls);
  data->Incref();
  nulls->Incref();
  delete arr;

  ASSERT_EQ(data->ref_count(), 1);
  ASSERT_EQ(nulls->ref_count(), 1);

  GC(data);
  GC(nulls);
}


TEST_F(TestArray, TestIsNull) {
  vector<uint8_t> nulls = {1, 0, 1, 1, 0, 1, 0, 0,
                           1, 0, 1, 1, 0, 1, 0, 0,
                           1, 0, 1, 1, 0, 1, 0, 0,
                           1, 0, 1, 1, 0, 1, 0, 0,
                           1, 0, 0, 1};

  Buffer* null_buf = bytes_to_null_buffer(nulls.data(), nulls.size());
  std::unique_ptr<Array> arr;
  arr.reset(new Array(int32, nulls.size(), null_buf));

  ASSERT_EQ(null_buf->size(), 5);
  for (int i = 0; i < nulls.size(); ++i) {
    ASSERT_EQ(static_cast<bool>(nulls[i]), arr->IsNull(i));
  }
}


TEST_F(TestArray, TestCopy) {

}


class TestStringArrayBasics : public TestBase {
 public:
  void SetUp() {
    TestBase::SetUp();

    chars_ = {'a', 'b', 'b', 'c', 'c', 'c'};
    offsets_ = {0, 1, 1, 1, 3, 6};
    nulls_ = {0, 0, 1, 0, 0};
    expected_ = {"a", "", "", "bb", "ccc"};

    MakeArray();
  }

  void MakeArray() {
    length_ = offsets_.size() - 1;
    size_t nchars = chars_.size();

    value_buf_ = to_buffer(chars_);
    values_ = ArrayPtr(new UInt8Array(nchars, value_buf_));

    offsets_buf_ = to_buffer(offsets_);

    nulls_buf_ = bytes_to_null_buffer(nulls_.data(), nulls_.size());
    strings_.Init(length_, offsets_buf_, values_, nulls_buf_);
  }

 protected:
  vector<int32_t> offsets_;
  vector<char> chars_;
  vector<uint8_t> nulls_;

  vector<string> expected_;

  Buffer* value_buf_;
  Buffer* offsets_buf_;
  Buffer* nulls_buf_;

  size_t length_;

  ArrayPtr values_;
  StringArray strings_;
};


TEST_F(TestStringArrayBasics, TestArrayBasics) {
  ASSERT_EQ(length_, strings_.length());
  ASSERT_TRUE(strings_.nullable());
}

TEST_F(TestStringArrayBasics, TestType) {
  TypePtr type = strings_.type();

  ASSERT_EQ(TypeEnum::STRING, type->type);
  ASSERT_EQ(TypeEnum::STRING, strings_.type_enum());
}


TEST_F(TestStringArrayBasics, TestListFunctions) {
  size_t pos = 0;
  for (size_t i = 0; i < expected_.size(); ++i) {
    ASSERT_EQ(pos, strings_.value_offset(i));
    ASSERT_EQ(expected_[i].size(), strings_.value_length(i));
    pos += expected_[i].size();
  }
}


TEST_F(TestStringArrayBasics, TestDestructor) {
  StringArray* arr = new StringArray(length_, offsets_buf_, values_, nulls_buf_);
  offsets_buf_->Incref();
  nulls_buf_->Incref();
  delete arr;

  ASSERT_EQ(1, offsets_buf_->ref_count());
  ASSERT_EQ(1, value_buf_->ref_count());
  ASSERT_EQ(1, nulls_buf_->ref_count());
}

TEST_F(TestStringArrayBasics, TestGetString) {
  for (size_t i = 0; i < expected_.size(); ++i) {
    if (nulls_[i]) {
      ASSERT_TRUE(strings_.IsNull(i));
    } else {
      ASSERT_EQ(expected_[i], strings_.GetString(i));
    }
  }
}


} // namespace gestalt
