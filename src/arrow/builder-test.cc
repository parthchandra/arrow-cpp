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

#include <gtest/gtest.h>

#include "arrow/builder.h"
#include "arrow/memory.h"
#include "arrow/test-util.h"

#include "arrow/types/integer.h"
#include "arrow/types/list.h"
#include "arrow/types/string.h"

using std::string;
using std::unique_ptr;
using std::vector;

namespace arrow {

// ----------------------------------------------------------------------
// Common array builder API

class TestBuilder : public TestBase {
 public:
  void SetUp() {
    TestBase::SetUp();

    type_ = TypePtr(new UInt8Type());
    type_nn_ = TypePtr(new UInt8Type(false));
    builder_.reset(new UInt8Builder(pool_.get(), type_));
    builder_nn_.reset(new UInt8Builder(pool_.get(), type_nn_));
  }
 protected:
  TypePtr type_;
  TypePtr type_nn_;
  unique_ptr<ArrayBuilder> builder_;
  unique_ptr<ArrayBuilder> builder_nn_;
};

TEST_F(TestBuilder, TestResize) {
  builder_->Init(10);
  ASSERT_EQ(2, builder_->nulls()->size());

  builder_->Resize(30);
  ASSERT_EQ(4, builder_->nulls()->size());
}

// ----------------------------------------------------------------------
// Primitive type tests

template <typename Attrs>
class TestPrimitiveBuilder : public TestBuilder {
 public:
  typedef typename Attrs::ArrayType ArrayType;
  typedef typename Attrs::BuilderType BuilderType;
  typedef typename Attrs::T T;

  void SetUp() {
    TestBuilder::SetUp();

    type_ = Attrs::type();
    type_nn_ = Attrs::type(false);

    ArrayBuilder* tmp;
    ASSERT_OK(make_builder(pool_.get(), type_, &tmp));
    builder_.reset(static_cast<BuilderType*>(tmp));

    ASSERT_OK(make_builder(pool_.get(), type_nn_, &tmp));
    builder_nn_.reset(static_cast<BuilderType*>(tmp));
  }

  void RandomData(size_t N, double pct_null = 0.1) {
    Attrs::draw(N, draws_);
    random_nulls(N, pct_null, nulls_);
  }

  void CheckNullable() {
    ArrayType result;
    ArrayType expected;
    size_t size = builder_->length();

    Buffer* ex_data = new Buffer(reinterpret_cast<uint8_t*>(draws_.data()),
        size * sizeof(T), false);
    Buffer* ex_nulls = bytes_to_null_buffer(nulls_.data(), size);

    expected.Init(size, ex_data, ex_nulls);
    ASSERT_OK(builder_->Transfer(&result));

    // Builder is now reset
    ASSERT_EQ(0, builder_->length());
    ASSERT_EQ(0, builder_->capacity());
    ASSERT_EQ(nullptr, builder_->buffer());

    ASSERT_TRUE(result.Equals(expected));
  }

  void CheckNonNullable() {
    ArrayType result;
    ArrayType expected;
    size_t size = builder_nn_->length();

    Buffer* ex_data = new Buffer(reinterpret_cast<uint8_t*>(draws_.data()),
        size * sizeof(T), false);

    expected.Init(size, ex_data);
    ASSERT_OK(builder_nn_->Transfer(&result));

    // Builder is now reset
    ASSERT_EQ(0, builder_nn_->length());
    ASSERT_EQ(0, builder_nn_->capacity());
    ASSERT_EQ(nullptr, builder_nn_->buffer());

    ASSERT_TRUE(result.Equals(expected));
  }

 protected:
  TypePtr type_;
  TypePtr type_nn_;
  unique_ptr<BuilderType> builder_;
  unique_ptr<BuilderType> builder_nn_;

  vector<T> draws_;
  vector<uint8_t> nulls_;
};

#define PTYPE_DECL(CapType, c_type)             \
  typedef CapType##Array ArrayType;             \
  typedef CapType##Builder BuilderType;         \
  typedef CapType##Type Type;                   \
  typedef c_type T;                             \
                                                \
  static TypePtr type(bool nullable = true) {   \
    return TypePtr(new Type(nullable));         \
  }

#define PINT_DECL(CapType, c_type, LOWER, UPPER)        \
  struct P##CapType {                                   \
    PTYPE_DECL(CapType, c_type);                        \
    static void draw(size_t N, vector<T>& draws) { \
      randint<T>(N, LOWER, UPPER, draws);               \
    }                                                   \
  }

PINT_DECL(UInt8, uint8_t, 0, UINT8_MAX);
PINT_DECL(UInt16, uint16_t, 0, UINT16_MAX);
PINT_DECL(UInt32, uint32_t, 0, UINT32_MAX);
PINT_DECL(UInt64, uint64_t, 0, UINT64_MAX);

PINT_DECL(Int8, int8_t, INT8_MIN, INT8_MAX);
PINT_DECL(Int16, int16_t, INT16_MIN, INT16_MAX);
PINT_DECL(Int32, int32_t, INT32_MIN, INT32_MAX);
PINT_DECL(Int64, int64_t, INT64_MIN, INT64_MAX);

typedef ::testing::Types<PUInt8, PUInt16, PUInt32, PUInt64,
                         PInt8, PInt16, PInt32, PInt64> Primitives;

TYPED_TEST_CASE(TestPrimitiveBuilder, Primitives);

#define PLIFT_TYPEDEFS()                                \
  typedef typename TestFixture::T T;                    \
  typedef typename TestFixture::ArrayType ArrayType;


TYPED_TEST(TestPrimitiveBuilder, TestInit) {
  PLIFT_TYPEDEFS();

  size_t n = 1000;
  ASSERT_OK(this->builder_->Init(n));
  ASSERT_EQ(n, this->builder_->capacity());
  ASSERT_EQ(n * sizeof(T), this->builder_->buffer()->size());

  // unsure if this should go in all builder classes
  ASSERT_EQ(0, this->builder_->num_children());
}


TYPED_TEST(TestPrimitiveBuilder, TestDestructor) {
  PLIFT_TYPEDEFS();

  ArrayBuilder* tmp;
  ASSERT_OK(make_builder(this->pool_.get(), this->type_, &tmp));
  ASSERT_OK(tmp->Init(1000));
  ASSERT_GE(1000 * sizeof(T) + sizeof(T) / 8, this->pool_->total_bytes());

  delete tmp;
  ASSERT_EQ(0, this->pool_->total_bytes());
}


TYPED_TEST(TestPrimitiveBuilder, TestAppendNull) {
  PLIFT_TYPEDEFS();

  size_t size = 10000;
  for (int i = 0; i < size; ++i) {
    ASSERT_OK(this->builder_->AppendNull());
  }

  Array* result;
  ASSERT_OK(this->builder_->ToArray(&result));
  unique_ptr<Array> holder(result);

  for (int i = 0; i < size; ++i) {
    ASSERT_TRUE(result->IsNull(i));
  }
}


TYPED_TEST(TestPrimitiveBuilder, TestAppendScalar) {
  PLIFT_TYPEDEFS();

  size_t size = 10000;

  vector<T>& draws = this->draws_;
  vector<uint8_t>& nulls = this->nulls_;

  this->RandomData(size);

  size_t i;
  // Append the first 1000
  for (i = 0; i < 1000; ++i) {
    ASSERT_OK(this->builder_->Append(draws[i], nulls[i] > 0));
    ASSERT_OK(this->builder_nn_->Append(draws[i]));
  }

  ASSERT_EQ(1000, this->builder_->length());
  ASSERT_EQ(1024, this->builder_->capacity());

  ASSERT_EQ(1000, this->builder_nn_->length());
  ASSERT_EQ(1024, this->builder_nn_->capacity());

  // Append the next 9000
  for (i = 1000; i < size; ++i) {
    ASSERT_OK(this->builder_->Append(draws[i], nulls[i] > 0));
    ASSERT_OK(this->builder_nn_->Append(draws[i]));
  }

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_EQ(util::next_power2(size), this->builder_->capacity());

  ASSERT_EQ(size, this->builder_nn_->length());
  ASSERT_EQ(util::next_power2(size), this->builder_nn_->capacity());

  this->CheckNullable();
  this->CheckNonNullable();
}


TYPED_TEST(TestPrimitiveBuilder, TestAppendVector) {
  PLIFT_TYPEDEFS();

  size_t size = 10000;
  this->RandomData(size);

  vector<T>& draws = this->draws_;
  vector<uint8_t>& nulls = this->nulls_;

  // first slug
  size_t K = 1000;

  ASSERT_OK(this->builder_->Append(draws.data(), K, nulls.data()));
  ASSERT_OK(this->builder_nn_->Append(draws.data(), K));

  ASSERT_EQ(1000, this->builder_->length());
  ASSERT_EQ(1024, this->builder_->capacity());

  ASSERT_EQ(1000, this->builder_nn_->length());
  ASSERT_EQ(1024, this->builder_nn_->capacity());

  // Append the next 9000
  ASSERT_OK(this->builder_->Append(draws.data() + K, size - K, nulls.data() + K));
  ASSERT_OK(this->builder_nn_->Append(draws.data() + K, size - K));

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_EQ(util::next_power2(size), this->builder_->capacity());

  this->CheckNullable();
  this->CheckNonNullable();
}

TYPED_TEST(TestPrimitiveBuilder, TestAdvance) {
  size_t n = 1000;
  ASSERT_OK(this->builder_->Init(n));

  ASSERT_OK(this->builder_->Advance(100));
  ASSERT_EQ(100, this->builder_->length());

  ASSERT_OK(this->builder_->Advance(900));
  ASSERT_RAISES(Invalid, this->builder_->Advance(1));
}

TYPED_TEST(TestPrimitiveBuilder, TestResize) {
  PLIFT_TYPEDEFS();
  size_t cap = MIN_BUILDER_CAPACITY * 2;

  ASSERT_OK(this->builder_->Resize(cap));
  ASSERT_EQ(cap, this->builder_->capacity());

  ASSERT_EQ(cap * sizeof(T), this->builder_->buffer()->size());
  ASSERT_EQ(util::ceil_byte(cap) / 8, this->builder_->nulls()->size());
}

TYPED_TEST(TestPrimitiveBuilder, TestReserve) {
  size_t n = 100;
  ASSERT_OK(this->builder_->Reserve(n));
  ASSERT_EQ(0, this->builder_->length());
  ASSERT_EQ(MIN_BUILDER_CAPACITY, this->builder_->capacity());

  ASSERT_OK(this->builder_->Advance(100));
  ASSERT_OK(this->builder_->Reserve(MIN_BUILDER_CAPACITY));

  ASSERT_EQ(util::next_power2(MIN_BUILDER_CAPACITY + 100),
      this->builder_->capacity());
}

// ----------------------------------------------------------------------
// List tests

class TestListBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();

    value_type_ = TypePtr(new Int32Type());
    type_ = TypePtr(new ListType(value_type_));

    ArrayBuilder* tmp;
    ASSERT_OK(make_builder(pool_.get(), type_, &tmp));
    builder_.reset(static_cast<ListBuilder*>(tmp));
  }

  void Done() {
    Array* out;
    ASSERT_OK(builder_->ToArray(&out));
    result_.reset(static_cast<ListArray*>(out));
  }

 protected:
  TypePtr value_type_;
  TypePtr type_;

  unique_ptr<ListBuilder> builder_;
  unique_ptr<ListArray> result_;
};


TEST_F(TestListBuilder, TestResize) {

}

TEST_F(TestListBuilder, TestAppendNull) {
  ASSERT_OK(builder_->AppendNull());
  ASSERT_OK(builder_->AppendNull());

  Done();

  ASSERT_TRUE(result_->IsNull(0));
  ASSERT_TRUE(result_->IsNull(1));

  ASSERT_EQ(0, result_->offsets()[0]);
  ASSERT_EQ(0, result_->offset(1));
  ASSERT_EQ(0, result_->offset(2));

  Int32Array* values = static_cast<Int32Array*>(result_->values().get());
  ASSERT_EQ(0, values->length());
}

TEST_F(TestListBuilder, TestBasics) {
  vector<int32_t> values = {0, 1, 2, 3, 4, 5, 6};
  vector<int> lengths = {3, 0, 4};
  vector<uint8_t> is_null = {0, 1, 0};

  Int32Builder* vb = static_cast<Int32Builder*>(builder_->value_builder());

  size_t pos = 0;
  for (int i = 0; i < lengths.size(); ++i) {
    ASSERT_OK(builder_->Append(is_null[i] > 0));
    for (int j = 0; j < lengths[i]; ++j) {
      ASSERT_OK(vb->Append(values[pos++]));
    }
  }

  Done();

  ASSERT_TRUE(result_->nullable());
  ASSERT_TRUE(result_->values()->nullable());

  ASSERT_EQ(3, result_->length());
  vector<int32_t> ex_offsets = {0, 3, 3, 7};
  for (int i = 0; i < ex_offsets.size(); ++i) {
    ASSERT_EQ(ex_offsets[i], result_->offset(i));
  }

  for (int i = 0; i < result_->length(); ++i) {
    ASSERT_EQ(static_cast<bool>(is_null[i]), result_->IsNull(i));
  }

  ASSERT_EQ(7, result_->values()->length());
  Int32Array* varr = static_cast<Int32Array*>(result_->values().get());

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(values[i], varr->Value(i));
  }
}

TEST_F(TestListBuilder, TestBasicsNonNullable) {

}


TEST_F(TestListBuilder, TestZeroLength) {
  // All buffers are null
  Done();
}


// ----------------------------------------------------------------------
// String builder tests

class TestStringBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();
    type_ = TypePtr(new StringType());

    ArrayBuilder* tmp;
    ASSERT_OK(make_builder(pool_.get(), type_, &tmp));
    builder_.reset(static_cast<StringBuilder*>(tmp));
  }

  void Done() {
    Array* out;
    ASSERT_OK(builder_->ToArray(&out));
    result_.reset(static_cast<StringArray*>(out));
  }

 protected:
  TypePtr type_;

  unique_ptr<StringBuilder> builder_;
  unique_ptr<StringArray> result_;
};

TEST_F(TestStringBuilder, TestAttrs) {
  ASSERT_FALSE(builder_->value_builder()->nullable());
}

TEST_F(TestStringBuilder, TestScalarAppend) {
  vector<string> strings = {"a", "bb", "", "", "ccc"};
  vector<uint8_t> is_null = {0, 0, 0, 1, 0};

  size_t N = strings.size();
  size_t reps = 1000;

  for (int j = 0; j < reps; ++j) {
    for (int i = 0; i < N; ++i) {
      if (is_null[i]) {
        builder_->AppendNull();
      } else {
        builder_->Append(strings[i]);
      }
    }
  }
  Done();

  ASSERT_EQ(reps * N, result_->length());
  ASSERT_EQ(reps * 6, result_->values()->length());

  size_t length;
  size_t pos = 0;
  for (int i = 0; i < N * reps; ++i) {
    if (is_null[i % N]) {
      ASSERT_TRUE(result_->IsNull(i));
    } else {
      ASSERT_FALSE(result_->IsNull(i));
      result_->GetValue(i, &length);
      ASSERT_EQ(pos, result_->offset(i));
      ASSERT_EQ(strings[i % N].size(), length);
      ASSERT_EQ(strings[i % N], result_->GetString(i));

      pos += length;
    }
  }
}

TEST_F(TestStringBuilder, TestZeroLength) {
  // All buffers are null
  Done();
}

} // namespace arrow
