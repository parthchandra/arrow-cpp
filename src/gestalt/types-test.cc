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

#include <string>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "gestalt/types.h"

using std::string;

namespace gestalt {

TEST(TypesTest, TestBytesType) {
  BytesType t1(3);

  ASSERT_EQ(t1.type, LayoutEnum::BYTE);
  ASSERT_EQ(t1.size, 3);
}


TEST(TypesTest, TestCharType) {
  CharType t1(5);

  ASSERT_EQ(t1.type, TypeEnum::CHAR);
  ASSERT_TRUE(t1.nullable);
  ASSERT_EQ(t1.size, 5);

  ASSERT_EQ(t1.ToString(), string("char(5)"));

  // Test copy constructor
  CharType t2 = t1;
  ASSERT_EQ(t2.type, TypeEnum::CHAR);
  ASSERT_TRUE(t2.nullable);
  ASSERT_EQ(t2.size, 5);
}


TEST(TypesTest, TestVarcharType) {
  VarcharType t1(5);

  ASSERT_EQ(t1.type, TypeEnum::VARCHAR);
  ASSERT_TRUE(t1.nullable);
  ASSERT_EQ(t1.size, 5);
  ASSERT_EQ(t1.physical_type.size, 6);

  ASSERT_EQ(t1.ToString(), string("varchar(5)"));

  // Test copy constructor
  VarcharType t2 = t1;
  ASSERT_EQ(t2.type, TypeEnum::VARCHAR);
  ASSERT_TRUE(t2.nullable);
  ASSERT_EQ(t2.size, 5);
  ASSERT_EQ(t2.physical_type.size, 6);
}


#define PRIMITIVE_TEST(KLASS, ENUM, NAME)       \
  TEST(TypesTest, TestPrimitive_##ENUM) {       \
    KLASS tp;                                   \
    KLASS tp_nn(false);                         \
                                                \
    ASSERT_EQ(tp.type, TypeEnum::ENUM);         \
    ASSERT_EQ(tp.name(), string(NAME));         \
    ASSERT_TRUE(tp.nullable);                   \
    ASSERT_FALSE(tp_nn.nullable);               \
                                                \
    KLASS tp_copy = tp_nn;                      \
    ASSERT_FALSE(tp_copy.nullable);             \
  }

PRIMITIVE_TEST(Int8Type, INT8, "int8");
PRIMITIVE_TEST(Int16Type, INT16, "int16");
PRIMITIVE_TEST(Int32Type, INT32, "int32");
PRIMITIVE_TEST(Int64Type, INT64, "int64");
PRIMITIVE_TEST(UInt8Type, UINT8, "uint8");
PRIMITIVE_TEST(UInt16Type, UINT16, "uint16");
PRIMITIVE_TEST(UInt32Type, UINT32, "uint32");
PRIMITIVE_TEST(UInt64Type, UINT64, "uint64");

PRIMITIVE_TEST(FloatType, FLOAT, "float");
PRIMITIVE_TEST(DoubleType, DOUBLE, "double");

PRIMITIVE_TEST(BooleanType, BOOL, "bool");

TEST(TypesTest, TestStringType) {
  StringType str;
  StringType str_nn(false);

  ASSERT_EQ(str.type, TypeEnum::STRING);
  ASSERT_EQ(str.name(), string("string"));
  ASSERT_TRUE(str.nullable);
  ASSERT_FALSE(str_nn.nullable);
}

TEST(TypesTest, TestListType) {
  std::shared_ptr<DataType> vt = std::make_shared<UInt8Type>();

  ListType list_type(vt);
  ListType list_type_nn(vt, false);

  ASSERT_EQ(list_type.type, TypeEnum::LIST);
  ASSERT_TRUE(list_type.nullable);
  ASSERT_FALSE(list_type_nn.nullable);

  ASSERT_EQ(list_type.name(), string("list"));
  ASSERT_EQ(list_type.ToString(), string("list<uint8>"));

  ASSERT_EQ(list_type.value_type->type, vt->type);
  ASSERT_EQ(list_type.value_type->type, vt->type);

  std::shared_ptr<DataType> st = std::make_shared<StringType>();
  std::shared_ptr<DataType> lt = std::make_shared<ListType>(st);
  ASSERT_EQ(lt->ToString(), string("list<string>"));

  ListType lt2(lt);
  ASSERT_EQ(lt2.ToString(), string("list<list<string>>"));
}

} // namespace gestalt
