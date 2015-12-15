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

#ifndef ARROW_TYPES_H
#define ARROW_TYPES_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace arrow {

// Physical data type that describes the memory layout of values. See details
// for each type
enum class LayoutEnum: char {
  // A physical type consisting of some non-negative number of bytes
  BYTE = 0,

  // A physical type consisting of some non-negative number of bits
  BIT = 1,

  // A parametric variable-length value type. Full specification requires a
  // child logical type
  LIST = 2,

  // A collection of multiple equal-length child arrays. Parametric type taking
  // 1 or more child logical types
  STRUCT = 3,

  // An array with heterogeneous value types. Parametric types taking 1 or more
  // child logical types
  DENSE_UNION = 4,
  SPARSE_UNION = 5
};


struct LayoutType {
  LayoutEnum type;
  explicit LayoutType(LayoutEnum type) : type(type) {}
};


// Data types in this library are all *logical*. They can be expressed as
// either a primitive physical type (bytes or bits of some fixed size), a
// nested type consisting of other data types, or another data type (e.g. a
// timestamp encoded as an int64)
//
// Any data type can be nullable

enum class TypeEnum: char {
  // A degerate NULL type represented as 0 bytes/bits
  NA = 0,

  // Little-endian integer types
  UINT8 = 1,
  INT8 = 2,
  UINT16 = 3,
  INT16 = 4,
  UINT32 = 5,
  INT32 = 6,
  UINT64 = 7,
  INT64 = 8,

  // A boolean value represented as 1 byte
  BOOL = 9,

  // A boolean value represented as 1 bit
  BIT = 10,

  // 4-byte floating point value
  FLOAT = 11,

  // 8-byte floating point value
  DOUBLE = 12,

  // CHAR(N): fixed-length UTF8 string with length N
  CHAR = 13,

  // UTF8 variable-length string as List<Char>
  STRING = 14,

  // VARCHAR(N): Null-terminated string type embedded in a CHAR(N + 1)
  VARCHAR = 15,

  // Variable-length bytes (no guarantee of UTF8-ness)
  BINARY = 16,

  // By default, int32 days since the UNIX epoch
  DATE = 17,

  // Exact timestamp encoded with int64 since UNIX epoch
  // Default unit millisecond
  TIMESTAMP = 18,

  // Timestamp as double seconds since the UNIX epoch
  TIMESTAMP_DOUBLE = 19,

  // Exact time encoded with int64, default unit millisecond
  TIME = 20,

  // Precision- and scale-based decimal type. Storage type depends on the
  // parameters.
  DECIMAL = 21,

  // Decimal value encoded as a text string
  DECIMAL_TEXT = 22,

  // A list of some logical data type
  LIST = 30,

  // Struct of logical types
  STRUCT = 31,

  // Unions of logical types
  DENSE_UNION = 32,
  SPARSE_UNION = 33,

  // Union<Null, Int32, Double, String, Bool>
  JSON_SCALAR = 50
};


struct DataType {

  TypeEnum type;
  bool nullable;

  DataType(TypeEnum type, bool nullable = true)
      : type(type), nullable(nullable) {}

  virtual std::string ToString() const = 0;
};


typedef std::shared_ptr<LayoutType> LayoutPtr;
typedef std::shared_ptr<DataType> TypePtr;


struct BytesType : public LayoutType {
  size_t size;

  explicit BytesType(size_t size)
      : LayoutType(LayoutEnum::BYTE),
        size(size) {}

  BytesType(const BytesType& other)
      : BytesType(other.size) {}
};


struct CharType : public DataType {
  size_t size;

  BytesType physical_type;

  CharType(size_t size, bool nullable = true)
      : DataType(TypeEnum::CHAR, nullable),
        size(size),
        physical_type(BytesType(size)) {}
  CharType(const CharType& other)
      : CharType(other.size, other.nullable) {}

  virtual std::string ToString() const {
    std::stringstream s;
    s << "char(" << size << ")";
    return s.str();
  }
};


// Variable-length, null-terminated strings, up to a certain length
struct VarcharType : public DataType {
  size_t size;

  BytesType physical_type;

  VarcharType(size_t size, bool nullable = true)
      : DataType(TypeEnum::VARCHAR, nullable),
        size(size),
        physical_type(BytesType(size + 1)) {}
  VarcharType(const VarcharType& other)
      : VarcharType(other.size, other.nullable) {}

  virtual std::string ToString() const {
    std::stringstream s;
    s << "varchar(" << size << ")";
    return s.str();
  }
};


struct ListLayoutType : public LayoutType {
  LayoutPtr value_type;

  explicit ListLayoutType(const LayoutPtr& value_type)
      : LayoutType(LayoutEnum::BYTE),
        value_type(value_type) {}
};


struct ListType : public DataType {
  // List can contain any other logical value type
  TypePtr value_type;

  ListType(const TypePtr& value_type,
      bool nullable = true)
      : DataType(TypeEnum::LIST, nullable),
        value_type(value_type) {}

  static char const *name() {
    return "list";
  }

  virtual std::string ToString() const {
    std::stringstream s;
    s << "list<" << value_type->ToString() << ">";
    return s.str();
  }
};


static const LayoutPtr byte1(new BytesType(1));
static const LayoutPtr byte2(new BytesType(2));
static const LayoutPtr byte4(new BytesType(4));
static const LayoutPtr byte8(new BytesType(8));
static const LayoutPtr physical_string = LayoutPtr(new ListLayoutType(byte1));


// String is a logical type consisting of a physical list of 1-byte values
struct StringType : public DataType {

  explicit StringType(bool nullable = true)
      : DataType(TypeEnum::STRING, nullable) {}

  StringType(const StringType& other)
      : StringType(other.nullable) {}

  const LayoutPtr& physical_type() {
    return physical_string;
  }

  static char const *name() {
    return "string";
  }

  virtual std::string ToString() const {
    return name();
  }
};


struct DateType : public DataType {

  enum class Unit: char {
    DAY = 0,
    MONTH = 1,
    YEAR = 2
  };

  Unit unit;

  DateType(Unit unit = Unit::DAY, bool nullable = true)
      : DataType(TypeEnum::DATE, nullable),
        unit(unit) {}

  DateType(const DateType& other)
      : DateType(other.unit, other.nullable) {}

  static char const *name() {
    return "date";
  }

  // virtual std::string ToString() {
  //   return name();
  // }
};


struct TimestampType : public DataType {

  enum class Unit: char {
    SECOND = 0,
    MILLI = 1,
    MICRO = 2,
    NANO = 3
  };

  Unit unit;

  TimestampType(Unit unit = Unit::MILLI, bool nullable = true)
      : DataType(TypeEnum::TIMESTAMP, nullable),
        unit(unit) {}

  TimestampType(const TimestampType& other)
      : TimestampType(other.unit, other.nullable) {}

  static char const *name() {
    return "timestamp";
  }

  // virtual std::string ToString() {
  //   return name();
  // }
};


struct DecimalType : public DataType {

  size_t precision;
  size_t scale;

};


template <typename Derived>
struct PrimitiveType : public DataType {
  explicit PrimitiveType(bool nullable = true)
      : DataType(Derived::type_enum, nullable) {}

  virtual std::string ToString() const {
    return std::string(static_cast<const Derived*>(this)->name());
  }
};


#define PRIMITIVE_DECL(TYPENAME, C_TYPE, ENUM, SIZE, NAME)  \
  typedef C_TYPE c_type;                                    \
  static constexpr TypeEnum type_enum = TypeEnum::ENUM;     \
  static constexpr size_t size = SIZE;                      \
                                                            \
  explicit TYPENAME(bool nullable = true)                   \
      : PrimitiveType<TYPENAME>(nullable) {}                \
                                                            \
  static const char* name() {                               \
    return NAME;                                            \
  }


struct NullType : public PrimitiveType<NullType> {
  PRIMITIVE_DECL(NullType, void, NA, 0, "null");
};

struct UInt8Type : public PrimitiveType<UInt8Type> {
  PRIMITIVE_DECL(UInt8Type, uint8_t, UINT8, 1, "uint8");
};

struct Int8Type : public PrimitiveType<Int8Type> {
  PRIMITIVE_DECL(Int8Type, int8_t, INT8, 1, "int8");
};

struct UInt16Type : public PrimitiveType<UInt16Type> {
  PRIMITIVE_DECL(UInt16Type, uint16_t, UINT16, 2, "uint16");
};

struct Int16Type : public PrimitiveType<Int16Type> {
  PRIMITIVE_DECL(Int16Type, int16_t, INT16, 2, "int16");
};

struct UInt32Type : public PrimitiveType<UInt32Type> {
  PRIMITIVE_DECL(UInt32Type, uint32_t, UINT32, 4, "uint32");
};

struct Int32Type : public PrimitiveType<Int32Type> {
  PRIMITIVE_DECL(Int32Type, int32_t, INT32, 4, "int32");
};

struct UInt64Type : public PrimitiveType<UInt64Type> {
  PRIMITIVE_DECL(UInt64Type, uint64_t, UINT64, 8, "uint64");
};

struct Int64Type : public PrimitiveType<Int64Type> {
  PRIMITIVE_DECL(Int64Type, int64_t, INT64, 8, "int64");
};

struct FloatType : public PrimitiveType<FloatType> {
  PRIMITIVE_DECL(FloatType, float, FLOAT, 4, "float");
};

struct DoubleType : public PrimitiveType<DoubleType> {
  PRIMITIVE_DECL(DoubleType, double, DOUBLE, 8, "double");
};

struct BooleanType : public PrimitiveType<BooleanType> {
  PRIMITIVE_DECL(BooleanType, uint8_t, BOOL, 1, "bool");
};


template <TypeEnum T>
struct CollectionType : public DataType {
  std::vector<TypePtr> child_types_;

  explicit CollectionType(bool nullable = true) : DataType(T, nullable) {}

  const TypePtr& child(size_t i) const {
    return child_types_[i];
  }

  size_t num_children() const {
    return child_types_.size();
  }
};


struct StructType : public CollectionType<TypeEnum::STRUCT> {

  typedef CollectionType<TypeEnum::STRUCT> Base;

  StructType(const std::vector<TypePtr>& child_types,
      bool nullable = true)
      : Base(nullable) {
    child_types_ = child_types;
  }
};

static inline std::string format_union(const std::vector<TypePtr>& child_types) {
  std::stringstream s;
  s << "union<";
  for (int i = 0; i < child_types.size(); ++i) {
    if (i) s << ", ";
    s << child_types[i]->ToString();
  }
  s << ">";
  return s.str();
}


struct DenseUnionType : public CollectionType<TypeEnum::DENSE_UNION> {

  typedef CollectionType<TypeEnum::DENSE_UNION> Base;

  DenseUnionType(const std::vector<TypePtr>& child_types,
      bool nullable = true)
      : Base(nullable) {
    child_types_ = child_types;
  }

  virtual std::string ToString() const {
    return format_union(child_types_);
  }
};


struct SparseUnionType : public CollectionType<TypeEnum::SPARSE_UNION> {

  typedef CollectionType<TypeEnum::SPARSE_UNION> Base;

  SparseUnionType(const std::vector<TypePtr>& child_types,
      bool nullable = true)
      : Base(nullable) {
    child_types_ = child_types;
  }

  virtual std::string ToString() const {
    return format_union(child_types_);
  }
};


struct JSONScalar : public DataType {

  bool dense;

  static TypePtr dense_type;
  static TypePtr sparse_type;

  JSONScalar(bool dense = true, bool nullable = true)
      : DataType(TypeEnum::JSON_SCALAR, nullable),
        dense(dense) {}
};

static const TypePtr Null(new NullType());
static const TypePtr Int32(new Int32Type());
static const TypePtr String(new StringType());
static const TypePtr Double(new DoubleType());
static const TypePtr Bool(new BooleanType());

static const std::vector<TypePtr> json_types = {Null, Int32, String,
                                                Double, Bool};
TypePtr JSONScalar::dense_type = TypePtr(new DenseUnionType(json_types));
TypePtr JSONScalar::sparse_type = TypePtr(new SparseUnionType(json_types));


} // namespace arrow

#endif  // ARROW_TYPES_H
