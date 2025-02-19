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

#ifndef ARROW_TYPES_STRING_H
#define ARROW_TYPES_STRING_H

#include <string>

#include "arrow/array.h"
#include "arrow/types.h"

#include "arrow/types/integer.h"
#include "arrow/types/list.h"

namespace arrow {


struct CharType : public DataType {
  size_t size;

  BytesType physical_type;

  explicit CharType(size_t size, bool nullable = true)
      : DataType(TypeEnum::CHAR, nullable),
        size(size),
        physical_type(BytesType(size)) {}
  CharType(const CharType& other)
      : CharType(other.size, other.nullable) {}

  virtual std::string ToString() const;
};


// Variable-length, null-terminated strings, up to a certain length
struct VarcharType : public DataType {
  size_t size;

  BytesType physical_type;

  explicit VarcharType(size_t size, bool nullable = true)
      : DataType(TypeEnum::VARCHAR, nullable),
        size(size),
        physical_type(BytesType(size + 1)) {}
  VarcharType(const VarcharType& other)
      : VarcharType(other.size, other.nullable) {}

  virtual std::string ToString() const;
};


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


// TODO: add a BinaryArray layer in between
class StringArray : public ListArray {
 public:
  StringArray() : ListArray(), bytes_(nullptr), raw_bytes_(nullptr) {}

  StringArray(size_t length, Buffer* offsets, const ArrayPtr& values,
      Buffer* nulls = nullptr) {
    Init(length, offsets, values, nulls);
  }

  void Init(const TypePtr& type, size_t length, Buffer* offsets, const ArrayPtr& values,
      Buffer* nulls = nullptr) {
    ListArray::Init(type, length, offsets, values, nulls);

    // TODO: type validation for values array

    // For convenience
    bytes_ = static_cast<UInt8Array*>(values.get());
    raw_bytes_ = bytes_->raw_data();
  }

  void Init(size_t length, Buffer* offsets, const ArrayPtr& values,
      Buffer* nulls = nullptr) {
    TypePtr type(new StringType(nulls != nullptr));
    Init(type, length, offsets, values, nulls);
  }

  // Compute the pointer t
  const uint8_t* GetValue(size_t i, size_t* out_length) const {
    int32_t pos = offsets_[i];
    *out_length = offsets_[i + 1] - pos;
    return raw_bytes_ + pos;
  }

  // Construct a std::string
  std::string GetString(size_t i) const {
    size_t nchars;
    const uint8_t* str = GetValue(i, &nchars);
    return std::string(reinterpret_cast<const char*>(str), nchars);
  }

 private:
  UInt8Array* bytes_;
  const uint8_t* raw_bytes_;
};

} // namespace arrow

#endif // ARROW_TYPES_STRING_H
