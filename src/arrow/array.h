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

// Immutable array data containers

#ifndef ARROW_ARRAY_H
#define ARROW_ARRAY_H

#include <string>
#include <vector>

#include "arrow/status.h"
#include "arrow/memory.h"
#include "arrow/types.h"
#include "arrow/util.h"

namespace arrow {

// Immutable data array with some logical type and some length. Any memory is
// owned by the respective Buffer instance (or its parents). May or may not be
// nullable.
//
// The base class only has a null array (if the data type is nullable)
//
// Any buffers used to initialize the array have their references "stolen". If
// you wish to use the buffer beyond the lifetime of the array, you need to
// explicitly increment its reference count
class Array {
 public:

  Array() : length_(0), nulls_(nullptr), null_bits_(nullptr) {}

  Array(const TypePtr& type, size_t length, Buffer* nulls = nullptr) {
    Init(type, length, nulls);
  }

  virtual ~Array() {
    if (nulls_ != nullptr) {
      nulls_->Decref();
    }
  }

  void Init(const TypePtr& type, size_t length, Buffer* nulls) {
    type_ = type;
    length_= length;
    nulls_ = nulls;

    nullable_ = type->nullable;
    if (nulls != nullptr) {
      null_bits_ = nulls->data();
    }
  }

  // Non-copyable
  Array(const Array& buf) = delete;
  Array& operator=(Array& other) = delete;

  // Determine if a slot if null. For inner loops. Does *not* boundscheck
  bool IsNull(size_t i) const {
    return nullable_ && util::get_bit(null_bits_, i);
  }

  size_t length() const { return length_;}
  bool nullable() const { return nullable_;}
  const TypePtr& type() const { return type_;}
  TypeEnum type_enum() const { return type_->type;}

  // virtual Array* Copy() = 0;

 protected:
  TypePtr type_;
  bool nullable_;
  size_t length_;

  Buffer* nulls_;
  const uint8_t* null_bits_;
};


typedef std::shared_ptr<Array> ArrayPtr;


// Base class for fixed-size logical types
class PrimitiveArray : public Array {
 public:
  PrimitiveArray() : Array(), data_(nullptr), raw_data_(nullptr) {}

  virtual ~PrimitiveArray() {
    if (data_ != nullptr) {
      data_->Decref();
    }
  }

  void Init(const TypePtr& type, size_t length, Buffer* data,
      Buffer* nulls = nullptr) {
    Array::Init(type, length, nulls);
    data_ = data;
    raw_data_ = data == nullptr? nullptr : data_->data();
  }

  Buffer* data() const { return data_;}

  bool Equals(const PrimitiveArray& other) const {
    if (this == &other) return true;
    if (type_->nullable != other.type_->nullable) return false;

    bool equal_data = data_->Equals(*other.data_, length_);
    if (type_->nullable) {
      return equal_data &&
        nulls_->Equals(*other.nulls_, util::ceil_byte(length_) / 8);
    } else {
      return equal_data;
    }
  }

 protected:
  Buffer* data_;
  const uint8_t* raw_data_;
};


template <typename TypeClass>
class PrimitiveArrayImpl : public PrimitiveArray {
 public:
  typedef typename TypeClass::c_type T;

  PrimitiveArrayImpl() : PrimitiveArray() {}

  PrimitiveArrayImpl(size_t length, Buffer* data, Buffer* nulls = nullptr) {
    Init(length, data, nulls);
  }

  void Init(size_t length, Buffer* data, Buffer* nulls = nullptr) {
    TypePtr type(new TypeClass(nulls != nullptr));
    PrimitiveArray::Init(type, length, data, nulls);
  }

  bool Equals(const PrimitiveArrayImpl& other) const {
    return PrimitiveArray::Equals(*static_cast<const PrimitiveArray*>(&other));
  }

  const T* raw_data() const { return reinterpret_cast<const T*>(raw_data_);}

  T Value(size_t i) const {
    return raw_data()[i];
  }

  TypeClass* exact_type() const {
    return static_cast<TypeClass*>(type_);
  }
};


typedef PrimitiveArrayImpl<UInt8Type> UInt8Array;
typedef PrimitiveArrayImpl<Int8Type> Int8Array;

typedef PrimitiveArrayImpl<UInt16Type> UInt16Array;
typedef PrimitiveArrayImpl<Int16Type> Int16Array;

typedef PrimitiveArrayImpl<UInt32Type> UInt32Array;
typedef PrimitiveArrayImpl<Int32Type> Int32Array;

typedef PrimitiveArrayImpl<UInt64Type> UInt64Array;
typedef PrimitiveArrayImpl<Int64Type> Int64Array;

typedef PrimitiveArrayImpl<FloatType> FloatArray;
typedef PrimitiveArrayImpl<DoubleType> DoubleArray;


class ListArray : public Array {
 public:
  ListArray() : Array(), offset_buf_(nullptr), offsets_(nullptr) {}

  ListArray(const TypePtr& type, size_t length, Buffer* offsets,
      const ArrayPtr& values, Buffer* nulls = nullptr) {
    Init(type, length, offsets, values, nulls);
  }

  virtual ~ListArray() {
    if (offset_buf_ != nullptr) {
      offset_buf_->Decref();
    }
  }

  void Init(const TypePtr& type, size_t length, Buffer* offsets,
      const ArrayPtr& values, Buffer* nulls = nullptr) {
    offset_buf_ = offsets;
    offsets_ = offsets == nullptr? nullptr :
      reinterpret_cast<const int32_t*>(offset_buf_->data());

    values_ = values;
    Array::Init(type, length, nulls);
  }

  // Return a shared pointer in case the requestor desires to share ownership
  // with this array.
  const ArrayPtr& values() const {return values_;}

  const int32_t* offsets() const { return offsets_;}

  int32_t offset(size_t i) const { return offsets_[i];}

  // Neither of these functions will perform boundschecking
  int32_t value_offset(size_t i) { return offsets_[i];}
  size_t value_length(size_t i) { return offsets_[i + 1] - offsets_[i];}

 protected:
  Buffer* offset_buf_;
  const int32_t* offsets_;
  ArrayPtr values_;
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


class UnionArray : public Array {
 public:
  UnionArray() : Array() {}

 protected:
  // The data are types encoded as int16
  Buffer* types_;
  std::vector<std::shared_ptr<Array> > children_;
};


class DenseUnionArray : public UnionArray {

 public:
  DenseUnionArray() : UnionArray() {}

 protected:
  Buffer* offset_buf_;
};


class SparseUnionArray : public UnionArray {
 public:
  SparseUnionArray() : UnionArray() {}
};

} // namespace arrow

#endif
