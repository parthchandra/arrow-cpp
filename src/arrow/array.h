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

#include "arrow/memory.h"
#include "arrow/types.h"

#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/status.h"

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

  void Init(const TypePtr& type, size_t length, Buffer* nulls);

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

  DISALLOW_COPY_AND_ASSIGN(Array);
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
      Buffer* nulls = nullptr);

  Buffer* data() const { return data_;}

  bool Equals(const PrimitiveArray& other) const;

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


} // namespace arrow

#endif
