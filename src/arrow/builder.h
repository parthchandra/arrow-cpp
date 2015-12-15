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

#ifndef ARROW_BUILDER_H
#define ARROW_BUILDER_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/types.h"
#include "arrow/memory.h"
#include "arrow/util.h"

namespace arrow {

static constexpr size_t MIN_BUILDER_CAPACITY = 1 << 8;

// Base class for all data array builders
class ArrayBuilder {
 public:
  ArrayBuilder(MemoryPool* pool, const TypePtr& type)
      : pool_(pool),
        type_(type),
        nullable_(type_->nullable),
        nulls_(nullptr), null_bits_(nullptr),
        length_(0),
        capacity_(0) {}

  virtual ~ArrayBuilder() {
    if (nulls_ != nullptr) {
      nulls_->Decref();
    }
  }

  // Non-copyable
  ArrayBuilder(const ArrayBuilder& buf) = delete;
  ArrayBuilder& operator=(ArrayBuilder& other) = delete;

  // For nested types. Since the objects are owned by this class instance, we
  // skip shared pointers and just return a raw pointer
  ArrayBuilder* child(size_t i) {
    return children_[i].get();
  }

  size_t num_children() const {
    return children_.size();
  }

  size_t length() const { return length_;}
  size_t capacity() const { return capacity_;}
  bool nullable() const { return nullable_;}

  // Allocates requires memory at this level, but children need to be
  // initialized independently
  Status Init(size_t capacity) {
    capacity_ = capacity;

    if (nullable_) {
      size_t to_alloc = util::ceil_byte(capacity) / 8;
      RETURN_NOT_OK(pool_->NewBuffer(to_alloc, &nulls_));
      null_bits_ = nulls_->data();
      memset(null_bits_, 0, to_alloc);
    }
    return Status::OK();
  }

  Status Resize(size_t new_bits) {
    if (nullable_) {
      size_t new_bytes = util::ceil_byte(new_bits) / 8;
      size_t old_bytes = nulls_->size();
      RETURN_NOT_OK(nulls_->Resize(new_bytes));
      null_bits_ = nulls_->data();
      if (old_bytes < new_bytes) {
        memset(null_bits_ + old_bytes, 0, new_bytes - old_bytes);
      }
    }
    return Status::OK();
  }

  // For cases where raw data was memcpy'd into the internal buffers, allows us
  // to advance the length of the builder. It is your responsibility to use
  // this function responsibly.
  Status Advance(size_t elements) {
    if (nullable_ && length_ + elements > capacity_) {
      return Status::Invalid("Builder must be expanded");
    }
    length_ += elements;
    return Status::OK();
  }

  Buffer* nulls() const { return nulls_;}

  // Creates new array object to hold the contents of the builder and transfers
  // ownership of the data
  virtual Status ToArray(Array** out) = 0;

 protected:
  MemoryPool* pool_;
  TypePtr type_;
  bool nullable_;

  // If the type is not nullable, then null_ is nullptr after initialization
  Buffer* nulls_;
  uint8_t* null_bits_;

  // Array length, so far. Also, the index of the next element to be added
  size_t length_;
  size_t capacity_;

  // Child value array builders. These are owned by this class
  std::vector<std::unique_ptr<ArrayBuilder> > children_;
};


template <typename Type, typename ArrayType>
class PrimitiveBuilder : public ArrayBuilder {
 public:
  typedef typename Type::c_type T;

  PrimitiveBuilder(MemoryPool* pool, const TypePtr& type)
      : ArrayBuilder(pool, type), values_(nullptr) {
    elsize_ = sizeof(T);
  }

  virtual ~PrimitiveBuilder() {
    if (values_ != nullptr) {
      values_->Decref();
    }
  }

  Status Resize(size_t capacity) {
    // XXX: Set floor size for now
    if (capacity < MIN_BUILDER_CAPACITY) {
      capacity = MIN_BUILDER_CAPACITY;
    }

    if (capacity_ == 0) {
      RETURN_NOT_OK(Init(capacity));
    } else {
      RETURN_NOT_OK(ArrayBuilder::Resize(capacity));
      RETURN_NOT_OK(values_->Resize(capacity * elsize_));
      capacity_ = capacity;
    }
    return Status::OK();
  }

  Status Init(size_t capacity) {
    RETURN_NOT_OK(ArrayBuilder::Init(capacity));
    return pool_->NewBuffer(capacity * elsize_, &values_);
  }

  Status Reserve(size_t elements) {
    if (length_ + elements > capacity_) {
      size_t new_capacity = util::next_power2(length_ + elements);
      return Resize(new_capacity);
    }
    return Status::OK();
  }

  Status Advance(size_t elements) {
    return ArrayBuilder::Advance(elements);
  }

  // Scalar append
  Status Append(T val, bool is_null = false) {
    if (length_ == capacity_) {
      // If the capacity was not already a multiple of 2, do so here
      RETURN_NOT_OK(Resize(util::next_power2(capacity_ + 1)));
    }
    if (nullable_) {
      util::set_bit(null_bits_, length_, is_null);
    }
    raw_buffer()[length_++] = val;
    return Status::OK();
  }

  // Vector append
  //
  // If passed, null_bytes is of equal length to values, and any nonzero byte
  // will be considered as a null for that slot
  Status Append(const T* values, size_t length, uint8_t* null_bytes = nullptr) {
    if (length_ + length > capacity_) {
      size_t new_capacity = util::next_power2(length_ + length);
      RETURN_NOT_OK(Resize(new_capacity));
    }
    memcpy(raw_buffer() + length_, values, length * elsize_);

    if (nullable_ && null_bytes != nullptr) {
      // If null_bytes is all not null, then none of the values are null
      for (size_t i = 0; i < length; ++i) {
        util::set_bit(null_bits_, length_ + i, static_cast<bool>(null_bytes[i]));
      }
    }

    length_ += length;
    return Status::OK();
  }

  Status AppendNull() {
    if (!nullable_) {
      return Status::Invalid("not nullable");
    }
    if (length_ == capacity_) {
      // If the capacity was not already a multiple of 2, do so here
      RETURN_NOT_OK(Resize(util::next_power2(capacity_ + 1)));
    }
    util::set_bit(null_bits_, length_++, true);
    return Status::OK();
  }

  // Initialize an array type instance with the results of this builder
  // Transfers ownership of all buffers
  Status Transfer(PrimitiveArray* out) {
    out->Init(type_, length_, values_, nulls_);
    values_ = nulls_ = nullptr;
    capacity_ = length_ = 0;
    return Status::OK();
  }

  Status Transfer(ArrayType* out) {
    return Transfer(static_cast<PrimitiveArray*>(out));
  }

  virtual Status ToArray(Array** out) {
    ArrayType* result = new ArrayType();
    RETURN_NOT_OK(Transfer(result));
    *out = static_cast<Array*>(result);
    return Status::OK();
  }

  T* raw_buffer() {
    return reinterpret_cast<T*>(values_->data());
  }

  Buffer* buffer() {
    return values_;
  }

 protected:
  Buffer* values_;
  size_t elsize_;
};

typedef PrimitiveBuilder<UInt8Type, UInt8Array> UInt8Builder;
typedef PrimitiveBuilder<UInt16Type, UInt16Array> UInt16Builder;
typedef PrimitiveBuilder<UInt32Type, UInt32Array> UInt32Builder;
typedef PrimitiveBuilder<UInt64Type, UInt64Array> UInt64Builder;

typedef PrimitiveBuilder<Int8Type, Int8Array> Int8Builder;
typedef PrimitiveBuilder<Int16Type, Int16Array> Int16Builder;
typedef PrimitiveBuilder<Int32Type, Int32Array> Int32Builder;
typedef PrimitiveBuilder<Int64Type, Int64Array> Int64Builder;

typedef PrimitiveBuilder<FloatType, FloatArray> FloatBuilder;
typedef PrimitiveBuilder<DoubleType, DoubleArray> DoubleBuilder;
// typedef PrimitiveBuilder<BooleanType, BooleanArray> BooleanBuilder;


// Builder class for variable-length list array value types
//
// To use this class, you must append values to the child array builder and use
// the Append function to delimit each distinct list value (once the values
// have been appended to the child array)
class ListBuilder : public Int32Builder {
 public:

  ListBuilder(MemoryPool* pool, const TypePtr& type, ArrayBuilder* value_builder)
      : Int32Builder(pool, type) {
    value_builder_.reset(value_builder);
  }

  Status Init(size_t elements) {
    // One more than requested.
    //
    // XXX: This is slightly imprecise, because we might trigger null mask
    // resizes that are unnecessary when creating arrays with power-of-two size
    return Int32Builder::Init(elements + 1);
  }

  Status Resize(size_t capacity) {
    // Need space for the end offset
    RETURN_NOT_OK(Int32Builder::Resize(capacity + 1));

    // Slight hack, as the "real" capacity is one less
    --capacity_;
    return Status::OK();
  }

  // Vector append
  //
  // If passed, null_bytes is of equal length to values, and any nonzero byte
  // will be considered as a null for that slot
  Status Append(T* values, size_t length, uint8_t* null_bytes = nullptr) {
    if (length_ + length > capacity_) {
      size_t new_capacity = util::next_power2(length_ + length);
      RETURN_NOT_OK(Resize(new_capacity));
    }
    memcpy(raw_buffer() + length_, values, length * elsize_);

    if (nullable_ && null_bytes != nullptr) {
      // If null_bytes is all not null, then none of the values are null
      for (size_t i = 0; i < length; ++i) {
        util::set_bit(null_bits_, length_ + i, static_cast<bool>(null_bytes[i]));
      }
    }

    length_ += length;
    return Status::OK();
  }

  // Initialize an array type instance with the results of this builder
  // Transfers ownership of all buffers
  template <typename Container>
  Status Transfer(Container* out) {
    Array* child_values;
    RETURN_NOT_OK(value_builder_->ToArray(&child_values));

    // Add final offset if the length is non-zero
    if (length_) {
      raw_buffer()[length_] = child_values->length();
    }

    out->Init(type_, length_, values_, ArrayPtr(child_values), nulls_);
    values_ = nulls_ = nullptr;
    capacity_ = length_ = 0;
    return Status::OK();
  }

  virtual Status ToArray(Array** out) {
    ListArray* result = new ListArray();
    RETURN_NOT_OK(Transfer(result));
    *out = static_cast<Array*>(result);
    return Status::OK();
  }

  // Start a new variable-length list slot
  //
  // This function should be called before beginning to append elements to the
  // value builder
  Status Append(bool is_null = false) {
    if (length_ == capacity_) {
      // If the capacity was not already a multiple of 2, do so here
      RETURN_NOT_OK(Resize(util::next_power2(capacity_ + 1)));
    }
    if (nullable_) {
      util::set_bit(null_bits_, length_, is_null);
    }

    raw_buffer()[length_++] = value_builder_->length();
    return Status::OK();
  }

  // Status Append(int32_t* offsets, size_t length, uint8_t* null_bytes) {
  //   return Int32Builder::Append(offsets, length, null_bytes);
  // }

  Status AppendNull() {
    return Append(true);
  }

  ArrayBuilder* value_builder() const { return value_builder_.get();}

 protected:
  std::unique_ptr<ArrayBuilder> value_builder_;
};


class StringBuilder : public ListBuilder {
 public:

  StringBuilder(MemoryPool* pool, const TypePtr& type)
      : ListBuilder(pool, type,
          static_cast<ArrayBuilder*>(new UInt8Builder(pool, value_type_))) {
    byte_builder_ = static_cast<UInt8Builder*>(value_builder_.get());
  }

  Status Append(const std::string& value) {
    RETURN_NOT_OK(ListBuilder::Append());
    return byte_builder_->Append(reinterpret_cast<const uint8_t*>(value.c_str()),
        value.size());
  }

  Status Append(const uint8_t* value, size_t length);
  Status Append(const std::vector<std::string>& values,
                uint8_t* null_bytes);

  virtual Status ToArray(Array** out) {
    StringArray* result = new StringArray();
    RETURN_NOT_OK(ListBuilder::Transfer(result));
    *out = static_cast<Array*>(result);
    return Status::OK();
  }

 protected:
  UInt8Builder* byte_builder_;

  static TypePtr value_type_;
};
TypePtr StringBuilder::value_type_ = TypePtr(new UInt8Type(false));


// class BinaryBuilder : protected ListBuilder {

// };


// class DenseUnionBuilder : protected ArrayBuilder {

// };



#define BUILDER_CASE(ENUM, BuilderType)                               \
    case TypeEnum::ENUM:                                                \
      *out = static_cast<ArrayBuilder*>(new BuilderType(pool, type));   \
      return Status::OK();


Status make_builder(MemoryPool* pool, const TypePtr& type, ArrayBuilder** out) {
  switch (type->type) {
    BUILDER_CASE(UINT8, UInt8Builder);
    BUILDER_CASE(INT8, Int8Builder);
    BUILDER_CASE(UINT16, UInt16Builder);
    BUILDER_CASE(INT16, Int16Builder);
    BUILDER_CASE(UINT32, UInt32Builder);
    BUILDER_CASE(INT32, Int32Builder);
    BUILDER_CASE(UINT64, UInt64Builder);
    BUILDER_CASE(INT64, Int64Builder);

    // BUILDER_CASE(BOOL, BooleanBuilder);

    BUILDER_CASE(FLOAT, FloatBuilder);
    BUILDER_CASE(DOUBLE, DoubleBuilder);

    BUILDER_CASE(STRING, StringBuilder);

    case TypeEnum::LIST:
      {
        ListType* list_type = static_cast<ListType*>(type.get());
        ArrayBuilder* value_builder;
        RETURN_NOT_OK(make_builder(pool, list_type->value_type, &value_builder));

        // The ListBuilder takes ownership of the value_builder
        ListBuilder* builder = new ListBuilder(pool, type, value_builder);
        *out = static_cast<ArrayBuilder*>(builder);
        return Status::OK();
      }
    // BUILDER_CASE(CHAR, CharBuilder);

    // BUILDER_CASE(VARCHAR, VarcharBuilder);
    // BUILDER_CASE(BINARY, BinaryBuilder);

    // BUILDER_CASE(DATE, DateBuilder);
    // BUILDER_CASE(TIMESTAMP, TimestampBuilder);
    // BUILDER_CASE(TIME, TimeBuilder);

    // BUILDER_CASE(LIST, ListBuilder);
    // BUILDER_CASE(STRUCT, StructBuilder);
    // BUILDER_CASE(DENSE_UNION, DenseUnionBuilder);
    // BUILDER_CASE(SPARSE_UNION, SparseUnionBuilder);

    default:
      return Status::NotImplemented(type->ToString());
  }
}

} // namespace arrow

#endif // ARROW_BUILDER_H_
