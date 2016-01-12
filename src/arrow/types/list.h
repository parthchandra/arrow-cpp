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

#ifndef ARROW_TYPES_LIST_H
#define ARROW_TYPES_LIST_H

#include <string>

#include "arrow/array.h"
#include "arrow/types.h"

namespace arrow {

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

  virtual std::string ToString() const;
};


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

} // namespace arrow

#endif // ARROW_TYPES_LIST_H
