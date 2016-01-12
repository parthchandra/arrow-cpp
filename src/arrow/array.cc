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

#include "arrow/array.h"

namespace arrow {

// ----------------------------------------------------------------------
// Base array class

void Array::Init(const TypePtr& type, size_t length, Buffer* nulls) {
  type_ = type;
  length_ = length;
  nulls_ = nulls;

  nullable_ = type->nullable;
  if (nulls != nullptr) {
    null_bits_ = nulls->data();
  }
}

// ----------------------------------------------------------------------
// Primitive array base

void PrimitiveArray::Init(const TypePtr& type, size_t length, Buffer* data,
    Buffer* nulls) {
  Array::Init(type, length, nulls);
  data_ = data;
  raw_data_ = data == nullptr? nullptr : data_->data();
}

bool PrimitiveArray::Equals(const PrimitiveArray& other) const {
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

} // namespace arrow
