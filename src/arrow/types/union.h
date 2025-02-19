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

#ifndef ARROW_TYPES_UNION_H
#define ARROW_TYPES_UNION_H

#include "arrow/array.h"
#include "arrow/types.h"

#include <memory>
#include <string>
#include <vector>

namespace arrow {

struct DenseUnionType : public CollectionType<TypeEnum::DENSE_UNION> {

  typedef CollectionType<TypeEnum::DENSE_UNION> Base;

  DenseUnionType(const std::vector<TypePtr>& child_types,
      bool nullable = true)
      : Base(nullable) {
    child_types_ = child_types;
  }

  virtual std::string ToString() const;
};


struct SparseUnionType : public CollectionType<TypeEnum::SPARSE_UNION> {

  typedef CollectionType<TypeEnum::SPARSE_UNION> Base;

  SparseUnionType(const std::vector<TypePtr>& child_types,
      bool nullable = true)
      : Base(nullable) {
    child_types_ = child_types;
  }

  virtual std::string ToString() const;
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

#endif // ARROW_TYPES_UNION_H
