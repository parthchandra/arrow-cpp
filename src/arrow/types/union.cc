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

#include "arrow/types/union.h"

#include <sstream>
#include <string>

#include "arrow/types.h"

namespace arrow {

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

std::string DenseUnionType::ToString() const {
  return format_union(child_types_);
}


std::string SparseUnionType::ToString() const {
  return format_union(child_types_);
}


} // namespace arrow
