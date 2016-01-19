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

#ifndef ARROW_TYPES_BOOLEAN_H
#define ARROW_TYPES_BOOLEAN_H

#include <string>

#include "arrow/array.h"
#include "arrow/types.h"

namespace arrow {

struct BooleanType : public PrimitiveType<BooleanType> {
  PRIMITIVE_DECL(BooleanType, uint8_t, BOOL, 1, "bool");
};

typedef PrimitiveArrayImpl<BooleanType> BooleanArray;

} // namespace arrow

#endif // ARROW_TYPES_BOOLEAN_H