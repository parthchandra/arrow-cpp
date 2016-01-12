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

#include "arrow/memory.h"

namespace arrow {

size_t Buffer::id_gen_ = 0;

Status Buffer::Resize(size_t new_size) {
  if (pool_ == nullptr) {
    return Status::Invalid("no memory allocator");
  }
  return static_cast<MemoryPool*>(pool_)->Resize(this, new_size);
}

} // namespace arrow
