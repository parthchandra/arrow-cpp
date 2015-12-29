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

// Reference-counted buffer classes plus memory allocation and accounting

#ifndef ARROW_MEMORY_H
#define ARROW_MEMORY_H

#include <cstdint>
#include <cstdlib>
#include <unordered_map>

#include "arrow/status.h"

namespace arrow {

class Buffer {
 public:
  Buffer(uint8_t* data, size_t size, bool own_data = true, size_t offset = 0,
      void* pool = nullptr, Buffer* parent = nullptr)
      : data_(data),
        size_(size),
        offset_(offset),
        own_data_(own_data),
        ref_count_(1),
        id_(id_gen_++),
        parent_(parent),
        pool_(pool) {}

  Buffer(const Buffer& buf) = delete;
  Buffer& operator=(Buffer& other) = delete;

  ~Buffer() {
    if (ref_count_ > 0) {
      // TODO: log a memory leak / bug
    }
  }

  void Incref() {
    ++ref_count_;
  }

  // Defined inline below because of circular object reference
  void Decref();

  // Return true if both buffers are the same size and contain the same bytes
  // up to the number of compared bytes
  bool Equals(const Buffer& other, size_t nbytes) const {
    return this == &other ||
      (size_ >= nbytes && other.size_ >= nbytes &&
          !memcmp(data_, other.data_, nbytes));
  }

  bool Equals(const Buffer& other) const {
    return this == &other ||
      (size_ == other.size_ && !memcmp(data_, other.data_, size_));
  }

  // Resize the buffer if we have access to a memory allocator and we do not
  // risk breaking any child references (i.e. ref_count is 1)
  //
  // Returns error status if ref_count > 1, or realloc fails, or memory limit
  // (if any) is exceeded.
  Status Resize(size_t new_size);

  void Slice(size_t offset, Buffer* out);

  void SetBuffer(uint8_t* data, size_t size, size_t offset = 0) {
    data_ = data;
    size_ = size;
    offset_ = offset;
  }

  uint8_t* data() const { return data_;}
  size_t size() const { return size_;}
  size_t ref_count() const { return ref_count_;}
  size_t id() const { return id_;}
  bool own_data() const { return own_data_;}

 protected:
  uint8_t* data_;
  size_t size_;
  size_t offset_;
  bool own_data_;
  size_t ref_count_;

  size_t id_;
  Buffer* parent_;
  void* pool_;

  // TODO: make atomic
  static size_t id_gen_;
};

size_t Buffer::id_gen_ = 0;


class BitBuffer : protected Buffer {
 private:
  bool IsSet(size_t i) const {
    if (offset_) i += offset_;

    // TODO
    return true;
  }
};


class DataContainer {
 public:
  // The memory footprint of this array only, excluding any child arrays
  virtual size_t footprint() = 0;

  // The memory footprint of this array including any child arrays (if
  // relevant). For primitive arrays this will be the same as footprint()
  virtual size_t tree_footprint() {
    return footprint();
  }
};


// Class responsible for policing memory allocations / reallocations and
// keeping track of the total memory footprint of array data
//
// TODO: configurable garbage collection strategies
class MemoryPool {
 public:
  explicit MemoryPool(size_t maximum_bytes = static_cast<size_t>(-1))
      : total_bytes_(0),
        maximum_bytes_(maximum_bytes) {}

  // Non-copyable
  MemoryPool(const MemoryPool& other) = delete;
  MemoryPool& operator=(MemoryPool& other) = delete;

  // Create a new
  // If the indicated number of bytes would cause this memory pool
  Status NewBuffer(size_t bytes, Buffer** out, bool round_pow2 = false) {
    if (total_bytes_ + bytes > maximum_bytes_) {
      return Status::OutOfMemory("Exceeded maximum_bytes");
    }

    uint8_t* data = reinterpret_cast<uint8_t*>(malloc(bytes));
    if (data == nullptr) {
      return Status::OutOfMemory("Malloc failed");
    }
    total_bytes_ += bytes;

    // TODO: these can raise std::bad_alloc
    Buffer* buf = new Buffer(data, bytes, true, 0, this);
    buffer_map_[buf->id()] = buf;

    *out = buf;

    return Status::OK();
  }

  // Resize buffer to the indicated size, if possible.
  //
  // Returns OutOfMemory status if realloc fails or if increased buffer size
  // causes the pool's memory limit to be exceeded.
  Status Resize(Buffer* buffer, size_t new_size, bool round_pow2 = false) {
    if (buffer->ref_count() > 1) {
      return Status::Invalid("buffer ref count must be 1 to resize");
    }

    if (!buffer->own_data()) {
      return Status::Invalid("Buffer does not own its buffer");
    }

    if (new_size > buffer->size()) {
      if (total_bytes_ + (new_size - buffer->size()) > maximum_bytes_) {
        return Status::OutOfMemory("Exceeded maximum_bytes");
      }
    }

    uint8_t* data = reinterpret_cast<uint8_t*>(
        realloc(buffer->data(), new_size));
    if (data == nullptr) {
      return Status::OutOfMemory("Realloc failed");
    }

    if (new_size > buffer->size()) {
      total_bytes_ += (new_size - buffer->size());
    } else {
      // Shrank buffer
      total_bytes_ -= (buffer->size() - new_size);
    }

    buffer->SetBuffer(data, new_size);
    return Status::OK();
  }

  void Free(Buffer* buffer) {
    if (!buffer->own_data()) {
      return;
    }

    // Delete from tracking
    auto it = buffer_map_.find(buffer->id());
    if (it != buffer_map_.end()) {
      // return Status::KeyError("key error");
      // TODO
      buffer_map_.erase(it);
      free(buffer->data());
      total_bytes_ -= buffer->size();
    }
  }

  // Look up buffer in the dictionary
  // Set a "borrowed" reference; you must Incref if you intend to retain the
  // buffer
  // Returns Status::KeyError if the buffer is not found
  Status GetBuffer(size_t id, Buffer** out) {
    auto it = buffer_map_.find(id);
    if (it == buffer_map_.end()) {
      return Status::KeyError("key error");
    }

    *out = it->second;
    return Status::OK();
  }

  size_t nbuffers() const { return buffer_map_.size();}
  size_t total_bytes() const { return total_bytes_;}

 private:
  // The total number of allocated bytes accounted for by this object
  size_t total_bytes_;

  // Limit the maximum size of tracked buffers to a particular size
  size_t maximum_bytes_;

  // A catalog of every buffer owning data tracked by this instance
  std::unordered_map<size_t, Buffer*> buffer_map_;
};

inline void Buffer::Decref() {
  --ref_count_;

  if (!ref_count_) {
    // Buffer is being deleted
    if (parent_ != nullptr) {
      // Data is owned by some other buffer
      parent_->Decref();
    } else if (own_data_) {
      if (pool_ != nullptr) {
        MemoryPool* pool = static_cast<MemoryPool*>(pool_);
        pool->Free(this);
      } else if (size_) {
        free(data_);
      }
    }
    // Self-destruct
    delete this;
  }
}

Status Buffer::Resize(size_t new_size) {
  if (pool_ == nullptr) {
    return Status::Invalid("no memory allocator");
  }
  return static_cast<MemoryPool*>(pool_)->Resize(this, new_size);
}

} // namespace arrow

#endif // ARROW_MEMORY_H
