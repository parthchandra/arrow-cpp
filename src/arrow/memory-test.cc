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

#include <cstdlib>
#include <memory>
#include <string>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "arrow/memory.h"
#include "arrow/test-util.h"

using std::string;

namespace arrow {

class TestBuffer : public TestBase {

};

TEST(UnitTestMemoryPool, ExceedMaximumBytes) {
  std::unique_ptr<MemoryPool> pool;
  pool.reset(new MemoryPool(100));

  Buffer* buf = nullptr;
  Buffer* tmp = nullptr;
  ASSERT_OK(pool->NewBuffer(100, &buf));

  ASSERT_RAISES(OutOfMemory, pool->NewBuffer(1, &tmp));
  GC(buf);

  ASSERT_RAISES(OutOfMemory, pool->NewBuffer(101, &buf));
}


TEST_F(TestBuffer, FailedMalloc) {
  Buffer* buf = nullptr;
  uint64_t to_alloc = UINT64_MAX >> 2;

  if (sizeof(size_t) == 8) {
    ASSERT_RAISES(OutOfMemory, pool_->NewBuffer(to_alloc, &buf));
  } else {
    // TODO: is size_t a uint32_t on any platforms?
  }
}

TEST_F(TestBuffer, AllocateIncrefDecref) {
  Buffer* buf = nullptr;
  size_t size = 1000;

  ASSERT_OK(pool_->NewBuffer(size, &buf));

  ASSERT_EQ(buf->size(), size);
  ASSERT_EQ(buf->ref_count(), 1);
  ASSERT_EQ(pool_->total_bytes(), size);
  ASSERT_EQ(pool_->nbuffers(), 1);

  buf->Incref();
  ASSERT_EQ(buf->ref_count(), 2);
  buf->Decref();
  buf->Decref();
  ASSERT_EQ(pool_->total_bytes(), 0);
  ASSERT_EQ(pool_->nbuffers(), 0);
}

TEST_F(TestBuffer, DecrefFailScenarios) {
  Buffer* buf = nullptr;
  size_t size = 1000;

  ASSERT_OK(pool_->NewBuffer(size, &buf));

  // degenerate case; if a buffer has a reference to the wrong memory pool
  // TODO

  GC(buf);
}

TEST_F(TestBuffer, TestGetBuffer) {
  Buffer* buf1 = nullptr;
  Buffer* buf2 = nullptr;
  Buffer* tmp = nullptr;
  size_t size = 1000;

  ASSERT_OK(pool_->NewBuffer(size, &buf1));
  ASSERT_OK(pool_->NewBuffer(size, &buf2));

  ASSERT_OK(pool_->GetBuffer(buf1->id(), &tmp));
  ASSERT_EQ(tmp->id(), buf1->id());

  // Reference count unchanged
  ASSERT_EQ(tmp->ref_count(), 1);

  ASSERT_RAISES(KeyError, pool_->GetBuffer(buf2->id() + 1, &tmp));

  // Prior pointer is still valid
  ASSERT_EQ(tmp->id(), buf1->id());

  GC(buf1);
  GC(buf2);
}


TEST_F(TestBuffer, NoOwnData) {
  size_t size = 100;
  uint8_t* data = reinterpret_cast<uint8_t*>(malloc(size));

  Buffer* buf = new Buffer(data, size, false);

  EXPECT_FALSE(buf->own_data());
  EXPECT_EQ(buf->size(), 100);
  buf->Decref();

  // TODO: is the better way to verify this data was not accidentally freed?
  free(data);
}


TEST_F(TestBuffer, Slice) {

}


TEST_F(TestBuffer, Copy) {

}


TEST_F(TestBuffer, Resize) {
  Buffer* buf = nullptr;

  ASSERT_OK(pool_->NewBuffer(100, &buf));
  ASSERT_EQ(pool_->total_bytes(), 100);

  ASSERT_OK(buf->Resize(200));
  ASSERT_EQ(buf->size(), 200);
  ASSERT_EQ(pool_->total_bytes(), 200);

  // Make it smaller, too
  ASSERT_OK(buf->Resize(50));
  ASSERT_EQ(buf->size(), 50);
  ASSERT_EQ(pool_->total_bytes(), 50);

  // Can't resize if ref_count > 1
  buf->Incref();
  ASSERT_RAISES(Invalid, buf->Resize(100));
  buf->Decref();

  GC(buf);
  ASSERT_EQ(pool_->total_bytes(), 0);

  // Can't resize if don't own the data
  Buffer* tmp = new Buffer(nullptr, 0, false);
  ASSERT_RAISES(Invalid, tmp->Resize(10));
  GC(tmp);
}

TEST_F(TestBuffer, ResizeExceedLimit) {
  std::unique_ptr<MemoryPool> pool;
  pool.reset(new MemoryPool(100));

  Buffer* buf = nullptr;
  ASSERT_OK(pool->NewBuffer(50, &buf));
  ASSERT_RAISES(OutOfMemory, buf->Resize(150));
}

TEST_F(TestBuffer, ResizeOOM) {
  // realloc fails, even though there may be no explicit limit
  Buffer* buf = nullptr;
  uint64_t to_alloc = UINT64_MAX >> 2;

  ASSERT_OK(pool_->NewBuffer(50, &buf));

  if (sizeof(size_t) == 8) {
    ASSERT_RAISES(OutOfMemory, buf->Resize(to_alloc));
  } else {
    // TODO: is size_t a uint32_t on any platforms?
  }
}

} // namespace arrow
