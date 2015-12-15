# Copyright 2014 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#######################################
# arrow_util
#######################################

set(UTIL_SRCS
)


# boost_system
# boost_thread

set(UTIL_LIBS
  glog
  rt)

# We use MallocExtension and HeapChecker, but not in the exported version of
# the library.
set(EXPORTED_UTIL_LIBS ${UTIL_LIBS})
if(${ARROW_TCMALLOC_AVAILABLE})
  list(APPEND UTIL_LIBS tcmalloc)
endif()

ADD_EXPORTABLE_LIBRARY(arrow_util
  SRCS ${UTIL_SRCS}
  DEPS ${UTIL_LIBS}
  EXPORTED_DEPS ${EXPORTED_UTIL_LIBS})

SET_TARGET_PROPERTIES(arrow_util PROPERTIES LINKER_LANGUAGE CXX)
SET_TARGET_PROPERTIES(arrow_util_exported PROPERTIES LINKER_LANGUAGE CXX)

#######################################
# arrow_test_util
#######################################

add_library(arrow_test_util)
target_link_libraries(arrow_test_util
  gflags
  glog
  gmock
  arrow_util)

SET_TARGET_PROPERTIES(arrow_test_util PROPERTIES LINKER_LANGUAGE CXX)

#######################################
# arrow_test_main
#######################################

add_library(arrow_test_main
  test_main.cc)
target_link_libraries(arrow_test_main
  gflags
  glog
  gmock
  arrow_util
  arrow_test_util
  dl
  rt)
