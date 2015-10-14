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
# gestalt_util
#######################################

set(UTIL_SRCS
)


set(UTIL_LIBS
  boost_system
  boost_thread
  glog
  rt)

# We use MallocExtension and HeapChecker, but not in the exported version of
# the library.
set(EXPORTED_UTIL_LIBS ${UTIL_LIBS})
if(${GSTALT_TCMALLOC_AVAILABLE})
  list(APPEND UTIL_LIBS tcmalloc)
endif()

ADD_EXPORTABLE_LIBRARY(gestalt_util
  SRCS ${UTIL_SRCS}
  DEPS ${UTIL_LIBS}
  EXPORTED_DEPS ${EXPORTED_UTIL_LIBS})

SET_TARGET_PROPERTIES(gestalt_util PROPERTIES LINKER_LANGUAGE CXX)
SET_TARGET_PROPERTIES(gestalt_util_exported PROPERTIES LINKER_LANGUAGE CXX)

#######################################
# gestalt_test_util
#######################################

add_library(gestalt_test_util)
target_link_libraries(gestalt_test_util
  gflags
  glog
  gmock
  gestalt_util)
  
SET_TARGET_PROPERTIES(gestalt_test_util PROPERTIES LINKER_LANGUAGE CXX)

#######################################
# gestalt_test_main
#######################################

add_library(gestalt_test_main
  test_main.cc)
target_link_libraries(gestalt_test_main
  gflags
  glog
  gmock
  gestalt_util
  gestalt_test_util
  dl
  rt)