# Copyright 2016 Cloudera, Inc.
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
  bit-util.cc
  status.cc
)

set(UTIL_LIBS
  rt)

add_library(arrow_util STATIC
  ${UTIL_SRCS}
)
target_link_libraries(arrow_util ${UTIL_LIBS})
SET_TARGET_PROPERTIES(arrow_util PROPERTIES LINKER_LANGUAGE CXX)

# Headers: top level
install(FILES
  bit-util.h
  macros.h
  status.h
  DESTINATION include/arrow/util)

#######################################
# arrow_test_util
#######################################

add_library(arrow_test_util)
target_link_libraries(arrow_test_util
  gmock
  arrow_util)

SET_TARGET_PROPERTIES(arrow_test_util PROPERTIES LINKER_LANGUAGE CXX)

#######################################
# arrow_test_main
#######################################

add_library(arrow_test_main
  test_main.cc)
target_link_libraries(arrow_test_main
  gmock
  arrow_util
  arrow_test_util
  dl
  rt)

ADD_ARROW_TEST(bit-util-test)
