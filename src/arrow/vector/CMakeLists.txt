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
# arrow_vector
#######################################

set(VECTOR_SRC
  decimal-utils.cc
  vector.cc
)

set(VECTOR_LIBS
  rt)

add_library(arrow_vector STATIC
  ${VECTOR_SRCS}
)
target_link_libraries(arrow_vector ${VECTOR_LIBS})
SET_TARGET_PROPERTIES(arrow_vector PROPERTIES LINKER_LANGUAGE CXX)

# Headers: top level
install(FILES
  decimal-util.h
  vector.h
  DESTINATION include/arrow/vector)

