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

# Headers: top level
install(FILES
  api.h
  array.h
  builder.h
  memory.h
  types.h
  DESTINATION include/arrow)

#######################################
# Unit tests
#######################################

set(ARROW_TEST_LINK_LIBS arrow_test_util ${ARROW_MIN_TEST_LIBS})

ADD_ARROW_TEST(types-test)
ADD_ARROW_TEST(memory-test)
ADD_ARROW_TEST(array-test)
ADD_ARROW_TEST(builder-test)
