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

set(GESTALT_SRCS
)

#######################################
# Unit tests
#######################################

set(GESTALT_TEST_LINK_LIBS gestalt_test_util ${GESTALT_MIN_TEST_LIBS})

ADD_GESTALT_TEST(types-test)
ADD_GESTALT_TEST(memory-test)
ADD_GESTALT_TEST(array-test)
ADD_GESTALT_TEST(builder-test)
ADD_GESTALT_TEST(util-test)
