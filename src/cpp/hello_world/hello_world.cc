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

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <string>

#include "hello_world.h"

DEFINE_bool(use_long_form, false, "");

using std::string;

namespace test {

string HelloWorld::SayHello() {

  string message;
  if (!FLAGS_use_long_form) {
    message = SayHelloShortForm();
  } else {
    message = SayHelloLongForm();
  }
  LOG(INFO) << "Hello world message: " << message;
  return message;
}

string HelloWorld::SayHelloShortForm() {
  return "Hi!";
}

string HelloWorld::SayHelloLongForm() {
  return "Hello world!";
}

} // namespace test



