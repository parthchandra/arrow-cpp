#!/bin/bash
# Copyright 2012 Cloudera, Inc.
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

set -x
set -e
TP_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

# We use -O2 instead of -O3 for thirdparty since benchmarks indicate
# that the benefits of a smaller code size outweight the benefits of
# more inlining.
#
# We also enable -fno-omit-frame-pointer so that profiling tools which
# use frame-pointer based stack unwinding can function correctly.
DEBUG_CFLAGS="-g -fno-omit-frame-pointer"
EXTRA_CXXFLAGS="-O2 $DEBUG_CFLAGS $CXXFLAGS "
if [[ "$OSTYPE" =~ ^linux ]]; then
  OS_LINUX=1
  DYLIB_SUFFIX="so"
elif [[ "$OSTYPE" == "darwin"* ]]; then
  OS_OSX=1
  EXTRA_CXXFLAGS="$EXTRA_CXXFLAGS -stdlib=libstdc++"
  DYLIB_SUFFIX="dylib"
fi

source $TP_DIR/vars.sh

################################################################################

if [ "$#" = "0" ]; then
  F_ALL=1
else
  # Allow passing specific libs to build on the command line
  for arg in "$*"; do
    case $arg in
      "gflags")     F_GFLAGS=1 ;;
      "glog")       F_GLOG=1 ;;
      "gmock")      F_GMOCK=1 ;;
      "gperftools") F_GPERFTOOLS=1 ;;
      "libunwind")  F_LIBUNWIND=1 ;;
      *)            echo "Unknown module: $arg"; exit 1 ;;
    esac
  done
fi

################################################################################

# Determine how many parallel jobs to use for make based on the number of cores
if [ "$OS_LINUX" ]; then
  PARALLEL=$(grep -c processor /proc/cpuinfo)
elif [ "$OS_OSX" ]; then
  PARALLEL=$(sysctl -n hw.ncpu)
else
  echo Unsupported platform $OSTYPE
  exit 1
fi

mkdir -p "$PREFIX/include"
mkdir -p "$PREFIX/lib"

# On some systems, autotools installs libraries to lib64 rather than lib.  Fix
# this by setting up lib64 as a symlink to lib.  We have to do this step first
# to handle cases where one third-party library depends on another.
ln -sf lib "$PREFIX/lib64"

# use the compiled tools
export PATH=$PREFIX/bin:$PATH

# build gflags
if [ -n "$F_ALL" -o -n "$F_GFLAGS" ]; then
  cd $GFLAGS_DIR
  CXXFLAGS=$EXTRA_CXXFLAGS ./configure --with-pic --prefix=$PREFIX
  make -j$PARALLEL install
fi

# build libunwind (glog consumes it)
# It depends on link.h which is unavaible on OSX, use MacPorts' instead.
if [ "$OS_LINUX" ]; then
  if [ -n "$F_ALL" -o -n "$F_LIBUNWIND" ]; then
    cd $LIBUNWIND_DIR
    # Disable minidebuginfo, which depends on liblzma, until/unless we decide to
    # add liblzma to thirdparty.
    ./configure --disable-minidebuginfo --with-pic --prefix=$PREFIX
    make -j$PARALLEL install
  fi
fi

# build glog
if [ -n "$F_ALL" -o -n "$F_GLOG" ]; then
  cd $GLOG_DIR
  # We need to set "-g -O2" because glog only provides those flags when CXXFLAGS is unset.
  # Help glog find libunwind.
  CXXFLAGS="$EXTRA_CXXFLAGS" \
    CPPFLAGS=-I$PREFIX/include \
    LDFLAGS=-L$PREFIX/lib \
    ./configure --with-pic --prefix=$PREFIX --with-gflags=$PREFIX
  make -j$PARALLEL install
fi

# build gperftools
if [ -n "$F_ALL" -o -n "$F_GPERFTOOLS" ]; then
  cd $GPERFTOOLS_DIR
  CXXFLAGS=$EXTRA_CXXFLAGS ./configure --enable-frame-pointers --with-pic --prefix=$PREFIX
  make -j$PARALLEL install
fi

# build gmock
if [ -n "$F_ALL" -o -n "$F_GMOCK" ]; then
  cd $GMOCK_DIR
  # Run the static library build, then the shared library build.
  for SHARED in OFF ON; do
    rm -rf CMakeCache.txt CMakeFiles/
    CXXFLAGS="-fPIC -g $EXTRA_CXXFLAGS" \
    cmake -DBUILD_SHARED_LIBS=$SHARED .
    make -j$PARALLEL
  done
  echo Installing gmock...
  cp -a libgmock.${DYLIB_SUFFIX} libgmock.a $PREFIX/lib/
  rsync -av include/ $PREFIX/include/
  rsync -av gtest/include/ $PREFIX/include/
fi

echo "---------------------"
echo "Thirdparty dependencies built and installed into $PREFIX successfully"
