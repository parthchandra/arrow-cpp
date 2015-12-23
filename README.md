# C++11 prototype for in-memory columnar


## Setup Build Environment

Arrow uses CMake as a build configuration system. Currently, it supports in-source and
out-of-source builds with the latter one being preferred. For third-party dependencies,
Arrow relies on the native-toolchain to provide suitable binaries.

Simple debug build:

    mkdir debug
    cd debug
    cmake ..
    make
    ctest

Simple release build:

    mkdir release
    cd release
    cmake .. -DCMAKE_BUILD_TYPE=Release
    make
    ctest

To clean up a build simply remove the build directory (`rm -Rf debug`). Please be aware
that for in-source builds, correctly cleaning up cached CMake state is not as easy
possible and thus out-of-source builds should be preferred.

This will download pre-built artifacts for the thirdparty dependencies and the compiler,
configure, build and test arrow. In case you want to have more control over the build and
toolchain environment, you can follow these steps.

    git clone https://github.com/cloudera/native-toolchain
    cd native-toolchain
    ./buildall.sh  # This will take about 2h
    cd ../arrow
    export NATIVE_TOOLCHAIN=`pwd`/../native-toolchain/build
    mkdir debug
    cd debug
    cmake ..
    make
    ctest
