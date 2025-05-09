#!/bin/bash
# Based on setup-ubuntu.sh from Facebook Velox
#
# Copyright (c) Facebook, Inc. and its affiliates.
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

# This script documents setting up a Ubuntu host for CelebornCpp
# development.  Running it should make you ready to compile.
#
# Environment variables:
# * INSTALL_PREREQUISITES="N": Skip installation of packages for build.
# * PROMPT_ALWAYS_RESPOND="n": Automatically respond to interactive prompts.
#     Use "n" to never wipe directories.
#
# You can also run individual functions below by specifying them as arguments:
# $ scripts/setup-ubuntu.sh install_googletest install_fmt
#

# Minimal setup for Ubuntu 22.04.
set -eufx -o pipefail
SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
source $SCRIPTDIR/setup-helper-functions.sh

# Folly must be built with the same compiler flags so that some low level types
# are the same size.
COMPILER_FLAGS=$(get_cxx_flags)
export COMPILER_FLAGS
NPROC=$(getconf _NPROCESSORS_ONLN)
BUILD_DUCKDB="${BUILD_DUCKDB:-true}"
export CMAKE_BUILD_TYPE=Release
SUDO="${SUDO:-"sudo --preserve-env"}"
USE_CLANG="${USE_CLANG:-false}"
export INSTALL_PREFIX=${INSTALL_PREFIX:-"/usr/local"}
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)/deps-download}

DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)/deps-download}
OS_CXXFLAGS=""

function install_clang15 {
  VERSION=`cat /etc/os-release | grep VERSION_ID`
  if [[ ! ${VERSION} =~ "22.04" && ! ${VERSION} =~ "24.04" ]]; then
    echo "Warning: using the Clang configuration is for Ubuntu 22.04 and 24.04. Errors might occur."
  fi
  CLANG_PACKAGE_LIST=clang-15
  if [[ ${VERSION} =~ "22.04" ]]; then
    CLANG_PACKAGE_LIST=${CLANG_PACKAGE_LIST} gcc-12 g++-12 libc++-12-dev
  fi
  ${SUDO} apt install ${CLANG_PACKAGE_LIST} -y
}

FB_OS_VERSION="v2024.07.01.00"
FMT_VERSION="10.1.1"
BOOST_VERSION="boost-1.84.0"
ARROW_VERSION="15.0.0"
STEMMER_VERSION="2.2.0"

# Install gtest library package for tests.
function install_gtest {
  ${SUDO} apt-get install -y libgtest-dev cmake
  mkdir -p $HOME/build
  pushd $HOME/build
  ${SUDO} cmake /usr/src/googletest/googletest
  ${SUDO} make
  ${SUDO} cp lib/libgtest* /usr/lib/
  popd
  ${SUDO} rm -rf $HOME/build
  ${SUDO} mkdir /usr/local/lib/googletest
  ${SUDO} ln -s /usr/lib/libgtest.a /usr/local/lib/googletest/libgtest.a
  ${SUDO} ln -s /usr/lib/libgtest_main.a /usr/local/lib/googletest/libgtest_main.a
}

# Install packages required for build.
function install_build_prerequisites {
  ${SUDO} apt update
  # The is an issue on 22.04 where a version conflict prevents glog install,
  # installing libunwind first fixes this.
  ${SUDO} apt install -y libunwind-dev
  ${SUDO} apt install -y \
    build-essential \
    python3-pip \
    ccache \
    curl \
    ninja-build \
    checkinstall \
    git \
    pkg-config \
    gdb \
    wget
    
  install_gtest

  # Install to /usr/local to make it available to all users.
  ${SUDO} pip3 install cmake==3.28.3

  if [[ ${USE_CLANG} != "false" ]]; then
    install_clang15
  fi

  # Install clang-format-15 for lint checking.
  ${SUDO} apt install -y clang-format-15
}

# Install packages required for build.
function install_celeborn_cpp_deps_from_apt {
  ${SUDO} apt update
  ${SUDO} apt install -y \
    libc-ares-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    libicu-dev \
    libdouble-conversion-dev \
    libgoogle-glog-dev \
    libbz2-dev \
    libgflags-dev \
    libgmock-dev \
    libevent-dev \
    libsodium-dev \
    libzstd-dev \
    libre2-dev
}

function install_fmt {
  wget_and_untar https://github.com/fmtlib/fmt/archive/${FMT_VERSION}.tar.gz fmt
  cmake_install_dir fmt -DFMT_TEST=OFF
}

function install_boost {
  wget_and_untar https://github.com/boostorg/boost/releases/download/${BOOST_VERSION}/${BOOST_VERSION}.tar.gz boost
  (
    cd ${DEPENDENCY_DIR}/boost
    if [[ ${USE_CLANG} != "false" ]]; then
      ./bootstrap.sh --prefix=${INSTALL_PREFIX} --with-toolset="clang-15"
      # Switch the compiler from the clang-15 toolset which doesn't exist (clang-15.jam) to
      # clang of version 15 when toolset clang-15 is used.
      # This reconciles the project-config.jam generation with what the b2 build system allows for customization.
      sed -i 's/using clang-15/using clang : 15/g' project-config.jam
      ${SUDO} ./b2 "-j$(nproc)" -d0 install threading=multi toolset=clang-15 --without-python
    else
      ./bootstrap.sh --prefix=${INSTALL_PREFIX}
      ${SUDO} ./b2 "-j$(nproc)" -d0 install threading=multi --without-python
    fi
  )
}

function install_protobuf {
  ### protobuf version must be aligned to CelebornJava.
  wget_and_untar https://github.com/protocolbuffers/protobuf/releases/download/v21.7/protobuf-all-21.7.tar.gz protobuf
  (
    cd ${DEPENDENCY_DIR}/protobuf
    ./configure CXXFLAGS="-fPIC" --prefix=${INSTALL_PREFIX}
    make "-j${NPROC}"
    ${SUDO} make install
    ${SUDO} ldconfig
  )
}

function install_folly {
  wget_and_untar https://github.com/facebook/folly/archive/refs/tags/${FB_OS_VERSION}.tar.gz folly
  cmake_install_dir folly -DBUILD_TESTS=OFF -DFOLLY_HAVE_INT128_T=ON
}

function install_fizz {
  wget_and_untar https://github.com/facebookincubator/fizz/archive/refs/tags/${FB_OS_VERSION}.tar.gz fizz
  cmake_install_dir fizz/fizz -DBUILD_TESTS=OFF
}

function install_wangle {
  wget_and_untar https://github.com/facebook/wangle/archive/refs/tags/${FB_OS_VERSION}.tar.gz wangle
  cmake_install_dir wangle/wangle -DBUILD_TESTS=OFF
}

function install_celeborn_cpp_deps {
  run_and_time install_celeborn_cpp_deps_from_apt
  run_and_time install_fmt
  run_and_time install_protobuf
  run_and_time install_boost
  run_and_time install_folly
  run_and_time install_fizz
  run_and_time install_wangle
}

function install_apt_deps {
  install_build_prerequisites
  install_celeborn_cpp_deps_from_apt
}

(return 2> /dev/null) && return # If script was sourced, don't run commands.

(
  if [[ ${USE_CLANG} != "false" ]]; then
    export CC=/usr/bin/clang-15
    export CXX=/usr/bin/clang++-15
  fi
  if [[ $# -ne 0 ]]; then
    for cmd in "$@"; do
      run_and_time "${cmd}"
    done
    echo "All specified dependencies installed!"
  else
    if [ "${INSTALL_PREREQUISITES:-Y}" == "Y" ]; then
      echo "Installing build dependencies"
      run_and_time install_build_prerequisites
    else
      echo "Skipping installation of build dependencies since INSTALL_PREREQUISITES is not set"
    fi
    install_celeborn_cpp_deps
    echo "All dependencies for CelebornCpp installed!"
    if [[ ${USE_CLANG} != "false" ]]; then
      echo "To use clang for the CelebornCpp build set the CC and CXX environment variables in your session."
      echo "  export CC=/usr/bin/clang-15"
      echo "  export CXX=/usr/bin/clang++-15"
    fi
  fi
)
