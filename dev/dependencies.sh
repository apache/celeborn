#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -ex

# Explicitly set locale in order to make `sort` output consistent across machines.
# See https://stackoverflow.com/questions/28881 for more details.
export LC_ALL=C

PWD=$(cd "$(dirname "$0")"/.. || exit; pwd)

MVN="${PWD}/build/mvn"
SBT="${PWD}/build/sbt"

SBT_ENABLED="false"
REPLACE="false"
CHECK="false"

DEP_PR="${PWD}/dev/deps/dependencyList.tmp"
DEP="${PWD}/dev/deps/dependencyList"

function mvn_build_classpath() {
  $MVN clean package -DskipTests
  $MVN dependency:build-classpath | \
    grep -v "INFO\|WARN" | \
    # This will skip the first two lines 
    tail -n +3 | \
    tr ":" "\n" | \
    awk -F '/' '{
      artifact_id=$(NF-2);
      version=$(NF-1);
      jar_name=$NF;
      classifier_start_index=length(artifact_id"-"version"-") + 1;
      classifier_end_index=index(jar_name, ".jar") - 1;
      classifier=substr(jar_name, classifier_start_index, classifier_end_index - classifier_start_index + 1);
      print artifact_id"/"version"/"classifier"/"jar_name
    }' | grep -v "celeborn" | sort -u >> "${DEP_PR}"
}

function sbt_build_classpath() {
  $SBT "clean; export externalDependencyClasspath" | \
    grep -v "\[info\]\|\[success\]" | \
    grep -v "Compile / externalDependencyClasspath" | \
    # This will skip the first two lines
    tail -n +3 | \
    # This will skip the last line 
    sed '$d' | \
    tr ":" "\n" | \
    awk -F '/' '{
      artifact_id=$(NF-2);
      version=$(NF-1);
      jar_name=$NF;
      classifier_start_index=length(artifact_id"-"version"-") + 1;
      classifier_end_index=index(jar_name, ".jar") - 1;
      classifier=substr(jar_name, classifier_start_index, classifier_end_index - classifier_start_index + 1);
      print artifact_id"/"version"/"classifier"/"jar_name
    }' | sort -u >> "${DEP_PR}"
}

function check_diff() {
    set +e
    the_diff=$(diff "${DEP}" "${DEP_PR}")
    set -e
    rm -rf "${DEP_PR}"
    if [[ -n "${the_diff}" ]]; then
        echo "Dependency List Changed Detected: "
        echo "${the_diff}"
        echo "To update the dependency file, run './dev/dependencies.sh --replace'."
        exit 1
    fi
}

function exit_with_usage() {
  echo "Usage: $0 [--sbt | --mvn] [--replace] [--check] [--help]"
  exit 1
}

rm -rf "${DEP_PR}"
cat >"${DEP_PR}"<<EOF
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

EOF

# Parse arguments
while (( "$#" )); do
  case $1 in
    --sbt)
      SBT_ENABLED="true"
      ;;
    --mvn)
      SBT_ENABLED="false"
      ;;
    --replace)
      REPLACE="true"
      ;;
    --check)
      CHECK="true"
      ;;
    --help)
      exit_with_usage
      ;;
    --*)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
    *)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
  esac
  shift
done

if [ "$SBT_ENABLED" == "true" ]; then
  sbt_build_classpath
else
  mvn_build_classpath
fi

if [ "$REPLACE" == "true" ]; then
  rm -rf "${DEP}"
  mv "${DEP_PR}" "${DEP}"
  exit 0
fi

if [ "$CHECK" == "true" ]; then
  check_diff
fi
