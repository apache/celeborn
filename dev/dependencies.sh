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
MODULE=""

DEP_PR=""
DEP=""

function mvn_build_classpath() {
  $MVN -P$MODULE clean install -DskipTests -am -pl $MVN_MODULES
  $MVN -P$MODULE dependency:build-classpath -am -pl $MVN_MODULES | \
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

function sbt_build_client_classpath() {
  PATTERN="$SBT_PROJECT / Runtime / externalDependencyClasspath"
  $SBT -P$MODULE "clean; export Runtime/externalDependencyClasspath" | \
    awk -v pat="$PATTERN" '$0 ~ pat { found=1 } found { print }' | \
    awk 'NR==2' | \
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

function sbt_build_server_classpath() {
  $SBT "error; clean; export externalDependencyClasspath" | \
    awk '/externalDependencyClasspath/ { found=1 } found { print }' | \
    awk 'NR % 2 == 0' | \
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
    echo "start===="
    cat ${DEP_PR}
    echo "end===="
}

function exit_with_usage() {
  echo "Usage: $0 [--sbt | --mvn] [--replace] [--check] [--module MODULE_VALUE] [--help]"
  exit 1
}

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
    --module)   # Support for --module parameter
      shift
      MODULE="$1"
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

case "$MODULE" in
  "spark-2.4")
    MVN_MODULES="client-spark/spark-2"
    SBT_PROJECT="celeborn-client-spark-2"
    ;;
  "spark-3.0" | "spark-3.1" | "spark-3.2" | "spark-3.3" | "spark-3.4")
    MVN_MODULES="client-spark/spark-3"
    SBT_PROJECT="celeborn-client-spark-3"
    ;;
  "flink-1.14")
    MVN_MODULES="client-flink/flink-1.14"
    SBT_PROJECT="celeborn-client-flink-1_14"
    ;;
  "flink-1.15")
    MVN_MODULES="client-flink/flink-1.15"
    SBT_PROJECT="celeborn-client-flink-1_15"
    ;;
  "flink-1.17")
    MVN_MODULES="client-flink/flink-1.17"
    SBT_PROJECT="celeborn-client-flink-1_17"
    ;;
  *)
    MVN_MODULES="worker,master"
    ;;
esac


if [ "$MODULE" = "server" ]; then
    DEP="${PWD}/dev/deps/dependencies-server"
    DEP_PR="${PWD}/dev/deps/dependencies-server.tmp"
else
    DEP="${PWD}/dev/deps/dependencies-client-$MODULE"
    DEP_PR="${PWD}/dev/deps/dependencies-client-$MODULE.tmp"
fi

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

if [ "$SBT_ENABLED" == "true" ]; then
  if [ "$MODULE" == "server" ]; then
    sbt_build_server_classpath
  else
    sbt_build_client_classpath
  fi
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
