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

set -o pipefail
set -e
set -x

PROJECT_DIR="$(cd "`dirname "$0"`/.."; pwd)"
DIST_DIR="$PROJECT_DIR/dist"
NAME="bin"
RELEASE="false"
MVN="$PROJECT_DIR/build/mvn"
SBT="$PROJECT_DIR/build/sbt"
SBT_ENABLED="false"

function exit_with_usage {
  echo "make-distribution.sh - tool for making binary distributions of Celeborn"
  echo ""
  echo "usage:"
  cl_options="[--name <custom_name>] [--release] [--sbt-enabled] [--mvn <mvn-command>]"
  echo "make-distribution.sh $cl_options <maven build options>"
  echo ""
  exit 1
}

# Parse arguments
while (( "$#" )); do
  case $1 in
    --name)
      NAME="bin-$2"
      shift
      ;;
    --mvn)
      MVN="$2"
      shift
      ;;
    --release)
      RELEASE="true"
      ;;
    --sbt-enabled)
      SBT_ENABLED="true"
      ;;
    --help)
      exit_with_usage
      ;;
    --*)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
    -*)
      break
      ;;
    *)
      echo "Error: $1 is not supported"
      exit_with_usage
      ;;
  esac
  shift
done

if [ -z "$JAVA_HOME" ]; then
  # Fall back on JAVA_HOME from rpm, if found
  if [ $(command -v rpm) ]; then
    RPM_JAVA_HOME="$(rpm -E %java_home 2>/dev/null)"
    if [ "$RPM_JAVA_HOME" != "%java_home" ]; then
      JAVA_HOME="$RPM_JAVA_HOME"
      echo "No JAVA_HOME set, proceeding with '$JAVA_HOME' learned from rpm"
    fi
  fi

  if [ -z "$JAVA_HOME" ]; then
    if [ `command -v java` ]; then
      # If java is in /usr/bin/java, we want /usr
      JAVA_HOME="$(dirname $(dirname $(which java)))"
    fi
  fi
fi

if [ -z "$JAVA_HOME" ]; then
  echo "Error: JAVA_HOME is not set, cannot proceed."
  exit -1
fi

export MAVEN_OPTS="${MAVEN_OPTS:--Xmx2g -XX:ReservedCodeCacheSize=1g}"

if [ ! "$(command -v "$MVN")" ] ; then
    echo -e "Could not locate Maven command: '$MVN'."
    exit -1;
fi

if [ $(command -v git) ]; then
    GITREV=$(git rev-parse --short HEAD 2>/dev/null || :)
    if [ ! -z "$GITREV" ]; then
        GITREVSTRING=" (git revision $GITREV)"
    fi
    unset GITREV
fi

cd "$PROJECT_DIR"

# Make directories
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

MVN_DIST_OPT="-DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip"

function build_service {
  VERSION=$("$MVN" help:evaluate -Dexpression=project.version $@ 2>/dev/null \
      | grep -v "INFO" \
      | grep -v "WARNING" \
      | tail -n 1)
  SCALA_VERSION=$("$MVN" help:evaluate -Dexpression=scala.binary.version $@ 2>/dev/null \
      | grep -v "INFO" \
      | grep -v "WARNING" \
      | tail -n 1)

  echo "Celeborn version is $VERSION"
  echo "Making apache-celeborn-$VERSION-$NAME.tgz"

  echo "Celeborn $VERSION$GITREVSTRING" > "$DIST_DIR/RELEASE"
  echo "Build flags: $@" >> "$DIST_DIR/RELEASE"

  # Store the command as an array because $MVN variable might have spaces in it.
  # Normal quoting tricks don't work.
  # See: http://mywiki.wooledge.org/BashFAQ/050
  BUILD_COMMAND=("$MVN" clean package $MVN_DIST_OPT -pl master,worker -am $@)

  # Actually build the jar
  echo -e "\nBuilding with..."
  echo -e "\$ ${BUILD_COMMAND[@]}\n"

  "${BUILD_COMMAND[@]}"

  mkdir -p "$DIST_DIR/jars"
  mkdir -p "$DIST_DIR/master-jars"
  mkdir -p "$DIST_DIR/worker-jars"

  ## Copy master jars
  cp "$PROJECT_DIR"/master/target/celeborn-master_$SCALA_VERSION-$VERSION.jar "$DIST_DIR/master-jars/"
  cp "$PROJECT_DIR"/master/target/scala-$SCALA_VERSION/jars/*.jar "$DIST_DIR/jars/"
  for jar in $(ls "$PROJECT_DIR/master/target/scala-$SCALA_VERSION/jars"); do
    (cd $DIST_DIR/master-jars; ln -snf "../jars/$jar" .)
  done
  ## Copy worker jars
  cp "$PROJECT_DIR"/worker/target/celeborn-worker_$SCALA_VERSION-$VERSION.jar "$DIST_DIR/worker-jars/"
  cp "$PROJECT_DIR"/worker/target/scala-$SCALA_VERSION/jars/*.jar "$DIST_DIR/jars/"
  for jar in $(ls "$PROJECT_DIR/worker/target/scala-$SCALA_VERSION/jars"); do
    (cd $DIST_DIR/worker-jars; ln -snf "../jars/$jar" .)
  done
}

function build_spark_client {
  VERSION=$("$MVN" help:evaluate -Dexpression=project.version $@ 2>/dev/null \
      | grep -v "INFO" \
      | grep -v "WARNING" \
      | tail -n 1)
  SCALA_VERSION=$("$MVN" help:evaluate -Dexpression=scala.binary.version $@ 2>/dev/null \
      | grep -v "INFO" \
      | grep -v "WARNING" \
      | tail -n 1)
  SPARK_VERSION=$("$MVN" help:evaluate -Dexpression=spark.version $@ 2>/dev/null \
      | grep -v "INFO" \
      | grep -v "WARNING" \
      | tail -n 1)
  SPARK_MAJOR_VERSION=${SPARK_VERSION%%.*}

  # Store the command as an array because $MVN variable might have spaces in it.
  # Normal quoting tricks don't work.
  # See: http://mywiki.wooledge.org/BashFAQ/050
  BUILD_COMMAND=("$MVN" clean package $MVN_DIST_OPT -pl :celeborn-client-spark-${SPARK_MAJOR_VERSION}-shaded_$SCALA_VERSION -am $@)

  # Actually build the jar
  echo -e "\nBuilding with..."
  echo -e "\$ ${BUILD_COMMAND[@]}\n"

  "${BUILD_COMMAND[@]}"

  mkdir -p "$DIST_DIR/spark"

  ## Copy spark client jars
  cp "$PROJECT_DIR"/client-spark/spark-$SPARK_MAJOR_VERSION-shaded/target/celeborn-client-spark-${SPARK_MAJOR_VERSION}-shaded_$SCALA_VERSION-$VERSION.jar "$DIST_DIR/spark/"
}

function build_flink_client {
  FLINK_VERSION=$("$MVN" help:evaluate -Dexpression=flink.version $@ 2>/dev/null \
      | grep -v "INFO" \
      | grep -v "WARNING" \
      | tail -n 1)
  SCALA_VERSION=$("$MVN" help:evaluate -Dexpression=scala.binary.version $@ 2>/dev/null \
      | grep -v "INFO" \
      | grep -v "WARNING" \
      | tail -n 1)
  FLINK_BINARY_VERSION=${FLINK_VERSION%.*}

  # Store the command as an array because $MVN variable might have spaces in it.
  # Normal quoting tricks don't work.
  # See: http://mywiki.wooledge.org/BashFAQ/050
  BUILD_COMMAND=("$MVN" clean package $MVN_DIST_OPT -pl :celeborn-client-flink-${FLINK_BINARY_VERSION}-shaded_$SCALA_VERSION -am $@)

  # Actually build the jar
  echo -e "\nBuilding with..."
  echo -e "\$ ${BUILD_COMMAND[@]}\n"

  "${BUILD_COMMAND[@]}"

  ## flink spark client jars
  mkdir -p "$DIST_DIR/flink"
  cp "$PROJECT_DIR"/client-flink/flink-$FLINK_BINARY_VERSION-shaded/target/celeborn-client-flink-${FLINK_BINARY_VERSION}-shaded_$SCALA_VERSION-$VERSION.jar "$DIST_DIR/flink/"
}

function build_mr_client {
  VERSION=$("$MVN" help:evaluate -Dexpression=project.version $@ 2>/dev/null \
        | grep -v "INFO" \
        | grep -v "WARNING" \
        | tail -n 1)
  BUILD_COMMAND=("$MVN" clean package $MVN_DIST_OPT -pl :celeborn-client-mr-shaded_${SCALA_VERSION} -am $@)

    # Actually build the jar
    echo -e "\nBuilding with..."
    echo -e "\$ ${BUILD_COMMAND[@]}\n"

    "${BUILD_COMMAND[@]}"

    ## flink spark client jars
    mkdir -p "$DIST_DIR/mr"
    cp "$PROJECT_DIR"/client-mr/mr-shaded/target/celeborn-client-mr-shaded_${SCALA_VERSION}-$VERSION.jar "$DIST_DIR/mr/"
}


#########################
#     sbt functions     #
#########################

function sbt_build_service {
  VERSION=$("$SBT" "Show / version" | awk '/\[info\]/{ver=$2} END{print ver}')

  SCALA_VERSION=$("$SBT" "Show / scalaBinaryVersion" | awk '/\[info\]/{ver=$2} END{print ver}')

  echo "Celeborn version is $VERSION"
  echo "Making apache-celeborn-$VERSION-$NAME.tgz"

  echo "Celeborn $VERSION$GITREVSTRING" > "$DIST_DIR/RELEASE"
  echo "Build flags: $@" >> "$DIST_DIR/RELEASE"

  BUILD_COMMAND=("$SBT" clean package)

  # Actually build the jar
  echo -e "\nBuilding with..."
  echo -e "\$ ${BUILD_COMMAND[@]}\n"

  "${BUILD_COMMAND[@]}"

  $SBT "celeborn-master/copyJars;celeborn-worker/copyJars"

  mkdir -p "$DIST_DIR/jars"
  mkdir -p "$DIST_DIR/master-jars"
  mkdir -p "$DIST_DIR/worker-jars"

  ## Copy master jars
  cp "$PROJECT_DIR"/master/target/scala-$SCALA_VERSION/celeborn-master_$SCALA_VERSION-$VERSION.jar "$DIST_DIR/master-jars/"
  cp "$PROJECT_DIR"/master/target/scala-$SCALA_VERSION/jars/*.jar "$DIST_DIR/jars/"
  for jar in $(ls "$PROJECT_DIR/master/target/scala-$SCALA_VERSION/jars"); do
    (cd $DIST_DIR/master-jars; ln -snf "../jars/$jar" .)
  done
  ## Copy worker jars
  cp "$PROJECT_DIR"/worker/target/scala-$SCALA_VERSION/celeborn-worker_$SCALA_VERSION-$VERSION.jar "$DIST_DIR/worker-jars/"
  cp "$PROJECT_DIR"/worker/target/scala-$SCALA_VERSION/jars/*.jar "$DIST_DIR/jars/"
  for jar in $(ls "$PROJECT_DIR/worker/target/scala-$SCALA_VERSION/jars"); do
    (cd $DIST_DIR/worker-jars; ln -snf "../jars/$jar" .)
  done
}

function sbt_build_client {
  PROFILE="$1"
  # get the client shaded project
  CLIENT_PROJECT=$("$SBT" "$PROFILE" projects | grep shaded | awk '{print$2}')
  # get the shaded jar file path
  ASSEMBLY_OUTPUT_PATH=$("$SBT" "$PROFILE" "show $CLIENT_PROJECT/assembly/assemblyOutputPath" | awk '/\[info\]/{ver=$2} END{print ver}')
  # build the shaded jar
  $SBT $PROFILE "clean;$CLIENT_PROJECT/assembly"

  PROFILE_PREFIX=$(echo "$PROFILE" | cut -d'-' -f2)
  CLIENT_SUB_DIR="${PROFILE_PREFIX:1}"
  mkdir -p "$DIST_DIR/$CLIENT_SUB_DIR"
  cp "$ASSEMBLY_OUTPUT_PATH" "$DIST_DIR/$CLIENT_SUB_DIR/"
}


if [ "$SBT_ENABLED" == "true" ]; then
  sbt_build_service "$@"
  if [ "$RELEASE" == "true" ]; then
    sbt_build_client -Pspark-2.4
    sbt_build_client -Pspark-3.4
    sbt_build_client -Pflink-1.14
    sbt_build_client -Pflink-1.15
    sbt_build_client -Pflink-1.17
    sbt_build_client -Pflink-1.18
    sbt_build_client -Pmr
  else
    echo "build client with $@"
    ENGINE_COUNT=0
    ENGINES=("spark" "flink" "mr")
    for single_engine in ${ENGINES[@]}
    do
      echo $single_engine
      if [[ $@ == *"${single_engine}"* ]];then
        ENGINE_COUNT=`expr ${ENGINE_COUNT} + 1`
      fi
    done
    if [[ ${ENGINE_COUNT} -eq 0  ]]; then
      echo "Skip building client."
    elif [[ ${ENGINE_COUNT} -ge 2 ]]; then
      echo "Error: unsupported build options: $@"
      echo "       currently we do not support compiling different engine clients at the same time."
      exit -1
    else
      sbt_build_client $@
    fi
  fi
else
  if [ "$RELEASE" == "true" ]; then
    build_service
    build_spark_client -Pspark-2.4
    build_spark_client -Pspark-3.4
    build_flink_client -Pflink-1.14
    build_flink_client -Pflink-1.15
    build_flink_client -Pflink-1.17
    build_flink_client -Pflink-1.18
    build_mr_client -Pmr
  else
    ## build release package on demand
    build_service $@
    echo "build client with $@"
    ENGINE_COUNT=0
    ENGINES=("spark" "flink" "mr")
    for single_engine in ${ENGINES[@]}
    do
      echo $single_engine
      if [[ $@ == *"${single_engine}"* ]];then
        ENGINE_COUNT=`expr ${ENGINE_COUNT} + 1`
      fi
    done
    if [[ ${ENGINE_COUNT} -eq 0  ]]; then
      echo "Skip building client."
    elif [[ ${ENGINE_COUNT} -ge 2 ]]; then
      echo "Error: unsupported build options: $@"
      echo "       currently we do not support compiling different engine clients at the same time."
      exit -1
    elif [[  $@ == *"spark"* ]]; then
      echo "build spark clients"
      build_spark_client $@
    elif [[  $@ == *"flink"* ]]; then
      echo "build flink clients"
      build_flink_client $@
    elif [[  $@ == *"mr"* ]]; then
      echo "build mr clients"
      build_mr_client $@
    fi
  fi
fi

# Copy configuration templates
mkdir "$DIST_DIR/conf"
cp "$PROJECT_DIR"/conf/*.template "$DIST_DIR/conf"

# Copy shell scripts
cp -r "$PROJECT_DIR/bin" "$DIST_DIR"
cp -r "$PROJECT_DIR/sbin" "$DIST_DIR"

# Copy container related resources
mkdir "$DIST_DIR/docker"
cp "$PROJECT_DIR/docker/Dockerfile" "$DIST_DIR/docker"

# Copy Helm Charts
cp -r "$PROJECT_DIR/charts" "$DIST_DIR"

# Copy license files
cp "$PROJECT_DIR/DISCLAIMER" "$DIST_DIR/DISCLAIMER"
if [[ -f $"$PROJECT_DIR/LICENSE-binary" ]]; then
  cp "$PROJECT_DIR/LICENSE-binary" "$DIST_DIR/LICENSE"
  cp -r "$PROJECT_DIR/licenses-binary" "$DIST_DIR/licenses"
  cp "$PROJECT_DIR/NOTICE-binary" "$DIST_DIR/NOTICE"
fi

# Create tarball
TARDIR_NAME="apache-celeborn-$VERSION-$NAME"
TARDIR="$PROJECT_DIR/$TARDIR_NAME"
rm -rf "$TARDIR"
cp -R "$DIST_DIR" "$TARDIR"
TAR="tar"
if [ "$(uname -s)" = "Darwin" ]; then
  TAR="tar --no-mac-metadata --no-xattrs --no-fflags"
fi
$TAR -czf "apache-celeborn-$VERSION-$NAME.tgz" -C "$PROJECT_DIR" "$TARDIR_NAME"
rm -rf "$TARDIR"
