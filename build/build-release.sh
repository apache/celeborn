#!/bin/bash
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

SKIP_GPG=${SKIP_GPG:-false}

exit_with_usage() {
  local NAME=$(basename $0)
  cat <<EOF
Usage: $NAME <source|binary>

Top level targets are:
  source: Create source release tarball
  binary: Create binary release tarball

All other inputs are environment variables:

SKIP_GPG        - (optional) Default false
EOF
  exit 1
}

PROJECT_DIR="$(
  cd "$(dirname "$0")/.."
  pwd
)"
DIST_DIR="$PROJECT_DIR/dist"
NAME="incubating-bin"

MVN="$PROJECT_DIR/build/mvn"

SHASUM="sha512sum"
if [ "$(uname)" == "Darwin" ]; then
  SHASUM="shasum -a 512"
fi

package_binary() {
  # Figure out where the RSS framework is installed

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
      if [ $(command -v java) ]; then
        # If java is in /usr/bin/java, we want /usr
        JAVA_HOME="$(dirname $(dirname $(which java)))"
      fi
    fi
  fi

  if [ -z "$JAVA_HOME" ]; then
    echo "Error: JAVA_HOME is not set, cannot proceed."
    exit -1
  fi

  if [ ! "$(command -v "$MVN")" ]; then
    echo -e "Could not locate Maven command: '$MVN'."
    exit -1
  fi

  if [ $(command -v git) ]; then
    GITREV=$(git rev-parse --short HEAD 2>/dev/null || :)
    if [ ! -z "$GITREV" ]; then
      GITREVSTRING=" (git revision $GITREV)"
    fi
    unset GITREV
  fi

  VERSION=$("$MVN" help:evaluate -Dexpression=project.version -Pspark-3.3 2>/dev/null | grep -v "INFO" |
    grep -v "WARNING" |
    tail -n 1)
  SCALA_VERSION=$("$MVN" help:evaluate -Dexpression=scala.binary.version -Pspark-3.3 2>/dev/null | grep -v "INFO" |
    grep -v "WARNING" |
    tail -n 1)
  SPARK_VERSION=$("$MVN" help:evaluate -Dexpression=spark.version -Pspark-3.3 2>/dev/null | grep -v "INFO" |
    grep -v "WARNING" |
    tail -n 1)

  SPARK_MAJOR_VERSION=${SPARK_VERSION%%.*}

  echo "Celeborn version is $VERSION"

  echo "Making apache-celeborn-$VERSION-$NAME.tgz"

  # Build uber fat JAR
  cd "$PROJECT_DIR"

  export MAVEN_OPTS="${MAVEN_OPTS:--Xmx2g -XX:ReservedCodeCacheSize=1g}"

  # Store the command as an array because $MVN variable might have spaces in it.
  # Normal quoting tricks don't work.
  # See: http://mywiki.wooledge.org/BashFAQ/050
  BUILD_COMMAND=("$MVN" clean package -DskipTests -Pspark-3.3)

  # Actually build the jar
  echo -e "\nBuilding with..."
  echo -e "\$ ${BUILD_COMMAND[@]}\n"

  "${BUILD_COMMAND[@]}"

  # Make directories
  rm -rf "$DIST_DIR"
  mkdir -p "$DIST_DIR/jars"
  mkdir -p "$DIST_DIR/master-jars"
  mkdir -p "$DIST_DIR/worker-jars"
  mkdir -p "$DIST_DIR/spark2"
  mkdir -p "$DIST_DIR/spark3"

  echo "Celeborn $VERSION$GITREVSTRING" >"$DIST_DIR/RELEASE"
  echo "Build flags: $@" >>"$DIST_DIR/RELEASE"

  # Copy jars
  ## Copy master jars
  cp "$PROJECT_DIR"/master/target/celeborn-master_$SCALA_VERSION-$VERSION.jar "$DIST_DIR/master-jars/"
  cp "$PROJECT_DIR"/master/target/scala-$SCALA_VERSION/jars/*.jar "$DIST_DIR/jars/"
  for jar in $(ls "$PROJECT_DIR/master/target/scala-$SCALA_VERSION/jars"); do
    (
      cd $DIST_DIR/master-jars
      ln -snf "../jars/$jar" .
    )
  done
  ## Copy worker jars
  cp "$PROJECT_DIR"/worker/target/celeborn-worker_$SCALA_VERSION-$VERSION.jar "$DIST_DIR/worker-jars/"
  cp "$PROJECT_DIR"/worker/target/scala-$SCALA_VERSION/jars/*.jar "$DIST_DIR/jars/"
  for jar in $(ls "$PROJECT_DIR/worker/target/scala-$SCALA_VERSION/jars"); do
    (
      cd $DIST_DIR/worker-jars
      ln -snf "../jars/$jar" .
    )
  done
  ## Copy spark client jars
  cp "$PROJECT_DIR"/client-spark/spark-$SPARK_MAJOR_VERSION-shaded/target/celeborn-client-spark-${SPARK_MAJOR_VERSION}-shaded_$SCALA_VERSION-$VERSION.jar "$DIST_DIR/spark3/"
  #build 2.4

  VERSION=$("$MVN" help:evaluate -Dexpression=project.version -Pspark-2.4 2>/dev/null | grep -v "INFO" |
    grep -v "WARNING" |
    tail -n 1)
  SCALA_VERSION=$("$MVN" help:evaluate -Dexpression=scala.binary.version -Pspark-2.4 2>/dev/null | grep -v "INFO" |
    grep -v "WARNING" |
    tail -n 1)
  SPARK_VERSION=$("$MVN" help:evaluate -Dexpression=spark.version -Pspark-2.4 2>/dev/null | grep -v "INFO" |
    grep -v "WARNING" |
    tail -n 1)
  SPARK_MAJOR_VERSION=${SPARK_VERSION%%.*}
  "$MVN" clean package -DskipTests -Pspark-2.4
  cp "$PROJECT_DIR"/client-spark/spark-$SPARK_MAJOR_VERSION-shaded/target/celeborn-client-spark-${SPARK_MAJOR_VERSION}-shaded_$SCALA_VERSION-$VERSION.jar "$DIST_DIR/spark2/"

  # Copy other things
  mkdir "$DIST_DIR/conf"
  cp "$PROJECT_DIR"/conf/*.template "$DIST_DIR/conf"
  cp -r "$PROJECT_DIR/bin" "$DIST_DIR"
  cp -r "$PROJECT_DIR/sbin" "$DIST_DIR"
  mkdir "$DIST_DIR/docker"
  cp "$PROJECT_DIR/docker/Dockerfile" "$DIST_DIR/docker"
  cp -r "$PROJECT_DIR/docker/helm" "$DIST_DIR/docker"
  # Copy notice ,license and disclaimer
  cp "$PROJECT_DIR/DISCLAIMER" "$DIST_DIR/"
  cp "$PROJECT_DIR/LICENSE-binary" "$DIST_DIR/LICENSE"
  cp "$PROJECT_DIR/NOTICE-binary" "$DIST_DIR/NOTICE"

  TARDIR_NAME="apache-celeborn-$VERSION-$NAME"
  TARDIR="$PROJECT_DIR/$TARDIR_NAME"
  rm -rf "$TARDIR"
  cp -R "$DIST_DIR" "$TARDIR"
  tar czf "apache-celeborn-$VERSION-$NAME.tgz" -C "$PROJECT_DIR" "$TARDIR_NAME"
  rm -rf "$TARDIR"

  if [ "$SKIP_GPG" == "false" ]; then
    gpg --armor --detach-sig "apache-celeborn-$VERSION-$NAME.tgz"
  fi
  $SHASUM "apache-celeborn-$VERSION-$NAME.tgz" >"apache-celeborn-$VERSION-$NAME.tgz.sha512"

}

package_source() {

  VERSION=$("$MVN" help:evaluate -Dexpression=project.version 2>/dev/null | grep -v "INFO" |
    grep -v "WARNING" |
    tail -n 1)

  SRC_TGZ_FILE="apache-celeborn-${VERSION}-incubating-source.tgz"
  SRC_TGZ="${SRC_TGZ_FILE}"

  rm -f "${SRC_TGZ}*"

  echo "Creating source release tarball ${SRC_TGZ_FILE}"

  git archive --prefix="apache-celeborn-${VERSION}-incubating-source/" -o "${SRC_TGZ}" HEAD

  if [ "$SKIP_GPG" == "false" ]; then
    gpg --armor --detach-sig "${SRC_TGZ}"
  fi
  $SHASUM "${SRC_TGZ_FILE}" >"${SRC_TGZ_FILE}.sha512"
}

if [[ "$1" == "source" ]]; then
  package_source
  exit 0
fi

if [[ "$1" == "binary" ]]; then
  package_binary
  exit 0
fi

exit_with_usage
