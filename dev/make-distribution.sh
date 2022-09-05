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

# Figure out where the RSS framework is installed
RSS_HOME="$(cd "`dirname "$0"`/.."; pwd)"
DISTDIR="$RSS_HOME/dist"
NAME=none

function exit_with_usage {
  echo "make-distribution.sh - tool for making binary distributions of Remote Shuffle Service"
  echo ""
  echo "usage:"
  cl_options="[--name <custom_name>]"
  echo "make-distribution.sh $cl_options <maven build options>"
  echo ""
  exit 1
}

# Parse arguments
while (( "$#" )); do
  case $1 in
    --name)
      NAME="$2"
      shift
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

MVN="$RSS_HOME/build/mvn"

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

VERSION=$("$MVN" help:evaluate -Dexpression=project.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
SCALA_VERSION=$("$MVN" help:evaluate -Dexpression=scala.binary.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)
SPARK_VERSION=$("$MVN" help:evaluate -Dexpression=spark.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)

SPARK_MAJOR_VERSION=${SPARK_VERSION%%.*}

echo "RSS version is $VERSION"

if [ "$NAME" == "none" ]; then
  NAME="release"
fi

echo "Making rss-$VERSION-bin-$NAME.tgz"

# Build uber fat JAR
cd "$RSS_HOME"

export MAVEN_OPTS="${MAVEN_OPTS:--Xmx2g -XX:ReservedCodeCacheSize=1g}"

# Store the command as an array because $MVN variable might have spaces in it.
# Normal quoting tricks don't work.
# See: http://mywiki.wooledge.org/BashFAQ/050
BUILD_COMMAND=("$MVN" clean package -DskipTests $@)

# Actually build the jar
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND[@]}\n"

"${BUILD_COMMAND[@]}"

# Make directories
rm -rf "$DISTDIR"
mkdir -p "$DISTDIR/master-jars"
mkdir -p "$DISTDIR/worker-jars"
mkdir -p "$DISTDIR/spark"

echo "RSS $VERSION$GITREVSTRING" > "$DISTDIR/RELEASE"
echo "Build flags: $@" >> "$DISTDIR/RELEASE"

# Copy jars
cp "$RSS_HOME"/server-master/target/master-$VERSION.jar "$DISTDIR/master-jars/"
cp "$RSS_HOME"/server-master/target/scala-$SCALA_VERSION/jars/*.jar "$DISTDIR/master-jars/"
cp "$RSS_HOME"/server-worker/target/worker-$VERSION.jar "$DISTDIR/worker-jars/"
cp "$RSS_HOME"/server-worker/target/scala-$SCALA_VERSION/jars/*.jar "$DISTDIR/worker-jars/"
cp "$RSS_HOME"/client-spark/shuffle-manager-$SPARK_MAJOR_VERSION/target/shuffle-manager-$SPARK_MAJOR_VERSION-$VERSION-shaded.jar "$DISTDIR/spark/"

# Copy other things
mkdir "$DISTDIR/conf"
cp "$RSS_HOME"/conf/*.template "$DISTDIR/conf"
cp -r "$RSS_HOME/bin" "$DISTDIR"
cp -r "$RSS_HOME/sbin" "$DISTDIR"
mkdir "$DISTDIR/docker"
cp "$RSS_HOME/docker/Dockerfile" "$DISTDIR/docker"
cp -r "$RSS_HOME/docker/helm" "$DISTDIR/docker"

TARDIR_NAME="rss-$VERSION-bin-$NAME"
TARDIR="$RSS_HOME/$TARDIR_NAME"
rm -rf "$TARDIR"
cp -r "$DISTDIR" "$TARDIR"
tar czf "rss-$VERSION-bin-$NAME.tgz" -C "$RSS_HOME" "$TARDIR_NAME"
rm -rf "$TARDIR"
