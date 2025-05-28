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

PROJECT_DIR="$(cd "$(dirname "$0")"/../..; pwd)"

ASF_USERNAME=${ASF_USERNAME:?"ASF_USERNAME is required"}
ASF_PASSWORD=${ASF_PASSWORD:?"ASF_PASSWORD is required"}
RELEASE_RC_NO=${RELEASE_RC_NO:?"RELEASE_RC_NO is required, e.g. 0"}
JAVA8_HOME=${JAVA8_HOME:?"JAVA8_HOME is required"}
JAVA11_HOME=${JAVA11_HOME:?"JAVA11_HOME is required"}
JAVA17_HOME=${JAVA17_HOME:?"JAVA17_HOME is required"}

RELEASE_VERSION=$(awk -F'"' '/ThisBuild \/ version/ {print $2}' version.sbt)

exit_with_usage() {
  local NAME=$(basename $0)
  cat << EOF
Usage: $NAME <publish|finalize>

Top level targets are:
  publish: Publish tarballs to SVN staging repository
  finalize: Finalize the release after an RC passes vote

All other inputs are environment variables

RELEASE_RC_NO   - Release RC number, (e.g. 0)
JAVA8_HOME - Location where the JDK or JRE is installed for JDK 8
JAVA11_HOME - Location where the JDK or JRE is installed for JDK 11

ASF_USERNAME - Username of ASF committer account
ASF_PASSWORD - Password of ASF committer account
EOF
  exit 1
}

if [[ ${RELEASE_VERSION} =~ .*-SNAPSHOT ]]; then
  echo "Can not release a SNAPSHOT version: ${RELEASE_VERSION}"
  exit_with_usage
  exit 1
fi

RELEASE_TAG="v${RELEASE_VERSION}-rc${RELEASE_RC_NO}"

SVN_STAGING_REPO="https://dist.apache.org/repos/dist/dev/celeborn"
SVN_RELEASE_REPO="https://dist.apache.org/repos/dist/release/celeborn"

RELEASE_DIR="${PROJECT_DIR}/tmp"
SVN_STAGING_DIR="${PROJECT_DIR}/tmp/svn-dev"
SVN_RELEASE_DIR="${PROJECT_DIR}/tmp/svn-release"

package() {
  SKIP_GPG="false" $PROJECT_DIR/build/release/create-package.sh source
  SKIP_GPG="false" $PROJECT_DIR/build/release/create-package.sh binary
}

upload_svn_staging() {
  svn checkout --depth=empty "${SVN_STAGING_REPO}" "${SVN_STAGING_DIR}"
  mkdir -p "${SVN_STAGING_DIR}/${RELEASE_TAG}"
  rm -f "${SVN_STAGING_DIR}/${RELEASE_TAG}/*"

  SRC_TGZ_FILE="apache-celeborn-${RELEASE_VERSION}-source.tgz"
  BIN_TGZ_FILE="apache-celeborn-${RELEASE_VERSION}-bin.tgz"

  echo "Copying release tarballs"
  cp "${RELEASE_DIR}/${SRC_TGZ_FILE}"        "${SVN_STAGING_DIR}/${RELEASE_TAG}/${SRC_TGZ_FILE}"
  cp "${RELEASE_DIR}/${SRC_TGZ_FILE}.asc"    "${SVN_STAGING_DIR}/${RELEASE_TAG}/${SRC_TGZ_FILE}.asc"
  cp "${RELEASE_DIR}/${SRC_TGZ_FILE}.sha512" "${SVN_STAGING_DIR}/${RELEASE_TAG}/${SRC_TGZ_FILE}.sha512"
  cp "${RELEASE_DIR}/${BIN_TGZ_FILE}"        "${SVN_STAGING_DIR}/${RELEASE_TAG}/${BIN_TGZ_FILE}"
  cp "${RELEASE_DIR}/${BIN_TGZ_FILE}.asc"    "${SVN_STAGING_DIR}/${RELEASE_TAG}/${BIN_TGZ_FILE}.asc"
  cp "${RELEASE_DIR}/${BIN_TGZ_FILE}.sha512" "${SVN_STAGING_DIR}/${RELEASE_TAG}/${BIN_TGZ_FILE}.sha512"

  svn add "${SVN_STAGING_DIR}/${RELEASE_TAG}"

  echo "Uploading release tarballs to ${SVN_STAGING_DIR}/${RELEASE_TAG}"
  (
    cd "${SVN_STAGING_DIR}" && \
    svn commit --username "${ASF_USERNAME}" --password "${ASF_PASSWORD}" --message "Apache Celeborn ${RELEASE_TAG}"
  )
  echo "Celeborn tarballs uploaded"
}

upload_nexus_staging() {
  export JAVA_HOME=$JAVA8_HOME

  echo "Deploying celeborn-client-spark-2-shaded_2.11"
  ${PROJECT_DIR}/build/sbt -Pspark-2.4 "clean;celeborn-client-spark-2-shaded/publishSigned"

  echo "Deploying celeborn-client-spark-3-shaded_2.12"
  ${PROJECT_DIR}/build/sbt -Pspark-3.4 "clean;celeborn-client-spark-3-shaded/publishSigned"

  echo "Deploying celeborn-client-spark-3-shaded_2.13"
  ${PROJECT_DIR}/build/sbt -Pspark-3.4 ++2.13.8 "clean;celeborn-client-spark-3-shaded/publishSigned"

  export JAVA_HOME=$JAVA17_HOME
  echo "Deploying celeborn-client-spark-4-shaded_2.13"
  ${PROJECT_DIR}/build/sbt -Pspark-4.0 "clean;celeborn-client-spark-4-shaded/publishSigned"
  export JAVA_HOME=$JAVA8_HOME

  echo "Deploying celeborn-client-flink-1.16-shaded_2.12"
  ${PROJECT_DIR}/build/sbt -Pflink-1.16 "clean;celeborn-client-flink-1_16-shaded/publishSigned"

  echo "Deploying celeborn-client-flink-1.17-shaded_2.12"
  ${PROJECT_DIR}/build/sbt -Pflink-1.17 "clean;celeborn-client-flink-1_17-shaded/publishSigned"

  echo "Deploying celeborn-client-flink-1.18-shaded_2.12"
  ${PROJECT_DIR}/build/sbt -Pflink-1.18 "clean;celeborn-client-flink-1_18-shaded/publishSigned"

  echo "Deploying celeborn-client-flink-1.19-shaded_2.12"
  ${PROJECT_DIR}/build/sbt -Pflink-1.19 "clean;celeborn-client-flink-1_19-shaded/publishSigned"

  echo "Deploying celeborn-client-flink-1.20-shaded_2.12"
  ${PROJECT_DIR}/build/sbt -Pflink-1.20 "clean;celeborn-client-flink-1_20-shaded/publishSigned"

  export JAVA_HOME=$JAVA11_HOME
  echo "Deploying celeborn-client-flink-2.0-shaded_2.12"
  ${PROJECT_DIR}/build/sbt -Pflink-2.0 "clean;celeborn-client-flink-2_0-shaded/publishSigned"
  export JAVA_HOME=$JAVA8_HOME

  echo "Deploying celeborn-client-mr-shaded_2.12"
  ${PROJECT_DIR}/build/sbt -Pmr "clean;celeborn-client-mr-shaded/publishSigned"

  echo "Deploying celeborn-openapi-client"
  ${PROJECT_DIR}/build/sbt "clean;celeborn-openapi-client/publishSigned"

  echo "Deploying celeborn-spi"
  ${PROJECT_DIR}/build/sbt "clean;celeborn-spi/publishSigned"

  echo "Deploying celeborn-cli"
  ${PROJECT_DIR}/build/sbt "clean;celeborn-cli/publishSigned"
}

finalize_svn() {
  echo "Moving Celeborn tarballs to the release directory"
  svn mv --username "${ASF_USERNAME}" --password "${ASF_PASSWORD}" --no-auth-cache \
     --message "Apache Celeborn ${RELEASE_VERSION}" \
     "${SVN_STAGING_REPO}/${RELEASE_TAG}" "${SVN_RELEASE_REPO}/celeborn-${RELEASE_VERSION}"
  echo "Celeborn tarballs moved"
}

if [[ "$1" == "publish" ]]; then
  package
  upload_svn_staging
  upload_nexus_staging
  exit 0
fi

if [[ "$1" == "finalize" ]]; then
  echo "THIS STEP IS IRREVERSIBLE! Make sure the vote has passed and you pick the right RC to finalize."
  read -p "You must be a PMC member to run this step. Continue? [y/N] " ANSWER
  if [ "$ANSWER" != "y" ]; then
    echo "Exiting."
    exit 1
  fi

  finalize_svn
  exit 0
fi

exit_with_usage
