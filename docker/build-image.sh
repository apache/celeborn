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

# Build the local Celeborn docker image (celeborn:dev) for docker-compose.
#
# Two steps:
#   1. ./build/make-distribution.sh   -> produces dist/ (bin/sbin/conf/jars/...
#      plus dist/docker/Dockerfile copied by the distribution script itself)
#   2. docker build                   -> builds the image with context = dist/
#      (the Dockerfile COPYs bin/sbin/... relative to the context root)
#
# The image is NOT built via the compose `build:` key, because make-distribution
# must run before docker build and dist/ may not exist yet when invoking
# `docker compose up`.
#
# When a Spark client profile is passed (e.g. -Pspark-3.5 or -Pspark-4.0),
# make-distribution also drops the client shaded jar at dist/spark/. Step 3
# then builds an additional Spark image (celeborn-spark:dev) on top of
# apache/spark with that jar baked in, so docker-compose can run an end-to-end
# Spark cluster whose shuffle is served by Celeborn.
#
# Usage:
#   ./docker/build-image.sh                      # core master/worker/cli
#   ./docker/build-image.sh -Pspark-3.5          # also build Spark 3 client + image
#   ./docker/build-image.sh -Pspark-4.0          # also build Spark 4 client + image
#   CELEBORN_IMAGE_TAG=celeborn:dev ./docker/build-image.sh
#   CELEBORN_SPARK_IMAGE_TAG=celeborn-spark:dev ./docker/build-image.sh -Pspark-3.5
#   CELEBORN_SPARK_BASE_TAG=3.5.9 ./docker/build-image.sh -Pspark-3.5
#
# Requirements: docker on PATH; build/mvn is bundled in the repo.

set -euo pipefail

# Resolve repo root (parent of this script's directory).
cd "$(dirname "$0")/.."

IMAGE_TAG="${CELEBORN_IMAGE_TAG:-celeborn:dev}"
SPARK_IMAGE_TAG="${CELEBORN_SPARK_IMAGE_TAG:-celeborn-spark:dev}"
# Base apache/spark image tag. Renamed away from SPARK_IMAGE_TAG because that
# env var is commonly exported by Spark's own tooling and would otherwise leak
# in here as the *output* image name (e.g. celeborn-spark:dev), producing an
# invalid "apache/spark:celeborn-spark:dev" FROM reference. When unset, the
# default is derived from the built client jar's Spark major version below.
BASE_SPARK_IMAGE_TAG="${CELEBORN_SPARK_BASE_TAG:-}"

echo "==> Building distribution (./build/make-distribution.sh --sbt-enabled $*) ..."
./build/make-distribution.sh --sbt-enabled "$@"

echo "==> Building docker image ${IMAGE_TAG} from dist/ ..."
if [ ! -f dist/docker/Dockerfile ]; then
  echo "ERROR: dist/docker/Dockerfile not found. make-distribution may have failed." >&2
  exit 1
fi
docker build -t "${IMAGE_TAG}" -f dist/docker/Dockerfile dist/

# --- Spark image (optional) ---------------------------------------------------
# Only build it when make-distribution produced a Spark client shaded jar, which
# happens when a -Pspark-3.x/-Pspark-4.x profile was passed. If absent, skip
# gracefully so the Celeborn-only workflow keeps working.
SPARK_CLIENT_JAR="$(ls dist/spark/celeborn-client-spark-*-shaded_*.jar 2>/dev/null || true)"

if [ -n "${SPARK_CLIENT_JAR}" ]; then
  echo "==> Found Spark client jar: $(basename "${SPARK_CLIENT_JAR}")"
  # Derive the default base image tag from the jar's Spark major version, e.g.
  # celeborn-client-spark-4-shaded_2.13-<version>.jar -> 4 -> apache/spark:4.0.4.
  SPARK_MAJOR_VERSION="$(basename "${SPARK_CLIENT_JAR}" | sed -E 's/celeborn-client-spark-([0-9]+)-shaded_.*/\1/')"
  if [ -z "${BASE_SPARK_IMAGE_TAG}" ]; then
    case "${SPARK_MAJOR_VERSION}" in
      3) BASE_SPARK_IMAGE_TAG="3.5.9" ;;
      4) BASE_SPARK_IMAGE_TAG="4.0.4" ;;
      *)
        echo "ERROR: unsupported Spark major version '${SPARK_MAJOR_VERSION}'," >&2
        echo "       set CELEBORN_SPARK_BASE_TAG explicitly." >&2
        exit 1
        ;;
    esac
  fi
  # Copy into the Spark image build context (excluded from git via .gitignore).
  cp "${SPARK_CLIENT_JAR}" docker/spark/
  echo "==> Building Spark docker image ${SPARK_IMAGE_TAG} (base apache/spark:${BASE_SPARK_IMAGE_TAG}) ..."
  docker build -t "${SPARK_IMAGE_TAG}" \
    --build-arg spark_image_tag="${BASE_SPARK_IMAGE_TAG}" \
    docker/spark/
  echo "==> Done. Images: ${IMAGE_TAG}, ${SPARK_IMAGE_TAG}"
  echo "Start the cluster (Celeborn + Spark):"
  echo "  docker compose -f docker/docker-compose.yaml up -d"
else
  echo "==> No Spark client jar under dist/spark/ (pass e.g. -Pspark-3.5 or -Pspark-4.0 to build one)."
  echo "==> Skipped Spark image. Done. Image: ${IMAGE_TAG}"
  echo "Start the Celeborn-only cluster:"
  echo "  docker compose -f docker/docker-compose.yaml up -d"
fi
