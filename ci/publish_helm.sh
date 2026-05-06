#!/bin/bash

set -euo pipefail

# Set up Java, Maven, Artifactory credentials, and the immutable version.
source ci/setup.sh

ARTIFACTORY_BASE_URL="https://artifactory.gateway.data-0.internal.api.openai.org/artifactory/helm-local/celeborn"
CHART_DIR="charts/celeborn"
OUT_DIR="$(mktemp -d -t celeborn-chart-release.XXXXXX)"
trap 'rm -rf "${OUT_DIR}"' EXIT

echo "==> Update chart dependencies"
helm dependency update "${CHART_DIR}"

echo "==> Lint chart"
helm lint "${CHART_DIR}"

echo "==> Package chart"
helm package "${CHART_DIR}" \
  --version "${HELM_CHART_VERSION}" \
  --destination "${OUT_DIR}"

echo "==> Resolve packaged chart"
PACKAGED_CHART_TGZ="${OUT_DIR}/celeborn-${HELM_CHART_VERSION}.tgz"
test -f "${PACKAGED_CHART_TGZ}"
CHART_BASENAME="$(basename "${PACKAGED_CHART_TGZ}")"
REMOTE_CHART_URL="${ARTIFACTORY_BASE_URL}/${CHART_BASENAME}"
EXISTING_INDEX="${OUT_DIR}/index-old.yaml"

echo "Built chart package: ${PACKAGED_CHART_TGZ}"
echo "Publishing chart to: ${REMOTE_CHART_URL}"

echo "==> Check whether chart already exists"
if curl -fsSL \
    -u "${ARTIFACTORY_USER}:${ARTIFACTORY_TOKEN}" \
    -o /dev/null \
    -I \
    "${REMOTE_CHART_URL}"; then
  echo "Chart already exists, skipping upload: ${REMOTE_CHART_URL}"
  exit 0
fi

echo "==> Fetch existing index.yaml to merge"
if ! curl -fsS \
    -u "${ARTIFACTORY_USER}:${ARTIFACTORY_TOKEN}" \
    -o "${EXISTING_INDEX}" \
    "${ARTIFACTORY_BASE_URL}/index.yaml"; then
  printf "apiVersion: v1\nentries: {}\n" > "${EXISTING_INDEX}"
fi

echo "==> Merge and rebuild index.yaml"
helm repo index "${OUT_DIR}" \
  --url "${ARTIFACTORY_BASE_URL}" \
  --merge "${EXISTING_INDEX}"

echo "==> Upload chart package"
curl -fsSL \
  -u "${ARTIFACTORY_USER}:${ARTIFACTORY_TOKEN}" \
  -X PUT \
  -T "${PACKAGED_CHART_TGZ}" \
  "${REMOTE_CHART_URL}"

echo "==> Upload merged index.yaml"
curl -fsSL \
  -u "${ARTIFACTORY_USER}:${ARTIFACTORY_TOKEN}" \
  -X PUT \
  -T "${OUT_DIR}/index.yaml" \
  "${ARTIFACTORY_BASE_URL}/index.yaml"

echo "Published chart URL: ${REMOTE_CHART_URL}"
echo "Updated index: ${ARTIFACTORY_BASE_URL}/index.yaml"
