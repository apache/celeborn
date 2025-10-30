#!/usr/bin/env bash
set -euo pipefail

REPO_BASE="https://artifactory.data-0.internal.api.openai.org/artifactory"
HELM_REPO="helm-local"
NAMESPACE="celeborn"
CHART_DIR="."
CHART_NAME="celeborn"
OUT_DIR="dist"
REPO_BASED_URL="$REPO_BASE/$HELM_REPO/$NAMESPACE"

# Auth must be exported in the env already:
#   export ARTIFACTORY_USER=...
#   export ARTIFACTORY_PASSWORD=...
: "${ARTIFACTORY_USER:?ARTIFACTORY_USER required}"
: "${ARTIFACTORY_PASSWORD:?ARTIFACTORY_PASSWORD required}"

echo "Using ARTIFACTORY_USER: $ARTIFACTORY_USER"
mkdir -p "$OUT_DIR"
rm -rf "${OUT_DIR:?}/"*

echo "==> Package chart (reads version from Chart.yaml)"
helm dependency update "$CHART_DIR"
helm lint "$CHART_DIR"
helm package "$CHART_DIR" --destination "$OUT_DIR"

# Resolve freshly created package path and name
CHART_TGZ="$(ls -1t "$OUT_DIR"/"$CHART_NAME"-*.tgz | head -1)"
BASENAME="$(basename "$CHART_TGZ")"
REMOTE_TGZ_URL="$REPO_BASED_URL/$BASENAME"

echo "==> Built package: $CHART_TGZ"
echo "==> Remote path : $REMOTE_TGZ_URL"

# Function: return 0 if remote file exists (HTTP 200), else nonzero
remote_exists() {
  local url="$1"
  local code
  # Use HEAD request; follow redirects; fail on >=400; capture http code
  code="$(curl -fsSL -u "$ARTIFACTORY_USER:$ARTIFACTORY_PASSWORD" \
           -o /dev/null -I -w '%{http_code}' "$url" || true)"
  [[ "$code" == "200" ]]
}

if remote_exists "$REMOTE_TGZ_URL"; then
  echo "==> SKIP: $BASENAME already exists in remote. No upload performed."
  echo "Chart URL: $REMOTE_TGZ_URL"
  echo "Repo URL : $REPO_BASED_URL"
  exit 0
fi

# If not present remotely, (optionally) merge existing index.yaml you host yourself.
# NOTE: If this repo is a Helm (calculated) repo, you can skip all index.yaml work.
EXISTING_INDEX="$OUT_DIR/index-old.yaml"
echo "==> Fetch existing index.yaml to merge (ignore if 404)"
if ! curl -fsS -u "$ARTIFACTORY_USER:$ARTIFACTORY_PASSWORD" \
     -o "$EXISTING_INDEX" \
     "$REPO_BASED_URL/index.yaml"; then
  printf "apiVersion: v1\nentries: {}\n" > "$EXISTING_INDEX"
fi

echo "==> Merge and rebuild index.yaml"
helm repo index "$OUT_DIR" \
  --url "$REPO_BASED_URL" \
  --merge "$EXISTING_INDEX"

echo "==> Upload chart package: $BASENAME"
curl -fsSL -u "$ARTIFACTORY_USER:$ARTIFACTORY_PASSWORD" \
  -X PUT -T "$CHART_TGZ" \
  "$REMOTE_TGZ_URL"

echo "==> Upload merged index.yaml"
curl -fsSL -u "$ARTIFACTORY_USER:$ARTIFACTORY_PASSWORD" \
  -X PUT -T "$OUT_DIR/index.yaml" \
  "$REPO_BASED_URL/index.yaml"

echo "==> Done."
echo "Chart URL: $REMOTE_TGZ_URL"
echo "Repo URL : $REPO_BASED_URL"