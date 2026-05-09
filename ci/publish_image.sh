#!/bin/bash

set -euo pipefail

# Set up Java, Maven, Artifactory credentials, and the immutable version.
source ci/setup.sh

ACR_NAME="openaiapibase"
ACR_SUBSCRIPTION="api"
ACR_IMAGE_REPOSITORY="${ACR_NAME}.azurecr.io/celeborn/celeborn"

AWS_ACCOUNT_ID="235562991189"
AWS_REGION="us-east-1"
AWS_ROLE="applied-ci-staging"
AZURE_TENANT_ID="a48cca56-e6da-484e-a814-9c849652bcb3"
ECR_REPOSITORY="celeborn/celeborn"
ECR_HOST="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
ECR_IMAGE_REPOSITORY="${ECR_HOST}/${ECR_REPOSITORY}"

DIST_PROFILES="-Pspark-3.5 -Pjdk-17 -Paws -Dproject.version=${VERSION}"

create_buildbox_context() {
  local context_name="$1"
  local service_name="$2"
  local description="$3"

  if docker context inspect "${context_name}" >/dev/null 2>&1; then
    return
  fi

  local ip
  ip="$(
    getent ahostsv4 "${service_name}" \
      | awk 'NR == 1 {print $1}'
  )"

  if [[ -z "${ip}" ]]; then
    echo "Build-pod service ${service_name} resolved to no addresses" >&2
    exit 1
  fi

  local host="${ip//./-}.all-${service_name}"
  echo "Creating Docker context ${context_name} for ${service_name} at tcp://${host}:2376"

  docker context create "${context_name}" \
    --description "${description}" \
    --docker "host=tcp://${host}:2376,skip-tls-verify=false,ca=/etc/secrets/tls/ca.pem,cert=/etc/secrets/tls/cert.pem,key=/etc/secrets/tls/key.pem"
}

# Authenticate to both registries before starting expensive image builds.
authenticate_acr() {
  az acr login \
    --name "${ACR_NAME}" \
    --subscription "${ACR_SUBSCRIPTION}" \
    --expose-token \
    --only-show-errors \
    --output tsv \
    --query accessToken \
    | docker login \
        "${ACR_NAME}.azurecr.io" \
        --username 00000000-0000-0000-0000-000000000000 \
        --password-stdin
}

authenticate_ecr() {
  AWS_TOKEN_FILE="$(mktemp)"
  trap 'rm -f "${AWS_TOKEN_FILE}"' EXIT

  az account get-access-token \
    --resource "api://${AZURE_TENANT_ID}/aws/${AWS_ACCOUNT_ID}/${AWS_ROLE}" \
    --tenant "${AZURE_TENANT_ID}" \
    -o tsv \
    --query accessToken \
    > "${AWS_TOKEN_FILE}"

  unset AWS_PROFILE AWS_DEFAULT_PROFILE
  export AWS_WEB_IDENTITY_TOKEN_FILE="${AWS_TOKEN_FILE}"
  export AWS_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${AWS_ROLE}"
  export AWS_REGION
  export AWS_DEFAULT_REGION="${AWS_REGION}"

  aws ecr get-login-password \
    --region "${AWS_REGION}" \
    | docker login \
        --username AWS \
        --password-stdin "${ECR_HOST}"
}

# Build and push the architecture-specific tags. The Dockerfile's
# runtime-from-source target runs build/make-distribution.sh inside each
# TARGETARCH build, so native artifacts stay matched to the image platform.
build_platform_image() {
  local platform="$1"
  local arch="$2"
  local docker_context="$3"

  docker --context "${docker_context}" buildx build \
    --platform="${platform}" \
    --target=runtime-from-source \
    --build-arg builder_java_image_tag=17-jdk-noble \
    --build-arg java_image_tag=17-jdk-noble \
    --build-arg "DIST_PROFILES=${DIST_PROFILES}" \
    -f docker/Dockerfile \
    -t "${ACR_IMAGE_REPOSITORY}:${IMAGE_VERSION}-${arch}" \
    -t "${ECR_IMAGE_REPOSITORY}:${IMAGE_VERSION}-${arch}" \
    --push \
    .
}

# Publish immutable multi-arch tags after both per-platform images are present.
publish_manifest() {
  local image="$1"

  docker buildx imagetools create \
    --tag "${image}" \
    "${image}-amd64" \
    "${image}-arm64"
}

authenticate_acr
authenticate_ecr

create_buildbox_context "build-box-amd64" "applied-build-pod" "Engineering Acceleration Buildbox (x86_64)"
create_buildbox_context "build-box-arm64" "applied-build-pod-arm" "Engineering Acceleration Buildbox (arm64)"

build_platform_image "linux/amd64" "amd64" "build-box-amd64"
build_platform_image "linux/arm64" "arm64" "build-box-arm64"

publish_manifest "${ACR_IMAGE_REPOSITORY}:${IMAGE_VERSION}"
publish_manifest "${ECR_IMAGE_REPOSITORY}:${IMAGE_VERSION}"

echo "Published multi-arch images:"
echo "  ${ACR_IMAGE_REPOSITORY}:${IMAGE_VERSION}"
echo "  ${ECR_IMAGE_REPOSITORY}:${IMAGE_VERSION}"
