#!/bin/bash

set -euo pipefail

# Set up Java, Maven, and the generated version used by the rest of CI.
source ci/setup.sh

test_modules="${1:?Maven module list is required}"

# Compile all modules once per shard so selected module tests can use local artifacts.
build/mvn --no-transfer-progress -T 1C -DskipTests -Dspotless.check.skip=true install

# Run tests for only the module group assigned by the Buildkite step.
build/mvn --no-transfer-progress -T 1C --fail-at-end -pl "${test_modules}" test
