# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

PYTHON_IMAGE ?= python:3.11-slim
DOCKER ?= docker
WORKDIR ?= /workspace
PIP_CACHE ?= celeborn-pip-cache
DOCS_OUTPUT ?= site
DOCS_PORT ?= 8000
DOCKER_RUN = $(DOCKER) run --rm -v $(CURDIR):$(WORKDIR) -w $(WORKDIR) -v $(PIP_CACHE):/root/.cache/pip

.PHONY: help docs docs-serve docs-clean check-docker

help: ## Show help for available make targets
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS=":.*##"} {printf "  %-12s %s\n", $$1, $$2}'

check-docker:
	@command -v $(DOCKER) >/dev/null 2>&1 || { echo "Docker CLI '$(DOCKER)' not found"; exit 1; }

docs: check-docker ## Build the MkDocs site inside a Python 3.11 container
	$(DOCKER_RUN) $(PYTHON_IMAGE) /bin/sh -c "pip install -r requirements.txt && mkdocs build"

docs-serve: check-docker ## Run mkdocs serve inside Docker and expose DOCS_PORT
	$(DOCKER_RUN) -p $(DOCS_PORT):$(DOCS_PORT) -it $(PYTHON_IMAGE) /bin/sh -c "pip install -r requirements.txt && mkdocs serve -a 0.0.0.0:$(DOCS_PORT)"

docs-clean: ## Remove the generated site directory
	rm -rf $(DOCS_OUTPUT)
