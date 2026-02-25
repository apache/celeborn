REGISTRY ?= openaiapibase.azurecr.io/celeborn/celeborn
VERSION  ?= 0.6.1
PLATFORM ?= linux/amd64
DIST_DIR ?= ./dist
RELEASE_DIST_ROOT ?= .release-dist
BUILDX_BUILDER ?= build-box
OPENAI_CHECKOUT ?= $(HOME)/code/openai
SOURCE_IMAGE_TARGET ?= runtime-from-source

DATE := $(shell date +%Y-%m-%d)
SHA  := $(shell git rev-parse --short HEAD)
TAG  := $(VERSION)-$(DATE)-$(SHA)

IMAGE := $(REGISTRY):$(TAG)
AMD64_IMAGE := $(IMAGE)-amd64
ARM64_IMAGE := $(IMAGE)-arm64
AMD64_DIST_DIR := $(RELEASE_DIST_ROOT)/linux-amd64
ARM64_DIST_DIR := $(RELEASE_DIST_ROOT)/linux-arm64

# Default target
.DEFAULT_GOAL := help

.PHONY: dist dist-amd64 dist-arm64 build-docker-image build-docker-image-amd64 build-docker-image-arm64 build-docker-image-buildbox build-release-image build-release-image-buildbox build-release-manifest help

# --- HELP -------------------------------------------------------------

# `make help` will print all targets with comments like:   target   description
help:
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_.-]+:.*##/ \
	{ printf "  \033[36m%-28s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""


# --- TARGETS ----------------------------------------------------------
dist: ## Build Celeborn distribution locally (requires Java 17)
	@echo ">>> Building Celeborn distribution (Java 17) ..."
	@if [ -z "$$JAVA_HOME" ]; then \
	  echo "ERROR: JAVA_HOME is not set. Please export JAVA_HOME to a JDK 17."; \
	  echo "If on Mac, you can use `brew install openjdk@17` to install JDK 17"; \
	  exit 1; \
	fi
	@if [ ! -x "$$JAVA_HOME/bin/java" ]; then \
	  echo "ERROR: $$JAVA_HOME/bin/java not found or not executable."; \
	  exit 1; \
	fi
	@ver=`"$$JAVA_HOME/bin/java" -version 2>&1 | head -n 1`; \
	case "$$ver" in \
	  *\"17.*\"*) ;; \
	  *) \
	    echo "ERROR: Java 17 required. Current is:"; \
	    "$$JAVA_HOME/bin/java" -version 2>&1 | sed 's/^/  /'; \
	    exit 1; \
	    ;; \
	esac
	./build/make-distribution.sh -Pspark-3.5 -Pjdk-17
	@echo ">>> ✅ Built Celeborn distribution."

dist-amd64: ## Build and stage a Linux amd64 release dist on a Linux amd64 builder
	@if [ "$$(uname -s)" != "Linux" ]; then \
	  echo "ERROR: dist-amd64 must run on Linux."; \
	  exit 1; \
	fi
	@if [ "$$(uname -m)" != "x86_64" ] && [ "$$(uname -m)" != "amd64" ]; then \
	  echo "ERROR: dist-amd64 must run on a Linux amd64 builder."; \
	  exit 1; \
	fi
	@$(MAKE) dist
	@rm -rf "$(AMD64_DIST_DIR)"
	@mkdir -p "$(AMD64_DIST_DIR)"
	@cp -R ./dist/. "$(AMD64_DIST_DIR)"
	@echo ">>> ✅ Staged Linux amd64 dist at $(AMD64_DIST_DIR)"

dist-arm64: ## Build and stage a Linux arm64 release dist on a Linux arm64 builder
	@if [ "$$(uname -s)" != "Linux" ]; then \
	  echo "ERROR: dist-arm64 must run on Linux so Maven auto-activates the aarch64 profile."; \
	  exit 1; \
	fi
	@if [ "$$(uname -m)" != "aarch64" ] && [ "$$(uname -m)" != "arm64" ]; then \
	  echo "ERROR: dist-arm64 must run on a Linux arm64 builder."; \
	  exit 1; \
	fi
	@$(MAKE) dist
	@rm -rf "$(ARM64_DIST_DIR)"
	@mkdir -p "$(ARM64_DIST_DIR)"
	@cp -R ./dist/. "$(ARM64_DIST_DIR)"
	@echo ">>> ✅ Staged Linux arm64 dist at $(ARM64_DIST_DIR)"

build-docker-image: ## Build + push a single-platform release docker image from DIST_DIR
	@if echo "$(PLATFORM)" | grep -q ','; then \
	  echo "ERROR: build-docker-image only accepts a single platform."; \
	  echo "Build per-arch images separately, then run make build-release-manifest."; \
	  exit 1; \
	fi
	@echo ">>> Building and pushing Celeborn release image: $(IMAGE)"
	docker buildx build -f docker/Dockerfile \
	  --platform=$(PLATFORM) \
	  --build-arg java_image_tag=17-jdk-noble \
	  -t $(IMAGE) \
	  --push \
	  $(DIST_DIR)
	@echo ">>> ✅ Pushed: $(IMAGE)"
	@echo ">>> 📌 Tag: $(TAG)"

build-docker-image-amd64: dist-amd64 ## Build + push the Linux amd64 release image from staged dist
	@$(MAKE) build-docker-image IMAGE=$(AMD64_IMAGE) PLATFORM=linux/amd64 DIST_DIR=$(AMD64_DIST_DIR)

build-docker-image-arm64: dist-arm64 ## Build + push the Linux arm64 release image from staged dist
	@$(MAKE) build-docker-image IMAGE=$(ARM64_IMAGE) PLATFORM=linux/arm64 DIST_DIR=$(ARM64_DIST_DIR)

build-docker-image-buildbox: ## Build + push from source on a build-box builder (supports single or multi-platform)
	@echo ">>> Building and pushing Celeborn release image via buildx builder $(BUILDX_BUILDER): $(IMAGE)"
	@if ! docker buildx inspect "$(BUILDX_BUILDER)" >/dev/null 2>&1; then \
	  if [ ! -d "$(OPENAI_CHECKOUT)" ]; then \
	    echo "ERROR: buildx builder $(BUILDX_BUILDER) not found and OPENAI_CHECKOUT=$(OPENAI_CHECKOUT) does not exist."; \
	    echo "Set OPENAI_CHECKOUT to your openai checkout, or preconfigure the builder manually."; \
	    exit 1; \
	  fi; \
	  echo ">>> buildx builder $(BUILDX_BUILDER) not found; configuring buildbox from $(OPENAI_CHECKOUT) ..."; \
	  (cd "$(OPENAI_CHECKOUT)" && applied configure buildbox --build-pod); \
	fi
	docker buildx build -f docker/Dockerfile \
	  --builder=$(BUILDX_BUILDER) \
	  --target=$(SOURCE_IMAGE_TARGET) \
	  --platform=$(PLATFORM) \
	  --build-arg builder_java_image_tag=17-jdk-noble \
	  --build-arg java_image_tag=17-jdk-noble \
	  -t $(IMAGE) \
	  --push \
	  .
	@echo ">>> ✅ Pushed: $(IMAGE)"
	@echo ">>> 📌 Tag: $(TAG)"

build-release-image: dist build-docker-image ## Full single-platform release: build local dist + push image
	@echo ">>> 🎉 Release complete: $(IMAGE)"

build-release-image-buildbox: build-docker-image-buildbox ## Full release from source via build-box builder
	@echo ">>> 🎉 Release complete: $(IMAGE)"

build-release-manifest: ## Combine the pushed amd64 and arm64 images into a multi-arch tag
	@echo ">>> Creating multi-arch manifest: $(IMAGE)"
	docker buildx imagetools create -t $(IMAGE) $(AMD64_IMAGE) $(ARM64_IMAGE)
	@echo ">>> ✅ Published multi-arch manifest: $(IMAGE)"
