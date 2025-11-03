REGISTRY ?= openaiapibase.azurecr.io/celeborn/celeborn
VERSION  ?= 0.6.1

DATE := $(shell date +%Y-%m-%d)
SHA  := $(shell git rev-parse --short HEAD)
TAG  := $(VERSION)-$(DATE)-$(SHA)

IMAGE := $(REGISTRY):$(TAG)

# Default target
.DEFAULT_GOAL := help

.PHONY: dist build-docker-image build-release-image help

# --- HELP -------------------------------------------------------------

# `make help` will print all targets with comments like:   target   description
help:
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_.-]+:.*##/ \
	{ printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
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

build-docker-image: ## Build + push release docker image from local dist
	@echo ">>> Building and pushing Celeborn release image: $(IMAGE)"
	docker buildx build -f docker/Dockerfile \
	  --platform=linux/amd64 \
	  --build-arg java_image_tag=17-jdk-noble \
	  -t $(IMAGE) \
	  --push \
	  ./dist/
	@echo ">>> ✅ Pushed: $(IMAGE)"
	@echo ">>> 📌 Tag: $(TAG)"

build-release-image: dist build-docker-image ## Full release: build dist + push image
	@echo ">>> 🎉 Release complete: $(IMAGE)"
