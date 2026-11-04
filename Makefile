# Convenience wrappers for the single root Go module. No target invokes live RPC.
# Integration runners retain their isolated resources for explicit operator review.
.DEFAULT_GOAL := help
SHELL := /bin/bash

ENV_FILE ?= .env.fixture
PROJECT ?= ethereum-fixture
FIXTURES ?= .mainnet/fixtures/stage13-3b
OPT_IN ?= 0
OUT ?=
WORKERS ?= 1
BATCH_SIZE ?= 64
BLOCKS ?= 3
REPEAT ?= 1
WARMUP ?= 0

export ETHEREUM_LIVE_MAINNET_OPT_IN := false
export ETHEREUM_CAPTURE_OPT_IN := false
export ETL_TEST_RETAIN := 1

COMPOSE = docker compose --env-file "$(ENV_FILE)" -p "$(PROJECT)" -f deployments/compose/compose.yaml -f deployments/compose/fixture.yaml --profile fixture

.PHONY: help fmt vet build test test-race test-architecture test-integration test-e2e test-offline test-performance docker-init docker-up docker-down docker-logs clean-build

help: ## List targets without starting workloads (the default).
	@awk 'BEGIN {FS = ":.*## "} /^[a-z0-9-]+:.*## / {printf "%-22s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt: ## Format tracked handwritten Go; leave generated protobuf files unchanged.
	git ls-files -z '*.go' ':!:gen/**' | xargs -0 gofmt -w

vet: ## Analyze all packages in the root Go module.
	go vet ./...

build: ## Compile all ordinary packages; does not start services.
	go build ./...

test: ## Run ordinary offline unit tests, including architecture checks.
	go test -count=1 ./...

test-race: ## Run the ordinary offline suite with the race detector.
	go test -race -count=1 ./...

test-architecture: ## Check inward imports and service-owned schema boundaries.
	go test -count=1 -v ./test/architecture

test-integration: ## Require Docker; run three PostgreSQL gates and real gRPC handoff.
	@set -e; for runner in extractor-postgres transformer-postgres loader-postgres handoff-postgres; do bash scripts/ci-integration.sh "$$runner"; done

test-e2e: ## Require Docker; run the deterministic full fixture E2E gate.
	bash scripts/ci-integration.sh fixture-e2e

test-offline: ## Full final offline gate; requires OPT_IN=1 and local FIXTURES.
	@test "$(OPT_IN)" = 1 || { echo 'Set OPT_IN=1 to authorize the full offline workload.' >&2; exit 2; }
	bash scripts/validate-final.sh offline --opt-in "$(FIXTURES)"

test-performance: ## Bounded synthetic baseline; requires OPT_IN=1 and a new OUT directory.
	@test "$(OPT_IN)" = 1 && test -n "$(OUT)" || { echo 'Set OPT_IN=1 and OUT to a new report directory.' >&2; exit 2; }
	bash scripts/validate-final.sh performance --opt-in "$(OUT)" --dataset synthetic --workers "$(WORKERS)" --loader-batch-size "$(BATCH_SIZE)" --blocks "$(BLOCKS)" --repeat "$(REPEAT)" --warmup "$(WARMUP)"

docker-init: ## Generate private fixture credentials; refuses to overwrite ENV_FILE.
	go run ./test/integration/cmd/fixture-e2e --init "$(ENV_FILE)"

docker-up: ## Start the local synthetic fixture Compose project; no external Ethereum RPC.
	$(COMPOSE) up -d --build

docker-down: ## Stop the fixture project, retaining containers, networks and volumes.
	$(COMPOSE) stop

docker-logs: ## Show the last 100 lines per fixture service; no private Compose rendering.
	$(COMPOSE) logs --tail=100

clean-build: ## Remove only optional local service binaries under bin/; retain all data.
	rm -f -- bin/extractor bin/transformer bin/loader
