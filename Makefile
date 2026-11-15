# The Go Docker client does not read Docker contexts, so tests that talk to
# the engine from the host need its socket spelled out.
export DOCKER_HOST ?= $(shell docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null)

.PHONY: up down demo usage counters test test-integration lint

up: ## build the image, start ClickHouse, ingest and the fake internet
	docker compose up -d --build --wait

down: ## remove the pairs launch created, then everything else
	-docker compose run --rm launch launch down
	docker compose down -v --remove-orphans

demo: up ## start the 17 pairs, let them run, show what each tenant stored
	docker compose run --rm launch
	@echo "letting the scenario run for 60 seconds (the last behaviour starts at 40 s)"; sleep 60
	@$(MAKE) --no-print-directory usage counters

usage: ## per-tenant usage from ClickHouse
	@docker compose exec -T clickhouse clickhouse-client --user tsi --password tsi-local-only --queries-file /usage.sql

counters: ## what each proxy counted: accepted, rejected by reason, egress by site class, blocked
	@for c in $$(docker ps --filter label=tsi.role=proxy --format '{{.Names}}' | sort); do \
		docker logs --tail 20 $$c 2>&1 | grep '"counters"' | tail -1 | cut -d, -f4- ; done

test: ## unit tests, no Docker needed
	go test -race -count=1 ./...

test-integration: up ## isolation tests against a real Docker engine
	go test -tags integration -count=1 -v ./internal/launch/

lint:
	go vet -tags integration ./...
	golangci-lint run --build-tags integration
