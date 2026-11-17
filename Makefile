# The Go Docker client does not read Docker contexts, so tests that talk to
# the engine from the host need its socket spelled out.
export DOCKER_HOST ?= $(shell docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null)

.PHONY: up down demo record ab usage actions test test-integration lint

NAME ?= default

QUERY = docker compose exec -T clickhouse clickhouse-client --user tsi --password tsi-local-only --queries-file

up: ## build the image, start ClickHouse, ingest, the guard and the fake internet
	docker compose up -d --build --wait

down: ## remove the pairs launch created, then everything else
	-docker compose run --rm launch launch down
	docker compose down -v --remove-orphans

# Every run starts clean. The guard remembers sandboxes by ID and a new run
# uses the same IDs, so the old pairs are removed, the guard is restarted and
# the tables are emptied.
demo: up ## start the 17 pairs, watch the guard contain the bad ones, show the audit trail and the cost per tenant
	@docker compose run --rm launch launch down
	@docker compose restart guard
	@for t in events guard_actions usage; do docker compose exec -T clickhouse clickhouse-client --user tsi --password tsi-local-only -q "TRUNCATE TABLE tsi.$$t"; done
	docker compose run --rm launch
	@$(MAKE) --no-print-directory actions usage

# A recording run. compose.record.yaml puts every rule in shadow, so the
# traffic is not shaped by any rule set, and has the guard write each
# heartbeat to NAME.ndjson.part. Only a run that completes, with no gap in
# the recording, replaces the committed one. It is copied by you and not
# written by the guard, which runs as root. Whatever happens, the cleanup
# trap removes the pairs and the guard goes back to enforcing.
RECORDING = recordings/$(NAME).ndjson
record: ## record scenarios/NAME.yaml into recordings/NAME.ndjson: make record NAME=default
	@trap 'rm -f $(RECORDING).part; docker compose run --rm launch launch down; $(MAKE) --no-print-directory up' EXIT; \
	trap 'exit 130' INT TERM; \
	COMPOSE_FILE=compose.yaml:compose.record.yaml TSI_RECORD=/$(RECORDING).part TSI_SCENARIO=/scenarios/$(NAME).yaml $(MAKE) demo && \
	docker compose stop guard && \
	if [ -s $(RECORDING).part ]; then cp $(RECORDING).part $(RECORDING); \
	else echo "the recording has a gap and was not kept, see: docker compose logs guard" >&2; exit 1; fi

ab: ## replay the committed recordings through rule sets A and B. No Docker needed.
	@for r in recordings/*.ndjson; do go run ./cmd/tsi ab --recording $$r --a rules/a.yaml --b rules/b.yaml || exit 1; done

actions: ## the audit trail: every decision the guard made
	@$(QUERY) /actions.sql

usage: ## cost per tenant, from the usage table the guard writes
	@$(QUERY) /usage.sql

test: ## unit tests, no Docker needed
	go test -race -count=1 ./...

test-integration: up ## isolation tests against a real Docker engine
	go test -tags integration -count=1 -v ./internal/launch/

lint:
	go vet -tags integration ./...
	golangci-lint run --build-tags integration
