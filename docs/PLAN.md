# Plan

Status: approved. Stage 1 is merged, stage 2 is delivered for review, stage 3 is not started.

This file is the plan agreed before stage 1. The original brief is [SPEC.md](SPEC.md). Once code exists, `DECISIONS.md` is the source of truth for tradeoffs.

## Machine check (2026-11-15)

| Tool | Found | Note |
|---|---|---|
| Go | 1.27.4 (Homebrew) | current stable |
| golangci-lint | 2.15.0 | current |
| Docker engine | 29.8.2 (OrbStack 2.2.5), API 1.56, arm64 | VM has 10 cores and 12 GB |
| Docker CLI | 29.8.2, bundled with OrbStack | |
| Compose | v5.5.0 plugin (`docker compose`), bundled with OrbStack | buildx v0.37.2 also bundled |
| Host | Apple M5, 10 cores, 24 GB | |

## Dependency versions (looked up, not remembered)

| Dependency | Version | Note |
|---|---|---|
| `github.com/google/cel-go` | v0.30.0 | |
| `github.com/ClickHouse/clickhouse-go/v2` | v2.51.0 | needs Go 1.26 or newer |
| `github.com/moby/moby/client` | v0.6.3 (api v1.57.0) | the old path `github.com/docker/docker` stopped at v28.5.2, so I use the new module |
| `go.yaml.in/yaml/v3` | v3.0.6 | maintained successor of `gopkg.in/yaml.v3` (last release 2022). cel-go already depends on it, so it adds no new module. v4.0.0 is 11 days old, too new. |
| ClickHouse server | 26.8 LTS image | amd64 and arm64 |
| Docker Engine (minimum) | 28.0 | first release with the `isolated` gateway mode used below |

## Network approach

One bridge network per sandbox, created by `launch` through the Docker API.

- Each network gets an explicit /29 subnet carved from a configurable base (default `10.77.0.0/16`, room for 8,192). Docker's default address pools (31 networks in total, shared with everything else on the machine) are never used.
- The network is `internal` (no NAT, no route out) with gateway mode `isolated` (the bridge has no address on the host, so the sandbox cannot reach host services either).
- The sandbox container joins only that network. Its proxy joins it plus `control` (guard, ingest) and `internet` (fake hosts).
- The proxy listens only on its address in the sandbox network and checks the peer address. It has no listener on `control` or `internet`.
- ClickHouse sits on a fourth network, `storage`, with only ingest and guard. A compromised proxy cannot reach it.
- Fake internet: one container on `internet` with aliases `allowed.test`, `paste.test`, `evil.test`.
- If the proxy dies, the sandbox's only link has nothing behind it. Fail closed by topology, not by code.

Rejected:

- **One shared sandbox network.** Sandboxes could reach each other and other proxies. Turning off inter-container traffic also cuts a sandbox off from its own proxy.
- **Sandbox shares the proxy's network namespace** (sidecar style). The sandbox inherits the proxy's routes to guard, ingest and internet. Fixing that needs firewall rules inside the namespace.
- **No network, Unix socket in a shared volume.** Strongest isolation and needs no networks, but untrusted code would need a custom dialer in place of plain `HTTP_PROXY`. Kept as the fallback if per-sandbox networks misbehave at 20.

Smoke test on this machine (one network, three throwaway containers, since removed): the sandbox got one address, one link-scope route and no default route. It reached its proxy and resolved the proxy's name. The guard on another network, a public IP and an external DNS name were all blocked. Two things learned:

- With `isolated` mode the bridge takes no address, so Docker hands the first address in the subnet to the first container. `launch` will assign addresses explicitly so the proxy's address is known before the sandbox starts.
- This engine's default pools are 30 networks of /24, different from upstream. Explicit subnets make that irrelevant.

Still to verify with 20 sandboxes: creation time, clean teardown by label, and a probe from inside a sandbox that reaches its own proxy and fails against guard, ingest, ClickHouse, fake internet, another sandbox's proxy, the gateway address, a public IP and an external DNS name.

## File layout

```
cmd/tsi/main.go        subcommand dispatch, flags and env
internal/clock/        Clock interface, real and fake
internal/event/        schema, validation, reason codes, UUIDv7
internal/sandbox/      the six behaviours, plus a probe mode for the isolation test
internal/proxy/        gateway.go, bucket.go, forward.go, egress.go, heartbeat.go (stage 2)
internal/ingest/       server.go (bounded queue, flush on rows or time), denylist.go (stage 2)
internal/chstore/      ClickHouse connection and inserts
internal/scenario/     scenario YAML types, shared by launch, guard and ab
internal/launch/       docker.go (networks, containers), timeline.go (stage 2)
internal/fakenet/      fake internet hosts
internal/guard/        window.go, rules.go, engine.go, server.go, runtime.go, audit.go (stage 2), recorder.go (stage 3)
internal/replay/       replay.go, report.go (stage 3)
clickhouse/            config.xml (memory cap), schema.sql, usage.sql
scenarios/default.yaml
rules/a.yaml, rules/b.yaml
recordings/
Dockerfile, compose.yaml, Makefile, .github/workflows/ci.yml
README.md, DECISIONS.md, LICENSE
```

Compose runs the fixed parts (ClickHouse, ingest, guard, fake internet). `launch` runs as a one-shot container with the Docker socket and creates the pairs. Quickstart is `make up` then `make demo`.

Rough budget: stage 1 about 1,100 lines, stage 2 about 750, stage 3 about 300. That is slightly over 2,000. First cuts if needed: TLS on the fake hosts, timeline formatting, `rejectstorm` (it is not in the default scenario).

## Spec readings, as agreed

The brief was updated to match these. Entries 1 to 3 have their reasoning in `DECISIONS.md`.

1. **Site classes.** Config sets each domain's class: `trusted`, `watched` or `bad`, with default deny. Watched bytes and bad-site attempts are rule variables.
2. **Two independent controls.** `block_egress` only stops outbound calls. "Cut off" means `throttle` with rate zero. The ingest deny list is killed sandboxes plus zero-rate sandboxes.
3. **Usage table.** The guard writes it from the counters it already receives. This moves the usage table and query to stage 2.
4. **Fake internet.** A seventh subcommand, `tsi fakenet`.
5. **Docker tests.** `make test` stays Docker-free. `make test-integration` runs the two isolation tests, and CI runs both.

## Things in the spec that will not work as written

- **Recordings must be captured with enforcement off.** If the guard enforces while recording, a killed sandbox stops producing snapshots and the traffic already reflects one rule set. A/B would be biased toward it. Plan: a guard flag that forces every rule to shadow for recording runs, and snapshots carry offered as well as accepted counts.
- **Guard restart loosens limits.** The spec covers the guard being unreachable, not a guard that comes back with empty memory and answers "no restrictions". Plan: the proxy echoes its applied policy in each heartbeat and a guard with no record adopts it.

## Settled

- **Git identity:** set in this repo only. Commits use the account's private relay address, so no personal address ends up in public history.
- **Remote:** `github.com/joachimzetterman/tiny-sandbox-infra`, private for now.
- **Flow:** one bootstrap commit on `main`, then each stage on a branch in a worktree (`../tiny-sandbox-infra-stage1`), delivered as a pull request.
- **Brief and plan:** both are committed under `docs/`. The brief is kept unedited, because deviations from it belong in `DECISIONS.md`.
- **Open:** the access token for the personal account lacks the `workflow` scope, so pushing the CI file will be rejected until `gh auth refresh -h github.com -s workflow` is run with that account active.
