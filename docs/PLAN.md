# Plan

Status: repo bootstrapped, no code written yet. Waiting for confirmation of the spec readings below.

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

## Spec readings I want you to confirm

1. **Allowlist versus `exfil`.** A strict allowlist blocks an upload to an unknown domain on byte zero, so "egress bytes sent before the block" would always be 0. Proposal: allowlist entries carry a class. `trusted` (`allowed.test`) and `watched` (`paste.test`: reachable, counted separately, this is the "unknown domain"). Anything unlisted is blocked and counted. `evil.test` is tagged `bad` so a rule can tell known bad from merely unlisted.
2. **"Cut off".** The deny list says "cut off or killed" but the ladder has no such step. Reading: `block_egress` is the cut-off. The proxy refuses all egress and all events (reason `cut_off`), and ingest drops that sandbox's events as the backstop.
3. **Usage query.** Rejects and egress bytes are not in `events`. Proposal: a third small table `usage`, one row per sandbox per second, written by the proxy through ingest. Metering then works in stage 1 and with the guard down.
4. **Fake internet.** A seventh subcommand `tsi fakenet` (about 50 lines) keeps one binary and one image.
5. **Docker tests.** The two isolation tests need a Docker engine. `make test` stays Docker-free; `make test-integration` runs them, and CI runs both.

## Things in the spec that will not work as written

- **Recordings must be captured with enforcement off.** If the guard enforces while recording, a killed sandbox stops producing snapshots and the traffic already reflects one rule set. A/B would be biased toward it. Plan: a guard flag that forces every rule to shadow for recording runs, and snapshots carry offered as well as accepted counts.
- **Guard restart loosens limits.** The spec covers the guard being unreachable, not a guard that comes back with empty memory and answers "no restrictions". Plan: the proxy echoes its applied policy in each heartbeat and a guard with no record adopts it.

## Settled

- **Git identity:** set in this repo only. Commits use the account's private relay address, so no personal address ends up in public history.
- **Remote:** `github.com/joachimzetterman/tiny-sandbox-infra`, private for now.
- **Flow:** one bootstrap commit on `main`, then each stage on a branch in a worktree (`../tiny-sandbox-infra-stage1`), delivered as a pull request.
- **Brief and plan:** both are committed under `docs/`. The brief is kept unedited, because deviations from it belong in `DECISIONS.md`.
- **Open:** the access token for the personal account lacks the `workflow` scope, so pushing the CI file will be rejected until `gh auth refresh -h github.com -s workflow` is run with that account active.
