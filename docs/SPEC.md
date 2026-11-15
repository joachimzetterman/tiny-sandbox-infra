# tiny-sandbox-infra: build spec

You are building a small open source project with me. Read this whole file before doing anything. Work in the three stages below and stop at the end of each one so I can review.

## What this is

A local, reproducible demo of control-plane infrastructure for running untrusted sandboxes for many tiny tenants. It has one idea:

> A guard that can throttle, cut off or kill a misbehaving sandbox within seconds, and a replay harness that A/B tests guard rules against recorded traffic before they are allowed to enforce.

The audience is senior infrastructure engineers who will spend ten minutes in the repo. They should be able to clone it, run two commands, watch a sandbox get contained, and read a report comparing two rule sets. Clarity and honesty matter more than features.

I will be asked to explain every design choice and every file. Optimise for code I can read end to end and defend.

## Ground rules

- **Language:** Go, one module, current stable version. Standard library first.
- **Allowed dependencies:** `cel-go` (rules), the official ClickHouse Go client, the Docker Go client, a YAML parser. Ask before adding anything else.
- **Check, do not assume:** look up current versions and APIs for every dependency and for ClickHouse before using them. Do not rely on memory.
- **Size:** about 2,000 lines of Go excluding tests and the optional stage 4. If a feature threatens that budget, cut the feature and tell me.
- **One binary:** a single `tsi` binary with subcommands (`sandbox`, `proxy`, `guard`, `ingest`, `launch`, `ab`, `fakenet`) and one Dockerfile.
- **Local only:** Docker Compose. No cloud accounts, no Kubernetes, no external services.
- **Laptop budget:** the full demo (about 40 containers including ClickHouse) must run in under 4 GB of RAM on a 16 GB laptop, on both Apple Silicon and x86. Cap ClickHouse's memory, keep the Go image small, and report the measured footprint in the README.
- **Docker access:** only `guard` and `launch` may talk to the Docker API. Mounting the Docker socket is acceptable here, but say in the README that it is a local shortcut and what would replace it in production.
- **Neutral:** no company names anywhere in the repo.
- **Style:** never use em dashes in code comments, docs or commit messages. Use commas, colons or parentheses. Plain, direct English. Comments explain why, not what.
- **Logging:** `log/slog`, structured, with `tenant_id` and `sandbox_id` on every line that has them.
- **Everything is bounded:** every buffer, batch, request body and outbound call has a size limit or a timeout. When a bound is hit, reject with a reason. Never grow or wait forever.
- **Fail closed on isolation, open on availability:** if a control component is down, existing limits stay in force and nothing loosens. If a proxy dies, its sandbox loses all connectivity.
- **Time is injected:** anything that reads the clock takes a clock interface, so tests and replay are deterministic.
- **Decisions log:** keep `DECISIONS.md`. One short entry per real tradeoff: what you chose, what you rejected, why.

## Architecture

Five roles, all in the one binary:

1. **sandbox**: simulates untrusted code. Behaviour is set by config: `normal`, `bursty` (legitimate spikes), `flood`, `exfil` (large upload to a watched domain), `baddest` (calls a known bad domain), `rejectstorm` (malformed events). Each behaviour can start at a given offset. It sends events to its proxy and makes outbound HTTP calls only through its proxy. Well-behaved behaviours (`normal`, `bursty`) back off when they get a 429.
2. **proxy**: one per sandbox, the trust boundary. Two jobs:
   - *Event gateway:* accepts event batches, overwrites `tenant_id` and `sandbox_id` with the values it was started with (whatever the sandbox sent is discarded), sets `event_id` (UUIDv7), clamps `event_time` to now plus or minus one hour, rejects oversized or unknown event types and counts every rejection, applies a token bucket, then batches and forwards to ingest. Every rejection carries a machine-readable reason code, so a well-behaved producer can fix itself instead of retrying forever. The buffer toward ingest is bounded: when it is full the gateway returns 429, it does not grow.
   - *Egress proxy:* forward proxy for all outbound HTTP and HTTPS (CONNECT). Every domain belongs to a site class, set in config: `trusted` (allowed), `watched` (allowed, and its bytes are counted separately) or `bad` (blocked, and every attempt is counted). Anything unlisted is denied by default. Counts bytes per domain.
   - Every second it reports counters to the guard (events/s, bytes/s, rejects by reason, egress bytes by domain, blocked attempts). The guard's reply carries the current policy for that sandbox, so policy rides on the heartbeat and the proxy needs no inbound control port. If the guard is unreachable the proxy keeps its last policy and its static limits.
3. **ingest**: receives batches from proxies and writes them to ClickHouse in few, large inserts (flush on rows or time). It also fetches a deny list from the guard (sandboxes that are killed, or cut off by a zero rate) and drops their events, so enforcement never depends on a single proxy behaving.
4. **guard**: keeps sliding windows per sandbox and tenant, evaluates rules, and applies the enforcement ladder.
5. **launch**: reads a scenario file and starts sandbox and proxy pairs through the Docker API. Also prints the live demo timeline.

### Network requirements

- A sandbox must have no route to anything except its own proxy. No direct egress, no access to ClickHouse, ingest or the guard.
- A proxy must only accept traffic from its own sandbox.
- Fake "internet" hosts (for example `allowed.test`, `paste.test`, `evil.test`) run as one small container (`tsi fakenet`) on a separate network that only proxies can reach.
- Note that Docker's default address pools limit how many networks you can create, so one network per sandbox may not scale. Pick an approach, test it with 20 sandboxes, and record the choice in `DECISIONS.md`.

### Guard

- **Two levels:** windows and limits exist per sandbox and per tenant, because a tenant can spread load across many sandboxes that each look harmless.
- **Baseline:** each window also exposes a baseline of that sandbox's or tenant's own recent behaviour (a slow moving average is enough). A rule can then ask whether a tenant is behaving unlike itself, not only whether it crossed a fixed limit.
- **Tiers:** every tenant is `free` or `paid`. Automation never kills a paid tenant's sandbox: the action is capped at `block_egress` and an approval request is logged for a human.
- **Rules** live in YAML. Each rule has: `name`, `expr` (CEL over the window aggregates), `for` (how long it must hold), `action`, `rate` (for `throttle` only: the new events per second), `mode` (`shadow` or `enforce`), and `ttl` (actions expire unless the rule still fires).
- **Ladder**, cheapest first: `observe`, `throttle` (shrink the token bucket; rate zero is the cut-off), `block_egress` (stop outbound calls only), `kill` (stop the sandbox and its proxy through the Docker API).
- **Two independent controls:** events and egress are limited separately. `block_egress` never discards events, so a paid tenant's data is not thrown away without human approval. "Cut off" always means `throttle` with rate zero.
- **Shadow mode:** a shadow rule records what it would have done and changes nothing.
- **Blast radius cap:** automation may kill at most a configured number of sandboxes per minute. Above that it stops killing, logs loudly that a human is needed, and keeps cheaper actions working.
- **Audit:** every decision, shadow or enforced, is written to ClickHouse with rule name, reason, inputs and expiry.
- **Rule variables:** define a small, fixed set of variables that CEL expressions can use (rates, reject ratio, egress bytes by site class including watched bytes, bad-site attempts, baselines, tier) and document them in the README.
- **Runtime interface:** killing goes through a small interface with two implementations: Docker, and a no-op used by replay and tests.

### ClickHouse

- `events`: shared table for all tenants, `ORDER BY (tenant_id, sandbox_id, event_time)`. Never a table or partition per tenant.
- `guard_actions`: the audit trail.
- `usage`: one row per sandbox per second, written by the guard from the counters it already receives in heartbeats. Proxies get no second write path.
- A per-tenant usage query over `usage` (events, bytes, rejects, egress bytes) to show that cost per tenant is measurable.

### Flight recorder and A/B replay

- The guard appends every counter snapshot it receives to an NDJSON recording, with a header holding the scenario's ground truth: which sandboxes misbehave, how, and from when.
- `tsi ab --recording R --a rules/a.yaml --b rules/b.yaml` replays the recording through both rule sets with the no-op runtime and prints a Markdown report. It must run without Docker or ClickHouse.
- Report, per rule set:
  - for each bad sandbox: time to first action, time to containment, events passed after onset, egress bytes sent before the block
  - misses: bad sandboxes that were never contained
  - false positives: good sandboxes that were throttled, blocked or killed
  - whether the blast radius cap triggered
- Replay is open loop: it cannot model how an action would have changed later traffic. Say so in the report footer and the README.
- Commit two or three small recordings so `make ab` works straight after cloning.

## Demo scenario

Ship this as the default: 17 sandboxes across 15 tenants.

| Sandboxes | Behaviour | Tier | Starts |
|---|---|---|---|
| 9 | `normal` | free | 0 s |
| 1 | `bursty` | paid | 0 s |
| 1 | `flood` | free | 10 s |
| 1 | `exfil` | free | 20 s |
| 1 | `baddest` | free | 30 s |
| 1 | `baddest` | paid | 35 s |
| 3, same tenant | `flood`, each just under the per-sandbox limit | free | 40 s |

Ship two rule sets:
- `rules/a.yaml`: baseline-relative thresholds and tenant-level limits. Contains every bad sandbox, catches the split flood, and never touches the bursty one.
- `rules/b.yaml`: fixed, aggressive, per-sandbox thresholds. Contains the single flood faster, but wrongly throttles the bursty sandbox and misses the split flood.

The A/B report should make that tradeoff obvious: B is faster, and it has one false positive and one miss. That is the point of the project.

The paid `baddest` sandbox must end with egress blocked and an approval request logged, not killed.

## Stages

Each stage must work on its own. At the end of each: tests pass, README is current, you commit, you give me a short summary of what you built and what you were unsure about, and you stop.

**Stage 1: proxy and sink.** `sandbox`, `proxy`, `ingest`, ClickHouse, Compose, `launch`. Events land with the trusted tenant ID. Tests prove: a spoofed `tenant_id` is overwritten, oversized and unknown events are rejected and counted, the token bucket returns 429 with `Retry-After`, an unlisted domain and a `bad` domain are blocked, `event_time` is clamped, rejections carry reason codes, and a slow ingest produces 429s and not a growing buffer. Integration tests (they need a Docker engine) prove: a sandbox cannot reach anything but its proxy, and a sandbox whose proxy dies can send nothing.

**Stage 2: guard.** Counters, windows, CEL rules, ladder, kill through Docker, blast radius cap, audit table, usage table and query, `make demo` with a readable live timeline (detected, throttled, egress blocked, killed, neighbours unaffected). Tests cover window maths, baselines, rule evaluation, `for` and `ttl` behaviour, tenant-level aggregation catching the split flood, the paid-tier cap, the blast radius cap, the ingest deny list, and the guard-down case. In the demo, the single flood must be contained within 5 seconds of onset, and the timeline prints the measured time.

**Stage 3: recorder and replay.** Recording, `tsi ab`, the report, committed recordings, `make ab`. Tests prove the replay is deterministic.

**Stage 4 (optional, only after I have reviewed stages 1 to 3): tenant-scoped reads.** A ClickHouse row policy keyed on a per-query setting, and a tiny `tsi query` that sets the tenant from the caller and allows read-only SQL with no table functions or system tables. A canary test proves a tenant can never read another tenant's rows, including with hostile SQL.

## Deliverables

- `make up`, `make down`, `make demo`, `make ab`, `make test` (no Docker needed), `make test-integration` (needs a Docker engine), `make lint`
- GitHub Actions workflow: vet, lint, `make test`, `make test-integration`, and `make ab` on the committed recordings
- `README.md` containing:
  - the one-paragraph pitch and a Mermaid architecture diagram
  - quickstart (two commands)
  - a pasted sample of the demo timeline and of the A/B report
  - **Limitations**, stated plainly: Docker containers are not a security boundary (a real system needs gVisor or microVMs), numbers are relative comparisons on a laptop and not capacity claims, replay is open loop
  - **What changes at real scale**, as a table mapping each demo piece to its production equivalent: stronger isolation, quota leases in place of per-sandbox buckets, a guard sharded by tenant, a quarantine pool, a durable queue between proxy and ingest, cells to cap blast radius
  - a `Design notes` link placeholder near the top, which I will fill in
- `DECISIONS.md`
- MIT licence

## Non-goals

Do not build: authentication, a UI, a message queue, a schema registry, multi-region, storage tiering, dashboards, or anything cloud-specific. If you think one of these is needed, ask first.

## How to work with me

1. Check the Go and Docker versions on this machine first.
2. Before writing code, write `PLAN.md`: the file layout, the network approach you intend to try, and any questions. Wait for my go-ahead.
3. Prefer the smallest design that satisfies the spec. When the spec is ambiguous, choose the simpler reading and note it in `DECISIONS.md`.
4. Small commits with clear messages.
5. If something in this spec is wrong or will not work, say so before working around it.
