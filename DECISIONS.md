# Decisions

One short entry per real tradeoff: what was chosen, what was rejected, and why.

## 1. Site classes with default deny, not a plain allowlist

- **Chose:** every domain has a class set in config: `trusted` (allowed), `watched` (allowed, bytes counted separately) or `bad` (blocked, attempts counted). Anything unlisted is denied. Watched bytes and bad-site attempts are rule variables.
- **Rejected:** a single allowlist where a domain is either allowed or blocked.
- **Why:** with a plain allowlist an upload to an unapproved domain is blocked on byte zero. The guard then has nothing to detect and the report column "egress bytes sent before the block" is always 0. Real egress policy also has a middle ground: sites that are legitimate but can be used to move data out (paste sites, file hosts). `watched` is that middle ground. Separating `bad` from unlisted lets a rule treat a call to a known bad host as much stronger evidence than a typo.

## 2. Events and egress are two independent controls

- **Chose:** the policy a proxy receives has two separate parts: an event rate and an egress switch. `block_egress` only stops outbound calls. "Cut off" means `throttle` with rate zero. The ingest deny list is killed sandboxes plus zero-rate sandboxes.
- **Rejected:** treating `block_egress` as a quarantine that also stops events.
- **Why:** blocking egress must not discard a paid tenant's data without human approval. Outbound calls are where the harm to others is; events are the tenant's own data and dropping them is not reversible. Keeping the controls apart also keeps the ladder honest: each step removes one capability, so the audit trail says exactly what was taken away.

## 3. Usage rows are written by the guard

- **Chose:** the guard writes one `usage` row per sandbox per second from the counters it already receives in heartbeats.
- **Rejected:** a second write path from each proxy through ingest.
- **Why:** one fewer path to build, bound and explain, and the proxy keeps a single upstream for control data. The cost: usage has gaps while the guard is down. Counters in heartbeats are cumulative, so the first heartbeat after an outage carries the missed totals and nothing is lost for billing, only the per-second shape.
- **Rows hold running totals, not growth.** Rejected: writing the growth since the last row. A guard that restarted has no last row, so it would either lose everything before the restart or count it twice. With totals no state is needed to write a row.
- **Each row also carries when its proxy started.** A proxy that restarts counts from zero again, so "the largest value seen" would lose everything before the restart. Usage is the largest value of each run, added up. Rejected: (a) taking a value that went down as a restart, in the query: it depends on row order, and misses a restart after which the counters had already passed their old values. (b) an epoch number kept by the guard: the guard forgets it when it restarts, and would then count one run twice.

## 4. One internal network per sandbox, with explicit subnets

- **Chose:** `launch` creates one bridge network per sandbox with an explicit /29 subnet from a configurable base, marked `internal` with gateway mode `isolated`. The sandbox joins only that network. Its proxy joins it plus the control and internet networks. Addresses are assigned explicitly.
- **Rejected:** (a) one shared sandbox network: sandboxes could reach each other and other proxies. (b) the sandbox sharing its proxy's network namespace: it would inherit the proxy's routes to guard, ingest and the internet. (c) no network at all and a Unix socket in a shared volume: strongest isolation, but untrusted code would need a custom dialer in place of plain `HTTP_PROXY`. (d) letting Docker pick subnets: the default pools allow about 30 networks in total, shared with everything else on the machine.
- **Why:** isolation comes from topology, not from code that could have a bug. A sandbox has one address, one link-scope route and no default route. `isolated` also removes the bridge address on the host, which a plain `internal` network leaves reachable. Explicit /29 subnets give room for 8,192 networks. The honest limit is elsewhere: every network is a Linux bridge plus firewall rules, which is fine at 20 and not at thousands.

## 5. Dependency choices

- **YAML:** `go.yaml.in/yaml/v3`. Rejected `gopkg.in/yaml.v3` (last release 2022, unmaintained) and v4 of the maintained fork (days old when this was written). cel-go already depends on v3 of the fork, so it adds no module.
- **Docker client:** `github.com/moby/moby/client`. Rejected `github.com/docker/docker`, the old import path, which stopped receiving releases at v28.

## 6. At-least-once delivery, by peeking the buffer

- **Chose:** the proxy and ingest each hold a bounded queue. A batch is read with Peek, sent, and removed with Drop only after the next hop accepted it. A failed send is retried with the same events.
- **Rejected:** (a) dropping a batch when the send fails: simple, but a short ingest outage silently loses accepted data. (b) a durable queue between proxy and ingest: the right answer at real scale and a non-goal here.
- **Why:** once the gateway answered 200 the event is ours to keep. The cost is duplicates: if ingest stores a batch and the reply is lost, the proxy sends it again. `event_id` is assigned once at the gateway, so duplicates share an ID and can be removed at read time. The demo does not deduplicate.
- **Two edges:** on a normal stop, a proxy and ingest first close their listeners and then hand over what they still hold, within a few seconds. And a batch that ingest refuses with a 4xx other than 429 is dropped, counted and logged, not retried: it can never succeed, and retrying would block every event behind it.

## 7. A batch that does not fit the token bucket is refused whole

- **Chose:** if the valid events in a request need more tokens than the bucket has, the whole request gets a 429 with `Retry-After`.
- **Rejected:** accepting as many events as there are tokens and refusing the rest.
- **Why:** a producer that gets "all or nothing" has one simple rule: wait, then resend the same request. Partial acceptance needs per-event bookkeeping on both sides. A batch larger than the bucket could never succeed, so it gets `batch_too_large` and not `rate_limited`, which would invite a pointless retry.
- **What a refusal costs:** nothing. If the buffer turns out to be full the tokens are given back, and bad events in a refused batch are counted only when the batch finally gets its 200. Counting them on every attempt would inflate the reject numbers the guard reads. The 429 still lists them, so the producer can fix them before it retries.

## 8. Compose runs the fixed parts, launch runs the pairs

- **Chose:** ClickHouse, ingest and the fake internet are Compose services. Sandbox and proxy pairs are created by `tsi launch` through the Docker API.
- **Rejected:** (a) everything in Compose: 17 near-identical pairs and 17 networks written out by hand, and no way to drive them from a scenario file. (b) everything through the Docker API: more code for no gain, Compose already does health checks and ordering.
- **Why:** the pairs are the part that varies per scenario and the part where the network layout is the design. Keeping them in code puts that layout in one function that can be read and tested.

## 9. ClickHouse table layout

- **Chose:** one shared `events` table, `ORDER BY (tenant_id, sandbox_id, event_time)`, partitioned by day, `tenant_id` as a plain `String`, written with explicit client-side batches.
- **Rejected:** (a) `LowCardinality(String)` for `tenant_id`: built for a few thousand distinct values, and many tiny tenants is the opposite. (b) server-side `async_insert`: it batches for you, but then backpressure is ClickHouse's decision and not ours. We want a full buffer to turn into a 429 at the gateway.
- **Why:** daily partitions are only safe because the proxy clamps `event_time`. That dependency is the reason the clamp exists, and it is written next to both.

## 10. The demo's fake hosts speak plain HTTP

- **Chose:** the fake internet serves HTTP only. The proxy supports HTTPS tunnels (CONNECT) and a unit test exercises one against a local TLS server.
- **Rejected:** generating a throwaway certificate in `fakenet` and having the simulated sandbox skip verification.
- **Why:** about 40 lines that would show nothing new. For a tunnel the proxy can only see the host name and count bytes, and both are covered by the test. The limit is stated in the README: the live demo never opens a tunnel.

## 11. Queues are bounded in bytes as well as in events

- **Chose:** every queue has two limits, a number of events and a number of bytes, and refuses a push that would pass either. Ingest also gets `GOMEMLIMIT` below its container limit.
- **Rejected:** a limit in events only. 50,000 events of 4 KiB are about 205 MB, in a container that is allowed 256 MB.
- **Why:** memory is what runs out, and an event can be 30 bytes or 4 KiB. With an event limit alone, a slow ClickHouse would get ingest killed before it ever answered 429. The event limit stays because it keeps one insert a predictable size.

## 12. Ingest checks what it receives

- **Chose:** ingest refuses a batch that contains an event no proxy would send (no tenant, no sandbox, no time, an ID that is not a UUID, an unknown type, an oversized payload).
- **Rejected:** accepting everything and dropping a batch when the insert fails with a permanent error. That means deciding which driver errors are permanent, and a wrong guess either throws away good data or stalls again.
- **Why:** the queue is shared by every tenant and a failed insert is retried. One event the store can never take would block everyone until a restart. Checking at the door keeps the retry loop simple: whatever is in the queue is storable, so an insert error means the store is unwell and pushing back is right.

## 13. A sandbox is judged when its heartbeat arrives

- **Chose:** the guard evaluates the rules for a sandbox inside the heartbeat request, and the reply carries the result. A timer only lifts expired actions.
- **Rejected:** evaluating every sandbox on a one-second timer. A decision would then wait for the next tick and after that for the next heartbeat to reach the proxy: up to two seconds for nothing.
- **Why:** policy rides on the heartbeat reply, so the fastest path from evidence to enforcement is to decide in that same round trip. In the demo the single flood is throttled 2.9 seconds after it starts: one second until a heartbeat shows it, two for the rule's `for`. It also makes replay simple: one recorded heartbeat in, one decision out, no timer to line up.

## 14. A window is the last three heartbeats

- **Chose:** a window is a fixed number of heartbeats (three, so two seconds). Rates divide by the real time between the first and the last.
- **Rejected:** (a) a window of fixed duration: with heartbeats one second apart, a millisecond of jitter decides whether it holds two or three intervals, and the rates jump. (b) a longer window, say ten seconds: smoother, but a flood needs several seconds to move it, and the demo must contain one within five.
- **Why:** two seconds smooths one odd second and still reacts at once. Telling a short burst from a flood is not the window's job, it is what `for` is for.

## 15. A baseline does not learn from what a rule objects to

- **Chose:** a sandbox's baseline is updated only when no rule's expression is true for any sandbox of its tenant. Its first window is taken as its normal on the same condition, and until then its baseline is zero.
- **Rejected:** always updating. A flood that lasts long enough then becomes the new normal, "six times the baseline" stops being true, the action expires and the flood is back.
- **Why:** the baseline is meant to describe normal behaviour, and the rules are the best available opinion on what is not. The cost: a rule that is wrong keeps the baseline frozen for as long as it fires, shadow rules included.
- **A tenant's baseline is the sum of its sandboxes' baselines.** Rejected: a slow average of the tenant's total rate, which is what this started as. It was seeded by whichever sandbox reported first, so a tenant starting four busy sandboxes looked like a split flood, and the freeze above then made that permanent. With the sum, a sandbox that starts or leaves moves the tenant's rate and baseline together. The cost: sandboxes that are bad from their first second raise the baseline with them, so only a fixed limit catches a split flood made of new sandboxes. While a rule is firing in the tenant a new sandbox gets no baseline, so it cannot be used to dilute a flood that is under way.
- **After a guard restart, a sandbox that comes back under a policy gets no baseline from what it is doing now.** Rejected: seeding it like a new sandbox. The first thing the new guard sees of a throttled flood is the flood, and with that as its baseline the rule never fires again. With baseline zero the fixed part of the rule decides, and a sandbox that has calmed down gets its baseline at its first quiet heartbeat. The cost: a rule with no fixed part keeps such a sandbox contained.

## 16. Escalation is the same expression with a longer `for`

- **Chose:** the ladder is not built into the guard. A rule set lists one rule per step: the same expression, a longer `for`, a harsher action. All actions held on a sandbox are merged and the strictest wins on each control.
- **Rejected:** rules with a list of steps, or a variable such as `throttled` that later rules could test. Both add state to the rule language.
- **Why:** each rule stays one line that can be read, tested and put in shadow on its own. The audit trail shows every step with the numbers it was taken on.

## 17. An action hides the evidence for it

- **Chose:** `ttl` is the only thing that ends an action, and the shipped `block_egress` rule has a long one (ten minutes). The proxy also checks the site class before the block, so a call to a bad site is still counted as one while egress is blocked.
- **Rejected:** a short `ttl` everywhere. Once egress is blocked no watched bytes flow, the rule goes quiet, the block is lifted and the upload resumes until the rule fires again: a leak of a few seconds every cycle.
- **Why:** a throttled flood keeps offering events, so its rule keeps firing and the throttle holds. A blocked upload looks cured. The guard cannot tell cured from blocked, so the `ttl` is really "how long until this sandbox gets another chance". This is the same blind spot that makes replay open loop.

## 18. A kill is a decision first and a Docker call second

- **Chose:** when a rule kills, the guard first marks the sandbox as killed, which cuts its events and egress by policy and puts it on the deny list, whatever happens next. Then it asks the runtime to stop the containers, with SIGKILL, sandbox first. The containers are kept, stopped.
- **Rejected:** (a) treating the Docker call as the kill: if it fails the sandbox runs on. (b) a graceful stop: the proxy would hand over its buffer on the way out, which is not what killing means.
- **Why:** fail closed. The Docker socket is the least reliable thing the guard touches, so containment must not depend on it. Keeping the containers means their logs can still be read.
- **A heartbeat from a killed sandbox is the sign that the kill did not take**, and the guard then asks the runtime again, at most every 10 seconds. Both containers are tried each time, and one that already stopped is not an error. Rejected: a retry timer of its own. The heartbeat is the evidence, and when it stops there is nothing left to kill.
- **Once a sandbox is killed no further rule is run against it.** Two kill rules that fire on the same heartbeat would otherwise kill it twice and use two slots of the blast radius cap.

## 19. What the guard does when it knows too little

- **A rule without `mode` is shadow.** Rejected: defaulting to enforce. A new rule should have to say that it may act.
- **A tenant without a tier is treated as paid.** Rejected: free. A kill cannot be taken back, so the guard does not kill what it cannot place.
- **A restarted guard takes over what each proxy enforces**, for 30 seconds. Rejected: answering "no restrictions" until the rules fire again, which would loosen every limit on every restart. What this means for baselines is in decision 15.
- **`buffer_full` is not held against a sandbox.** It is left out of `reject_ratio` and `offered_rate`. Rejected: counting every refusal. A slow ingest would then make every tenant look like an attacker.
- **A kill that automation may not carry out becomes `block_egress`**, for a paid tenant and for a kill over the blast radius cap alike. Rejected: (a) doing nothing when the cap refuses, which left the fourth sandbox calling a bad site fully open while a paid one doing the same was blocked. (b) the full cut-off: a wrong rule firing everywhere would then discard every tenant's events, which is the damage the cap is there to limit. Blocking egress stops the harm to others and keeps the tenant's data.
- **A kill refused by the blast radius cap is tried again** when the minute has room, while its rule still fires. Rejected: dropping it. The cap is a speed limit, not a pardon.
- **A throttle rule must say its rate.** Rejected: a default. Left out, it used to read as zero, which is the cut-off.
- **A sandbox silent for a minute, with nothing held against it, is forgotten.** Rejected: remembering every sandbox ever seen: the limit of 10,000 would then fill up for good and every new sandbox would go unjudged and unbilled. A kill is remembered for its `ttl`. A sandbox that comes back is admitted like a new one, and if its proxy still enforces a policy, like one from before a restart.
- **A store that is down delays the audit trail, not the guard.** Rows wait in memory up to a limit. Rejected: refusing to act without a written record. Every decision is also a log line.

## 20. Small choices


- **Settings by environment variable**, not flags or config files: it is what a container gets most easily, and a missing, unparsable, zero or negative limit falls back to the default limit, never to no limit or a crash.
- **UUIDv7 written by hand** (about 10 lines), not a dependency: the standard library has no UUID type and the layout is fixed by RFC 9562.
- **The proxy checks the caller's address** although the network already guarantees it: one `if`, and it turns a future mistake in the network layout into a refusal and not a leak.
- **The build image is pinned to the exact Go version in `go.mod`**: a floating tag can resolve to an older cached image and fail the build.
- **Start offsets count from each sandbox's own start**, not a shared clock: no coordination needed, and launch knows when it started each one.
- **`for` is compared with half a second of slack.** Heartbeats are a second apart give or take a millisecond, and without slack that millisecond decides whether a rule fires now or a second later.
- **The guard kills through the launcher's Docker code**, not a client of its own: the code that names and builds a pair is the code that stops it.
- **The timeline is polled from the guard by `launch`**, not read from ClickHouse: launch is not on the storage network, and it already knows when each behaviour starts.
