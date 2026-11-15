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

## 4. One internal network per sandbox, with explicit subnets

- **Chose:** `launch` creates one bridge network per sandbox with an explicit /29 subnet from a configurable base, marked `internal` with gateway mode `isolated`. The sandbox joins only that network. Its proxy joins it plus the control and internet networks. Addresses are assigned explicitly.
- **Rejected:** (a) one shared sandbox network: sandboxes could reach each other and other proxies. (b) the sandbox sharing its proxy's network namespace: it would inherit the proxy's routes to guard, ingest and the internet. (c) no network at all and a Unix socket in a shared volume: strongest isolation, but untrusted code would need a custom dialer in place of plain `HTTP_PROXY`. (d) letting Docker pick subnets: the default pools allow about 30 networks in total, shared with everything else on the machine.
- **Why:** isolation comes from topology, not from code that could have a bug. A sandbox has one address, one link-scope route and no default route. `isolated` also removes the bridge address on the host, which a plain `internal` network leaves reachable. Explicit /29 subnets give room for 8,192 networks. The honest limit is elsewhere: every network is a Linux bridge plus firewall rules, which is fine at 20 and not at thousands.

## 5. Dependency choices

- **YAML:** `go.yaml.in/yaml/v3`. Rejected `gopkg.in/yaml.v3` (last release 2022, unmaintained) and v4 of the maintained fork (days old when this was written). cel-go already depends on v3 of the fork, so it adds no module.
- **Docker client:** `github.com/moby/moby/client`. Rejected `github.com/docker/docker`, the old import path, which stopped receiving releases at v28.
