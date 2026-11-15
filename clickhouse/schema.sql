-- One shared table for every tenant. Never a table or a partition per
-- tenant: with many tiny tenants that would mean thousands of tiny parts.
--
-- ORDER BY starts with tenant_id, so one tenant's rows sit together on disk
-- and a per-tenant query reads only that range.
--
-- tenant_id is a plain String, not LowCardinality: that type is built for
-- a few thousand distinct values, and "many tiny tenants" is the opposite.
--
-- PARTITION BY day is safe only because the proxy clamps event_time to now
-- plus or minus one hour. Unclamped, a sandbox could create a partition for
-- every date it invents.
CREATE TABLE IF NOT EXISTS tsi.events
(
    tenant_id   String,
    sandbox_id  String,
    event_time  DateTime64(3, 'UTC'),
    event_id    UUID,
    type        LowCardinality(String),
    payload     String,
    received_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (tenant_id, sandbox_id, event_time)
TTL toDateTime(event_time) + INTERVAL 7 DAY;

-- The audit trail: one row for every decision the guard makes, enforced or
-- shadow, and one when an action is lifted. inputs holds the rule variables
-- at that moment, so "why" can be answered later without the guard.
CREATE TABLE IF NOT EXISTS tsi.guard_actions
(
    at         DateTime64(3, 'UTC'),
    tenant_id  String,
    sandbox_id String,
    tier       LowCardinality(String),
    rule       LowCardinality(String),
    mode       LowCardinality(String),  -- shadow or enforce
    action     LowCardinality(String),  -- what the rule asked for
    effect     LowCardinality(String),  -- what was done, empty for nothing
    note       LowCardinality(String),  -- why the two differ: shadow, approval_requested, blast_radius_cap, lifted, adopted
    rate       Float64,                 -- throttle only
    inputs     Map(LowCardinality(String), Float64),
    expires    DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toDate(at)
ORDER BY (tenant_id, sandbox_id, at)
TTL toDateTime(at) + INTERVAL 30 DAY;

-- One row per sandbox per second, written by the guard from the heartbeats
-- it already receives. The values are the proxy's running totals, not the
-- growth since the last row. A guard that was down for a minute then loses
-- nothing: the next row carries the totals, and only the shape of that
-- minute is gone.
--
-- A proxy that restarts counts from zero again, and proxy_started tells the
-- two runs apart. Usage is the largest value of each run, added up. For a
-- period: per run, the largest value in the period minus the largest before
-- it.
CREATE TABLE IF NOT EXISTS tsi.usage
(
    tenant_id       String,
    sandbox_id      String,
    proxy_started   DateTime64(3, 'UTC'),
    at              DateTime('UTC'),
    events_accepted UInt64,
    bytes_accepted  UInt64,
    events_rejected UInt64,
    egress_bytes    UInt64,
    watched_bytes   UInt64,
    blocked_calls   UInt64
)
ENGINE = MergeTree
PARTITION BY toDate(at)
ORDER BY (tenant_id, sandbox_id, at)
TTL at + INTERVAL 7 DAY;
