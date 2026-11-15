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
