-- Cost per tenant, from the usage table the guard writes. A row holds a
-- proxy's running totals, so the usage of one run of a proxy is the largest
-- value seen. A proxy that restarted counts from zero again: that is a new
-- run (proxy_started), and the runs are added up.
SELECT
    tenant_id,
    uniqExact(sandbox_id)                  AS sandboxes,
    sum(events)                            AS events,
    formatReadableSize(sum(bytes))         AS payload,
    sum(rejected)                          AS rejected,
    formatReadableSize(sum(egress))        AS egress,
    formatReadableSize(sum(watched))       AS watched,
    sum(blocked)                           AS blocked_calls
FROM
(
    SELECT
        tenant_id,
        sandbox_id,
        proxy_started,
        max(events_accepted) AS events,
        max(bytes_accepted)  AS bytes,
        max(events_rejected) AS rejected,
        max(egress_bytes)    AS egress,
        max(watched_bytes)   AS watched,
        max(blocked_calls)   AS blocked
    FROM tsi.usage
    GROUP BY tenant_id, sandbox_id, proxy_started
)
GROUP BY tenant_id
ORDER BY tenant_id
FORMAT PrettyCompactMonoBlock;
