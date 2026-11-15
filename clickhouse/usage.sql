-- Cost per tenant, from the usage table the guard writes. A row holds a
-- sandbox's running totals, so its usage is the largest value seen.
SELECT
    tenant_id,
    count()                                AS sandboxes,
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
        max(events_accepted) AS events,
        max(bytes_accepted)  AS bytes,
        max(events_rejected) AS rejected,
        max(egress_bytes)    AS egress,
        max(watched_bytes)   AS watched,
        max(blocked_calls)   AS blocked
    FROM tsi.usage
    GROUP BY tenant_id, sandbox_id
)
GROUP BY tenant_id
ORDER BY tenant_id
FORMAT PrettyCompactMonoBlock;
