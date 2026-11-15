-- What each tenant stored. Stage 2 adds rejects and egress bytes from the
-- usage table the guard writes.
SELECT
    tenant_id,
    uniqExact(sandbox_id)                    AS sandboxes,
    count()                                  AS events,
    formatReadableSize(sum(length(payload))) AS payload_bytes,
    min(event_time)                          AS first_event,
    max(event_time)                          AS last_event
FROM tsi.events
GROUP BY tenant_id
ORDER BY tenant_id
FORMAT PrettyCompactMonoBlock;
