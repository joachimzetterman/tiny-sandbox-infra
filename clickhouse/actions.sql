-- The audit trail: every decision the guard made, in order.
SELECT
    formatDateTime(at, '%T')  AS time,
    sandbox_id,
    tenant_id,
    tier,
    rule,
    mode,
    action,
    effect,
    note,
    rate,
    round(inputs['offered_rate'])            AS offered,
    round(inputs['baseline_rate'])           AS baseline,
    round(inputs['tenant_offered_rate'])     AS tenant_offered,
    round(inputs['watched_bytes_rate'])      AS watched_bytes,
    inputs['bad_site_attempts']              AS bad_calls
FROM tsi.guard_actions
ORDER BY at, sandbox_id
FORMAT PrettyCompactMonoBlock;
