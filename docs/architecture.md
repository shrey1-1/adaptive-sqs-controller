# Architecture and event semantics

The simulator owns a visible-message list, latest receipt handles, receive counters, active attempts, worker occupancy, an event heap, and an idempotency ledger. Events are ordered by `(simulated_time, insertion_order)`, making ties deterministic.

## State transitions

```text
created --send--> visible --receive--> in flight
                                  |         |
                    valid delete  |         +--visibility expiry--> visible
                                  v         +--worker failure------> waits for expiry
                               deleted      +--bounded extension---> in flight
```

An expired attempt may keep processing. Redelivery creates a new attempt and receipt. This overlap is counted as duplicate processing. The first successful attempt commits a logical result; later commits are rejected. The first successful active attempt deletes the message, a declared finite-simulation simplification because AWS may accept an old receipt handle without guaranteeing deletion.

## Controller pseudocode

```text
on_receive(job_class):
    h = recent_successful_durations[job_class]
    if len(h) == 0:
        return default_timeout
    if len(h) < minimum_history:            # partial-history cold start
        return clamp(1.5 * max(h) + safety_margin, T_min, T_max)
    estimate = percentile(h, 90)
    return clamp(estimate + safety_margin, T_min, T_max)

on_first_logical_success(job_class, duration):
    append duration to bounded history[job_class]

on_heartbeat(attempt):
    if worker healthy and extension_count < limit:
        deadline = min(now + controller_timeout, start + maximum_total_visibility)
        extend only if deadline increases
```

Only first logical successes update history, avoiding extra weighting from duplicate attempts.
