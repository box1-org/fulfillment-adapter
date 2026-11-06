# Runbook

## Operator access

Writes and operator actions need a box1-realm token with the `fulfillment-adapter` client role `fulfillment-writer`
(granted in the Keycloak admin console). `scripts/operator-login.py` logs you in through the browser
(Authorization Code + PKCE, loopback redirect) and makes one call; the token stays in memory and is never printed:

```sh
scripts/operator-login.py create request.json        # body: fulfillmentRequestId, orderId, reservationId, siteId, sku, quantity
scripts/operator-login.py status ful_1042            # no login while the read policy is permit
scripts/operator-login.py reconcile ful_1042
scripts/operator-login.py reconcile-all UPLOAD_EXHAUSTED --limit 500
```

`--api` and `--realm` select another environment. A 403 means the token lacks the role; a 401 means no token, an
expired token or a wrong audience.

## Health

- `/q/health/ready` depends on PostgreSQL only. Kafka and the ERP are deliberately not readiness dependencies: an ERP
  outage must not stop acceptance.
- `/q/health/live` is the Golden Path liveness check.

## Metrics

| Metric | Meaning | Watch for |
|---|---|---|
| `fulfillment_backlog` | accepted, not yet acknowledged by the ERP | growth while the ERP is up |
| `fulfillment_oldest_unacknowledged_seconds` | age of the oldest such request | above the acknowledgement timeout |
| `fulfillment_oldest_unacknowledged_upload_seconds` | age of the oldest unacknowledged upload | |
| `fulfillment_queued` | requests waiting for an upload (due now or backing off) | stays high: SFTP down or credentials wrong |
| `fulfillment_needs_attention` | automatic work stopped | any non-zero |
| `fulfillment_awaiting_operator`, `fulfillment_held` | ERP `PENDING`/`HELD`; expected in MANUAL mode | |
| `fulfillment_resends_total{reason}` | `NO_ACK`, `PROBE`, `ERR_RESEND`, `OPERATOR` | `NO_ACK` rising |
| `fulfillment_uploads_total{result,reason}` | uploads and failures | `failed` rising |
| `fulfillment_erp_errors_total{code}` | `ERR` files by code | any |
| `fulfillment_quarantined_total{reason}`, `fulfillment_quarantined_files` | results not applied | any |
| `fulfillment_results_total{outcome}` | `APPLY`, `LATE`, `DUPLICATE`, `QUARANTINE` | |
| `fulfillment_outbox_pending`, `fulfillment_outbox_oldest_seconds` | events not yet confirmed by Kafka | growth |
| `fulfillment_events_consumed_total{outcome}` | Kafka input outcomes, including `dead_lettered` | |
| `fulfillment_outbox_publish_lag_seconds` | commit-to-broker time of outcome events | p99 rising |
| `fulfillment_metrics_refresh_age_seconds` | age of the database-backed gauges above | above 60: gauges are stale (database unreachable) |
| `kafka_consumer_fetch_manager_records_lag_max` (client metric, if exported) | consumer lag on `box1.order.events.v1` | growth |

## Needs-attention

`GET /api/fulfillments/{id}` shows `needsAttentionReason`:

| Reason | Meaning | Action |
|---|---|---|
| `NO_ACKNOWLEDGEMENT` | uploads succeeded but the ERP never answered | check the ERP worker and `inbound/`; then reconcile |
| `UPLOAD_EXHAUSTED` | SFTP failed up to the bound; `lastError` names the cause | fix connectivity, key or host key; then reconcile (bulk allowed) |
| `ERR_RESENDS_EXHAUSTED` | the ERP kept answering a transport-damage `ERR` (`TRUNCATED`, `COUNT_MISMATCH`, `ENCODING`, `UNREADABLE`) | check permissions and the transfer path; then reconcile (bulk allowed) |
| `ERP_REJECTED_FILE:<code>` | the ERP rejected a file the adapter wrote | adapter defect or protocol disagreement: do not reconcile until understood |
| `CONFLICTING_REUSE` | the ERP holds a different request under this ID | resolve in the ERP with its owner; never resend |

Reconcile (audited, role `fulfillment-writer`): `scripts/operator-login.py reconcile <id>`.

After an outage, requeue every request with one transport reason at once (each is audited; at most `limit`, 1–1000):

```sh
scripts/operator-login.py reconcile-all UPLOAD_EXHAUSTED --limit 500
```

Bulk reconcile accepts only `NO_ACKNOWLEDGEMENT`, `UPLOAD_EXHAUSTED` and `ERR_RESENDS_EXHAUSTED`. During an outage, only the
first upload of each dispatch batch spends an attempt; the rest of the batch is put back untouched, so a long outage
exhausts budgets slowly (40 attempts with backoff up to 5 minutes is at least about 3 hours per request).

A valid ERP result arriving later always applies and clears needs-attention, except `CONFLICTING_REUSE`.

`lastError` categories: `HOST_KEY_REJECTED`, `AUTH_FAILED`, `CONNECTION_REFUSED`, `CONNECTION_CLOSED`, `TIMEOUT`,
`UNKNOWN_HOST`, `IO_<type>`, `ERR_<code>`.

## Host-key rotation

The adapter trusts only the keys in `known_hosts` from its SFTP Secret. To rotate: add the new key next to the old one
in the Secret, restart the adapter, let the VM switch keys, then remove the old key. A key mismatch shows as
`lastError=HOST_KEY_REJECTED`; nothing is uploaded and accepted work stays queued.

## Dead letters and replay

Records that cannot be accepted (invalid JSON or envelope, values ERP1 cannot carry, conflicting reuse, or
`RETRIES_EXHAUSTED` after 20 non-database failures) are recorded in `rejected_command`, then sent unchanged to
`box1.fulfillment.dlq.v1`. A database that cannot serve (connection, resources, shutdown, credentials, missing database,
read-only primary, lock timeout, or a commit-time rollback) holds the channel and never dead-letters a valid event.

Find a record:

```sql
SELECT event_id, fulfillment_request_id, reason, topic, kafka_partition, kafka_offset, received_at
FROM rejected_command ORDER BY received_at DESC LIMIT 20;
```

Replay is manual and **writes to a shared topic, so it needs explicit approval each time**. Fix the cause, read the
record from `box1.fulfillment.dlq.v1` at that offset, and produce its value unchanged (same `eventId`) to
`box1.order.events.v1` with key = the `orderId` (the record's original key), so per-order ordering holds. The adapter
deduplicates by `fulfillmentRequestId`. Inventory also consumes that topic and deduplicates by `eventId`; keeping the
`eventId` unchanged makes the replay a no-op for it.

## Restart and outages

Accepted work, uploads, results and outbox rows are in PostgreSQL. After any restart, due uploads resume, expired
leases are reclaimed, and unacknowledged requests are resent under new nonces. An upload interrupted before its
rename leaves at most a `.tmp` name, which the ERP never reads.

## Files

The adapter deletes its own `consumed/` files after `FULFILLMENT_CONSUMED_RETENTION` (7 days), measured from the file's
modification time on the SFTP server. A rename keeps the ERP's write time, so retention counts from when the ERP wrote
the file, not from consumption, and depends on the two clocks agreeing. Every result and
quarantined file's bytes are also kept in `erp_result` and `erp_file_receipt`.

## Rollback

Roll forward with a new digest; reverting the promotion restores the previous image. The V1 schema is new, so there
is no earlier schema to return to. This procedure has not been exercised.
