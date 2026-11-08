# Architecture

## Boundary

The adapter protects the legacy ERP from unreliable callers and transport. It owns request acceptance, upload and
acknowledgement state, deduplicated external effects, bounded retries and reconciliation. It never owns order state
or the ERP ledger, and it never calls the ERP over HTTP: the only interface is ERP1 files over SFTP.

```text
Kafka fulfillment.requested.v1 ─┐
                                ├─► route 1 fulfillment-accept ─► PostgreSQL (request + dispatch intent) ─► 202 / commit offset
POST /api/fulfillments ─────────┘
route 2 fulfillment-dispatch: claim due rows ─► Erp1DataFormat ─► sftp inbound/.REQ.<id>.<nonce>.erp1.tmp ─► rename
route 3 erp-results: sftp outbound/ RES|ERR ─► Erp1DataFormat ─► ERP1 §7 table ─► commit (+ outbox) ─► rename consumed/<name>.<nonce>
route 4 fulfillment-reconcile: ack deadlines ─► equivalent resend (new nonce) │ needs-attention
outbox publisher ─► box1.fulfillment.events.v1 (shipment.confirmed.v1 / shipment.failed.v1)
```

## Status model

A request has four facets that never collapse into one: `acceptance` (committed), `dispatch` (`QUEUED`, `UPLOADED`,
`NEEDS_ATTENTION`), `erpAcknowledgement` (any applied `RES`), and `outcome` (`AWAITING_ERP`, `PENDING`, `HELD`,
`SHIPPED`, `REJECTED`). A completed SFTP rename means *uploaded*; revision 1 `PENDING` means *accepted by the ERP*; an
`ERR` means that one upload was not accepted. `HELD` is an operator hold and never a transport failure.

## Durability rules

- A new request and its dispatch intent commit in one transaction before `202` or before the Kafka offset commits.
  A database outage answers `503` and holds the Kafka channel; nothing is ever accepted in memory only.
- Each upload is recorded (`upload`, state `STARTED`) before the transfer, with a fresh 128-bit nonce, so every name
  sent is on record and a retry never overwrites another upload.
- Each outbound file's effect (applied or late result, duplicate, quarantine, ERR action) and any outcome event commit
  before the rename to `consumed/`. A crash in between redelivers the file, which the ERP1 §7 table makes a duplicate.
- Claims use leases in database time; a crashed claimer's work becomes due again when its lease expires.

## ERP1

`org.box1.fulfillment.erp1` implements ERP1 at the pinned contracts commit: framing (UTF-8 without BOM, LF, escapes,
code-point lengths) and the SPEC §3 validation order in `Erp1Reader`; `Erp1Writer` reads every file back before it
is uploaded. `Erp1ResultRules` is the §7 consumer table. Legacy records exist only in that package,
`backend/LegacyErp1Backend`, `backend/Erp1DataFormat` and the ERP1 exchange routes (`routes/erp1-exchange.camel.yaml`); the modern domain never sees delimiters, ERP status codes or
exchange paths. Camel Bindy is not used, because ERP1 escaping and code-point lengths need their own framing (§9).

## Recovery by equivalent resend

ERP1 §6 makes an equivalent repeat harmless: the ERP republishes its latest result. The adapter uses that for every
recovery: no acknowledgement within the deadline (lost upload or lost first result), transport-damage `ERR` codes,
and a slow probe for acknowledged requests still `PENDING` or `HELD` (lost later result). It never sends a
non-equivalent request under an existing ID, and a `CONFLICTING_REUSE` `ERR` stops automatic work.

## Strangler boundary

`backend.BackendSelector` chooses the backend at acceptance and the choice is stored. Only `legacy-erp1` exists.
Routing changes apply to new requests; an uncertain legacy request is never redirected, because the ERP may already
have committed its shipment.

## Security

- `POST /api/fulfillments` and `/internal/**` need a box1-realm JWT with audience `fulfillment-adapter` and client role
  `fulfillment-writer` (from `resource_access`, never a top-level `groups` claim). Anonymous requests get 401.
- SFTP uses key authentication with strict host-key checking against a provisioned `known_hosts`; there is no
  accept-new mode. File names are generated; no caller input becomes a path.
- Events and logs carry IDs and codes only. The ERP operator's `reasonText` stays in the stored file bytes and is
  never copied into events, logs or the API.

## Observability

JSON logs carry `fulfillmentRequestId`, `orderId`, `correlationId` and, for results, `resultId` and the outcome
`eventId`, so one request can be followed from acceptance to event. `traceparent` and `correlationId` travel inside
ERP1 records and into the outcome event; no span crosses the ERP. Metrics are listed in the runbook.
