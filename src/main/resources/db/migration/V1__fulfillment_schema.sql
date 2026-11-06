-- Fulfillment adapter schema. The adapter owns every table; no other service reads them.
-- All times are database time (now()), so scheduling never mixes clock domains.

-- One row per accepted fulfillment request. Inserted only by the acceptance transaction.
CREATE TABLE fulfillment_request (
    fulfillment_request_id  varchar(128) PRIMARY KEY,
    order_id                varchar(128) NOT NULL,
    reservation_id          varchar(128) NOT NULL,
    site_id                 varchar(128) NOT NULL,
    sku                     varchar(80)  NOT NULL,
    quantity                bigint       NOT NULL CHECK (quantity BETWEEN 1 AND 999999999),
    origin                  varchar(5)   NOT NULL CHECK (origin IN ('EVENT', 'API')),
    requested_at            timestamptz  NOT NULL,
    correlation_id          varchar(128),
    traceparent             varchar(55),
    causation_id            varchar(128) NOT NULL,
    backend                 varchar(32)  NOT NULL,
    accepted_at             timestamptz  NOT NULL DEFAULT now(),
    -- Dispatch facet. QUEUED: an upload is due. UPLOADED: waiting for the ERP. NEEDS_ATTENTION: automatic work stopped.
    dispatch_state          varchar(16)  NOT NULL DEFAULT 'QUEUED'
                            CHECK (dispatch_state IN ('QUEUED', 'UPLOADED', 'NEEDS_ATTENTION')),
    next_attempt_at         timestamptz,
    -- Why the next upload is due: FIRST, RETRY, NO_ACK, PROBE, ERR_RESEND or OPERATOR.
    pending_upload_reason   varchar(16)  DEFAULT 'FIRST'
                            CHECK (pending_upload_reason IN ('FIRST', 'RETRY', 'NO_ACK', 'PROBE', 'ERR_RESEND', 'OPERATOR')),
    failed_upload_attempts  integer      NOT NULL DEFAULT 0,
    unacknowledged_uploads  integer      NOT NULL DEFAULT 0,
    total_uploads           integer      NOT NULL DEFAULT 0,
    resends                 integer      NOT NULL DEFAULT 0,
    -- ERR-driven resends since the last acknowledgement; never reset by a successful upload.
    err_resends             integer      NOT NULL DEFAULT 0,
    last_upload_at          timestamptz,
    last_error              varchar(64),
    needs_attention_reason  varchar(64),
    claimed_by              varchar(64),
    claim_expires_at        timestamptz,
    -- ERP facet: the highest applied result.
    acknowledged_at         timestamptz,
    applied_revision        integer      NOT NULL DEFAULT 0,
    erp_status              varchar(8)   CHECK (erp_status IN ('PENDING', 'HELD', 'SHIPPED', 'REJECTED')),
    shipment_id             varchar(128),
    status_at               timestamptz,
    reason_code             varchar(32),
    updated_at              timestamptz  NOT NULL DEFAULT now()
);
CREATE INDEX ix_request_due ON fulfillment_request (next_attempt_at)
    WHERE next_attempt_at IS NOT NULL AND dispatch_state <> 'NEEDS_ATTENTION';
CREATE INDEX ix_request_order ON fulfillment_request (order_id);

-- Every upload attempt, by generated name. A nonce is never reused.
CREATE TABLE upload (
    file_name               varchar(255) PRIMARY KEY,
    nonce                   varchar(32)  NOT NULL UNIQUE,
    fulfillment_request_id  varchar(128) NOT NULL REFERENCES fulfillment_request,
    state                   varchar(10)  NOT NULL CHECK (state IN ('STARTED', 'UPLOADED', 'FAILED')),
    reason                  varchar(16)  NOT NULL CHECK (reason IN ('FIRST', 'RETRY', 'NO_ACK', 'PROBE', 'ERR_RESEND', 'OPERATOR')),
    started_at              timestamptz  NOT NULL DEFAULT now(),
    finished_at             timestamptz,
    error                   varchar(64)
);
CREATE INDEX ix_upload_request ON upload (fulfillment_request_id);

-- Every result applied or recorded late: (revision, resultId, bytes), as ERP1 section 7 requires.
CREATE TABLE erp_result (
    fulfillment_request_id  varchar(128) NOT NULL REFERENCES fulfillment_request,
    result_id               varchar(36)  NOT NULL,
    revision                integer      NOT NULL,
    status                  varchar(8)   NOT NULL CHECK (status IN ('PENDING', 'HELD', 'SHIPPED', 'REJECTED')),
    shipment_id             varchar(128),
    status_at               timestamptz  NOT NULL,
    reason_code             varchar(32),
    bytes                   bytea        NOT NULL,
    outcome                 varchar(5)   NOT NULL CHECK (outcome IN ('APPLY', 'LATE')),
    file_name               varchar(255) NOT NULL,
    received_at             timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (fulfillment_request_id, result_id),
    UNIQUE (fulfillment_request_id, revision)
);

-- ERR files. An ERR never changes business state; action records what the adapter did about it.
CREATE TABLE erp_error (
    error_id                varchar(36)  PRIMARY KEY,
    inbound_file_name       varchar(255) NOT NULL,
    fulfillment_request_id  varchar(128),
    error_code              varchar(32)  NOT NULL,
    action                  varchar(16)  NOT NULL CHECK (action IN ('RESEND', 'NEEDS_ATTENTION', 'RECORDED', 'UNMATCHED')),
    received_at             timestamptz  NOT NULL DEFAULT now()
);

-- Every file taken from outbound/, including duplicates and quarantined files, with the name it was moved to.
CREATE TABLE erp_file_receipt (
    id                      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_name               varchar(1024) NOT NULL,
    consumed_name           varchar(1100) NOT NULL,
    fulfillment_request_id  varchar(128),
    outcome                 varchar(10)  NOT NULL CHECK (outcome IN ('APPLY', 'LATE', 'DUPLICATE', 'QUARANTINE', 'ERR')),
    reason                  varchar(64),
    size_bytes              bigint       NOT NULL,
    bytes_sha256            char(64)     NOT NULL,
    bytes                   bytea,
    received_at             timestamptz  NOT NULL DEFAULT now()
);
-- A file processed again because its move to consumed/ failed is recorded once per outcome.
CREATE UNIQUE INDEX ux_receipt_once ON erp_file_receipt (file_name, bytes_sha256, outcome);
CREATE INDEX ix_receipt_quarantine ON erp_file_receipt (received_at) WHERE outcome = 'QUARANTINE';

-- Kafka records that could not be accepted (poison or conflicting reuse), recorded before dead-lettering.
CREATE TABLE rejected_command (
    id                      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id                varchar(64),
    fulfillment_request_id  varchar(128),
    reason                  varchar(64)  NOT NULL,
    topic                   varchar(255) NOT NULL,
    kafka_partition         integer      NOT NULL,
    kafka_offset            bigint       NOT NULL,
    received_at             timestamptz  NOT NULL DEFAULT now(),
    UNIQUE (topic, kafka_partition, kafka_offset)
);

-- Operator actions on requests (reconcile).
CREATE TABLE audit (
    id                      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fulfillment_request_id  varchar(128) NOT NULL REFERENCES fulfillment_request,
    action                  varchar(32)  NOT NULL,
    actor                   varchar(255) NOT NULL,
    detail                  varchar(255),
    at                      timestamptz  NOT NULL DEFAULT now()
);

-- Outcome events, written in the same transaction as the result that causes them (inventory/order pattern).
CREATE TABLE outbox (
    seq                     bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    event_id                uuid         PRIMARY KEY,
    aggregate_id            varchar(128) NOT NULL,
    fulfillment_request_id  varchar(128) NOT NULL,
    event_type              varchar(64)  NOT NULL,
    payload                 jsonb        NOT NULL,
    created_at              timestamptz  NOT NULL DEFAULT now(),
    published_at            timestamptz,
    attempts                integer      NOT NULL DEFAULT 0,
    next_attempt_at         timestamptz  NOT NULL DEFAULT now(),
    claimed_by              varchar(64),
    claim_expires_at        timestamptz,
    last_error              varchar(64)
);
-- One outcome event per request, whatever the redelivery.
CREATE UNIQUE INDEX ux_outbox_one_outcome ON outbox (fulfillment_request_id);
CREATE INDEX ix_outbox_unpublished ON outbox (seq) WHERE published_at IS NULL;
-- Supports the claim's oldest-unpublished-per-aggregate check.
CREATE INDEX ix_outbox_aggregate_unpublished ON outbox (aggregate_id, seq) WHERE published_at IS NULL;
