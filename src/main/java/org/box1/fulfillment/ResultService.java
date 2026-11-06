package org.box1.fulfillment;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.box1.fulfillment.backend.LegacyErp1Backend;
import org.box1.fulfillment.erp1.Erp1ErrorCode;
import org.box1.fulfillment.erp1.Erp1File;
import org.box1.fulfillment.erp1.Erp1FileType;
import org.box1.fulfillment.erp1.Erp1Reader;
import org.box1.fulfillment.erp1.Erp1Record;
import org.box1.fulfillment.erp1.Erp1ResultRules;
import org.box1.fulfillment.messaging.EventFactory;
import org.box1.fulfillment.store.FulfillmentStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Database side of route 3 (ERP result and error ingestion). Each method commits the file's effect (applied result,
 * late record, duplicate, quarantine or ERR action) and any outcome event before returning; only then does the route
 * rename the file to {@code consumed/}. A crash between the two redelivers the file, which deduplicates.
 */
@ApplicationScoped
public class ResultService {
    private static final Logger LOG = Logger.getLogger(ResultService.class);
    /** ERR codes that mean transport damage or permissions; the adapter self-validates every file it sends (D-005). */
    private static final Set<Erp1ErrorCode> RESENDABLE =
            Set.of(Erp1ErrorCode.TRUNCATED, Erp1ErrorCode.COUNT_MISMATCH, Erp1ErrorCode.ENCODING, Erp1ErrorCode.UNREADABLE);

    private final FulfillmentStore store;
    private final EventFactory events;
    private final FulfillmentMetrics metrics;
    private final long probeSeconds;
    private final int uploadMaxAttempts;

    public ResultService(FulfillmentStore store, EventFactory events, FulfillmentMetrics metrics,
            @ConfigProperty(name = "fulfillment.ack.probe-interval-seconds") long probeSeconds,
            @ConfigProperty(name = "fulfillment.upload.max-attempts") int uploadMaxAttempts) {
        this.store = store;
        this.events = events;
        this.metrics = metrics;
        this.probeSeconds = probeSeconds;
        this.uploadMaxAttempts = uploadMaxAttempts;
    }

    /** The name a processed file is moved to, with a fresh nonce so re-emitted files never collide. */
    public static String consumedName(String fileName) {
        return org.box1.fulfillment.erp1.Erp1Names.consumedName(fileName, LegacyErp1Backend.newNonce().substring(0, 16));
    }

    /** A file the adapter cannot read: unexpected name, or invalid content. Recorded with its bytes, never applied. */
    @Transactional
    public void quarantine(String fileName, String consumedName, byte[] bytes, long size, String reason) {
        byte[] kept = bytes != null && bytes.length <= Erp1Reader.MAX_FILE_BYTES ? bytes : null;
        store.receipt(fileName, consumedName, null, "QUARANTINE", reason, size, kept);
        metrics.quarantined(reason);
        LOG.warnv("Quarantined an outbound file: reason={0} size={1}", reason, size);
    }

    /** A valid RES or ERR file. Returns the receipt outcome. */
    @Transactional
    public String ingest(Erp1File file, byte[] bytes, String fileName, String consumedName) {
        Erp1Record body = file.body();
        if (file.fileType() == Erp1FileType.ERR) {
            return error(body, bytes, fileName, consumedName);
        }
        if (file.fileType() != Erp1FileType.RES) {
            quarantine(fileName, consumedName, bytes, bytes.length, "UNEXPECTED_TYPE");
            return "QUARANTINE";
        }
        String id = body.get("fulfillmentRequestId");
        MDC.put("fulfillmentRequestId", id);
        MDC.put("resultId", body.get("resultId"));
        if (!body.get("correlationId").isEmpty()) {
            MDC.put("correlationId", body.get("correlationId"));
        }
        try {
            return result(id, body, bytes, fileName, consumedName);
        } finally {
            MDC.remove("fulfillmentRequestId");
            MDC.remove("resultId");
            MDC.remove("correlationId");
        }
    }

    private String result(String id, Erp1Record res, byte[] bytes, String fileName, String consumedName) {
        FulfillmentStore.Locked locked = store.lockForResult(id).orElse(null);
        List<Erp1ResultRules.Seen> seen = locked == null ? List.of() : store.seenResults(id).stream()
                .map(s -> new Erp1ResultRules.Seen(s.resultId(), s.revision(), s.status(), s.shipmentId(), s.bytes())).toList();
        Erp1ResultRules.Decision decision = Erp1ResultRules.decide(res, bytes, locked == null ? null : locked.orderId(),
                seen, locked == null ? 0 : locked.appliedRevision(), locked == null ? null : locked.erpStatus());
        int revision = Integer.parseInt(res.get("revision"));
        String status = res.get("status");
        Instant statusAt = Instant.parse(res.get("statusAt"));
        switch (decision.outcome()) {
            case QUARANTINE -> {
                store.receipt(fileName, consumedName, locked == null ? null : id, "QUARANTINE", decision.reason(),
                        bytes.length, bytes);
                metrics.quarantined(decision.reason());
                LOG.warnv("Quarantined a result: reason={0} revision={1}", decision.reason(), revision);
            }
            case DUPLICATE -> {
                store.receipt(fileName, consumedName, id, "DUPLICATE", null, bytes.length, null);
                LOG.infov("Duplicate result ignored: revision={0}", revision);
            }
            case LATE -> {
                store.insertResult(id, res.get("resultId"), revision, status, res.get("shipmentId"), statusAt,
                        res.get("reasonCode"), bytes, "LATE", fileName);
                store.receipt(fileName, consumedName, id, "LATE", null, bytes.length, null);
                LOG.infov("Late result recorded without changing state: revision={0} status={1} applied={2}", revision,
                        status, locked.appliedRevision());
            }
            case APPLY -> apply(locked, res, bytes, fileName, consumedName, revision, status, statusAt);
            default -> throw new IllegalStateException("unknown outcome");
        }
        metrics.result(decision.outcome().name());
        return decision.outcome().name();
    }

    private void apply(FulfillmentStore.Locked locked, Erp1Record res, byte[] bytes, String fileName, String consumedName,
            int revision, String status, Instant statusAt) {
        String id = locked.fulfillmentRequestId();
        store.insertResult(id, res.get("resultId"), revision, status, res.get("shipmentId"), statusAt, res.get("reasonCode"),
                bytes, "APPLY", fileName);
        store.applyResult(id, revision, status, res.get("shipmentId"), statusAt, res.get("reasonCode"), probeSeconds);
        store.receipt(fileName, consumedName, id, "APPLY", null, bytes.length, null);
        Instant now = Instant.now();
        String eventId = null;
        if ("SHIPPED".equals(status)) {
            eventId = events.confirmed(locked, res.get("shipmentId"), statusAt, now).toString();
        } else if ("REJECTED".equals(status)) {
            // reasonCode only; the operator's reason text stays in the stored file bytes and never reaches an event.
            eventId = events.rejected(locked, res.get("reasonCode"), now).toString();
        }
        if (eventId != null) {
            MDC.put("eventId", eventId);
        }
        try {
            LOG.infov("Applied ERP result: revision={0} status={1} outcomeEvent={2}", revision, status,
                    eventId == null ? "none" : eventId);
        } finally {
            MDC.remove("eventId");
        }
    }

    private String error(Erp1Record err, byte[] bytes, String fileName, String consumedName) {
        String errorId = err.get("errorId");
        String inbound = err.get("inboundFileName");
        Erp1ErrorCode code = Erp1ErrorCode.fromText(err.get("errorCode"));
        FulfillmentStore.UploadRef ref = store.upload(inbound).orElse(null);
        String action;
        if (ref == null) {
            action = "UNMATCHED";
        } else if (code == Erp1ErrorCode.CONFLICTING_REUSE) {
            // The ERP holds a different request under this ID. Never resend; a person must decide.
            action = "NEEDS_ATTENTION";
        } else if (ref.acknowledged() || "NEEDS_ATTENTION".equals(ref.dispatchState())) {
            action = "RECORDED";
        } else if (RESENDABLE.contains(code)) {
            action = "RESEND";
        } else {
            action = "NEEDS_ATTENTION";
        }
        String id = ref == null ? null : ref.fulfillmentRequestId();
        if (!store.insertError(errorId, inbound, id, code.name(), action)) {
            store.receipt(fileName, consumedName, id, "DUPLICATE", null, bytes.length, null);
            return "DUPLICATE";
        }
        store.receipt(fileName, consumedName, id, "ERR", code.name(), bytes.length, bytes);
        if (id != null) {
            MDC.put("fulfillmentRequestId", id);
        }
        try {
            switch (action) {
                case "RESEND" -> {
                    boolean exhausted = store.errResend(id, code.name(), uploadMaxAttempts);
                    metrics.resend("ERR_RESEND");
                    if (exhausted) {
                        metrics.needsAttention("UPLOAD_EXHAUSTED");
                    }
                }
                case "NEEDS_ATTENTION" -> {
                    String reason = code == Erp1ErrorCode.CONFLICTING_REUSE ? "CONFLICTING_REUSE" : "ERP_REJECTED_FILE:" + code;
                    store.needsAttention(id, reason);
                    metrics.needsAttention(reason);
                }
                default -> {
                    // RECORDED or UNMATCHED: no state change.
                }
            }
            metrics.erpError(code.name());
            LOG.warnv("ERP rejected an upload: code={0} action={1}", code, action);
        } finally {
            MDC.remove("fulfillmentRequestId");
        }
        return "ERR";
    }
}
