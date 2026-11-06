package org.box1.fulfillment.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.box1.fulfillment.domain.FulfillmentCommand;
import org.box1.fulfillment.domain.FulfillmentView;

/**
 * Fulfillment SQL. Methods marked MANDATORY join the caller's business transaction; the services own transaction
 * boundaries. Every time comparison and schedule uses database {@code now()}.
 */
@ApplicationScoped
public class FulfillmentStore {
    private static final String TERMINAL = "('SHIPPED', 'REJECTED')";

    private final DataSource dataSource;

    public FulfillmentStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    // ---------------------------------------------------------------- acceptance

    /** Insert the request and its dispatch intent. False when the ID already exists. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean insertIfAbsent(FulfillmentCommand c, String backend) {
        String sql = "INSERT INTO fulfillment_request (fulfillment_request_id, order_id, reservation_id, site_id, sku, quantity,"
                + " origin, requested_at, correlation_id, traceparent, causation_id, backend, dispatch_state, next_attempt_at,"
                + " pending_upload_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'QUEUED', now(), 'FIRST')"
                + " ON CONFLICT (fulfillment_request_id) DO NOTHING";
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setString(1, c.fulfillmentRequestId());
            s.setString(2, c.orderId());
            s.setString(3, c.reservationId());
            s.setString(4, c.siteId());
            s.setString(5, c.sku());
            s.setLong(6, c.quantity());
            s.setString(7, c.origin().name());
            s.setTimestamp(8, Timestamp.from(c.requestedAt()));
            s.setString(9, c.correlationId());
            s.setString(10, c.traceparent());
            s.setString(11, c.causationId());
            s.setString(12, backend);
            return s.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    /** The stored business fields for an equivalence check. */
    @Transactional(Transactional.TxType.MANDATORY)
    public Optional<Core> core(String id) {
        String sql = "SELECT order_id, reservation_id, site_id, sku, quantity FROM fulfillment_request"
                + " WHERE fulfillment_request_id = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setString(1, id);
            try (ResultSet r = s.executeQuery()) {
                return r.next() ? Optional.of(new Core(r.getString(1), r.getString(2), r.getString(3), r.getString(4),
                        r.getLong(5))) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    @Transactional(Transactional.TxType.SUPPORTS)
    public Optional<FulfillmentView> view(String id) {
        String sql = "SELECT fulfillment_request_id, order_id, reservation_id, site_id, sku, quantity, origin, backend,"
                + " accepted_at, dispatch_state, total_uploads, resends, failed_upload_attempts, last_upload_at, last_error,"
                + " needs_attention_reason, acknowledged_at, erp_status, applied_revision, shipment_id, status_at,"
                + " reason_code, updated_at FROM fulfillment_request WHERE fulfillment_request_id = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setString(1, id);
            try (ResultSet r = s.executeQuery()) {
                if (!r.next()) {
                    return Optional.empty();
                }
                Instant ack = instant(r, 17);
                return Optional.of(new FulfillmentView(r.getString(1), r.getString(2), r.getString(3), r.getString(4),
                        r.getString(5), r.getLong(6), r.getString(7), r.getString(8), "ACCEPTED", instant(r, 9),
                        r.getString(10), r.getInt(11), r.getInt(12), r.getInt(13), instant(r, 14), r.getString(15),
                        r.getString(16), ack == null ? "NONE" : "ACKNOWLEDGED", ack,
                        FulfillmentView.outcomeOf(r.getString(18)), r.getInt(19), r.getString(20), instant(r, 21),
                        r.getString(22), instant(r, 23)));
            }
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    // ---------------------------------------------------------------- dispatch

    /** Claim due uploads under a lease. A crashed claimer's lease expires and the rows become due again. */
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public List<Due> claimDueUploads(String owner, int limit, int leaseSeconds) {
        String sql = """
                WITH c AS (
                    SELECT fulfillment_request_id FROM fulfillment_request
                    WHERE dispatch_state = 'QUEUED' AND next_attempt_at <= now()
                      AND (claim_expires_at IS NULL OR claim_expires_at < now())
                    ORDER BY next_attempt_at LIMIT ? FOR UPDATE SKIP LOCKED)
                UPDATE fulfillment_request f SET claimed_by = ?, claim_expires_at = now() + make_interval(secs => ?)
                FROM c WHERE f.fulfillment_request_id = c.fulfillment_request_id
                RETURNING f.fulfillment_request_id, f.order_id, f.reservation_id, f.site_id, f.sku, f.quantity, f.origin,
                    f.requested_at, f.correlation_id, f.traceparent, f.backend, f.pending_upload_reason,
                    f.failed_upload_attempts
                """;
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setInt(1, limit);
            s.setString(2, owner);
            s.setInt(3, leaseSeconds);
            List<Due> due = new ArrayList<>();
            try (ResultSet r = s.executeQuery()) {
                while (r.next()) {
                    due.add(new Due(r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5),
                            r.getLong(6), r.getString(7), instant(r, 8), r.getString(9), r.getString(10), r.getString(11),
                            r.getString(12), r.getInt(13)));
                }
            }
            return due;
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    /** Record an upload before it starts, so every nonce used is on record even if the process dies mid-transfer. */
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void startUpload(String id, String fileName, String nonce, String reason) {
        String sql = "INSERT INTO upload (file_name, nonce, fulfillment_request_id, state, reason) VALUES (?, ?, ?, 'STARTED', ?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setString(1, fileName);
            s.setString(2, nonce);
            s.setString(3, id);
            s.setString(4, reason);
            s.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    /**
     * The rename completed: the file is uploaded, not yet accepted by the ERP. Schedules the acknowledgement check, or
     * the slow probe if the ERP had already acknowledged, or nothing once terminal.
     */
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void uploadSucceeded(String id, String fileName, String owner, long ackBaseSeconds, long ackMaxSeconds,
            long probeSeconds) {
        exec("UPDATE upload SET state = 'UPLOADED', finished_at = now() WHERE file_name = ?", fileName);
        String sql = "UPDATE fulfillment_request SET"
                + " dispatch_state = CASE WHEN dispatch_state = 'NEEDS_ATTENTION' THEN dispatch_state ELSE 'UPLOADED' END,"
                + " total_uploads = total_uploads + 1, failed_upload_attempts = 0, last_upload_at = now(), last_error = NULL,"
                + " unacknowledged_uploads = CASE WHEN acknowledged_at IS NULL THEN unacknowledged_uploads + 1 ELSE 0 END,"
                + " next_attempt_at = CASE WHEN erp_status IN " + TERMINAL + " THEN NULL"
                + "   WHEN acknowledged_at IS NULL THEN now() + make_interval(secs => least(?::double precision,"
                + "     ? * power(2, LEAST(unacknowledged_uploads, 20))))"
                + "   ELSE now() + make_interval(secs => ?) END,"
                + " pending_upload_reason = NULL, claimed_by = NULL, claim_expires_at = NULL, updated_at = now()"
                + " WHERE fulfillment_request_id = ? AND claimed_by = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setLong(1, ackMaxSeconds);
            s.setLong(2, ackBaseSeconds);
            s.setLong(3, probeSeconds);
            s.setString(4, id);
            s.setString(5, owner);
            s.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    /** The upload failed. Retry after the backoff, or stop at the bound and ask for attention. */
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public boolean uploadFailed(String id, String fileName, String owner, String error, long backoffSeconds, int maxAttempts) {
        exec("UPDATE upload SET state = 'FAILED', finished_at = now(), error = ? WHERE file_name = ?", error, fileName);
        String sql = "UPDATE fulfillment_request SET failed_upload_attempts = failed_upload_attempts + 1, last_error = ?,"
                + " dispatch_state = CASE WHEN failed_upload_attempts + 1 >= ? THEN 'NEEDS_ATTENTION' ELSE dispatch_state END,"
                + " needs_attention_reason = CASE WHEN failed_upload_attempts + 1 >= ? THEN 'UPLOAD_EXHAUSTED'"
                + "   ELSE needs_attention_reason END,"
                + " next_attempt_at = CASE WHEN failed_upload_attempts + 1 >= ? THEN NULL"
                + "   ELSE now() + make_interval(secs => ?) END,"
                + " pending_upload_reason = CASE WHEN failed_upload_attempts + 1 >= ? THEN pending_upload_reason ELSE 'RETRY' END,"
                + " claimed_by = NULL, claim_expires_at = NULL, updated_at = now()"
                + " WHERE fulfillment_request_id = ? AND claimed_by = ? RETURNING dispatch_state";
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setString(1, error);
            s.setInt(2, maxAttempts);
            s.setInt(3, maxAttempts);
            s.setInt(4, maxAttempts);
            s.setLong(5, backoffSeconds);
            s.setInt(6, maxAttempts);
            s.setString(7, id);
            s.setString(8, owner);
            try (ResultSet r = s.executeQuery()) {
                return r.next() && "NEEDS_ATTENTION".equals(r.getString(1));
            }
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    // ---------------------------------------------------------------- reconciliation

    /** Uploaded requests whose acknowledgement deadline or probe time has passed. Locked for the caller's transaction. */
    @Transactional(Transactional.TxType.MANDATORY)
    public List<Waiting> lockDueWaiting(int limit) {
        String sql = "SELECT fulfillment_request_id, order_id, acknowledged_at IS NOT NULL, unacknowledged_uploads"
                + " FROM fulfillment_request WHERE dispatch_state = 'UPLOADED' AND next_attempt_at <= now()"
                + " AND (erp_status IS NULL OR erp_status NOT IN " + TERMINAL + ")"
                + " ORDER BY next_attempt_at LIMIT ? FOR UPDATE SKIP LOCKED";
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setInt(1, limit);
            List<Waiting> out = new ArrayList<>();
            try (ResultSet r = s.executeQuery()) {
                while (r.next()) {
                    out.add(new Waiting(r.getString(1), r.getString(2), r.getBoolean(3), r.getInt(4)));
                }
            }
            return out;
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    /** Queue an equivalent resend now, under a new nonce chosen at upload time. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void queueResend(String id, String reason) {
        exec("UPDATE fulfillment_request SET dispatch_state = 'QUEUED', next_attempt_at = now(), pending_upload_reason = ?,"
                + " resends = resends + 1, updated_at = now() WHERE fulfillment_request_id = ?"
                + " AND dispatch_state <> 'NEEDS_ATTENTION'", reason, id);
    }

    /** Stop automatic work. Results are still ingested and a valid one clears this state. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void needsAttention(String id, String reason) {
        exec("UPDATE fulfillment_request SET dispatch_state = 'NEEDS_ATTENTION', needs_attention_reason = ?,"
                + " next_attempt_at = NULL, updated_at = now() WHERE fulfillment_request_id = ?", reason, id);
    }

    /** Operator restart of a request in needs-attention, with a fresh retry budget. False if not in that state. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean operatorRequeue(String id) {
        return exec("UPDATE fulfillment_request SET dispatch_state = 'QUEUED', next_attempt_at = now(),"
                + " pending_upload_reason = 'OPERATOR', failed_upload_attempts = 0, unacknowledged_uploads = 0,"
                + " needs_attention_reason = NULL, updated_at = now()"
                + " WHERE fulfillment_request_id = ? AND dispatch_state = 'NEEDS_ATTENTION'", id) == 1;
    }

    @Transactional(Transactional.TxType.MANDATORY)
    public void audit(String id, String action, String actor, String detail) {
        exec("INSERT INTO audit (fulfillment_request_id, action, actor, detail) VALUES (?, ?, ?, ?)", id, action, actor, detail);
    }

    // ---------------------------------------------------------------- results

    /** Lock the request for result processing. Empty when the adapter never sent it. */
    @Transactional(Transactional.TxType.MANDATORY)
    public Optional<Locked> lockForResult(String id) {
        String sql = "SELECT order_id, applied_revision, erp_status, correlation_id, traceparent, causation_id"
                + " FROM fulfillment_request WHERE fulfillment_request_id = ? FOR UPDATE";
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setString(1, id);
            try (ResultSet r = s.executeQuery()) {
                return r.next() ? Optional.of(new Locked(id, r.getString(1), r.getInt(2), r.getString(3), r.getString(4),
                        r.getString(5), r.getString(6))) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    @Transactional(Transactional.TxType.MANDATORY)
    public List<SeenResult> seenResults(String id) {
        String sql = "SELECT result_id, revision, status, shipment_id, bytes FROM erp_result WHERE fulfillment_request_id = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setString(1, id);
            List<SeenResult> out = new ArrayList<>();
            try (ResultSet r = s.executeQuery()) {
                while (r.next()) {
                    out.add(new SeenResult(r.getString(1), r.getInt(2), r.getString(3),
                            r.getString(4) == null ? "" : r.getString(4), r.getBytes(5)));
                }
            }
            return out;
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    @Transactional(Transactional.TxType.MANDATORY)
    public void insertResult(String id, String resultId, int revision, String status, String shipmentId, Instant statusAt,
            String reasonCode, byte[] bytes, String outcome, String fileName) {
        String sql = "INSERT INTO erp_result (fulfillment_request_id, result_id, revision, status, shipment_id, status_at,"
                + " reason_code, bytes, outcome, file_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setString(1, id);
            s.setString(2, resultId);
            s.setInt(3, revision);
            s.setString(4, status);
            s.setString(5, emptyToNull(shipmentId));
            s.setTimestamp(6, Timestamp.from(statusAt));
            s.setString(7, emptyToNull(reasonCode));
            s.setBytes(8, bytes);
            s.setString(9, outcome);
            s.setString(10, fileName);
            s.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    /**
     * Apply a result as the new ERP state. Any result is an acknowledgement, clears needs-attention and resets the
     * resend budget; a terminal result ends all scheduled work.
     */
    @Transactional(Transactional.TxType.MANDATORY)
    public void applyResult(String id, int revision, String status, String shipmentId, Instant statusAt, String reasonCode,
            long probeSeconds) {
        // A CONFLICTING_REUSE stays in needs-attention: the ERP holds a different request under this ID.
        String sql = "UPDATE fulfillment_request SET applied_revision = ?, erp_status = ?, shipment_id = ?, status_at = ?,"
                + " reason_code = ?, acknowledged_at = COALESCE(acknowledged_at, now()), unacknowledged_uploads = 0,"
                + " needs_attention_reason = CASE WHEN needs_attention_reason = 'CONFLICTING_REUSE'"
                + "   THEN needs_attention_reason ELSE NULL END,"
                + " dispatch_state = CASE WHEN needs_attention_reason = 'CONFLICTING_REUSE' THEN 'NEEDS_ATTENTION'"
                + "   ELSE 'UPLOADED' END,"
                + " next_attempt_at = CASE WHEN ? OR needs_attention_reason = 'CONFLICTING_REUSE' THEN NULL"
                + "   ELSE now() + make_interval(secs => ?) END,"
                + " updated_at = now() WHERE fulfillment_request_id = ?";
        boolean terminal = "SHIPPED".equals(status) || "REJECTED".equals(status);
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setInt(1, revision);
            s.setString(2, status);
            s.setString(3, emptyToNull(shipmentId));
            s.setTimestamp(4, Timestamp.from(statusAt));
            s.setString(5, emptyToNull(reasonCode));
            s.setBoolean(6, terminal);
            s.setLong(7, probeSeconds);
            s.setString(8, id);
            s.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    @Transactional(Transactional.TxType.MANDATORY)
    public void receipt(String fileName, String consumedName, String id, String outcome, String reason, long size, byte[] bytes) {
        String sql = "INSERT INTO erp_file_receipt (file_name, consumed_name, fulfillment_request_id, outcome, reason,"
                + " size_bytes, bytes) VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setString(1, fileName);
            s.setString(2, consumedName);
            s.setString(3, id);
            s.setString(4, outcome);
            s.setString(5, reason);
            s.setLong(6, size);
            if (bytes == null) {
                s.setNull(7, Types.BINARY);
            } else {
                s.setBytes(7, bytes);
            }
            s.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    // ---------------------------------------------------------------- ERR files

    /** The upload with this exact name, if the adapter made it. */
    @Transactional(Transactional.TxType.MANDATORY)
    public Optional<UploadRef> upload(String fileName) {
        String sql = "SELECT u.fulfillment_request_id, f.acknowledged_at IS NOT NULL, f.dispatch_state FROM upload u"
                + " JOIN fulfillment_request f USING (fulfillment_request_id) WHERE u.file_name = ? FOR UPDATE OF f";
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setString(1, fileName);
            try (ResultSet r = s.executeQuery()) {
                return r.next() ? Optional.of(new UploadRef(r.getString(1), r.getBoolean(2), r.getString(3))) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    /** Record an ERR once. False when this errorId was already recorded (a redelivery). */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean insertError(String errorId, String inboundFileName, String id, String code, String action) {
        return exec("INSERT INTO erp_error (error_id, inbound_file_name, fulfillment_request_id, error_code, action)"
                + " VALUES (?, ?, ?, ?, ?) ON CONFLICT (error_id) DO NOTHING", errorId, inboundFileName, id, code, action) == 1;
    }

    /** An ERR on an unacknowledged upload: count the failed transfer and resend, or stop at the bound. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean errResend(String id, String code, int maxAttempts) {
        String sql = "UPDATE fulfillment_request SET failed_upload_attempts = failed_upload_attempts + 1, last_error = ?,"
                + " dispatch_state = CASE WHEN failed_upload_attempts + 1 >= ? THEN 'NEEDS_ATTENTION' ELSE 'QUEUED' END,"
                + " needs_attention_reason = CASE WHEN failed_upload_attempts + 1 >= ? THEN 'UPLOAD_EXHAUSTED' ELSE NULL END,"
                + " next_attempt_at = CASE WHEN failed_upload_attempts + 1 >= ? THEN NULL ELSE now() END,"
                + " pending_upload_reason = 'ERR_RESEND', resends = resends + 1, updated_at = now()"
                + " WHERE fulfillment_request_id = ? AND dispatch_state <> 'NEEDS_ATTENTION' RETURNING dispatch_state";
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            s.setString(1, "ERR_" + code);
            s.setInt(2, maxAttempts);
            s.setInt(3, maxAttempts);
            s.setInt(4, maxAttempts);
            s.setString(5, id);
            try (ResultSet r = s.executeQuery()) {
                return r.next() && "NEEDS_ATTENTION".equals(r.getString(1));
            }
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    // ---------------------------------------------------------------- metrics

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public Gauges gauges() {
        String sql = "SELECT"
                + " count(*) FILTER (WHERE acknowledged_at IS NULL AND dispatch_state <> 'NEEDS_ATTENTION'),"
                + " count(*) FILTER (WHERE dispatch_state = 'QUEUED'),"
                + " count(*) FILTER (WHERE dispatch_state = 'NEEDS_ATTENTION'),"
                + " count(*) FILTER (WHERE erp_status IN ('PENDING', 'HELD')),"
                + " count(*) FILTER (WHERE erp_status = 'HELD'),"
                + " COALESCE(EXTRACT(EPOCH FROM now() - min(accepted_at) FILTER (WHERE acknowledged_at IS NULL)), 0),"
                + " COALESCE(EXTRACT(EPOCH FROM now() - min(last_upload_at)"
                + "   FILTER (WHERE acknowledged_at IS NULL AND last_upload_at IS NOT NULL)), 0),"
                + " (SELECT count(*) FROM erp_file_receipt WHERE outcome = 'QUARANTINE'),"
                + " (SELECT count(*) FROM outbox WHERE published_at IS NULL),"
                + " COALESCE((SELECT EXTRACT(EPOCH FROM now() - min(created_at)) FROM outbox WHERE published_at IS NULL), 0)"
                + " FROM fulfillment_request";
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql);
                ResultSet r = s.executeQuery()) {
            r.next();
            return new Gauges(r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4), r.getLong(5), r.getDouble(6),
                    r.getDouble(7), r.getLong(8), r.getLong(9), r.getDouble(10));
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    // ---------------------------------------------------------------- helpers

    private int exec(String sql, Object... args) {
        try (Connection conn = dataSource.getConnection(); PreparedStatement s = conn.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                s.setObject(i + 1, args[i]);
            }
            return s.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    private static Instant instant(ResultSet r, int column) throws SQLException {
        Timestamp t = r.getTimestamp(column);
        return t == null ? null : t.toInstant();
    }

    private static String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }

    /** Stored business fields used for equivalence. */
    public record Core(String orderId, String reservationId, String siteId, String sku, long quantity) {}

    /** A request whose upload is due, with what the backend needs to build the file. */
    public record Due(String fulfillmentRequestId, String orderId, String reservationId, String siteId, String sku,
            long quantity, String origin, Instant requestedAt, String correlationId, String traceparent, String backend,
            String reason, int failedUploadAttempts) {}

    /** An uploaded request whose timer has expired. */
    public record Waiting(String fulfillmentRequestId, String orderId, boolean acknowledged, int unacknowledgedUploads) {}

    /** A request locked for result processing. */
    public record Locked(String fulfillmentRequestId, String orderId, int appliedRevision, String erpStatus,
            String correlationId, String traceparent, String causationId) {}

    /** A result already seen (applied or late). */
    public record SeenResult(String resultId, int revision, String status, String shipmentId, byte[] bytes) {}

    /** The request behind an upload name. */
    public record UploadRef(String fulfillmentRequestId, boolean acknowledged, String dispatchState) {}

    /** Values for the business gauges. */
    public record Gauges(long backlog, long queued, long needsAttention, long awaitingOperator, long held,
            double oldestUnacknowledgedSeconds, double oldestUnacknowledgedUploadSeconds, long quarantined,
            long outboxPending, double outboxOldestSeconds) {}
}
