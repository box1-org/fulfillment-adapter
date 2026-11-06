package org.box1.fulfillment;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import org.box1.fulfillment.backend.LegacyErp1Backend;
import org.box1.fulfillment.erp1.Erp1Request;
import org.box1.fulfillment.store.FulfillmentStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Database side of route 2 (dispatch). Each step is its own transaction, so no transaction spans the SFTP transfer.
 * The upload row is written before the transfer starts, so every nonce used is on record.
 */
@ApplicationScoped
public class DispatchService {
    private static final Logger LOG = Logger.getLogger(DispatchService.class);

    private final FulfillmentStore store;
    private final FulfillmentMetrics metrics;
    private final String owner = "dispatch-" + UUID.randomUUID();
    private final int batchSize;
    private final int leaseSeconds;
    private final long uploadBackoffBase;
    private final long uploadBackoffMax;
    private final int uploadMaxAttempts;
    private final long ackTimeoutBase;
    private final long ackTimeoutMax;
    private final long probeSeconds;

    public DispatchService(FulfillmentStore store, FulfillmentMetrics metrics,
            @ConfigProperty(name = "fulfillment.dispatch.batch-size") int batchSize,
            @ConfigProperty(name = "fulfillment.dispatch.lease-seconds") int leaseSeconds,
            @ConfigProperty(name = "fulfillment.upload.backoff-base-seconds") long uploadBackoffBase,
            @ConfigProperty(name = "fulfillment.upload.backoff-max-seconds") long uploadBackoffMax,
            @ConfigProperty(name = "fulfillment.upload.max-attempts") int uploadMaxAttempts,
            @ConfigProperty(name = "fulfillment.ack.timeout-base-seconds") long ackTimeoutBase,
            @ConfigProperty(name = "fulfillment.ack.timeout-max-seconds") long ackTimeoutMax,
            @ConfigProperty(name = "fulfillment.ack.probe-interval-seconds") long probeSeconds) {
        this.store = store;
        this.metrics = metrics;
        this.batchSize = batchSize;
        this.leaseSeconds = leaseSeconds;
        this.uploadBackoffBase = uploadBackoffBase;
        this.uploadBackoffMax = uploadBackoffMax;
        this.uploadMaxAttempts = uploadMaxAttempts;
        this.ackTimeoutBase = ackTimeoutBase;
        this.ackTimeoutMax = ackTimeoutMax;
        this.probeSeconds = probeSeconds;
    }

    /** Claim due requests and record one new upload (fresh nonce) for each. */
    public List<UploadJob> claim() {
        List<UploadJob> jobs = new ArrayList<>();
        for (FulfillmentStore.Due due : store.claimDueUploads(owner, batchSize, leaseSeconds)) {
            String nonce = LegacyErp1Backend.newNonce();
            String fileName = LegacyErp1Backend.requestFileName(due.fulfillmentRequestId(), nonce);
            String reason = due.reason() == null ? "FIRST" : due.reason();
            store.startUpload(due.fulfillmentRequestId(), fileName, nonce, reason);
            jobs.add(new UploadJob(due, fileName, reason, LegacyErp1Backend.toLegacy(due, Instant.now())));
        }
        return jobs;
    }

    public void succeeded(UploadJob job) {
        store.uploadSucceeded(job.due().fulfillmentRequestId(), job.fileName(), owner, ackTimeoutBase, ackTimeoutMax,
                probeSeconds);
        metrics.upload("uploaded", job.reason());
        withMdc(job, () -> LOG.infov("Uploaded request file: reason={0}", job.reason()));
    }

    /** Transport failure: retry with backoff, bounded. Never a business outcome. */
    public void failed(UploadJob job, Throwable error) {
        String type = error.getClass().getSimpleName();
        long backoff = Math.min(uploadBackoffBase * (1L << Math.min(job.due().failedUploadAttempts(), 20)), uploadBackoffMax);
        boolean exhausted = store.uploadFailed(job.due().fulfillmentRequestId(), job.fileName(), owner, type, backoff,
                uploadMaxAttempts);
        metrics.upload("failed", job.reason());
        withMdc(job, () -> LOG.warnv("Upload failed: error={0} attempt={1} needsAttention={2}", type,
                job.due().failedUploadAttempts() + 1, exhausted));
    }

    private static void withMdc(UploadJob job, Runnable action) {
        MDC.put("fulfillmentRequestId", job.due().fulfillmentRequestId());
        MDC.put("orderId", job.due().orderId());
        MDC.put("uploadFile", job.fileName());
        if (job.due().correlationId() != null) {
            MDC.put("correlationId", job.due().correlationId());
        }
        try {
            action.run();
        } finally {
            MDC.remove("fulfillmentRequestId");
            MDC.remove("orderId");
            MDC.remove("uploadFile");
            MDC.remove("correlationId");
        }
    }

    /** One upload in flight. */
    public record UploadJob(FulfillmentStore.Due due, String fileName, String reason, Erp1Request request) {}
}
