package org.box1.fulfillment;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.box1.fulfillment.store.FulfillmentStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Database side of route 4 (reconciliation). An unacknowledged upload past its deadline gets an equivalent resend,
 * up to a bound, then needs-attention. An acknowledged request that is still pending or held gets a slow probe:
 * waiting for an operator is never a transport failure.
 */
@ApplicationScoped
public class ReconcileService {
    private static final Logger LOG = Logger.getLogger(ReconcileService.class);

    private final FulfillmentStore store;
    private final FulfillmentMetrics metrics;
    private final int maxUnacknowledged;
    private final int batchSize;

    public ReconcileService(FulfillmentStore store, FulfillmentMetrics metrics,
            @ConfigProperty(name = "fulfillment.ack.max-unacknowledged-uploads") int maxUnacknowledged,
            @ConfigProperty(name = "fulfillment.dispatch.batch-size") int batchSize) {
        this.store = store;
        this.metrics = metrics;
        this.maxUnacknowledged = maxUnacknowledged;
        this.batchSize = batchSize;
    }

    @Transactional
    public int tick() {
        int acted = 0;
        for (FulfillmentStore.Waiting w : store.lockDueWaiting(batchSize)) {
            MDC.put("fulfillmentRequestId", w.fulfillmentRequestId());
            MDC.put("orderId", w.orderId());
            try {
                if (w.acknowledged()) {
                    store.queueResend(w.fulfillmentRequestId(), "PROBE");
                    metrics.resend("PROBE");
                    LOG.info("Queued an equivalent status probe for a request the ERP is still working on");
                } else if (w.unacknowledgedUploads() >= maxUnacknowledged) {
                    store.needsAttention(w.fulfillmentRequestId(), "NO_ACKNOWLEDGEMENT");
                    metrics.needsAttention("NO_ACKNOWLEDGEMENT");
                    LOG.warnv("No ERP acknowledgement after {0} uploads; needs attention", w.unacknowledgedUploads());
                } else {
                    store.queueResend(w.fulfillmentRequestId(), "NO_ACK");
                    metrics.resend("NO_ACK");
                    LOG.warnv("No ERP acknowledgement; queued equivalent resend {0}", w.unacknowledgedUploads());
                }
                acted++;
            } finally {
                MDC.remove("fulfillmentRequestId");
                MDC.remove("orderId");
            }
        }
        return acted;
    }

    /** Operator action: move a needs-attention request back to dispatch with a fresh budget. */
    @Transactional
    public boolean operatorReconcile(String id, String actor) {
        boolean requeued = store.operatorRequeue(id);
        store.audit(id, requeued ? "RECONCILE_REQUEUED" : "RECONCILE_NOOP", actor, null);
        if (requeued) {
            metrics.resend("OPERATOR");
        }
        LOG.infov("Operator reconcile: requeued={0}", requeued);
        return requeued;
    }
}
