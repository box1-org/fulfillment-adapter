package org.box1.fulfillment;

import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.fulfillment.store.FulfillmentStore;
import org.jboss.logging.Logger;

/**
 * Business metrics. Gauges are read from the database on a schedule, so they survive restarts and agree with the
 * status API. Label values are fixed codes, never IDs.
 */
@ApplicationScoped
public class FulfillmentMetrics {
    private static final Logger LOG = Logger.getLogger(FulfillmentMetrics.class);

    private final MeterRegistry registry;
    private final FulfillmentStore store;
    private final AtomicReference<FulfillmentStore.Gauges> gauges =
            new AtomicReference<>(new FulfillmentStore.Gauges(0, 0, 0, 0, 0, 0, 0, 0, 0, 0));

    public FulfillmentMetrics(MeterRegistry registry, FulfillmentStore store) {
        this.registry = registry;
        this.store = store;
        gauge("fulfillment_backlog", "Accepted requests the ERP has not acknowledged", g -> g.backlog());
        gauge("fulfillment_queued", "Requests with an upload due", g -> g.queued());
        gauge("fulfillment_needs_attention", "Requests whose automatic work stopped", g -> g.needsAttention());
        gauge("fulfillment_awaiting_operator", "Acknowledged requests the ERP holds as PENDING or HELD", g -> g.awaitingOperator());
        gauge("fulfillment_held", "Requests an ERP operator has held", g -> g.held());
        gauge("fulfillment_oldest_unacknowledged_seconds", "Age of the oldest accepted, unacknowledged request",
                g -> g.oldestUnacknowledgedSeconds());
        gauge("fulfillment_oldest_unacknowledged_upload_seconds", "Age of the oldest unacknowledged upload",
                g -> g.oldestUnacknowledgedUploadSeconds());
        gauge("fulfillment_quarantined_files", "Outbound files quarantined since the database was created", g -> g.quarantined());
        gauge("fulfillment_outbox_pending", "Outcome events not yet confirmed by the broker", g -> g.outboxPending());
        gauge("fulfillment_outbox_oldest_seconds", "Age of the oldest unpublished outcome event", g -> g.outboxOldestSeconds());
    }

    private void gauge(String name, String help, java.util.function.ToDoubleFunction<FulfillmentStore.Gauges> f) {
        Gauge.builder(name, gauges, ref -> f.applyAsDouble(ref.get())).description(help).register(registry);
    }

    @Scheduled(every = "${fulfillment.metrics.refresh-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void refresh() {
        try {
            gauges.set(store.gauges());
        } catch (RuntimeException e) {
            LOG.warnv("Metric refresh failed: {0}", e.getClass().getSimpleName());
        }
    }

    public void accepted(String origin, String result) {
        count("fulfillment_accepted_total", "origin", origin, "result", result);
    }

    public void consumed(String outcome) {
        count("fulfillment_events_consumed_total", "outcome", outcome);
    }

    public void upload(String result, String reason) {
        count("fulfillment_uploads_total", "result", result, "reason", reason);
    }

    public void resend(String reason) {
        count("fulfillment_resends_total", "reason", reason);
    }

    public void needsAttention(String reason) {
        String label = reason.startsWith("ERP_REJECTED_FILE") ? "ERP_REJECTED_FILE" : reason;
        count("fulfillment_needs_attention_total", "reason", label);
    }

    public void result(String outcome) {
        count("fulfillment_results_total", "outcome", outcome);
    }

    public void quarantined(String reason) {
        count("fulfillment_quarantined_total", "reason", reason);
    }

    public void erpError(String code) {
        count("fulfillment_erp_errors_total", "code", code);
    }

    public void published(double lagSeconds) {
        count("fulfillment_outbox_published_total");
    }

    public void publishFailed() {
        count("fulfillment_outbox_publish_failures_total");
    }

    public void retried() {
        count("fulfillment_consumer_retries_total");
    }

    public void heldForDatabase() {
        count("fulfillment_consumer_held_for_database_total");
    }

    private void count(String name, String... tags) {
        Counter.builder(name).tags(tags).register(registry).increment();
    }
}
