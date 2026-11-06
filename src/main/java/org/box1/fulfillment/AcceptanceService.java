package org.box1.fulfillment;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.box1.fulfillment.backend.BackendSelector;
import org.box1.fulfillment.domain.AcceptanceResult;
import org.box1.fulfillment.domain.CommandValidator;
import org.box1.fulfillment.domain.FulfillmentCommand;
import org.box1.fulfillment.store.FulfillmentStore;
import org.jboss.logging.Logger;

/**
 * The one acceptance path for REST and Kafka. A new request and its dispatch intent commit in one transaction before
 * the caller is answered; an unavailable database throws, so nothing is ever accepted in memory only.
 */
@ApplicationScoped
public class AcceptanceService {
    private static final Logger LOG = Logger.getLogger(AcceptanceService.class);

    private final FulfillmentStore store;
    private final BackendSelector backends;
    private final FulfillmentMetrics metrics;

    public AcceptanceService(FulfillmentStore store, BackendSelector backends, FulfillmentMetrics metrics) {
        this.store = store;
        this.backends = backends;
        this.metrics = metrics;
    }

    @Transactional
    public AcceptanceResult accept(FulfillmentCommand command) {
        CommandValidator.validate(command);
        // The backend is chosen once, here, and stored; later routing changes never move accepted work.
        String backend = backends.select(command);
        AcceptanceResult result;
        if (store.insertIfAbsent(command, backend)) {
            result = AcceptanceResult.CREATED;
        } else {
            FulfillmentStore.Core core = store.core(command.fulfillmentRequestId()).orElseThrow();
            result = command.equivalentTo(core.orderId(), core.reservationId(), core.siteId(), core.sku(), core.quantity())
                    ? AcceptanceResult.EQUIVALENT : AcceptanceResult.CONFLICT;
        }
        metrics.accepted(command.origin().name(), result.name());
        LOG.infov("Fulfillment request {0}: origin={1} backend={2}", result.name().toLowerCase(java.util.Locale.ROOT),
                command.origin(), backend);
        return result;
    }
}
