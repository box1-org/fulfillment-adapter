package org.box1.fulfillment.backend;

import jakarta.enterprise.context.ApplicationScoped;
import org.box1.fulfillment.domain.FulfillmentCommand;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Chooses the backend for a new request. Routing rules apply only at acceptance; an uncertain legacy fulfillment is
 * never redirected, because the ERP may already have committed its shipment.
 */
@ApplicationScoped
public class BackendSelector {
    private final String enabled;

    public BackendSelector(@ConfigProperty(name = "fulfillment.backend") String enabled) {
        if (!LegacyErp1Backend.ID.equals(enabled)) {
            throw new IllegalStateException("Only the " + LegacyErp1Backend.ID + " backend exists");
        }
        this.enabled = enabled;
    }

    public String select(FulfillmentCommand command) {
        return enabled;
    }
}
