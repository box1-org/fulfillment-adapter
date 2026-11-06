package org.box1.fulfillment.backend;

/**
 * The strangler boundary. Each backend fulfils accepted requests its own way; a request keeps the backend chosen at
 * acceptance for its whole life. Only {@link LegacyErp1Backend} exists today.
 */
public interface FulfillmentBackend {
    String id();
}
