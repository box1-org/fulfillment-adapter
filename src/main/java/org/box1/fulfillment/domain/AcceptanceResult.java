package org.box1.fulfillment.domain;

/** What acceptance did with a command. */
public enum AcceptanceResult {
    /** Committed now, with its dispatch intent. */
    CREATED,
    /** An equivalent request already exists; nothing changed. */
    EQUIVALENT,
    /** The ID exists with a different order, reservation, site or line; nothing changed. */
    CONFLICT
}
