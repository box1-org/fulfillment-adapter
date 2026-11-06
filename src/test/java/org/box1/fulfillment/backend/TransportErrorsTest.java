package org.box1.fulfillment.backend;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.ConnectException;

import org.junit.jupiter.api.Test;

class TransportErrorsTest {
    @Test
    void rootCausesAreNamed() {
        assertEquals("HOST_KEY_REJECTED", TransportErrors.classify(new IllegalStateException("wrap",
                new IOException("reject HostKey: [127.0.0.1]:22213"))));
        assertEquals("HOST_KEY_REJECTED", TransportErrors.classify(new IOException("HostKey has been changed")));
        assertEquals("AUTH_FAILED", TransportErrors.classify(new IOException("Auth fail for methods 'publickey'")));
        assertEquals("CONNECTION_REFUSED", TransportErrors.classify(new IOException("x", new ConnectException("refused"))));
        assertEquals("CONNECTION_CLOSED", TransportErrors.classify(new IOException("connection is closed by foreign host")));
        assertEquals("IO_IOException", TransportErrors.classify(new IOException("other")));
    }
}
