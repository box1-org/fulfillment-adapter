package org.box1.fulfillment.camel;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.camel.Exchange;
import org.apache.camel.spi.DataFormat;
import org.apache.camel.support.service.ServiceSupport;
import org.box1.fulfillment.erp1.Erp1File;
import org.box1.fulfillment.erp1.Erp1Reader;
import org.box1.fulfillment.erp1.Erp1Request;
import org.box1.fulfillment.erp1.Erp1Writer;

/**
 * ERP1 framing as a Camel DataFormat (SPEC section 9: Bindy alone cannot read ERP1). Marshal writes an
 * {@link Erp1Request} to bytes; unmarshal reads bytes to an {@link Erp1File}. Both use {@code CamelFileName} as the
 * name the file is checked under, and both apply the full validation order.
 */
public class Erp1DataFormat extends ServiceSupport implements DataFormat {
    @Override
    public void marshal(Exchange exchange, Object graph, OutputStream stream) throws Exception {
        String name = exchange.getIn().getHeader(Exchange.FILE_NAME, String.class);
        stream.write(Erp1Writer.write((Erp1Request) graph, name));
    }

    @Override
    public Object unmarshal(Exchange exchange, InputStream stream) throws Exception {
        String name = exchange.getIn().getHeader(Exchange.FILE_NAME_ONLY, String.class);
        byte[] bytes = stream.readNBytes(Erp1Reader.MAX_FILE_BYTES + 1);
        return Erp1Reader.read(bytes, name);
    }
}
