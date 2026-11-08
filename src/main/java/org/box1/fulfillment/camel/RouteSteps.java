package org.box1.fulfillment.camel;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import org.apache.camel.Exchange;
import org.apache.camel.Header;
import org.box1.fulfillment.AcceptanceService;
import org.box1.fulfillment.ReconcileService;
import org.box1.fulfillment.backend.Erp1DataFormat;
import org.box1.fulfillment.backend.HiddenFileSweeper;
import org.box1.fulfillment.backend.LegacyDispatchService;
import org.box1.fulfillment.backend.LegacyDispatchService.UploadJob;
import org.box1.fulfillment.backend.LegacyResultService;
import org.box1.fulfillment.domain.AcceptanceResult;
import org.box1.fulfillment.domain.FulfillmentCommand;
import org.box1.fulfillment.erp1.Erp1Exception;
import org.box1.fulfillment.erp1.Erp1File;
import org.box1.fulfillment.erp1.Erp1Names;
import org.box1.fulfillment.erp1.Erp1Reader;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * The Java steps called by the YAML routes in {@code src/main/resources/routes} (as {@code bean: routeSteps}).
 * The routes hold the flow, so Kaoto can show and edit it; this class holds only what needs Java.
 * <ol>
 * <li>{@code fulfillment-accept}: REST and Kafka commands enter one durable acceptance path;</li>
 * <li>{@code fulfillment-dispatch}: claim due work, frame ERP1, upload to {@code inbound/} as a temp name and rename;</li>
 * <li>{@code erp-results}: read ready {@code RES}/{@code ERR} files, commit, then move to {@code consumed/};</li>
 * <li>{@code fulfillment-reconcile}: acknowledgement deadlines, equivalent resends, needs-attention.</li>
 * </ol>
 * Plus {@code consumed-retention}, which deletes the adapter's own {@code consumed/} files after the retention period.
 */
@ApplicationScoped
@Named("routeSteps")
public class RouteSteps {
    private static final Logger LOG = Logger.getLogger(RouteSteps.class);
    /** Exchange property names; the YAML routes use the same literals. */
    static final String JOB = "box1UploadJob";
    static final String RAW = "box1RawBytes";
    /** Header naming the file in {@code consumed/}; the results route's {@code move} option reads it. */
    static final String CONSUMED_NAME = "box1ConsumedName";

    private final AcceptanceService acceptance;
    private final LegacyDispatchService dispatch;
    private final LegacyResultService results;
    private final ReconcileService reconcile;
    private final HiddenFileSweeper hidden;
    private final boolean sftpEnabled;

    public RouteSteps(AcceptanceService acceptance, LegacyDispatchService dispatch, LegacyResultService results,
            ReconcileService reconcile, HiddenFileSweeper hidden,
            @ConfigProperty(name = "fulfillment.sftp.enabled") boolean sftpEnabled) {
        this.acceptance = acceptance;
        this.dispatch = dispatch;
        this.results = results;
        this.reconcile = reconcile;
        this.hidden = hidden;
        this.sftpEnabled = sftpEnabled;
    }

    void warnIfSftpDisabled(@Observes StartupEvent event) {
        if (!sftpEnabled) {
            LOG.warn("SFTP is disabled: accepted requests stay queued until it is configured");
        }
    }

    /** The ERP1 framing, used by {@code marshal}/{@code unmarshal} in the exchange routes. */
    @Produces
    @Named("erp1")
    @ApplicationScoped
    Erp1DataFormat erp1DataFormat() {
        return new Erp1DataFormat();
    }

    // Route 1: acceptance. Commits before returning; exceptions propagate to the caller.

    public AcceptanceResult accept(FulfillmentCommand command) {
        return acceptance.accept(command);
    }

    // Route 4: reconciliation.

    public void reconcile() {
        reconcile.tick();
    }

    // Route 2: dispatch.

    public List<UploadJob> claimUploads() {
        return dispatch.claim();
    }

    /** False once the endpoint is known to be down: the job is given back without spending an attempt. */
    public boolean endpointUp(Exchange e) {
        return !dispatch.skipIfEndpointDown(e.getProperty(JOB, UploadJob.class));
    }

    public void uploadFailed(Exchange e) {
        dispatch.failed(e.getProperty(JOB, UploadJob.class), e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class));
    }

    public void uploadSucceeded(Exchange e) {
        dispatch.succeeded(e.getProperty(JOB, UploadJob.class));
    }

    public void batchDone() {
        dispatch.batchDone();
    }

    // Route 3: results.

    /** An unexpected outbound name still moves to consumed/; keep it a plain, bounded file name. */
    public String consumedName(@Header(Exchange.FILE_NAME_ONLY) String name) {
        return LegacyResultService.consumedName(HiddenFileSweeper.safeName(name));
    }

    /** Never read more than ERP1's maximum plus one byte: a huge file is quarantined, not loaded. */
    public void readHead(Exchange e) throws IOException {
        try (InputStream in = e.getMessage().getBody(InputStream.class)) {
            byte[] head = in == null ? new byte[0] : in.readNBytes(Erp1Reader.MAX_FILE_BYTES + 1);
            e.setProperty(RAW, head);
            e.getMessage().setBody(head);
        }
    }

    public boolean readyName(@Header(Exchange.FILE_NAME_ONLY) String name) {
        return Erp1Names.classify("outbound", name) == Erp1Names.Disposition.READY;
    }

    public void ingest(Exchange e) {
        results.ingest(e.getMessage().getBody(Erp1File.class), e.getProperty(RAW, byte[].class),
                e.getMessage().getHeader(Exchange.FILE_NAME_ONLY, String.class),
                e.getMessage().getHeader(CONSUMED_NAME, String.class));
    }

    public void quarantineUnexpectedName(Exchange e) {
        quarantine(e, "UNEXPECTED_NAME");
    }

    public void quarantineInvalid(Exchange e) {
        quarantine(e, "INVALID_" + e.getProperty(Exchange.EXCEPTION_CAUGHT, Erp1Exception.class).code().name());
    }

    private void quarantine(Exchange e, String reason) {
        byte[] raw = e.getProperty(RAW, byte[].class);
        Long length = e.getMessage().getHeader(Exchange.FILE_LENGTH, Long.class);
        long size = length == null ? (raw == null ? 0 : raw.length) : length;
        results.quarantine(e.getMessage().getHeader(Exchange.FILE_NAME_ONLY, String.class),
                e.getMessage().getHeader(CONSUMED_NAME, String.class), raw, size, reason);
    }

    /** Names starting with a dot are invisible to the results route; ERP1 still requires quarantining them. */
    public void sweepHidden() throws JSchException, SftpException {
        hidden.sweep();
    }
}
