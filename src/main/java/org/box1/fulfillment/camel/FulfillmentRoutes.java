package org.box1.fulfillment.camel;

import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.file.GenericFile;
import org.apache.camel.component.file.GenericFileFilter;
import org.apache.camel.model.rest.RestBindingMode;
import org.box1.fulfillment.AcceptanceService;
import org.box1.fulfillment.backend.Erp1DataFormat;
import org.box1.fulfillment.backend.HiddenFileSweeper;
import org.box1.fulfillment.backend.LegacyDispatchService;
import org.box1.fulfillment.ReconcileService;
import org.box1.fulfillment.backend.LegacyResultService;
import org.box1.fulfillment.domain.FulfillmentCommand;
import org.box1.fulfillment.erp1.Erp1Exception;
import org.box1.fulfillment.erp1.Erp1File;
import org.box1.fulfillment.erp1.Erp1Names;
import org.box1.fulfillment.erp1.Erp1Reader;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * The adapter's Camel routes, inside this Quarkus application:
 * <ol>
 * <li>{@code fulfillment-accept}: REST and Kafka commands enter one durable acceptance path;</li>
 * <li>{@code fulfillment-dispatch}: claim due work, frame ERP1, upload to {@code inbound/} as a temp name and rename;</li>
 * <li>{@code erp-results}: read ready {@code RES}/{@code ERR} files, commit, then move to {@code consumed/};</li>
 * <li>{@code fulfillment-reconcile}: acknowledgement deadlines, equivalent resends, needs-attention.</li>
 * </ol>
 * Plus {@code consumed-retention}, which deletes the adapter's own {@code consumed/} files after the retention period.
 */
@ApplicationScoped
public class FulfillmentRoutes extends RouteBuilder {
    static final String CONSUMED_NAME = "box1ConsumedName";
    private static final String JOB = "box1UploadJob";
    private static final String RAW = "box1RawBytes";
    private static final String UPLOADED = "box1Uploaded";

    private final AcceptanceService acceptance;
    private final LegacyDispatchService dispatch;
    private final LegacyResultService results;
    private final ReconcileService reconcile;
    private final RestHandlers rest;
    private final HiddenFileSweeper hidden;
    private final boolean sftpEnabled;

    public FulfillmentRoutes(AcceptanceService acceptance, LegacyDispatchService dispatch, LegacyResultService results,
            ReconcileService reconcile, RestHandlers rest, HiddenFileSweeper hidden,
            @ConfigProperty(name = "fulfillment.sftp.enabled") boolean sftpEnabled) {
        this.acceptance = acceptance;
        this.dispatch = dispatch;
        this.results = results;
        this.reconcile = reconcile;
        this.rest = rest;
        this.hidden = hidden;
        this.sftpEnabled = sftpEnabled;
    }

    @Override
    public void configure() {
        restConfiguration().component("platform-http").bindingMode(RestBindingMode.off);
        rest("/api/fulfillments")
                .post().id("fulfillment-rest-post").consumes("application/json").produces("application/json")
                .to("direct:rest-post")
                .get("/{fulfillmentRequestId}").id("fulfillment-rest-get").produces("application/json")
                .to("direct:rest-get");
        rest("/internal/fulfillments")
                .post("/reconcile").id("fulfillment-rest-reconcile-bulk").produces("application/json")
                .to("direct:rest-reconcile-bulk")
                .post("/{fulfillmentRequestId}/reconcile").id("fulfillment-rest-reconcile").produces("application/json")
                .to("direct:rest-reconcile");

        // REST routes only: an unexpected failure never returns a stack trace or echoes request headers. This handler
        // is deliberately route-scoped; on the file routes a handled exception would move an uncommitted file.
        from("direct:rest-post").routeId("fulfillment-rest")
                .onException(Exception.class).handled(true).process(rest::unexpected).end()
                .process(rest::post);
        from("direct:rest-get").routeId("fulfillment-rest-status")
                .onException(Exception.class).handled(true).process(rest::unexpected).end()
                .process(rest::get);
        from("direct:rest-reconcile").routeId("fulfillment-rest-operator")
                .onException(Exception.class).handled(true).process(rest::unexpected).end()
                .process(rest::reconcile);
        from("direct:rest-reconcile-bulk").routeId("fulfillment-rest-operator-bulk")
                .onException(Exception.class).handled(true).process(rest::unexpected).end()
                .process(rest::reconcileAll);

        // Route 1: the single acceptance path. Commits before returning; exceptions propagate to the caller.
        from("direct:accept-command").routeId("fulfillment-accept")
                .process(e -> e.getMessage().setBody(acceptance.accept(e.getMessage().getBody(FulfillmentCommand.class))));

        // Route 4: reconciliation. Runs without SFTP too, so deadlines and needs-attention stay current.
        from("scheduler:reconcile?delay={{fulfillment.reconcile.poll-interval-ms}}&initialDelay=5000")
                .routeId("fulfillment-reconcile")
                .process(e -> reconcile.tick());

        if (!sftpEnabled) {
            log.warn("SFTP is disabled: accepted requests stay queued until it is configured");
            return;
        }
        String sftp = "sftp://{{fulfillment.sftp.host}}:{{fulfillment.sftp.port}}/";
        String auth = "?username={{fulfillment.sftp.username}}&privateKeyFile={{fulfillment.sftp.private-key-file}}"
                + "&knownHostsFile={{fulfillment.sftp.known-hosts-file}}&strictHostKeyChecking=yes"
                + "&useUserKnownHostsFile=false&autoCreate=false&binary=true"
                + "&connectTimeout={{fulfillment.sftp.connect-timeout-ms}}&timeout={{fulfillment.sftp.timeout-ms}}"
                + "&serverAliveInterval=15000&serverAliveCountMax=2";

        // Route 2: dispatch. Each job is independent; a failure records a bounded retry, never a business outcome.
        from("scheduler:dispatch?delay={{fulfillment.dispatch.poll-interval-ms}}&initialDelay=3000")
                .routeId("fulfillment-dispatch")
                .process(e -> e.getMessage().setBody(dispatch.claim()))
                .split(body())
                    .setProperty(JOB, body())
                    .setProperty(UPLOADED, constant(false))
                    .filter(e -> !dispatch.skipIfEndpointDown(e.getProperty(JOB, LegacyDispatchService.UploadJob.class)))
                        // Only the framing and transfer are inside the catch: a failure to record success afterwards
                        // must not be logged as a transport failure (the file was uploaded).
                        .doTry()
                            .process(e -> {
                                LegacyDispatchService.UploadJob job = e.getProperty(JOB, LegacyDispatchService.UploadJob.class);
                                e.getMessage().setHeader(Exchange.FILE_NAME, job.fileName());
                                e.getMessage().setBody(job.request());
                            })
                            .marshal(new Erp1DataFormat())
                            .to(sftp + "inbound" + auth + "&tempFileName=.${file:onlyname}.tmp&fileExist=Fail"
                                    + "&maximumReconnectAttempts=0&disconnect=true")
                            .setProperty(UPLOADED, constant(true))
                        .doCatch(Exception.class)
                            .process(e -> dispatch.failed(e.getProperty(JOB, LegacyDispatchService.UploadJob.class),
                                    e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class)))
                        .end()
                        .filter(exchangeProperty(UPLOADED))
                            .process(e -> dispatch.succeeded(e.getProperty(JOB, LegacyDispatchService.UploadJob.class)))
                        .end()
                    .end()
                .end()
                .process(e -> dispatch.batchDone());

        // Route 3: results. The move to consumed/ happens only after the processor's transaction commits; an
        // exception leaves the file in outbound/ for the next poll.
        from(sftp + "outbound" + auth + "&delay={{fulfillment.results.poll-interval-ms}}&initialDelay=4000"
                + "&exclude=^\\..*\\.tmp$&recursive=false&maxMessagesPerPoll=100&streamDownload=true"
                + "&move=../consumed/${header." + CONSUMED_NAME + "}&readLock=none&disconnect=false"
                + "&throwExceptionOnConnectFailed=true&bridgeErrorHandler=true")
                .routeId("erp-results")
                .process(e -> {
                    String name = e.getMessage().getHeader(Exchange.FILE_NAME_ONLY, String.class);
                    e.getMessage().setHeader(CONSUMED_NAME, LegacyResultService.consumedName(safeName(name)));
                    // Never read more than ERP1's maximum plus one byte: a huge file is quarantined, not loaded.
                    try (java.io.InputStream in = e.getMessage().getBody(java.io.InputStream.class)) {
                        byte[] head = in == null ? new byte[0] : in.readNBytes(Erp1Reader.MAX_FILE_BYTES + 1);
                        e.setProperty(RAW, head);
                        e.getMessage().setBody(head);
                    }
                })
                .choice()
                    .when(e -> Erp1Names.classify("outbound", e.getMessage().getHeader(Exchange.FILE_NAME_ONLY, String.class))
                            != Erp1Names.Disposition.READY)
                        .process(e -> quarantine(e, "UNEXPECTED_NAME"))
                    .otherwise()
                        .doTry()
                            .unmarshal(new Erp1DataFormat())
                            .process(e -> results.ingest(e.getMessage().getBody(Erp1File.class), e.getProperty(RAW, byte[].class),
                                    e.getMessage().getHeader(Exchange.FILE_NAME_ONLY, String.class),
                                    e.getMessage().getHeader(CONSUMED_NAME, String.class)))
                        .doCatch(Erp1Exception.class)
                            .process(e -> quarantine(e, "INVALID_"
                                    + e.getProperty(Exchange.EXCEPTION_CAUGHT, Erp1Exception.class).code().name()))
                        .end()
                .end();

        // Names starting with a dot are invisible to the route above; ERP1 still requires quarantining them.
        from("scheduler:hidden-sweep?delay={{fulfillment.results.hidden-sweep-interval-ms}}&initialDelay=10000")
                .routeId("outbound-hidden-sweep")
                .process(e -> hidden.sweep());

        // Retention: the adapter owns consumed/ and deletes its files after the retention period (D-011).
        from(sftp + "consumed" + auth + "&delay={{fulfillment.consumed.sweep-interval-ms}}&initialDelay=60000"
                + "&download=false&delete=true&filter=#consumedRetentionFilter&recursive=false&maxMessagesPerPoll=500")
                .routeId("consumed-retention")
                .log("Deleted a consumed file past retention");
    }

    private void quarantine(Exchange e, String reason) {
        byte[] raw = e.getProperty(RAW, byte[].class);
        long size = e.getMessage().getHeader(Exchange.FILE_LENGTH, Long.class) == null ? (raw == null ? 0 : raw.length)
                : e.getMessage().getHeader(Exchange.FILE_LENGTH, Long.class);
        results.quarantine(e.getMessage().getHeader(Exchange.FILE_NAME_ONLY, String.class),
                e.getMessage().getHeader(CONSUMED_NAME, String.class), raw, size, reason);
    }

    /** An unexpected outbound name still moves to consumed/; keep it a plain, bounded file name. */
    static String safeName(String name) {
        String plain = name.replaceAll("[^A-Za-z0-9._-]", "_");
        if (plain.startsWith(".")) {
            plain = "_" + plain.substring(1);
        }
        return plain.length() > 200 ? plain.substring(0, 200) : plain;
    }

    /** Accepts consumed/ files older than the retention period. */
    @jakarta.inject.Named("consumedRetentionFilter")
    @ApplicationScoped
    public static class RetentionFilter implements GenericFileFilter<Object> {
        private final Duration retention;

        public RetentionFilter(@ConfigProperty(name = "fulfillment.consumed.retention") Duration retention) {
            this.retention = retention;
        }

        @Override
        public boolean accept(GenericFile<Object> file) {
            return !file.isDirectory() && file.getLastModified() > 0
                    && file.getLastModified() < System.currentTimeMillis() - retention.toMillis();
        }
    }
}
