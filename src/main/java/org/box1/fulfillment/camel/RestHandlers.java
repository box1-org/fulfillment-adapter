package org.box1.fulfillment.camel;

import java.time.Instant;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.vertx.http.runtime.security.QuarkusHttpUser;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.Exchange;
import org.box1.fulfillment.ReconcileService;
import org.box1.fulfillment.domain.AcceptanceResult;
import org.box1.fulfillment.domain.FulfillmentCommand;
import org.box1.fulfillment.domain.FulfillmentView;
import org.box1.fulfillment.domain.InvalidCommandException;
import org.box1.fulfillment.domain.Origin;
import org.box1.fulfillment.store.FulfillmentStore;
import org.box1.fulfillment.store.StoreException;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * REST boundary processors. Authentication and roles are enforced before these run (HTTP permissions in
 * application.properties); these parse strictly, call the acceptance route and map outcomes to HTTP. Errors are
 * RFC 9457 problem documents that name fields, never values.
 */
@ApplicationScoped
public class RestHandlers {
    private static final Logger LOG = Logger.getLogger(RestHandlers.class);
    private static final String PROBLEM = "application/problem+json";
    private static final Set<String> FIELDS =
            Set.of("fulfillmentRequestId", "orderId", "reservationId", "siteId", "sku", "quantity");
    private static final Pattern ID = Pattern.compile("^ful_[A-Za-z0-9_-]{1,124}$");
    private static final Pattern TRACEPARENT = Pattern.compile("^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$");

    private final ObjectMapper mapper;
    private final org.apache.camel.ProducerTemplate camel;
    private final FulfillmentStore store;
    private final ReconcileService reconcile;

    public RestHandlers(ObjectMapper mapper, org.apache.camel.ProducerTemplate camel, FulfillmentStore store,
            ReconcileService reconcile) {
        this.mapper = mapper;
        this.camel = camel;
        this.store = store;
        this.reconcile = reconcile;
    }

    /** POST /api/fulfillments. */
    public void post(Exchange e) {
        FulfillmentCommand command;
        try {
            command = parse(e.getMessage().getBody(String.class), e);
        } catch (InvalidCommandException invalid) {
            problem(e, 400, "Invalid fulfillment request", invalid.getMessage());
            return;
        }
        MDC.put("fulfillmentRequestId", command.fulfillmentRequestId());
        MDC.put("orderId", command.orderId());
        if (command.correlationId() != null) {
            MDC.put("correlationId", command.correlationId());
        }
        try {
            AcceptanceResult result = camel.requestBody("direct:accept-command", command, AcceptanceResult.class);
            String location = "/api/fulfillments/" + command.fulfillmentRequestId();
            switch (result) {
                case CREATED -> {
                    ObjectNode body = mapper.createObjectNode();
                    body.put("fulfillmentRequestId", command.fulfillmentRequestId());
                    body.put("acceptance", "ACCEPTED");
                    body.put("status", location);
                    respond(e, 202, body.toString(), "application/json");
                    e.getMessage().setHeader("Location", location);
                }
                case EQUIVALENT -> {
                    respond(e, 200, json(store.view(command.fulfillmentRequestId()).orElseThrow()), "application/json");
                    e.getMessage().setHeader("Location", location);
                }
                case CONFLICT -> problem(e, 409, "Conflicting reuse of fulfillmentRequestId",
                        "A different request already uses this fulfillmentRequestId");
                default -> throw new IllegalStateException("unknown result");
            }
        } catch (org.apache.camel.CamelExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof InvalidCommandException invalid) {
                problem(e, 400, "Invalid fulfillment request", invalid.getMessage());
            } else if (cause instanceof StoreException) {
                LOG.warnv("Acceptance failed: storage unavailable ({0})", ((StoreException) cause).sqlState());
                problem(e, 503, "Storage unavailable", "The request was not accepted; retry later");
            } else {
                LOG.warnv("Acceptance failed: {0}", cause == null ? "unknown" : cause.getClass().getSimpleName());
                problem(e, 503, "Acceptance unavailable", "The request was not accepted; retry later");
            }
        } finally {
            MDC.remove("fulfillmentRequestId");
            MDC.remove("orderId");
            MDC.remove("correlationId");
        }
    }

    /** GET /api/fulfillments/{fulfillmentRequestId}. */
    public void get(Exchange e) {
        String id = e.getMessage().getHeader("fulfillmentRequestId", String.class);
        if (id == null || !ID.matcher(id).matches()) {
            problem(e, 404, "Not found", "No fulfillment request with this ID");
            return;
        }
        try {
            Optional<FulfillmentView> view = store.view(id);
            if (view.isEmpty()) {
                problem(e, 404, "Not found", "No fulfillment request with this ID");
            } else {
                respond(e, 200, json(view.get()), "application/json");
            }
        } catch (StoreException failure) {
            problem(e, 503, "Storage unavailable", "Retry later");
        }
    }

    /** POST /internal/fulfillments/{id}/reconcile: needs-attention back to dispatch, audited. */
    public void reconcile(Exchange e) {
        String id = e.getMessage().getHeader("fulfillmentRequestId", String.class);
        if (id == null || !ID.matcher(id).matches() || store.view(id).isEmpty()) {
            problem(e, 404, "Not found", "No fulfillment request with this ID");
            return;
        }
        boolean requeued = reconcile.operatorReconcile(id, actor(e));
        if (!requeued) {
            problem(e, 409, "Not in needs-attention", "Only a request in NEEDS_ATTENTION can be reconciled");
            return;
        }
        respond(e, 202, json(store.view(id).orElseThrow()), "application/json");
    }

    private FulfillmentCommand parse(String body, Exchange e) {
        JsonNode root;
        try {
            root = body == null ? null : mapper.readTree(body);
        } catch (JsonProcessingException failure) {
            throw new InvalidCommandException("INVALID_JSON", "Body is not valid JSON");
        }
        if (root == null || !root.isObject()) {
            throw new InvalidCommandException("NOT_AN_OBJECT", "Body must be a JSON object");
        }
        for (Iterator<String> names = root.fieldNames(); names.hasNext();) {
            String name = names.next();
            if (!FIELDS.contains(name)) {
                throw new InvalidCommandException("UNKNOWN_FIELD", "Unknown field " + name.replaceAll("[^A-Za-z0-9_]", "?"));
            }
        }
        JsonNode quantity = root.get("quantity");
        if (quantity == null || !quantity.isIntegralNumber() || !quantity.canConvertToLong()) {
            throw new InvalidCommandException("INVALID_QUANTITY", "quantity must be an integer from 1 to 999999999");
        }
        return new FulfillmentCommand(text(root, "fulfillmentRequestId"), text(root, "orderId"), text(root, "reservationId"),
                text(root, "siteId"), text(root, "sku"), quantity.asLong(), Origin.API, Instant.now(), correlationId(e),
                traceparent(e), "api-" + UUID.randomUUID());
    }

    private static String text(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isTextual()) {
            throw new InvalidCommandException("MISSING_FIELD", field + " is required and must be a string");
        }
        return value.asText();
    }

    /** Optional X-Correlation-ID, kept only if ERP1 can carry it; otherwise ignored rather than failing the request. */
    private static String correlationId(Exchange e) {
        String value = e.getMessage().getHeader("X-Correlation-ID", String.class);
        if (value == null || value.isEmpty() || value.codePointCount(0, value.length()) > 128
                || value.chars().anyMatch(c -> c <= 0x1f || (c >= 0x7f && c <= 0x9f))) {
            return null;
        }
        return value;
    }

    /** The server span's context, so logs, the ERP1 file and the outcome event share the trace; else the header. */
    private static String traceparent(Exchange e) {
        SpanContext span = Span.current().getSpanContext();
        if (span.isValid()) {
            return "00-" + span.getTraceId() + "-" + span.getSpanId() + "-" + span.getTraceFlags().asHex();
        }
        String header = e.getMessage().getHeader("traceparent", String.class);
        return header != null && TRACEPARENT.matcher(header).matches() ? header : null;
    }

    private static String actor(Exchange e) {
        Object user = e.getMessage().getHeader("CamelVertxPlatformHttpAuthenticatedUser");
        if (user instanceof QuarkusHttpUser http) {
            SecurityIdentity identity = http.getSecurityIdentity();
            if (identity != null && identity.getPrincipal() != null) {
                return identity.getPrincipal().getName();
            }
        }
        return "unknown";
    }

    private String json(FulfillmentView view) {
        try {
            return mapper.writeValueAsString(view);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private void problem(Exchange e, int status, String title, String detail) {
        ObjectNode body = mapper.createObjectNode();
        body.put("type", "about:blank");
        body.put("title", title);
        body.put("status", status);
        body.put("detail", detail);
        respond(e, status, body.toString(), PROBLEM);
    }

    private static void respond(Exchange e, int status, String body, String type) {
        // Request headers (Authorization included) must never be echoed into the response.
        e.getMessage().removeHeaders("*");
        e.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, status);
        e.getMessage().setHeader(Exchange.CONTENT_TYPE, type);
        e.getMessage().setBody(body);
    }
}
