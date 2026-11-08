package org.box1.fulfillment.camel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.model.ModelCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.rest.RestDefinition;
import org.apache.camel.support.DefaultExchange;
import org.box1.fulfillment.domain.FulfillmentCommand;
import org.box1.fulfillment.domain.InvalidCommandException;
import org.box1.fulfillment.domain.Origin;
import org.junit.jupiter.api.Test;

/**
 * The routes come from the Camel YAML DSL files Kaoto edits, and nothing else. With SFTP off (the default) only the
 * REST, acceptance and reconciliation routes exist; {@link YamlRoutesSftpTest} covers the ERP1 exchange routes.
 */
@QuarkusTest
class YamlRoutesTest {
    static final List<String> FILES = List.of("rest-api", "fulfillment", "erp1-exchange");
    static final Set<String> ALWAYS = Set.of("fulfillment-rest", "fulfillment-rest-status", "fulfillment-rest-operator",
            "fulfillment-rest-operator-bulk", "fulfillment-accept", "fulfillment-reconcile");

    @Inject
    CamelContext context;

    @Inject
    ProducerTemplate producer;

    @Test
    void sftpRoutesAreNotCreatedWhenSftpIsDisabled() {
        assertEquals(new TreeSet<>(ALWAYS), routeIds(context));
        assertAllFromYaml(context);
    }

    @Test
    void restApiIsDeclaredInYaml() {
        Map<String, String> operations = ((ModelCamelContext) context).getRestDefinitions().stream()
                .flatMap((RestDefinition rest) -> rest.getVerbs().stream()
                        .map(verb -> Map.entry(verb.asVerb().toUpperCase() + " " + rest.getPath()
                                + (verb.getPath() == null ? "" : verb.getPath()), verb.getTo().getUri())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertEquals(Map.of(
                "POST /api/fulfillments", "direct:rest-post",
                "GET /api/fulfillments/{fulfillmentRequestId}", "direct:rest-get",
                "POST /internal/fulfillments/reconcile", "direct:rest-reconcile-bulk",
                "POST /internal/fulfillments/{fulfillmentRequestId}/reconcile", "direct:rest-reconcile"), operations);
    }

    /** A REST route answers an unexpected failure with a fixed problem document, not the exception. */
    @Test
    void restRoutesHandleUnexpectedFailures() throws Exception {
        context.getRouteController().startRoute("fulfillment-rest-status");
        try {
            // The REST DSL inlines direct:rest-get into the REST route, so drive the route's consumer processor.
            Exchange reply = new DefaultExchange(context);
            reply.getMessage().setHeader("fulfillmentRequestId", new Object() {
                @Override
                public String toString() {
                    throw new IllegalStateException("boom");
                }
            });
            context.getRoute("fulfillment-rest-status").getConsumer().getProcessor().process(reply);
            assertNull(reply.getException());
            assertEquals(500, reply.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
            assertEquals("application/problem+json", reply.getMessage().getHeader(Exchange.CONTENT_TYPE));
            assertTrue(reply.getMessage().getBody(String.class).contains("\"title\":\"Internal error\""));
        } finally {
            context.getRouteController().stopRoute("fulfillment-rest-status");
        }
    }

    /**
     * The REST handler must not reach the acceptance route: its callers (REST and Kafka) map the original exception
     * themselves. A file route that handled an exception would move an uncommitted file.
     */
    @Test
    void acceptanceFailurePropagatesToTheCaller() throws Exception {
        context.getRouteController().startRoute("fulfillment-accept");
        try {
            FulfillmentCommand invalid = new FulfillmentCommand("ful_1", "ord_1", "res_1", "site_1", "SKU-1", 0,
                    Origin.API, Instant.now(), null, null, "api-test");
            Exchange reply = producer.send("direct:accept-command", e -> e.getMessage().setBody(invalid));
            assertInstanceOf(InvalidCommandException.class, reply.getException());
        } finally {
            context.getRouteController().stopRoute("fulfillment-accept");
        }
    }

    /** Kaoto validates against the same schema; a file that fails it would not open in the designer. */
    @Test
    void filesMatchTheCamelYamlDslSchema() throws IOException {
        JsonSchema schema;
        try (InputStream in = CamelContext.class.getClassLoader().getResourceAsStream("schema/camelYamlDsl.json")) {
            schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4).getSchema(in);
        }
        ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
        for (String file : FILES) {
            try (InputStream in = getClass().getClassLoader().getResourceAsStream("routes/" + file + ".camel.yaml")) {
                JsonNode routes = yaml.readTree(in);
                Set<ValidationMessage> errors = schema.validate(routes);
                assertTrue(errors.isEmpty(), file + ": " + errors);
            }
        }
    }

    static Set<String> routeIds(CamelContext context) {
        return ((ModelCamelContext) context).getRouteDefinitions().stream().map(RouteDefinition::getRouteId)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    static void assertAllFromYaml(CamelContext context) {
        for (RouteDefinition route : ((ModelCamelContext) context).getRouteDefinitions()) {
            String location = route.getResource() == null ? "java" : route.getResource().getLocation();
            assertTrue(FILES.stream().anyMatch(f -> location.equals("classpath:routes/" + f + ".camel.yaml")),
                    route.getRouteId() + " loaded from " + location);
        }
    }
}
