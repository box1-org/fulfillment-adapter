package org.box1.fulfillment.camel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.util.URISupport;
import org.junit.jupiter.api.Test;

/**
 * With SFTP on, the ERP1 exchange routes load from YAML (routes are created but not started in tests). Their
 * endpoints must be exactly the ones the Java DSL built: each URI below is the Java DSL's, compared by scheme, path
 * and decoded options.
 */
@QuarkusTest
@TestProfile(YamlRoutesSftpTest.SftpEnabled.class)
class YamlRoutesSftpTest {
    private static final String SFTP = "sftp://{{fulfillment.sftp.host}}:{{fulfillment.sftp.port}}/";
    private static final String AUTH = "?username={{fulfillment.sftp.username}}"
            + "&privateKeyFile={{fulfillment.sftp.private-key-file}}"
            + "&knownHostsFile={{fulfillment.sftp.known-hosts-file}}&strictHostKeyChecking=yes"
            + "&useUserKnownHostsFile=false&autoCreate=false&binary=true"
            + "&connectTimeout={{fulfillment.sftp.connect-timeout-ms}}&timeout={{fulfillment.sftp.timeout-ms}}"
            + "&serverAliveInterval=15000&serverAliveCountMax=2";

    public static class SftpEnabled implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("fulfillment.sftp.enabled", "true");
        }
    }

    @Inject
    CamelContext context;

    @Test
    void allRoutesLoadFromYaml() {
        Set<String> expected = new TreeSet<>(YamlRoutesTest.ALWAYS);
        expected.addAll(Set.of("fulfillment-dispatch", "erp-results", "outbound-hidden-sweep", "consumed-retention"));
        assertEquals(expected, YamlRoutesTest.routeIds(context));
        YamlRoutesTest.assertAllFromYaml(context);
    }

    @Test
    void endpointsMatchTheJavaDsl() throws Exception {
        assertEndpoint(SFTP + "inbound" + AUTH + "&tempFileName=.${file:onlyname}.tmp&fileExist=Fail"
                + "&maximumReconnectAttempts=0&disconnect=true");
        assertEndpoint(SFTP + "outbound" + AUTH + "&delay={{fulfillment.results.poll-interval-ms}}&initialDelay=4000"
                + "&exclude=^\\..*\\.tmp$&recursive=false&maxMessagesPerPoll=100&streamDownload=true"
                + "&move=../consumed/${header.box1ConsumedName}&readLock=none&disconnect=false"
                + "&throwExceptionOnConnectFailed=true&bridgeErrorHandler=true");
        assertEndpoint(SFTP + "consumed" + AUTH + "&delay={{fulfillment.consumed.sweep-interval-ms}}&initialDelay=60000"
                + "&download=false&delete=true&filter=#consumedRetentionFilter&recursive=false&maxMessagesPerPoll=500");
        assertEndpoint("scheduler:dispatch?delay={{fulfillment.dispatch.poll-interval-ms}}&initialDelay=3000");
        assertEndpoint("scheduler:reconcile?delay={{fulfillment.reconcile.poll-interval-ms}}&initialDelay=5000");
        assertEndpoint("scheduler:hidden-sweep?delay={{fulfillment.results.hidden-sweep-interval-ms}}&initialDelay=10000");
    }

    /** Same scheme, path and options; the YAML DSL only differs in encoding and in wrapping values in RAW(). */
    private void assertEndpoint(String javaDslUri) throws Exception {
        String expected = canonical(context.resolvePropertyPlaceholders(javaDslUri));
        List<String> actual = new ArrayList<>();
        for (Endpoint endpoint : context.getEndpoints()) {
            actual.add(canonical(endpoint.getEndpointUri()));
        }
        assertTrue(actual.contains(expected), expected + " not among " + actual);
    }

    private static String canonical(String uri) throws Exception {
        String normalized = URISupport.normalizeUri(uri);
        int query = normalized.indexOf('?');
        Map<String, Object> options = new TreeMap<>();
        if (query >= 0) {
            URISupport.parseQuery(normalized.substring(query + 1), true, true).forEach((key, value) -> {
                String text = String.valueOf(value);
                options.put(key, text.startsWith("RAW(") && text.endsWith(")") ? text.substring(4, text.length() - 1)
                        : URLDecoder.decode(text, StandardCharsets.UTF_8));
            });
        }
        return (query < 0 ? normalized : normalized.substring(0, query)) + options;
    }
}
