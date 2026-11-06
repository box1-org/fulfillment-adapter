package org.box1.fulfillment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.smallrye.jwt.build.Jwt;

/**
 * Issues tokens from a key generated for this test run only. Nothing here reaches Keycloak. The service verifies them with
 * the same issuer, audience and role rules it uses in production, and only the key location differs.
 */
public final class TestTokens implements QuarkusTestResourceLifecycleManager {
    static final String ISSUER = "https://keycloak.apps.ocp.box1.org/auth/realms/box1";
    private static final KeyPair KEYS = generate();
    private static final KeyPair OTHER_KEYS = generate();

    private static KeyPair generate() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            return generator.generateKeyPair();
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException(error);
        }
    }

    @Override
    public Map<String, String> start() {
        try {
            Path pem = Files.createTempFile("fulfillment-test-jwt", ".pem");
            String body = Base64.getMimeEncoder().encodeToString(KEYS.getPublic().getEncoded());
            Files.writeString(pem, "-----BEGIN PUBLIC KEY-----\n" + body + "\n-----END PUBLIC KEY-----\n");
            return Map.of("mp.jwt.verify.publickey.location", pem.toUri().toString());
        } catch (IOException error) {
            throw new IllegalStateException(error);
        }
    }

    @Override
    public void stop() {
        // The key file is in the system temp directory, and holds only a public key.
    }

    public static String admin(String subject) {
        return token(subject, ISSUER, "fulfillment-adapter", List.of("fulfillment-writer"), KEYS);
    }

    public static String viewer() {
        return token("subject-viewer", ISSUER, "fulfillment-adapter", List.of("viewer"), KEYS);
    }

    public static String wrongAudience() {
        return token("subject-audience", ISSUER, "other-service", List.of("fulfillment-writer"), KEYS);
    }

    public static String wrongIssuer() {
        return token("subject-issuer", "https://example.invalid/realms/box1", "fulfillment-adapter", List.of("fulfillment-writer"), KEYS);
    }

    public static String wrongKey() {
        return token("subject-key", ISSUER, "fulfillment-adapter", List.of("fulfillment-writer"), OTHER_KEYS);
    }

    /** Signed correctly, with the role only in a top-level groups claim. */
    public static String groupsOnly() {
        return Jwt.issuer(ISSUER).audience("fulfillment-adapter").subject("subject-groups")
                .groups(java.util.Set.of("fulfillment-writer")).sign(KEYS.getPrivate());
    }

    public static String expired() {
        return Jwt.issuer(ISSUER).audience("fulfillment-adapter").subject("subject-expired").expiresAt(1000L)
                .claim("resource_access", Map.of("fulfillment-adapter", Map.of("roles", List.of("fulfillment-writer"))))
                .sign(KEYS.getPrivate());
    }

    private static String token(String subject, String issuer, String audience, List<String> roles, KeyPair keys) {
        return Jwt.issuer(issuer).audience(audience).subject(subject)
                .claim("resource_access", Map.of("fulfillment-adapter", Map.of("roles", roles)))
                .sign(keys.getPrivate());
    }
}
