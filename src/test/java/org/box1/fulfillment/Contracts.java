package org.box1.fulfillment;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reads box1-ops-contracts at the commit pinned in pom.xml ({@code contracts.ref}). Files are fetched, never copied
 * into this repository. Set {@code -Dcontracts.dir=<checkout at that commit>} to read a local checkout instead.
 */
public final class Contracts {
    private static final Map<String, byte[]> CACHE = new ConcurrentHashMap<>();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL).build();

    private Contracts() {
    }

    public static String ref() {
        String ref = System.getProperty("contracts.ref");
        if (ref == null || !ref.matches("[0-9a-f]{40}")) {
            throw new IllegalStateException("contracts.ref must be a full commit SHA");
        }
        return ref;
    }

    public static byte[] bytes(String path) {
        return CACHE.computeIfAbsent(path, Contracts::load);
    }

    private static byte[] load(String path) {
        try {
            String dir = System.getProperty("contracts.dir");
            if (dir != null && !dir.isBlank()) {
                return Files.readAllBytes(Path.of(dir).resolve(path));
            }
            URI uri = URI.create("https://raw.githubusercontent.com/box1-org/box1-ops-contracts/" + ref() + "/" + path);
            HttpResponse<byte[]> response = HTTP.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(30)).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("HTTP " + response.statusCode() + " for " + uri);
            }
            return response.body();
        } catch (IOException e) {
            throw new IllegalStateException("cannot read contracts file " + path, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
