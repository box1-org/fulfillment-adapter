package org.box1.fulfillment;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * Legacy ERP1 types stay in the translation layer (packages {@code erp1}, {@code backend} and the Camel wiring in
 * {@code camel}). The modern core never imports them, so delimiters, legacy codes and exchange paths cannot leak upstream.
 */
class ArchitectureTest {
    private static final Path MAIN = Path.of("src/main/java/org/box1/fulfillment");

    @Test
    void modernCoreHasNoLegacyImports() throws IOException {
        List<Path> core;
        try (Stream<Path> files = Files.walk(MAIN)) {
            core = files.filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> !p.startsWith(MAIN.resolve("erp1")) && !p.startsWith(MAIN.resolve("backend"))
                            && !p.startsWith(MAIN.resolve("camel")))
                    .toList();
        }
        assertTrue(core.size() >= 15, "found the core sources: " + core.size());
        for (Path p : core) {
            String source = Files.readString(p);
            assertTrue(!source.contains("org.box1.fulfillment.erp1") && !source.contains("org.box1.fulfillment.backend.Legacy")
                    && !source.contains("org.box1.fulfillment.backend.Erp1"), p + " imports a legacy type");
        }
    }
}
