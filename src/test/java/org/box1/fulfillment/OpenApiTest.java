package org.box1.fulfillment;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

/** The served contract includes the adapter's paths and keeps the Golden Path paths the release smoke checks. */
@QuarkusTest
class OpenApiTest {
    @Test
    void servedContractHasAdapterAndGoldenPathPaths() {
        given().get("/q/openapi").then().statusCode(200).body(containsString("/api/fulfillments:"))
                .body(containsString("/api/fulfillments/{fulfillmentRequestId}:"))
                .body(containsString("/internal/fulfillments/{fulfillmentRequestId}/reconcile:"))
                .body(containsString("/api/greetings")).body(containsString("/api/info"));
    }
}
