package org.box1.fulfillment;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.response.Response;
import org.junit.jupiter.api.Test;

/** REST acceptance against real PostgreSQL: durability before 202, equivalence, conflicts, validation and access. */
@QuarkusTest
@ContainersTag
class AcceptanceScenariosTest {
    static String id() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 20);
    }

    static String body(String n, String sku, long quantity) {
        return "{\"fulfillmentRequestId\":\"ful_" + n + "\",\"orderId\":\"ord_" + n + "\",\"reservationId\":\"res_" + n
                + "\",\"siteId\":\"site_lab\",\"sku\":\"" + sku + "\",\"quantity\":" + quantity + "}";
    }

    static Response post(String token, String json) {
        var spec = given().contentType("application/json").body(json);
        if (token != null) {
            spec = spec.header("Authorization", "Bearer " + token);
        }
        return spec.post("/api/fulfillments");
    }

    @Test
    void acceptedIsCommittedBeforeTheResponse() {
        String n = id();
        post(TestTokens.admin("op"), body(n, "PUMP-1", 2)).then().statusCode(202)
                .header("Location", "/api/fulfillments/ful_" + n).body("acceptance", equalTo("ACCEPTED"));
        // Read with an independent connection the moment 202 is seen: the row and its dispatch intent exist.
        assertEquals("QUEUED", Db.one("SELECT dispatch_state FROM fulfillment_request WHERE fulfillment_request_id = ?",
                "ful_" + n));
        assertEquals("API", Db.one("SELECT origin FROM fulfillment_request WHERE fulfillment_request_id = ?", "ful_" + n));
        assertEquals("legacy-erp1", Db.one("SELECT backend FROM fulfillment_request WHERE fulfillment_request_id = ?",
                "ful_" + n));
    }

    @Test
    void equivalentRepeatResolvesToTheSameResource() {
        String n = id();
        post(TestTokens.admin("op"), body(n, "PUMP-1", 2)).then().statusCode(202);
        post(TestTokens.admin("op"), body(n, "PUMP-1", 2)).then().statusCode(200)
                .header("Location", "/api/fulfillments/ful_" + n).body("fulfillmentRequestId", equalTo("ful_" + n));
        assertEquals(1L, Db.one("SELECT count(*) FROM fulfillment_request WHERE fulfillment_request_id = ?", "ful_" + n));
    }

    @Test
    void conflictingReuseIs409AndChangesNothing() {
        String n = id();
        post(TestTokens.admin("op"), body(n, "PUMP-1", 2)).then().statusCode(202);
        post(TestTokens.admin("op"), body(n, "PUMP-1", 3)).then().statusCode(409)
                .contentType("application/problem+json");
        assertEquals(2L, Db.one("SELECT quantity FROM fulfillment_request WHERE fulfillment_request_id = ?", "ful_" + n));
    }

    @Test
    void anonymousWrongRoleAudienceIssuerOrKeyCannotCreate() {
        String n = id();
        post(null, body(n, "PUMP-1", 1)).then().statusCode(401);
        post(TestTokens.viewer(), body(n, "PUMP-1", 1)).then().statusCode(403);
        post(TestTokens.groupsOnly(), body(n, "PUMP-1", 1)).then().statusCode(403);
        post(TestTokens.wrongAudience(), body(n, "PUMP-1", 1)).then().statusCode(401);
        post(TestTokens.wrongIssuer(), body(n, "PUMP-1", 1)).then().statusCode(401);
        post(TestTokens.wrongKey(), body(n, "PUMP-1", 1)).then().statusCode(401);
        post(TestTokens.expired(), body(n, "PUMP-1", 1)).then().statusCode(401);
        given().post("/internal/fulfillments/ful_" + n + "/reconcile").then().statusCode(401);
        assertEquals(0L, Db.one("SELECT count(*) FROM fulfillment_request WHERE fulfillment_request_id = ?", "ful_" + n));
    }

    @Test
    void readsNeedATokenUnderTheDefaultPolicy() {
        String n = id();
        post(TestTokens.admin("op"), body(n, "PUMP-1", 1)).then().statusCode(202);
        given().get("/api/fulfillments/ful_" + n).then().statusCode(401);
        given().header("Authorization", "Bearer " + TestTokens.viewer()).get("/api/fulfillments/ful_" + n).then()
                .statusCode(200).body("dispatch", equalTo("QUEUED")).body("erpAcknowledgement", equalTo("NONE"))
                .body("outcome", equalTo("AWAITING_ERP"));
        given().header("Authorization", "Bearer " + TestTokens.viewer()).get("/api/fulfillments/ful_nothere").then()
                .statusCode(404);
    }

    @Test
    void valuesErp1CannotCarryAreRejectedNotTruncated() {
        String n = id();
        String token = TestTokens.admin("op");
        post(token, body(n, "PUMP-1", 1_000_000_000L)).then().statusCode(400).contentType("application/problem+json");
        post(token, body(n, "A".repeat(81), 1)).then().statusCode(400);
        post(token, body(n, "TAB\\tSKU", 1)).then().statusCode(400);
        post(token, body(n, "PUMP-1", 0)).then().statusCode(400);
        post(token, "{\"fulfillmentRequestId\":\"ful_" + n + "\",\"extra\":1}").then().statusCode(400);
        post(token, body(n, "PUMP-1", 1).replace("\"quantity\":1", "\"quantity\":\"1\"")).then().statusCode(400);
        post(token, body(n, "PUMP-1", 1).replace("ful_", "bad_")).then().statusCode(400);
        // 80 code points of a supplementary character is 160 UTF-16 units and still valid.
        String emoji = "🚀".repeat(80);
        post(token, body(n, emoji, 1)).then().statusCode(202);
        assertEquals(emoji, Db.one("SELECT sku FROM fulfillment_request WHERE fulfillment_request_id = ?", "ful_" + n));
    }

    @Test
    void responsesNeverEchoTheAuthorizationHeader() {
        String n = id();
        Response r = post(TestTokens.admin("op"), body(n, "PUMP-1", 1));
        assertEquals(null, r.getHeader("Authorization"));
        r.then().header("Content-Type", startsWith("application/json"));
    }
}
