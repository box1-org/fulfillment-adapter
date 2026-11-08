# fulfillment-adapter

The modern integration boundary in front of the legacy ERP appliance ([box1-docs#13](https://github.com/box1-org/box1-docs/issues/13)).
It durably accepts fulfillment requests, translates them into ERP1 legacy files, exchanges them with the ERP over SFTP,
reconciles the ERP's results and publishes `shipment.confirmed.v1` and `shipment.failed.v1`. Apache Camel routes run
inside this Quarkus application; there is no Camel K, no extra broker, and no HTTP call to the ERP.

The routes are Camel YAML DSL in [`src/main/resources/routes/`](src/main/resources/routes/), so Kaoto opens and edits
them (in Dev Spaces too). Steps that need Java are methods on `camel/RouteSteps` and `camel/RestHandlers`, called as
`bean` steps. The ERP1 exchange routes in `erp1-exchange.camel.yaml` have a `precondition` and are created only when
`FULFILLMENT_SFTP_ENABLED` is true.

Owner: `user:default/mike`. Quarkus 3.40.1.1, Camel Quarkus 3.40.0 (Camel 4.22.1), Java 21.

| Concern | Where |
|---|---|
| Inputs | Kafka `fulfillment.requested.v1` on `box1.order.events.v1` (group `fulfillment`, production) and `POST /api/fulfillments` (demos, operators); one acceptance path |
| Legacy protocol | ERP1, [`box1-ops-contracts/legacy/erp-file/v1/SPEC.md`](https://github.com/box1-org/box1-ops-contracts/blob/eef13ead19aa2ed4b6bae071f26e14dd1676f8f1/legacy/erp-file/v1/SPEC.md), pinned in `pom.xml` (`contracts.ref`) and never copied |
| Outputs | `box1.fulfillment.events.v1`, keyed by `orderId`, via a transactional outbox |
| Dead letters | `box1.fulfillment.dlq.v1` (must exist before Kafka input is enabled) |
| State | the service's own PostgreSQL (Flyway `V1__fulfillment_schema.sql`) |
| API | `src/main/resources/META-INF/openapi.yaml`, served at `/q/openapi` |
| Design | [docs/architecture.md](docs/architecture.md) |
| Operations | [docs/runbook.md](docs/runbook.md) |

## Configuration

All settings are environment variables; none has a real credential as a default.

| Variable | Default | Meaning |
|---|---|---|
| `QUARKUS_DATASOURCE_JDBC_URL`, `_USERNAME`, `_PASSWORD` | none | PostgreSQL; password from a Secret reference |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka |
| `FULFILLMENT_KAFKA_INPUT_ENABLED` | `true` | consume `fulfillment.requested.v1` |
| `FULFILLMENT_SFTP_ENABLED` | `false` | dispatch and result routes; accepted work stays queued while off |
| `FULFILLMENT_SFTP_HOST`, `_PORT`, `_USERNAME` | `localhost`, `22`, `erpsftp` | ERP exchange |
| `FULFILLMENT_SFTP_PRIVATE_KEY_FILE`, `_KNOWN_HOSTS_FILE` | `/var/run/secrets/box1/sftp/...` | mounted from a Secret; strict host-key checking is always on |
| `FULFILLMENT_READ_POLICY` | `authenticated` | `GET` access: `authenticated` or `permit` |
| `FULFILLMENT_JWT_ISSUER`, `FULFILLMENT_JWKS_URL` | Keycloak realm `box1` | token verification; audience `fulfillment-adapter`, role `fulfillment-writer` |
| `FULFILLMENT_ACK_TIMEOUT_BASE_SECONDS`, `_MAX_SECONDS`, `FULFILLMENT_ACK_MAX_UPLOADS` | 120, 3600, 8 | resend schedule without acknowledgement |
| `FULFILLMENT_UPLOAD_BACKOFF_BASE_SECONDS`, `_MAX_SECONDS`, `FULFILLMENT_UPLOAD_MAX_ATTEMPTS` | 10, 300, 40 | upload retry bound |
| `FULFILLMENT_ERR_MAX_RESENDS` | 5 | resends after transport-damage `ERR` files, with backoff |
| `FULFILLMENT_PROBE_INTERVAL_SECONDS` | 21600 | equivalent-resend probe for acknowledged, unfinished requests |
| `FULFILLMENT_CONSUMED_RETENTION` | `P7D` | deletion of the adapter's `consumed/` files |

## Tests

```sh
./mvnw verify -DskipITs=false                 # what CI runs: unit, fixture and HTTP tests, Checkstyle, SpotBugs
export DOCKER_HOST=unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}') TESTCONTAINERS_RYUK_DISABLED=true
./mvnw verify -DskipITs=false -Pcontainers    # adds PostgreSQL and Kafka scenarios (containers named run13-tc-*)
```

ERP1 fixture tests fetch `box1-ops-contracts` at `contracts.ref` over HTTPS; set `-Dcontracts.dir=<checkout>` to
read a local checkout at that commit instead. SFTP and ERP scenarios run against the ERP container harness from
[box1-ops-erp-simulator](https://github.com/box1-org/box1-ops-erp-simulator); the scripts are in the
box1-docs run state for #13 (`agent-runs/13-fulfillment-adapter/evidence/scripts/`).

The Golden Path greeting endpoint and `box1_greetings_total` metric remain because the shared release smoke test
asserts them.

## Local development

Install JDK 21 and use the included Maven wrapper:

```sh
./mvnw quarkus:dev
./mvnw verify -DskipITs=false -Dgit.ref="$(git rev-parse HEAD)"
curl http://localhost:8080/api/greetings?name=Ada
curl http://localhost:8080/api/info
```

Verification runs unit tests, HTTP tests, packaged JVM integration tests,
Checkstyle, and SpotBugs/FindSecBugs. `/api/info` includes the immutable Git SHA
embedded in the image; local builds default to `unknown` unless supplied above.
Java package `org.box1` is deliberately stable across generated services.

## First deployment

Create the service in Developer Hub. Its task tracks repository creation,
validated development onboarding, platform readiness, initial CI, image promotion,
and the verified first deployment. When the task completes, its output links to
source, catalog, initial pipeline, Argo CD, and the running service. No manual
onboarding merge or extra source commit is required.

If onboarding fails, the task reports the failed state and diagnostic link.
Submit the same name, owner, visibility, and description to resume the recorded
workflow. The repository and development resources are reused. A concurrent
request for the same service is rejected while the first task holds its lease.
The initial CI run is named `onb-fulfillment-adapter-<revision>`; a retried failed
build has a numbered suffix. Completed builds are reused.

Route: https://fulfillment-adapter-dev.apps.ocp.box1.org

## Delivery and configuration

PRs validate without registry credentials. Main releases use centrally managed
Tasks from `box1-gitops/platform/pipelines/tasks`, resolved from `box1-ci`.
GitHub checks link to cluster PipelineRuns named `fulfillment-adapter-validate-*`
and `fulfillment-adapter-release-*`. Pipelines as Code and Developer Hub use
separate GitHub Apps, both installed for all `box1-org` repositories.

Jib creates JVM images from a digest-pinned UBI 9 / Java 21 runtime without a
Docker daemon. Images use `registry.apps.ocp.box1.org/box1-ci/fulfillment-adapter:<SHA>`;
OpenShift pulls by digest through the internal registry service.

After onboarding, the GitOps repository is the authoritative deployment source.
Change the Route, probes, resources, or
`GREETING_PREFIX` there. The default smoke contract expects `Hello from box1`.
The ConfigMap also sets JVM container memory limits; runtime has no API token.

Endpoints: `/api/greetings`, `/api/info`, `/q/health/live`, `/q/health/ready`,
`/q/health/started`, `/q/openapi`, `/q/metrics`.

The development promotion policy is automated after checks. Other environments
need explicit platform configuration and approval. Static analysis is included;
no external dependency CVE database scan is implied.

## API documentation and endpoint testing

Developer Hub populates **Provides APIs** with `fulfillment-adapter-api`.
Open the API's **Definition** tab to browse the deployed OpenAPI contract and use
**Try it out**. The catalog reads `https://fulfillment-adapter-dev.apps.ocp.box1.org/q/openapi`,
so the API entity becomes available after the first successful deployment.
Swagger UI is also available at `https://fulfillment-adapter-dev.apps.ocp.box1.org/q/swagger-ui/`.
The production contract targets the public HTTPS Route; override `PUBLIC_API_URL`
when deploying behind another public address. CORS allows the Developer Hub
origin to make tester requests without cookies. API authentication, if added,
should be declared in the OpenAPI contract and supplied through **Authorize**.

## Documentation

Developer Hub publishes `docs/` on the service's **Docs** tab using TechDocs.
List new pages in `mkdocs.yml`.
