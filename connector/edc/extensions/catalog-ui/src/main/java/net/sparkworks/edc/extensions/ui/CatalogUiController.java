package net.sparkworks.edc.extensions.ui;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.Result;
import io.minio.messages.Item;
import org.eclipse.edc.connector.controlplane.asset.spi.index.AssetIndex;
import org.eclipse.edc.connector.controlplane.contract.spi.offer.store.ContractDefinitionStore;
import org.eclipse.edc.connector.controlplane.policy.spi.store.PolicyDefinitionStore;
import org.eclipse.edc.connector.controlplane.services.spi.contractagreement.ContractAgreementService;
import org.eclipse.edc.connector.controlplane.services.spi.contractnegotiation.ContractNegotiationService;
import org.eclipse.edc.connector.controlplane.services.spi.transferprocess.TransferProcessService;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.query.QuerySpec;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.stream.Collectors;

@Path("/")
public class CatalogUiController {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Set<String> SKIP_PROPS = Set.of("name", "contenttype", "description", "id");
    private static final String EDC_NS = "https://w3id.org/edc/v0.0.1/ns/";
    private static final DateTimeFormatter UTC_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"));
    private static final int PREVIEW_MAX_BYTES = 65536;
    private static final String AIRFLOW_URL = System.getenv().getOrDefault("AIRFLOW_URL", "http://airflow:8080");
    private static final String AIRFLOW_USER = System.getenv().getOrDefault("AIRFLOW_USER", "airflow");
    private static final String AIRFLOW_PASSWORD = System.getenv().getOrDefault("AIRFLOW_PASSWORD", "airflow");
    // Forced to HTTP/1.1: the default client negotiates HTTP/2 via cleartext
    // "Upgrade: h2c" headers, which Airflow's Uvicorn-based API server doesn't
    // handle and rejects outright ("Invalid HTTP request received.") instead
    // of just ignoring the upgrade attempt.
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();

    private final AssetIndex assetIndex;
    private final ContractDefinitionStore contractDefinitionStore;
    private final PolicyDefinitionStore policyDefinitionStore;
    private final ContractAgreementService contractAgreementService;
    private final ContractNegotiationService contractNegotiationService;
    private final TransferProcessService transferProcessService;
    private final Monitor monitor;
    private final boolean submitEnabled;

    public CatalogUiController(AssetIndex assetIndex,
                               ContractDefinitionStore contractDefinitionStore,
                               PolicyDefinitionStore policyDefinitionStore,
                               ContractAgreementService contractAgreementService,
                               ContractNegotiationService contractNegotiationService,
                               TransferProcessService transferProcessService,
                               Monitor monitor,
                               boolean submitEnabled) {
        this.assetIndex = assetIndex;
        this.contractDefinitionStore = contractDefinitionStore;
        this.policyDefinitionStore = policyDefinitionStore;
        this.contractAgreementService = contractAgreementService;
        this.contractNegotiationService = contractNegotiationService;
        this.transferProcessService = transferProcessService;
        this.monitor = monitor;
        this.submitEnabled = submitEnabled;
    }

    @GET
    @Path("catalog")
    @Produces(MediaType.TEXT_HTML)
    public Response catalogPage() {
        return servePage("web/catalog.html");
    }

    @GET
    @Path("catalog/submit")
    @Produces(MediaType.TEXT_HTML)
    public Response submitPage() {
        if (!submitEnabled) {
            return submitDisabled();
        }
        return servePage("web/submit-dataset.html");
    }

    private Response submitDisabled() {
        return Response.status(404).entity("Dataset submission is disabled on this connector").build();
    }

    private Response servePage(String resource) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resource)) {
            if (is == null) {
                return Response.status(404).entity(resource + " not found").build();
            }
            String html = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            if (!submitEnabled) {
                // Strip everything between the markers so the nav link to the
                // (disabled) submit page is not rendered.
                html = html.replaceAll("(?s)<!--SUBMIT_LINK-->.*?<!--/SUBMIT_LINK-->", "");
            }
            return Response.ok(html).build();
        } catch (Exception e) {
            monitor.severe("Failed to serve " + resource, e);
            return Response.serverError().entity("Error loading page").build();
        }
    }

    @GET
    @Path("catalog/api/assets")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listAssets() {
        try {
            var query = QuerySpec.Builder.newInstance().build();
            var assets = assetIndex.queryAssets(query).collect(Collectors.toList());

            ArrayNode arr = MAPPER.createArrayNode();
            for (var asset : assets) {
                ObjectNode node = MAPPER.createObjectNode();
                node.put("id", asset.getId());
                node.put("name", asset.getName() != null ? asset.getName() : asset.getId());
                node.put("contentType", getProperty(asset, "contenttype"));
                node.put("description", getProperty(asset, "description"));

                var da = asset.getDataAddress();
                ObjectNode daNode = MAPPER.createObjectNode();
                daNode.put("type", da.getType());
                putIfPresent(daNode, "bucketName", da.getStringProperty("bucketName"));
                putIfPresent(daNode, "prefix", da.getStringProperty("prefix"));
                putIfPresent(daNode, "endpoint", da.getStringProperty("endpoint"));
                putIfPresent(daNode, "baseUrl", da.getStringProperty("baseUrl"));
                node.set("dataAddress", daNode);

                ObjectNode meta = MAPPER.createObjectNode();
                for (var entry : asset.getProperties().entrySet()) {
                    String key = entry.getKey();
                    if (key.startsWith(EDC_NS)) {
                        key = key.substring(EDC_NS.length());
                    }
                    if (SKIP_PROPS.contains(key)) continue;
                    Object value = entry.getValue();
                    // Structured properties (e.g. "semantic_description", a JSON-LD
                    // literal - see submit-dataset.html) need to reach the UI as real
                    // JSON, not Java's Map/List toString() - only flatten plain scalars.
                    if (value instanceof java.util.Map || value instanceof java.util.List) {
                        meta.set(key, MAPPER.valueToTree(value));
                    } else {
                        meta.put(key, String.valueOf(value));
                    }
                }
                node.set("metadata", meta);

                arr.add(node);
            }

            return Response.ok(MAPPER.writeValueAsString(arr)).build();
        } catch (Exception e) {
            monitor.severe("Failed to list assets", e);
            return jsonError(e);
        }
    }

    @GET
    @Path("catalog/api/contracts")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listContracts() {
        try {
            var query = QuerySpec.Builder.newInstance().build();
            var defs = contractDefinitionStore.findAll(query).collect(Collectors.toList());

            ArrayNode arr = MAPPER.createArrayNode();
            for (var def : defs) {
                ObjectNode node = MAPPER.createObjectNode();
                node.put("id", def.getId());
                node.put("accessPolicyId", def.getAccessPolicyId());
                node.put("contractPolicyId", def.getContractPolicyId());
                arr.add(node);
            }

            return Response.ok(MAPPER.writeValueAsString(arr)).build();
        } catch (Exception e) {
            monitor.severe("Failed to list contract definitions", e);
            return jsonError(e);
        }
    }

    @GET
    @Path("catalog/api/policies")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listPolicies() {
        try {
            var query = QuerySpec.Builder.newInstance().build();
            var policies = policyDefinitionStore.findAll(query).collect(Collectors.toList());

            ArrayNode arr = MAPPER.createArrayNode();
            for (var policy : policies) {
                ObjectNode node = MAPPER.createObjectNode();
                node.put("id", policy.getId());
                arr.add(node);
            }

            return Response.ok(MAPPER.writeValueAsString(arr)).build();
        } catch (Exception e) {
            monitor.severe("Failed to list policies", e);
            return jsonError(e);
        }
    }

    @GET
    @Path("catalog/api/agreements")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listAgreements() {
        try {
            var query = QuerySpec.Builder.newInstance().build();
            var result = contractAgreementService.search(query);
            if (result.failed()) {
                return Response.serverError().entity("{\"error\":\"" + result.getFailureDetail() + "\"}").build();
            }

            ArrayNode arr = MAPPER.createArrayNode();
            for (var agreement : result.getContent()) {
                ObjectNode node = MAPPER.createObjectNode();
                node.put("id", agreement.getId());
                node.put("assetId", agreement.getAssetId());
                node.put("providerId", agreement.getProviderId());
                node.put("consumerId", agreement.getConsumerId());
                node.put("signingDate", formatEpoch(agreement.getContractSigningDate()));
                arr.add(node);
            }

            return Response.ok(MAPPER.writeValueAsString(arr)).build();
        } catch (Exception e) {
            monitor.severe("Failed to list agreements", e);
            return jsonError(e);
        }
    }

    @GET
    @Path("catalog/api/negotiations")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listNegotiations() {
        try {
            var query = QuerySpec.Builder.newInstance().build();
            var result = contractNegotiationService.search(query);
            if (result.failed()) {
                return Response.serverError().entity("{\"error\":\"" + result.getFailureDetail() + "\"}").build();
            }

            ArrayNode arr = MAPPER.createArrayNode();
            for (var neg : result.getContent()) {
                ObjectNode node = MAPPER.createObjectNode();
                node.put("id", neg.getId());
                node.put("state", neg.stateAsString());
                node.put("type", neg.getType().name());
                node.put("counterPartyId", neg.getCounterPartyId());
                node.put("counterPartyAddress", neg.getCounterPartyAddress());
                node.put("protocol", neg.getProtocol());
                var agreement = neg.getContractAgreement();
                node.put("agreementId", agreement != null ? agreement.getId() : "");
                arr.add(node);
            }

            return Response.ok(MAPPER.writeValueAsString(arr)).build();
        } catch (Exception e) {
            monitor.severe("Failed to list negotiations", e);
            return jsonError(e);
        }
    }

    @GET
    @Path("catalog/api/transfers")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listTransfers() {
        try {
            var query = QuerySpec.Builder.newInstance().build();
            var result = transferProcessService.search(query);
            if (result.failed()) {
                return Response.serverError().entity("{\"error\":\"" + result.getFailureDetail() + "\"}").build();
            }

            ArrayNode arr = MAPPER.createArrayNode();
            for (var tp : result.getContent()) {
                ObjectNode node = MAPPER.createObjectNode();
                node.put("id", tp.getId());
                node.put("state", tp.stateAsString());
                node.put("type", tp.getType().name());
                node.put("assetId", tp.getAssetId());
                node.put("contractId", tp.getContractId());
                node.put("transferType", tp.getTransferType());
                node.put("counterPartyAddress", tp.getCounterPartyAddress());
                node.put("destinationType", tp.getDestinationType());
                node.put("stateTimestamp", formatEpochMillis(tp.getStateTimestamp()));
                arr.add(node);
            }

            return Response.ok(MAPPER.writeValueAsString(arr)).build();
        } catch (Exception e) {
            monitor.severe("Failed to list transfers", e);
            return jsonError(e);
        }
    }

    @POST
    @Path("catalog/api/upload")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    public Response uploadFile(@jakarta.ws.rs.QueryParam("bucket") String bucket,
                               @jakarta.ws.rs.QueryParam("key") String key,
                               @jakarta.ws.rs.QueryParam("endpoint") String endpoint,
                               @jakarta.ws.rs.QueryParam("accessKey") String accessKey,
                               @jakarta.ws.rs.QueryParam("secretKey") String secretKey,
                               InputStream body) {
        if (!submitEnabled) {
            return submitDisabled();
        }
        try {
            if (bucket == null || key == null) {
                return Response.status(400).entity("{\"error\":\"bucket and key are required\"}").build();
            }
            var ep = endpoint != null ? endpoint : "http://rustfs:9000";
            var ak = accessKey != null ? accessKey : "participant-admin";
            var sk = secretKey != null ? secretKey : "participant-secret-2024";

            var client = MinioClient.builder().endpoint(ep).credentials(ak, sk).build();
            client.putObject(PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(key)
                    .stream(body, -1, 10485760)
                    .contentType("application/octet-stream")
                    .build());

            ObjectNode result = MAPPER.createObjectNode();
            result.put("bucket", bucket);
            result.put("key", key);
            result.put("status", "uploaded");
            return Response.ok(MAPPER.writeValueAsString(result)).build();
        } catch (Exception e) {
            monitor.severe("Failed to upload file: " + key, e);
            return jsonError(e);
        }
    }

    @GET
    @Path("catalog/api/download")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response downloadAsset(@jakarta.ws.rs.QueryParam("assetId") String assetId) {
        try {
            var asset = assetIndex.findById(assetId);
            if (asset == null) {
                return Response.status(404).entity("Asset not found: " + assetId).build();
            }

            var da = asset.getDataAddress();
            var type = da.getType();

            if ("MinioAsset".equals(type) || "MinioFiles".equals(type)) {
                var endpoint = da.getStringProperty("endpoint");
                var bucket = da.getStringProperty("bucketName");
                var accessKey = da.getStringProperty("accessKey");
                var secretKey = da.getStringProperty("secretKey");
                var prefix = da.getStringProperty("prefix");

                var client = MinioClient.builder()
                        .endpoint(endpoint)
                        .credentials(accessKey, secretKey)
                        .build();

                var stream = client.getObject(GetObjectArgs.builder()
                        .bucket(bucket)
                        .object(prefix)
                        .build());

                var fileName = prefix.contains("/") ? prefix.substring(prefix.lastIndexOf('/') + 1) : prefix;

                return Response.ok(stream)
                        .header("Content-Disposition", "attachment; filename=\"" + fileName + "\"")
                        .build();

            } else if ("HttpData".equals(type)) {
                var baseUrl = da.getStringProperty("baseUrl");
                return Response.temporaryRedirect(java.net.URI.create(baseUrl)).build();

            } else {
                return Response.status(400).entity("Download not supported for type: " + type).build();
            }

        } catch (Exception e) {
            monitor.severe("Failed to download asset: " + assetId, e);
            return Response.serverError().entity("Download failed: " + e.getMessage()).build();
        }
    }

    @POST
    @Path("catalog/api/trigger-validation")
    @Produces(MediaType.APPLICATION_JSON)
    public Response triggerValidation(@jakarta.ws.rs.QueryParam("assetId") String assetId) {
        if (!submitEnabled) {
            return submitDisabled();
        }
        try {
            if (assetId == null || assetId.isBlank()) {
                return Response.status(400).entity("{\"error\":\"assetId is required\"}").build();
            }

            String dagId = "hackfest_validate_dataset";
            String auth = "Bearer " + fetchAirflowToken();

            // Airflow pauses every DAG it discovers until someone unpauses it (in
            // the UI or via this same API) - unpause first so triggering works
            // even if nobody has opened the Airflow UI for this DAG yet.
            HttpRequest unpause = HttpRequest.newBuilder()
                    .uri(URI.create(AIRFLOW_URL + "/api/v2/dags/" + dagId))
                    .header("Content-Type", "application/json")
                    .header("Authorization", auth)
                    .method("PATCH", HttpRequest.BodyPublishers.ofString("{\"is_paused\": false}"))
                    .build();
            HTTP_CLIENT.send(unpause, HttpResponse.BodyHandlers.discarding());

            ObjectNode conf = MAPPER.createObjectNode();
            conf.put("asset_id", assetId);
            ObjectNode body = MAPPER.createObjectNode();
            body.set("conf", conf);
            // Airflow 3's DAG-run creation schema requires logical_date even for
            // a manually-triggered, unscheduled DAG - "now" is the usual value.
            body.put("logical_date", Instant.now().toString());

            HttpRequest trigger = HttpRequest.newBuilder()
                    .uri(URI.create(AIRFLOW_URL + "/api/v2/dags/" + dagId + "/dagRuns"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", auth)
                    .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
                    .build();
            HttpResponse<String> response = HTTP_CLIENT.send(trigger, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return Response.ok(response.body()).build();
            }
            monitor.warning("Airflow returned " + response.statusCode() + " triggering " + dagId + ": " + response.body());
            return Response.status(response.statusCode()).entity(response.body()).build();
        } catch (Exception e) {
            monitor.severe("Failed to trigger validation DAG for asset: " + assetId, e);
            return jsonError(e);
        }
    }

    /**
     * Airflow 3's Simple Auth Manager (see participant/docker-compose.yml's
     * AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS) doesn't accept HTTP Basic
     * Auth directly on the /api/v2 REST API the way Airflow 2's auth
     * backends did - it's JWT-based, so username/password first has to be
     * exchanged for a bearer token via its login endpoint.
     */
    private String fetchAirflowToken() throws Exception {
        ObjectNode creds = MAPPER.createObjectNode();
        creds.put("username", AIRFLOW_USER);
        creds.put("password", AIRFLOW_PASSWORD);

        HttpRequest tokenReq = HttpRequest.newBuilder()
                .uri(URI.create(AIRFLOW_URL + "/auth/token"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(creds)))
                .build();
        HttpResponse<String> tokenResp = HTTP_CLIENT.send(tokenReq, HttpResponse.BodyHandlers.ofString());
        if (tokenResp.statusCode() < 200 || tokenResp.statusCode() >= 300) {
            throw new RuntimeException("Airflow login failed: " + tokenResp.statusCode() + " " + tokenResp.body());
        }
        JsonNode json = MAPPER.readTree(tokenResp.body());
        JsonNode token = json.get("access_token");
        if (token == null) {
            throw new RuntimeException("Airflow login response had no access_token: " + tokenResp.body());
        }
        return token.asText();
    }

    @GET
    @Path("catalog/api/preview")
    @Produces(MediaType.APPLICATION_JSON)
    public Response previewAsset(@jakarta.ws.rs.QueryParam("assetId") String assetId) {
        try {
            if (assetId == null || assetId.isBlank()) {
                return Response.status(400).entity("{\"error\":\"assetId is required\"}").build();
            }

            var asset = assetIndex.findById(assetId);
            if (asset == null) {
                return Response.status(404).entity("{\"error\":\"asset not found: " + assetId + "\"}").build();
            }

            var da = asset.getDataAddress();
            var type = da.getType();
            if (!"MinioAsset".equals(type) && !"MinioFiles".equals(type)) {
                return Response.status(400).entity("{\"error\":\"preview is only supported for MinIO-backed assets\"}").build();
            }

            var endpoint = da.getStringProperty("endpoint");
            var bucket = da.getStringProperty("bucketName");
            var accessKey = da.getStringProperty("accessKey");
            var secretKey = da.getStringProperty("secretKey");
            var prefix = da.getStringProperty("prefix");

            var client = MinioClient.builder().endpoint(endpoint).credentials(accessKey, secretKey).build();

            // Reads at most PREVIEW_MAX_BYTES+1 bytes off the front of the object
            // (the "+1" is just to detect truncation) rather than a ranged GET —
            // simpler, and fine for the small demo files this hackfest deals with.
            byte[] bytes;
            try (var stream = client.getObject(GetObjectArgs.builder().bucket(bucket).object(prefix).build())) {
                bytes = stream.readNBytes(PREVIEW_MAX_BYTES + 1);
            }
            boolean truncated = bytes.length > PREVIEW_MAX_BYTES;
            String content = new String(
                    truncated ? java.util.Arrays.copyOf(bytes, PREVIEW_MAX_BYTES) : bytes,
                    StandardCharsets.UTF_8
            );

            var fileName = prefix != null && prefix.contains("/") ? prefix.substring(prefix.lastIndexOf('/') + 1) : prefix;

            ObjectNode result = MAPPER.createObjectNode();
            result.put("fileName", fileName);
            result.put("contentType", getProperty(asset, "contenttype"));
            result.put("content", content);
            result.put("truncated", truncated);
            return Response.ok(MAPPER.writeValueAsString(result)).build();
        } catch (Exception e) {
            monitor.severe("Failed to preview asset: " + assetId, e);
            return jsonError(e);
        }
    }

    @GET
    @Path("catalog/api/validation")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getValidationReport(@jakarta.ws.rs.QueryParam("assetId") String assetId) {
        try {
            if (assetId == null || assetId.isBlank()) {
                return Response.status(400).entity("{\"error\":\"assetId is required\"}").build();
            }

            var asset = assetIndex.findById(assetId);
            if (asset == null) {
                return Response.status(404).entity("{\"error\":\"asset not found: " + assetId + "\"}").build();
            }

            var da = asset.getDataAddress();
            var type = da.getType();
            if (!"MinioAsset".equals(type) && !"MinioFiles".equals(type)) {
                return Response.status(400).entity("{\"error\":\"validation reports are only supported for MinIO-backed assets\"}").build();
            }

            var endpoint = da.getStringProperty("endpoint");
            var bucket = da.getStringProperty("bucketName");
            var accessKey = da.getStringProperty("accessKey");
            var secretKey = da.getStringProperty("secretKey");
            var assetPrefix = da.getStringProperty("prefix");

            var client = MinioClient.builder().endpoint(endpoint).credentials(accessKey, secretKey).build();

            // hackfest_validate_dataset's upload_results task (see dali.datalake) writes
            // the report next to the original file, in the *same* bucket, named after it
            // with the extension dropped: "<original-basename>_<timestamp>.gx.json" — the
            // exact timestamp isn't known up front, so the latest is found by prefix
            // listing and picking the lexicographically greatest key.
            var basePrefix = assetPrefix.contains(".") ? assetPrefix.substring(0, assetPrefix.lastIndexOf('.')) : assetPrefix;
            String reportPrefix = basePrefix + "_";
            String latestKey = null;
            Iterable<Result<Item>> results = client.listObjects(
                    ListObjectsArgs.builder().bucket(bucket).prefix(reportPrefix).recursive(true).build());
            for (Result<Item> result : results) {
                Item item = result.get();
                String key = item.objectName();
                if (key.endsWith(".gx.json") && (latestKey == null || key.compareTo(latestKey) > 0)) {
                    latestKey = key;
                }
            }

            if (latestKey == null) {
                return Response.status(404).entity("{\"error\":\"no validation report found for asset " + assetId + "\"}").build();
            }

            try (var stream = client.getObject(GetObjectArgs.builder().bucket(bucket).object(latestKey).build())) {
                String json = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
                return Response.ok(json).header("X-Report-Key", latestKey).build();
            }
        } catch (Exception e) {
            monitor.severe("Failed to fetch validation report for asset: " + assetId, e);
            return jsonError(e);
        }
    }

    private String getProperty(org.eclipse.edc.connector.controlplane.asset.spi.domain.Asset asset, String key) {
        var val = asset.getProperty(key);
        return val != null ? val.toString() : "";
    }

    private void putIfPresent(ObjectNode node, String key, String value) {
        if (value != null) {
            node.put(key, value);
        }
    }

    private String formatEpoch(long epochSeconds) {
        if (epochSeconds <= 0) return "";
        return UTC_FMT.format(Instant.ofEpochSecond(epochSeconds));
    }

    private String formatEpochMillis(long epochMillis) {
        if (epochMillis <= 0) return "";
        return UTC_FMT.format(Instant.ofEpochMilli(epochMillis));
    }

    private Response jsonError(Exception e) {
        try {
            ObjectNode err = MAPPER.createObjectNode();
            err.put("error", e.getMessage());
            return Response.serverError().entity(MAPPER.writeValueAsString(err)).build();
        } catch (Exception ex) {
            return Response.serverError().entity("{\"error\":\"internal error\"}").build();
        }
    }
}
