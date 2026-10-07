package net.sparkworks.edc.extensions.ui;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.edc.connector.controlplane.asset.spi.domain.Asset;
import org.eclipse.edc.connector.controlplane.asset.spi.index.AssetIndex;
import org.eclipse.edc.connector.controlplane.contract.spi.offer.store.ContractDefinitionStore;
import org.eclipse.edc.connector.controlplane.contract.spi.types.offer.ContractDefinition;
import org.eclipse.edc.connector.controlplane.policy.spi.PolicyDefinition;
import org.eclipse.edc.connector.controlplane.policy.spi.store.PolicyDefinitionStore;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.spi.types.domain.DataAddress;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Lets a testbed operator register the testbed's dataset bucket as an EDC asset
 * from the connector's own API, instead of running the provider preparation
 * script against the management API (which is not published).
 *
 * Creates, in one call: the asset (a 6GDaliTestbedExperiments data address),
 * the no-constraint policy and a contract definition offering every asset under
 * it. Policy and contract definition are created only if absent.
 *
 * The /api port has no authentication of its own, so everything here is guarded
 * by an admin key (setting edc.catalog.ui.asset.admin.key, sent as X-Api-Key).
 * With the setting unset the page and endpoints answer 404.
 *
 * A testbed has a single asset. Registering one is refused while an asset of the testbed type already
 * exists; an existing asset can instead be selected as the testbed asset. The choice is stored on the
 * asset itself, as the public property {@code testbedAsset=true}, so it also shows in the catalogue.
 */
@Path("/")
public class AssetRegistrationController {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ASSET_TYPE = "6GDaliTestbedExperiments";
    private static final String EDC_NS = "https://w3id.org/edc/v0.0.1/ns/";
    private static final String TESTBED_ASSET_PROPERTY = EDC_NS + "testbedAsset";
    private static final String POLICY_ID = "no-constraint-policy";
    private static final String CONTRACT_DEFINITION_ID = "contract-definition";

    private final AssetIndex assetIndex;
    private final PolicyDefinitionStore policyStore;
    private final ContractDefinitionStore contractStore;
    private final Monitor monitor;
    private final String adminKey;

    public AssetRegistrationController(AssetIndex assetIndex, PolicyDefinitionStore policyStore,
                                       ContractDefinitionStore contractStore, Monitor monitor, String adminKey) {
        this.assetIndex = assetIndex;
        this.policyStore = policyStore;
        this.contractStore = contractStore;
        this.monitor = monitor;
        this.adminKey = adminKey;
    }

    private boolean enabled() {
        return adminKey != null && !adminKey.isBlank();
    }

    private boolean authorised(String presented) {
        return presented != null && MessageDigest.isEqual(
                presented.getBytes(StandardCharsets.UTF_8), adminKey.getBytes(StandardCharsets.UTF_8));
    }

    private Response guard(String presented) {
        if (!enabled()) {
            return Response.status(404).entity("Asset registration is disabled on this connector").build();
        }
        if (!authorised(presented)) {
            return Response.status(401).type(MediaType.APPLICATION_JSON).entity("{\"error\":\"Invalid or missing X-Api-Key\"}").build();
        }
        return null;
    }

    @GET
    @Path("catalog/register-asset")
    @Produces(MediaType.TEXT_HTML)
    public Response page() {
        if (!enabled()) {
            return Response.status(404).entity("Asset registration is disabled on this connector").build();
        }
        // The registration form now lives on the catalogue page (its "Testbed asset" panel). This old URL
        // is kept so existing links and bookmarks still work.
        return Response.status(302).header("Location", "../catalog#testbed-asset").build();
    }

    @POST
    @Path("catalog/api/admin/assets")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response register(@HeaderParam("X-Api-Key") String key, String body) {
        var denied = guard(key);
        if (denied != null) {
            return denied;
        }
        try {
            JsonNode in = MAPPER.readTree(body);
            String assetId = text(in, "assetId");
            String endpoint = text(in, "endpoint");
            String bucket = text(in, "bucketName");
            String accessKey = text(in, "accessKey");
            String secretKey = text(in, "secretKey");
            if (assetId.isEmpty() || endpoint.isEmpty() || bucket.isEmpty() || accessKey.isEmpty() || secretKey.isEmpty()) {
                return error(400, "assetId, endpoint, bucketName, accessKey and secretKey are required");
            }

            var existing = assetsOfTestbedType();
            if (!existing.isEmpty()) {
                return error(409, "This testbed already has an asset (" + existing.get(0).getId() + "). A testbed registers a "
                        + "single asset: select it as the testbed asset, or remove it first.");
            }

            var address = DataAddress.Builder.newInstance()
                    .type(ASSET_TYPE)
                    .property("endpoint", endpoint)
                    .property("bucketName", bucket)
                    .property("accessKey", accessKey)
                    .property("secretKey", secretKey)
                    .property("prefix", text(in, "prefix"))
                    .build();
            var asset = Asset.Builder.newInstance().id(assetId).property(TESTBED_ASSET_PROPERTY, "true").dataAddress(address).build();
            var created = assetIndex.create(asset);
            if (created.failed()) {
                return error(409, "Could not create asset: " + created.getFailureDetail());
            }

            ObjectNode out = MAPPER.createObjectNode();
            out.put("assetId", assetId);
            out.put("policy", ensurePolicy());
            out.put("contractDefinition", ensureContractDefinition());
            monitor.info("Registered asset " + assetId + " for bucket " + bucket);
            return Response.status(201).entity(MAPPER.writeValueAsString(out)).build();
        } catch (Exception e) {
            monitor.severe("Failed to register asset", e);
            return error(500, e.getMessage());
        }
    }

    /**
     * Selects an existing asset as the testbed's asset. Only assets of the testbed type qualify, because the
     * data plane cannot transfer any other. Any other asset marked before is unmarked, and the asset is
     * made offerable (no-constraint policy and contract definition are created if missing).
     */
    @POST
    @Path("catalog/api/admin/testbed-asset")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response select(@HeaderParam("X-Api-Key") String key, String body) {
        var denied = guard(key);
        if (denied != null) {
            return denied;
        }
        try {
            String assetId = text(MAPPER.readTree(body), "assetId");
            if (assetId.isEmpty()) {
                return error(400, "assetId is required");
            }
            var chosen = assetIndex.findById(assetId);
            if (chosen == null) {
                return error(404, "Asset '" + assetId + "' not found");
            }
            if (chosen.getDataAddress() == null || !ASSET_TYPE.equals(chosen.getDataAddress().getType())) {
                return error(409, "Asset '" + assetId + "' has type '"
                        + (chosen.getDataAddress() == null ? "unknown" : chosen.getDataAddress().getType())
                        + "'. Only " + ASSET_TYPE + " assets can be the testbed asset: remove it and register it again.");
            }

            for (Asset asset : assetsOfTestbedType()) {
                boolean isChosen = asset.getId().equals(assetId);
                boolean marked = "true".equals(String.valueOf(asset.getProperty(TESTBED_ASSET_PROPERTY)));
                if (isChosen == marked) {
                    continue;
                }
                Map<String, Object> properties = new HashMap<>(asset.getProperties());
                if (isChosen) {
                    properties.put(TESTBED_ASSET_PROPERTY, "true");
                } else {
                    properties.remove(TESTBED_ASSET_PROPERTY);
                }
                var updated = assetIndex.updateAsset(Asset.Builder.newInstance()
                        .id(asset.getId())
                        .properties(properties)
                        .privateProperties(asset.getPrivateProperties())
                        .dataAddress(asset.getDataAddress())
                        .createdAt(asset.getCreatedAt())
                        .build());
                if (updated.failed()) {
                    return error(500, "Could not update asset '" + asset.getId() + "': " + updated.getFailureDetail());
                }
            }

            ObjectNode out = MAPPER.createObjectNode();
            out.put("assetId", assetId);
            out.put("policy", ensurePolicy());
            out.put("contractDefinition", ensureContractDefinition());
            monitor.info("Selected asset " + assetId + " as the testbed asset");
            return Response.ok(MAPPER.writeValueAsString(out)).build();
        } catch (Exception e) {
            monitor.severe("Failed to select the testbed asset", e);
            return error(500, e.getMessage());
        }
    }

    private List<Asset> assetsOfTestbedType() {
        return assetIndex.queryAssets(QuerySpec.Builder.newInstance().build())
                .filter(a -> a.getDataAddress() != null && ASSET_TYPE.equals(a.getDataAddress().getType()))
                .collect(Collectors.toList());
    }

    @DELETE
    @Path("catalog/api/admin/assets/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response delete(@HeaderParam("X-Api-Key") String key, @PathParam("id") String id) {
        var denied = guard(key);
        if (denied != null) {
            return denied;
        }
        var result = assetIndex.deleteById(id);
        if (result.failed()) {
            return error(404, "Could not delete asset: " + result.getFailureDetail());
        }
        return Response.ok("{\"deleted\":\"" + id.replace("\"", "") + "\"}").build();
    }

    private String ensurePolicy() {
        if (policyStore.findById(POLICY_ID) != null) {
            return "exists";
        }
        var definition = PolicyDefinition.Builder.newInstance().id(POLICY_ID)
                .policy(Policy.Builder.newInstance().build()).build();
        return policyStore.create(definition).succeeded() ? "created" : "exists";
    }

    private String ensureContractDefinition() {
        if (contractStore.findById(CONTRACT_DEFINITION_ID) != null) {
            return "exists";
        }
        var definition = ContractDefinition.Builder.newInstance().id(CONTRACT_DEFINITION_ID)
                .accessPolicyId(POLICY_ID).contractPolicyId(POLICY_ID).build();
        return contractStore.save(definition).succeeded() ? "created" : "exists";
    }

    private static String text(JsonNode node, String field) {
        return node.hasNonNull(field) ? node.get(field).asText().trim() : "";
    }

    private static Response error(int status, String message) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("error", message == null ? "error" : message);
        return Response.status(status).type(MediaType.APPLICATION_JSON).entity(node.toString()).build();
    }
}
