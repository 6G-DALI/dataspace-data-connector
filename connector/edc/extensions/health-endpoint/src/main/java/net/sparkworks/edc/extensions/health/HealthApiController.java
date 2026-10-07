package net.sparkworks.edc.extensions.health;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.edc.spi.monitor.Monitor;

/**
 * Minimal liveness endpoint, following the pattern from
 * https://github.com/eclipse-edc/Samples/blob/main/basic/basic-02-health-endpoint/README.md
 * - served under the connector's default API (web.http.port/web.http.path),
 * so it needs no extra port published to be usable as a docker-compose
 * healthcheck target.
 *
 * It also says which build of the image is running: the commit SHA and the build time, passed to the
 * image at build time (Docker build arguments GIT_SHA and BUILD_TIME, see connector.dockerfile and the
 * deploy workflow) and read here from the BUILD_SHA and BUILD_TIME environment variables. A build that
 * did not set them (a local build) reports "unknown".
 */
@Consumes({MediaType.APPLICATION_JSON})
@Produces({MediaType.APPLICATION_JSON})
@Path("/")
public class HealthApiController {

    private final Monitor monitor;
    private final String buildSha;
    private final String buildTime;

    public HealthApiController(Monitor monitor) {
        this.monitor = monitor;
        this.buildSha = clean(System.getenv("BUILD_SHA"));
        this.buildTime = clean(System.getenv("BUILD_TIME"));
    }

    /** Only plain identifier characters are echoed back, so a malformed value can never break the JSON. */
    private static String clean(String value) {
        return value != null && value.matches("[A-Za-z0-9._:+-]{1,64}") ? value : "unknown";
    }

    /** For the startup log, for example "1a47112 (2026-10-07T06:52:30Z)". */
    public String buildSummary() {
        return shortSha() + " (" + buildTime + ")";
    }

    private String shortSha() {
        return buildSha.matches("[0-9a-fA-F]{7,64}") ? buildSha.substring(0, 7) : buildSha;
    }

    @GET
    @Path("health")
    public String checkHealth() {
        monitor.debug("[health] Received a health request");
        return "{\"response\":\"I'm alive!\",\"build\":{\"sha\":\"" + buildSha + "\",\"shortSha\":\"" + shortSha()
                + "\",\"time\":\"" + buildTime + "\"}}";
    }
}
