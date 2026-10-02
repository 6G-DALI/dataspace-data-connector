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
 */
@Consumes({MediaType.APPLICATION_JSON})
@Produces({MediaType.APPLICATION_JSON})
@Path("/")
public class HealthApiController {

    private final Monitor monitor;

    public HealthApiController(Monitor monitor) {
        this.monitor = monitor;
    }

    @GET
    @Path("health")
    public String checkHealth() {
        monitor.debug("[health] Received a health request");
        return "{\"response\":\"I'm alive!\"}";
    }
}
