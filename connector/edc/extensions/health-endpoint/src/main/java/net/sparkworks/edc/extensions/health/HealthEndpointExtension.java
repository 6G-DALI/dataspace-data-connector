package net.sparkworks.edc.extensions.health;

import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.web.spi.WebService;

public class HealthEndpointExtension implements ServiceExtension {

    @Inject
    private WebService webService;

    @Override
    public String name() {
        return "Health Endpoint";
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        var controller = new HealthApiController(context.getMonitor());
        webService.registerResource(controller);
        context.getMonitor().info("Health endpoint available at http://localhost:<http-port>/api/health, build "
                + controller.buildSummary());
    }
}
