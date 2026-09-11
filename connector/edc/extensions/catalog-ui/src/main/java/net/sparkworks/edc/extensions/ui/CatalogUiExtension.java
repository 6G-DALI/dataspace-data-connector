package net.sparkworks.edc.extensions.ui;

import org.eclipse.edc.connector.controlplane.asset.spi.index.AssetIndex;
import org.eclipse.edc.connector.controlplane.contract.spi.offer.store.ContractDefinitionStore;
import org.eclipse.edc.connector.controlplane.policy.spi.store.PolicyDefinitionStore;
import org.eclipse.edc.connector.controlplane.services.spi.contractagreement.ContractAgreementService;
import org.eclipse.edc.connector.controlplane.services.spi.contractnegotiation.ContractNegotiationService;
import org.eclipse.edc.connector.controlplane.services.spi.transferprocess.TransferProcessService;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.web.spi.WebService;

public class CatalogUiExtension implements ServiceExtension {

    @Setting(value = "Expose the '+ Submit Dataset' page and its upload/validation endpoints", defaultValue = "false", required = false)
    private static final String SUBMIT_ENABLED = "edc.catalog.ui.submit.enabled";

    @Inject
    private WebService webService;

    @Inject
    private AssetIndex assetIndex;

    @Inject
    private ContractDefinitionStore contractDefinitionStore;

    @Inject
    private PolicyDefinitionStore policyDefinitionStore;

    @Inject
    private ContractAgreementService contractAgreementService;

    @Inject
    private ContractNegotiationService contractNegotiationService;

    @Inject
    private TransferProcessService transferProcessService;

    @Override
    public String name() {
        return "Catalog UI";
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        var monitor = context.getMonitor();
        boolean submitEnabled = context.getSetting(SUBMIT_ENABLED, false);

        var controller = new CatalogUiController(
                assetIndex,
                contractDefinitionStore,
                policyDefinitionStore,
                contractAgreementService,
                contractNegotiationService,
                transferProcessService,
                monitor,
                submitEnabled
        );
        webService.registerResource(controller);

        monitor.info("Catalog UI available at http://localhost:<http-port>/api/catalog"
                + (submitEnabled ? " (dataset submission enabled)" : " (dataset submission disabled)"));
    }
}
