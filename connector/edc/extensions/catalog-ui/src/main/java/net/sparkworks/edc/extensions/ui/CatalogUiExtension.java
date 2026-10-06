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

    @Setting(value = "Admin key (sent as X-Api-Key) enabling the 'Register asset' page, where an operator registers the dataset bucket as an asset. Unset = page and endpoints disabled", required = false)
    private static final String ASSET_ADMIN_KEY = "edc.catalog.ui.asset.admin.key";

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
        String participantId = context.getSetting("edc.participant.id", "unknown");

        var controller = new CatalogUiController(
                assetIndex,
                contractDefinitionStore,
                policyDefinitionStore,
                contractAgreementService,
                contractNegotiationService,
                transferProcessService,
                monitor,
                submitEnabled,
                participantId
        );
        webService.registerResource(controller);

        String assetAdminKey = context.getSetting(ASSET_ADMIN_KEY, "");
        webService.registerResource(new AssetRegistrationController(
                assetIndex, policyDefinitionStore, contractDefinitionStore, monitor, assetAdminKey));
        if (!assetAdminKey.isBlank()) {
            monitor.info("Asset registration page available at http://localhost:<http-port>/api/catalog/register-asset");
        }

        monitor.info("Catalog UI available at http://localhost:<http-port>/api/catalog"
                + (submitEnabled ? " (dataset submission enabled)" : " (dataset submission disabled)"));
    }
}
