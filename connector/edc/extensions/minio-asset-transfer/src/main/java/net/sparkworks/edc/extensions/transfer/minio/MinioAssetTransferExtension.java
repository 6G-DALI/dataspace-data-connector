/*
 *  Copyright (c) 2024 SparkWorks
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       SparkWorks - initial implementation
 *
 */

package net.sparkworks.edc.extensions.transfer.minio;

import org.eclipse.edc.connector.dataplane.selector.spi.DataPlaneSelectorService;
import org.eclipse.edc.connector.dataplane.selector.spi.instance.DataPlaneInstance;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.web.spi.configuration.context.ControlApiUrl;

import java.util.Set;

public class MinioAssetTransferExtension implements ServiceExtension {

    @Inject
    private PipelineService pipelineService;

    @Inject
    private DataPlaneSelectorService dataPlaneSelectorService;

    @Inject
    private ControlApiUrl controlApiUrl;

    private ServiceExtensionContext context;

    @Override
    public String name() {
        return "MinIO Asset Transfer";
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        this.context = context;
        var monitor = context.getMonitor();

        pipelineService.registerFactory(new MinioAssetDataSourceFactory(monitor));

        monitor.info("MinIO Asset Transfer extension registered (source type: MinioAsset)");
    }

    @Override
    public void start() {
        var monitor = context.getMonitor();

        var url = controlApiUrl.get().toString() + "/v1/dataflows";
        monitor.info("Registering data plane 'minio-asset-dataplane' at " + url);

        var instance = DataPlaneInstance.Builder.newInstance()
                .id("minio-asset-dataplane")
                .url(url)
                .allowedSourceTypes(Set.of("MinioAsset", "6GDaliTestbedExperiments", "HttpData"))
                .allowedTransferType(Set.of("HttpData-PUSH", "PresignedHttpData-PUSH"))
                .build();

        dataPlaneSelectorService.addInstance(instance)
                .onSuccess(it -> monitor.info("Data plane 'minio-asset-dataplane' registered successfully"))
                .onFailure(f -> monitor.severe("Failed to register data plane: " + f.getFailureDetail()));
    }
}