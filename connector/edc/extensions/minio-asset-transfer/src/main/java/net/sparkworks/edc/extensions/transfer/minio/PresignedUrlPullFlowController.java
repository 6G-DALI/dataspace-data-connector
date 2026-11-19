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

import io.minio.GetPresignedObjectUrlArgs;
import io.minio.MinioClient;
import io.minio.http.Method;
import org.eclipse.edc.connector.controlplane.asset.spi.index.AssetIndex;
import org.eclipse.edc.connector.controlplane.transfer.spi.flow.DataFlowController;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.DataFlowResponse;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.connector.controlplane.asset.spi.domain.Asset;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.eclipse.edc.spi.response.ResponseStatus.FATAL_ERROR;

/**
 * Provider-side flow for the {@value #TRANSFER_TYPE} transfer type: the consumer asks for a presigned URL to
 * download the asset from, instead of handing the provider a presigned URL to upload to (PresignedHttpData-PUSH).
 * <p>
 * The URL is returned as the DataAddress of the DataFlowResponse. The control plane sends it to the consumer in the
 * transfer start message, where the consumer's EDR store keeps it.
 */
public class PresignedUrlPullFlowController implements DataFlowController {

    public static final String TRANSFER_TYPE = "PresignedUrl-PULL";

    private static final Set<String> SOURCE_TYPES = Set.of("MinioAsset", "6GDaliTestbedExperiments");

    private final AssetIndex assetIndex;
    private final Monitor monitor;
    private final int expirySeconds;

    public PresignedUrlPullFlowController(AssetIndex assetIndex, Monitor monitor, int expirySeconds) {
        this.assetIndex = assetIndex;
        this.monitor = monitor;
        this.expirySeconds = expirySeconds;
    }

    @Override
    public boolean canHandle(TransferProcess transferProcess) {
        return TRANSFER_TYPE.equals(transferProcess.getTransferType())
                && transferProcess.getType() == TransferProcess.Type.PROVIDER;
    }

    @Override
    public StatusResult<DataFlowResponse> provision(TransferProcess transferProcess, Policy policy) {
        return StatusResult.success(DataFlowResponse.Builder.newInstance().build());
    }

    @Override
    public @NotNull StatusResult<DataFlowResponse> start(TransferProcess transferProcess, Policy policy) {
        var asset = assetIndex.findById(transferProcess.getAssetId());
        if (asset == null) {
            return StatusResult.failure(FATAL_ERROR, "Asset not found: " + transferProcess.getAssetId());
        }

        var source = asset.getDataAddress();
        if (!SOURCE_TYPES.contains(source.getType())) {
            return StatusResult.failure(FATAL_ERROR,
                    "Asset " + asset.getId() + " has source type " + source.getType() + ", which cannot be presigned");
        }

        try {
            var endpoint = source.getStringProperty("endpoint");
            var bucket = source.getStringProperty("bucketName");
            var object = source.getStringProperty("prefix");

            var client = MinioClient.builder()
                    .endpoint(endpoint)
                    .credentials(source.getStringProperty("accessKey"), source.getStringProperty("secretKey"))
                    // With no region the client asks the server for the bucket's region before signing, which
                    // fails when this connector cannot reach the endpoint. Signing itself is local.
                    .region(source.getStringProperty("region", "us-east-1"))
                    .build();

            var url = client.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
                    .method(Method.GET)
                    .bucket(bucket)
                    .object(object)
                    .expiry(expirySeconds, TimeUnit.SECONDS)
                    .build());

            monitor.info("Presigned GET URL issued for asset " + asset.getId() + " (transfer "
                    + transferProcess.getId() + ", valid " + expirySeconds + "s)");

            var address = DataAddress.Builder.newInstance()
                    .type("HttpData")
                    // "endpoint" is the property the DSP start message carries to the consumer's EDR;
                    // "baseUrl" is kept for HttpData-style readers.
                    .property("endpoint", url)
                    .property("baseUrl", url)
                    .property("method", "GET")
                    .property("expiresInSeconds", String.valueOf(expirySeconds))
                    .build();

            return StatusResult.success(DataFlowResponse.Builder.newInstance().dataAddress(address).build());
        } catch (Exception e) {
            monitor.severe("Failed to presign asset " + asset.getId(), e);
            return StatusResult.failure(FATAL_ERROR, "Failed to create presigned URL: " + e.getMessage());
        }
    }

    @Override
    public StatusResult<Void> suspend(TransferProcess transferProcess) {
        // A presigned URL cannot be revoked, it expires on its own.
        return StatusResult.success();
    }

    @Override
    public StatusResult<Void> terminate(TransferProcess transferProcess) {
        return StatusResult.success();
    }

    @Override
    public Set<String> transferTypesFor(Asset asset) {
        String type = asset.getDataAddress() == null ? null : asset.getDataAddress().getType();
        return SOURCE_TYPES.contains(type) ? Set.of(TRANSFER_TYPE) : Set.of();
    }
}
