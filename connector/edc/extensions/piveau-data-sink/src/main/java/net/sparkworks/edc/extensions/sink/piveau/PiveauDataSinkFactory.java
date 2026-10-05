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

package net.sparkworks.edc.extensions.sink.piveau;

import com.rabbitmq.client.ConnectionFactory;
import io.minio.MinioClient;
import okhttp3.OkHttpClient;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSinkFactory;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;

/**
 * Factory for creating PiveauDataSink instances.
 *
 * <p>The destination data address supplies the data-lake S3 endpoint, bucket and credentials.
 * Catalogue (Piveau) properties are no longer read here: the s3-asset-monitor registers files
 * with Piveau once they appear in the data lake.
 */
public class PiveauDataSinkFactory implements DataSinkFactory {

    private final Monitor monitor;
    private final ExecutorService executorService;
    private final ConnectionFactory rabbitConnectionFactory;
    private final String rabbitQueue;
    private final String experimentPrefix;
    // Shared by every sink this factory creates: the source-directory -> UUID mapping.
    private final DatasetDirectoryRegistry directories = new DatasetDirectoryRegistry();

    public PiveauDataSinkFactory(Monitor monitor, ExecutorService executorService,
                                 ConnectionFactory rabbitConnectionFactory, String rabbitQueue,
                                 String experimentPrefix) {
        this.monitor = monitor;
        this.executorService = executorService;
        this.rabbitConnectionFactory = rabbitConnectionFactory;
        this.rabbitQueue = rabbitQueue;
        this.experimentPrefix = experimentPrefix;
    }

    @Override
    public String supportedType() {
        return "PiveauData";
    }

    @Override
    public @NotNull Result<Void> validateRequest(DataFlowStartMessage request) {
        var dest = request.getDestinationDataAddress();
        if (dest.getStringProperty("endpoint") == null) {
            return Result.failure("MinIO endpoint is required in destination data address");
        }
        if (dest.getStringProperty("bucketName") == null) {
            return Result.failure("MinIO bucketName is required in destination data address");
        }
        if (dest.getStringProperty("accessKey") == null) {
            return Result.failure("MinIO accessKey is required in destination data address");
        }
        if (dest.getStringProperty("secretKey") == null) {
            return Result.failure("MinIO secretKey is required in destination data address");
        }
        return Result.success();
    }

    @Override
    public DataSink createSink(DataFlowStartMessage request) {
        try {
            monitor.info("Creating PiveauDataSink for request: " + request.getId());

            var dest = request.getDestinationDataAddress();
            String endpoint   = dest.getStringProperty("endpoint");
            String bucketName = dest.getStringProperty("bucketName");
            String accessKey  = dest.getStringProperty("accessKey");
            String secretKey  = dest.getStringProperty("secretKey");
            String prefix     = dest.getStringProperty("prefix", "");

            monitor.info("  MinIO endpoint: " + endpoint);
            monitor.info("  MinIO bucket:   " + bucketName);
            monitor.info("  MinIO prefix:   " + (prefix == null || prefix.isEmpty() ? "(root)" : prefix));

            OkHttpClient httpClient = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();

            MinioClient minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .httpClient(httpClient)
                .build();

            String httpDestinationUrl = dest.getStringProperty("baseUrl");
            String authKey            = dest.getStringProperty("authKey");

            return new PiveauDataSink(minioClient, bucketName, prefix, monitor, executorService, rabbitConnectionFactory, rabbitQueue, httpDestinationUrl, authKey, experimentPrefix, directories);
        } catch (Exception e) {
            monitor.severe("createSink failed for request " + request.getId() + ": " + e.getMessage(), e);
            throw e;
        }
    }
}
