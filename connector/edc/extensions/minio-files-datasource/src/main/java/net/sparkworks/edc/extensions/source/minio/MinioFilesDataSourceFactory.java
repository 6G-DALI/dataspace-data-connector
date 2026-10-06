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

package net.sparkworks.edc.extensions.source.minio;

import io.minio.MinioClient;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.jetbrains.annotations.NotNull;

public class MinioFilesDataSourceFactory implements DataSourceFactory {
    
    private final Monitor monitor;
    
    public MinioFilesDataSourceFactory(Monitor monitor) {
        this.monitor = monitor;
    }
    
    @Override
    public String supportedType() {
        return "6GDaliTestbedExperiments";
    }
    
    @Override
    public DataSource createSource(DataFlowStartMessage dataFlowStartMessage) {
        var sourceDataAddress = dataFlowStartMessage.getSourceDataAddress();
        
        // Extract MinIO configuration from source data address
        String endpoint = sourceDataAddress.getStringProperty("endpoint");
        String bucketName = sourceDataAddress.getStringProperty("bucketName");
        String accessKey = sourceDataAddress.getStringProperty("accessKey");
        String secretKey = sourceDataAddress.getStringProperty("secretKey");
        String prefix = sourceDataAddress.getStringProperty("prefix", "");
        
        monitor.info("Creating MinioFilesDataSource");
        monitor.info("  Endpoint: " + endpoint);
        monitor.info("  Bucket: " + bucketName);
        monitor.info("  Prefix: " + (prefix.isEmpty() ? "(root)" : prefix));
        
        // Create MinIO client
        MinioClient minioClient = MinioClient.builder().endpoint(endpoint).credentials(accessKey, secretKey).build();
        
        return new MinioFilesDataSource(minioClient, bucketName, prefix, monitor);
    }
    
    @Override
    public @NotNull Result<Void> validateRequest(DataFlowStartMessage dataFlowStartMessage) {
        var sourceDataAddress = dataFlowStartMessage.getSourceDataAddress();
        
        // Validate required properties
        String endpoint = sourceDataAddress.getStringProperty("endpoint");
        String bucketName = sourceDataAddress.getStringProperty("bucketName");
        String accessKey = sourceDataAddress.getStringProperty("accessKey");
        String secretKey = sourceDataAddress.getStringProperty("secretKey");
        
        if (endpoint == null || endpoint.isEmpty()) {
            return Result.failure("MinIO endpoint is required");
        }
        if (bucketName == null || bucketName.isEmpty()) {
            return Result.failure("MinIO bucketName is required");
        }
        if (accessKey == null || accessKey.isEmpty()) {
            return Result.failure("MinIO accessKey is required");
        }
        if (secretKey == null || secretKey.isEmpty()) {
            return Result.failure("MinIO secretKey is required");
        }
        
        return Result.success();
    }
}
