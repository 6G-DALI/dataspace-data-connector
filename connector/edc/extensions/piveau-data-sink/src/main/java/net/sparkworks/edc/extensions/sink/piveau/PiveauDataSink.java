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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.spi.monitor.Monitor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Data sink that writes transferred files to the destination MinIO / S3-compatible bucket
 * (the data lake), under {@code <experiment-id>/<file>}, and handles each by extension:
 * - .json files (dataset metadata): uploaded as-is
 * - .csv files: uploaded, then a completion message is published to RabbitMQ (when configured)
 *
 * <p>Registering datasets and distributions in the Piveau catalogue is not done here: the
 * s3-asset-monitor watches the data lake and registers them when the files appear.
 */
public class PiveauDataSink implements DataSink {
    private final MinioClient minioClient;
    private final String bucketName;
    private final String prefix;
    private final Monitor monitor;
    private final ExecutorService executorService;
    private final ConnectionFactory rabbitConnectionFactory;
    private final String rabbitQueue;
    private final String httpDestinationUrl;
    private final String authKey;
    private final String experimentPrefix;

    public PiveauDataSink(MinioClient minioClient, String bucketName, String prefix,
                          Monitor monitor, ExecutorService executorService,
                          ConnectionFactory rabbitConnectionFactory, String rabbitQueue,
                          String httpDestinationUrl, String authKey, String experimentPrefix) {
        this.minioClient = minioClient;
        this.bucketName = bucketName;
        this.prefix = prefix != null ? prefix : "";
        this.monitor = monitor;
        this.executorService = executorService;
        this.rabbitConnectionFactory = rabbitConnectionFactory;
        this.rabbitQueue = rabbitQueue;
        this.httpDestinationUrl = httpDestinationUrl;
        this.authKey = authKey;
        this.experimentPrefix = experimentPrefix;
    }
    
    @Override
    public CompletableFuture<StreamResult<Object>> transfer(DataSource source) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                monitor.info("PiveauRoutingDataSink starting transfer");
                
                var streamResult = source.openPartStream();
                if (streamResult.failed()) {
                    monitor.severe("Failed to open source stream: " + streamResult.getFailureDetail());
                    return StreamResult.error("Failed to open source stream: " + streamResult.getFailureDetail());
                }
                
                var stream = streamResult.getContent();
                
                stream.forEach(part -> {
                    String fileName = extractFileName(part.name());
                    monitor.info("Processing file: " + fileName);
                    
                    try {
                        if (fileName.toLowerCase().endsWith(".json")) {
                            handleJsonFile(part);
                        } else if (fileName.toLowerCase().endsWith(".csv")) {
                            handleCsvFile(part);
                        }
                    } catch (Exception e) {
                        monitor.severe("Error processing file: " + fileName, e);
                    }
                });
                
                monitor.info("✓ PiveauRoutingDataSink transfer completed");
                return StreamResult.success();
                
            } catch (Exception e) {
                monitor.severe("PiveauRoutingDataSink transfer failed", e);
                return StreamResult.error("Transfer failed: " + e.getMessage());
            }
        }, executorService);
    }
    
    /**
     * Handle a JSON (dataset metadata) file - upload it to the data lake next to the data files.
     */
    private void handleJsonFile(DataSource.Part part) {
        String dirName = extractDirName(part.name());
        String fileName = extractFileName(part.name());
        String experimentId = experimentPrefix + "-" + dirName;
        String destinationBucket = experimentPrefix == null ? bucketName : experimentPrefix;
        monitor.info("════════════════════════════════════════════════");
        monitor.info("Part Name: " + part.name());
        monitor.info("JSON file detected: " + fileName);
        monitor.info("ExperimentId: " + experimentId);
        monitor.info("DestinationBucket: " + destinationBucket);
        monitor.info("════════════════════════════════════════════════");

        try {
            byte[] fileBytes;
            try (var inputStream = part.openStream()) {
                fileBytes = inputStream.readAllBytes();
            }

            boolean exists = minioClient.bucketExists(BucketExistsArgs.builder().bucket(destinationBucket).build());
            if (!exists) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(destinationBucket).build());
                monitor.info("Created bucket: " + destinationBucket);
            }
            final String objectKey = experimentId + "/" + fileName;
            minioClient.putObject(
                PutObjectArgs.builder()
                    .bucket(destinationBucket)
                    .object(objectKey)
                    .stream(new ByteArrayInputStream(fileBytes), fileBytes.length, -1)
                    .contentType("application/json")
                    .build()
            );
            monitor.info("✓ Uploaded '" + objectKey + "' (" + fileBytes.length + " bytes) to bucket '" + destinationBucket + "'");

        } catch (Exception e) {
            monitor.severe("✗ Failed to upload JSON file to MinIO: " + fileName, e);
        }
    }

    private void handleCsvFile(DataSource.Part part) {
        String dirName = extractDirName(part.name());
        String fileName = extractFileName(part.name());
        String experimentId = experimentPrefix + "-" + dirName;
        String destinationBucket = experimentPrefix == null ? bucketName : experimentPrefix;
        monitor.info("════════════════════════════════════════════════");
        monitor.info("Registering dataset to Data Lake (s3):");
        monitor.info("Part Name: " + part.name());
        monitor.info("CSV file detected: " + fileName);
        monitor.info("ExperimentId: " + experimentId);
        monitor.info("DestinationBucket: " + destinationBucket);
        monitor.info("════════════════════════════════════════════════");

        try {
            // Ensure bucket exists
            boolean exists = minioClient.bucketExists(BucketExistsArgs.builder().bucket(destinationBucket).build());
            if (!exists) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(destinationBucket).build());
                monitor.info("Created bucket: " + destinationBucket);
            }

            try (var inputStream = part.openStream()) {
                byte[] fileContent = inputStream.readAllBytes();

                final String objectKey = experimentId + "/" + fileName;

                minioClient.putObject(
                    PutObjectArgs.builder()
                        .bucket(destinationBucket)
                        .object(objectKey)
                        .stream(new ByteArrayInputStream(fileContent), fileContent.length, -1)
                        .contentType("application/octet-stream")
                        .build()
                );

                monitor.info("✓ Uploaded '" + objectKey + "' (" + fileContent.length + " bytes) to bucket '" + destinationBucket + "'");

                sendRabbitNotification(part.name(), "SUCCESS");
            }

        } catch (Exception e) {
            monitor.warning("⚠ MinIO upload failed for '" + fileName + "', falling back to HTTP: " + e.getMessage());
            if (httpDestinationUrl != null && !httpDestinationUrl.isEmpty()) {
                handleCsvFileOld(part);
            } else {
                monitor.severe("✗ MinIO failed and no HTTP fallback configured for: " + fileName);
                sendRabbitNotification(part.name(), "FAILED");
            }
        }
    }

    private void sendRabbitNotification(String requestId, String status) {
        if (rabbitConnectionFactory == null || rabbitQueue == null || rabbitQueue.isEmpty()) {
            monitor.warning("RabbitMQ not configured, skipping notification");
            return;
        }
        try (Connection connection = rabbitConnectionFactory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(rabbitQueue, true, false, false, null);
            String payload = "{\"request_id\": \"" + requestId + "\", \"status\": \"" + status + "\"}";
            channel.basicPublish("", rabbitQueue, null, payload.getBytes());
            monitor.info("✓ RabbitMQ notification sent to queue '" + rabbitQueue + "': " + payload);
        } catch (Exception e) {
            monitor.warning("⚠ Failed to send RabbitMQ notification: " + e.getMessage());
        }
    }

    private void handleCsvFileOld(DataSource.Part part) {
        String dirName = extractDirName(part.name());
        String fileName = extractFileName(part.name());
        String experimentId = experimentPrefix + "-" + dirName;
        String destinationBucket = experimentPrefix == null ? bucketName : experimentPrefix;
        monitor.info("════════════════════════════════════════════════");
        monitor.info("Registering dataset to Data Lake (http):");
        monitor.info("Part Name: " + part.name());
        monitor.info("CSV file detected: " + fileName);
        monitor.info("ExperimentId: " + experimentId);
        monitor.info("DestinationBucket: " + destinationBucket);
        monitor.info("════════════════════════════════════════════════");

        try (var inputStream = part.openStream()) {
            byte[] fileContent = inputStream.readAllBytes();

            var requestBody = RequestBody.create(fileContent, MediaType.parse("application/octet-stream"));

            var requestBuilder = new Request.Builder()
                .url(httpDestinationUrl)
                .post(requestBody)
                .header("X-File-Path", experimentId)
                .header("X-File-Name", fileName)
                .header("X-file-bucket", destinationBucket)
                .header("Content-Type", "application/octet-stream");

            if (authKey != null && !authKey.isEmpty()) {
                requestBuilder.header("Authorization", "Bearer " + authKey);
            }

            try (var response = new OkHttpClient().newCall(requestBuilder.build()).execute()) {
                if (response.isSuccessful()) {
                    monitor.info("✓ Successfully forwarded file: " + fileName + " (status: " + response.code() + ")");
                    sendRabbitNotification(part.name(), "SUCCESS");
                } else {
                    monitor.warning("⚠ HTTP forward failed for file: " + fileName + " (status: " + response.code() + ")");
                    sendRabbitNotification(part.name(), "FAILED");
                }
            }

        } catch (IOException e) {
            monitor.severe("✗ Failed to process CSV file (HTTP): " + fileName, e);
            sendRabbitNotification(part.name(), "FAILED");
        }
    }

    private String buildObjectKey(String partName) {
        if (prefix == null || prefix.isEmpty()) {
            return partName;
        }
        String normalizedPrefix = prefix.endsWith("/") ? prefix : prefix + "/";
        return normalizedPrefix + partName;
    }

    /**
     * Extract filename from full path
     */
    private String extractFileName(String fullPath) {
        if (fullPath == null) {
            return "unknown";
        }
        int lastSlash = Math.max(fullPath.lastIndexOf('/'), fullPath.lastIndexOf('\\'));
        return lastSlash >= 0 ? fullPath.substring(lastSlash + 1) : fullPath;
    }
    
    /**
     * Extract dirname from full path
     */
    private String extractDirName(String fullPath) {
        if (fullPath == null) {
            return null;
        }
        final String[] parts = fullPath.split("/");
        if (parts.length > 1) {
            return parts[parts.length - 2];
        } else {
            return null;
        }
    }
}
