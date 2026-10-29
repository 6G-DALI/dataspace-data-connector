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
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
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
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Data sink that writes transferred files to the destination MinIO / S3-compatible bucket
 * (the data lake), and handles each by extension:
 * - metadata.json (dataset metadata): uploaded as {@code <dataset-uuid>/metadata.json}
 * - data files (see {@link #DATA_FILE_EXTENSIONS}; any .json but metadata.json): uploaded as {@code <dataset-uuid>/<file-uuid>.<ext>}, then a
 *   completion message is published to RabbitMQ (when configured)
 *
 * <p>Nothing in the data lake carries the testbed's own directory or file names. A dataset's
 * directory is a random UUID (see {@link DatasetDirectoryRegistry}), and each data file is
 * stored under a random UUID with its original extension. Both UUIDs are remembered, so the
 * same dataset or file sent again goes to the same place. The original file name is kept as
 * the {@code original-name} user metadata of the object (URL-encoded).
 *
 * <p>Registering datasets and distributions in the Piveau catalogue is not done here: the
 * s3-asset-monitor watches the data lake and registers them when the files appear.
 */
public class PiveauDataSink implements DataSink {
    /** The dataset's metadata file, kept under its own name next to the data files. */
    static final String METADATA_FILE_NAME = "metadata.json";
    /**
     * Data files uploaded to the data lake (lowercase, with the dot). A .json file other than
     * {@link #METADATA_FILE_NAME} is data too. Anything else is skipped.
     */
    static final List<String> DATA_FILE_EXTENSIONS = List.of(
        ".csv", ".tsv", ".json", ".jsonl", ".parquet", ".pcap", ".pcapng", ".h5", ".hdf5", ".mat");
    /** Content type per data file extension; anything not listed is stored as application/octet-stream. */
    private static final Map<String, String> CONTENT_TYPES = Map.of(
        ".csv", "text/csv",
        ".tsv", "text/tab-separated-values",
        ".json", "application/json",
        ".jsonl", "application/jsonl",
        ".parquet", "application/parquet",
        ".pcap", "application/vnd.tcpdump.pcap",
        ".pcapng", "application/vnd.tcpdump.pcap",
        ".h5", "application/x-hdf5",
        ".hdf5", "application/x-hdf5",
        ".mat", "application/x-matlab-data");
    /** Object metadata holding the file's MD5 (x-amz-meta-content-md5), read by the s3-asset-monitor too. */
    static final String CONTENT_MD5 = "content-md5";
    /** Multipart upload part size: data files are streamed, not held in memory. */
    private static final long UPLOAD_PART_SIZE = 10L * 1024 * 1024;

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
    /** Where every object is written: the transfer's bucketName, else the connector's experiment prefix. */
    private final String destinationBucket;
    private final DatasetDirectoryRegistry directories;

    public PiveauDataSink(MinioClient minioClient, String bucketName, String prefix,
                          Monitor monitor, ExecutorService executorService,
                          ConnectionFactory rabbitConnectionFactory, String rabbitQueue,
                          String httpDestinationUrl, String authKey, String experimentPrefix,
                          DatasetDirectoryRegistry directories) {
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
        this.destinationBucket = bucketName != null && !bucketName.isEmpty() ? bucketName : experimentPrefix;
        this.directories = directories;
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
                        if (METADATA_FILE_NAME.equalsIgnoreCase(fileName)) {
                            handleJsonFile(part);
                        } else if (isDataFile(fileName)) {
                            handleDataFile(part);
                        } else {
                            monitor.info("Skipping '" + fileName + "': not a data file (" + String.join(", ", DATA_FILE_EXTENSIONS) + ")");
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
        String experimentId = destinationBucket + "-" + dirName;
        monitor.info("════════════════════════════════════════════════");
        monitor.info("Part Name: " + part.name());
        monitor.info("JSON file detected: " + fileName);
        monitor.info("Source dataset: " + experimentId);
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
            // The dataset's directory in the lake is a random UUID, not its name at the testbed.
            final String datasetDirectory = directories.directoryFor(minioClient, destinationBucket, experimentId);
            final String objectKey = datasetDirectory + "/" + fileName;
            monitor.info("Dataset directory: " + datasetDirectory);
            final String md5 = md5Hex(new ByteArrayInputStream(fileBytes));
            if (md5.equals(storedMd5(objectKey))) {
                monitor.info("= '" + objectKey + "' is already in bucket '" + destinationBucket + "' with the same content: not uploaded again");
                return;
            }
            minioClient.putObject(
                PutObjectArgs.builder()
                    .bucket(destinationBucket)
                    .object(objectKey)
                    .stream(new ByteArrayInputStream(fileBytes), fileBytes.length, -1)
                    .contentType("application/json")
                    .userMetadata(Map.of(CONTENT_MD5, md5))
                    .build()
            );
            monitor.info("✓ Uploaded '" + objectKey + "' (" + fileBytes.length + " bytes) to bucket '" + destinationBucket + "'");

        } catch (Exception e) {
            monitor.severe("✗ Failed to upload JSON file to MinIO: " + fileName, e);
        }
    }

    private void handleDataFile(DataSource.Part part) {
        String dirName = extractDirName(part.name());
        String fileName = extractFileName(part.name());
        String experimentId = destinationBucket + "-" + dirName;
        monitor.info("════════════════════════════════════════════════");
        monitor.info("Registering dataset to Data Lake (s3):");
        monitor.info("Part Name: " + part.name());
        monitor.info("Data file detected: " + fileName);
        monitor.info("Source dataset: " + experimentId);
        monitor.info("DestinationBucket: " + destinationBucket);
        monitor.info("════════════════════════════════════════════════");

        try {
            // Ensure bucket exists
            boolean exists = minioClient.bucketExists(BucketExistsArgs.builder().bucket(destinationBucket).build());
            if (!exists) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(destinationBucket).build());
                monitor.info("Created bucket: " + destinationBucket);
            }

            // The file is stored under a random UUID (same extension) in its dataset's UUID
            // directory; its original name travels as object metadata. The UUID is remembered
            // per source file, so sending the same file again overwrites the same object.
            final String datasetDirectory = directories.directoryFor(minioClient, destinationBucket, experimentId);
            final String fileId = directories.fileIdFor(minioClient, destinationBucket, experimentId, fileName);
            final String objectKey = datasetDirectory + "/" + fileId + extensionOf(fileName);

            // Every file in the testbed's bucket is offered when the transfer starts, including after a
            // restart, so a file the data lake already holds with the same content is not sent again. The
            // MD5 is read from the testbed's own storage first (cheap next to the upload) and kept on the
            // object, since the ETag of a multipart upload is not the file's MD5.
            final String md5;
            try (var inputStream = part.openStream()) {
                md5 = md5Hex(inputStream);
            }
            if (md5.equals(storedMd5(objectKey))) {
                monitor.info("= '" + fileName + "' is already in bucket '" + destinationBucket + "' as '" + objectKey + "': not uploaded again");
                return;
            }

            // Streamed in parts rather than read into memory: packet captures can be large.
            try (var inputStream = part.openStream()) {
                var written = minioClient.putObject(
                    PutObjectArgs.builder()
                        .bucket(destinationBucket)
                        .object(objectKey)
                        .stream(inputStream, part.size() >= 0 ? part.size() : -1, UPLOAD_PART_SIZE)
                        .contentType(contentTypeOf(fileName))
                        .userMetadata(Map.of(
                            "original-name", URLEncoder.encode(fileName, StandardCharsets.UTF_8),
                            CONTENT_MD5, md5))
                        .build()
                );

                monitor.info("✓ Uploaded '" + fileName + "' as '" + objectKey + "' (etag " + written.etag() + ") to bucket '" + destinationBucket + "'");

                sendRabbitNotification(part.name(), "SUCCESS");
            }

        } catch (Exception e) {
            monitor.warning("⚠ MinIO upload failed for '" + fileName + "', falling back to HTTP: " + e.getMessage());
            if (httpDestinationUrl != null && !httpDestinationUrl.isEmpty()) {
                handleDataFileOld(part);
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

    private void handleDataFileOld(DataSource.Part part) {
        String dirName = extractDirName(part.name());
        String fileName = extractFileName(part.name());
        String experimentId = destinationBucket + "-" + dirName;
        monitor.info("════════════════════════════════════════════════");
        monitor.info("Registering dataset to Data Lake (http):");
        monitor.info("Part Name: " + part.name());
        monitor.info("Data file detected: " + fileName);
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
            monitor.severe("✗ Failed to process data file (HTTP): " + fileName, e);
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

    /** The MD5 of a stream, as lowercase hex. Reads it to the end without holding it in memory. */
    static String md5Hex(InputStream in) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        byte[] buffer = new byte[64 * 1024];
        for (int n; (n = in.read(buffer)) > 0; ) {
            digest.update(buffer, 0, n);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    /**
     * The MD5 of the object at {@code objectKey} in the destination bucket, or {@code null} when there is no
     * such object (or its MD5 is not known). Taken from the {@value #CONTENT_MD5} metadata this sink writes, else
     * from the ETag when that is a plain MD5 (an object uploaded in one part).
     */
    private String storedMd5(String objectKey) throws Exception {
        StatObjectResponse stat;
        try {
            stat = minioClient.statObject(StatObjectArgs.builder().bucket(destinationBucket).object(objectKey).build());
        } catch (ErrorResponseException e) {
            String code = e.errorResponse().code();
            if ("NoSuchKey".equals(code) || "NoSuchObject".equals(code)) {
                return null;
            }
            throw e;
        }
        for (var entry : stat.userMetadata().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(CONTENT_MD5)) {
                return entry.getValue().toLowerCase();
            }
        }
        String etag = stat.etag() == null ? "" : stat.etag().replace("\"", "").toLowerCase();
        return etag.matches("[0-9a-f]{32}") ? etag : null;
    }

    private static boolean isDataFile(String fileName) {
        String lower = fileName.toLowerCase();
        return DATA_FILE_EXTENSIONS.stream().anyMatch(lower::endsWith);
    }

    private String contentTypeOf(String fileName) {
        return CONTENT_TYPES.getOrDefault(extensionOf(fileName).toLowerCase(), "application/octet-stream");
    }

    /** The file's extension including the dot ({@code .csv}), or an empty string when it has none. */
    private String extensionOf(String fileName) {
        int lastDot = fileName.lastIndexOf('.');
        return lastDot > 0 ? fileName.substring(lastDot) : "";
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
