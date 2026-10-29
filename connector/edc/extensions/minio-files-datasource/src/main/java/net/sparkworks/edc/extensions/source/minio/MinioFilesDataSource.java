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

import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.spi.monitor.Monitor;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.stream.StreamSupport.stream;

/**
 * Data source that offers the files of a MinIO bucket to the sink: first every file already in
 * the bucket, then each new or modified one as it appears. The sink decides what to do with
 * each (the PiveauData sink uploads it unless the destination already has the same content).
 */
public class MinioFilesDataSource implements DataSource, Closeable {
    
    private final MinioClient minioClient;
    private final String bucketName;
    private final String prefix;
    private final Monitor monitor;
    
    // Deduplication - track processed files
    private final Map<String, Long> lastProcessedTimes = new HashMap<>();
    private static final long POLL_INTERVAL_MS = 5000; // Poll every 5 seconds
    private static final long DEBOUNCE_MILLIS = 1000;
    
    public MinioFilesDataSource(MinioClient minioClient, String bucketName, String prefix, Monitor monitor) {
        this.minioClient = minioClient;
        this.bucketName = bucketName;
        this.prefix = prefix != null ? prefix : "";
        this.monitor = monitor;
        
        monitor.info("Creating MinioFilesDataSource");
        monitor.info("  Bucket: " + bucketName);
        monitor.info("  Prefix: " + (this.prefix.isEmpty() ? "(root)" : this.prefix));
        monitor.info("  JSON files will trigger Piveau Hub Repo API");
        monitor.info("  CSV files will be transferred to subscribers");
        
    }
    
    @Override
    public StreamResult<Stream<Part>> openPartStream() {
        Stream<Part> stream = openObjectStream().filter(Objects::nonNull) //empty
                .filter(this::shouldProcess)  // Debounce
                .map(item -> new MinioStreamingPart(item, minioClient, bucketName));
        
        return StreamResult.success(stream);
    }
    
    /**
     * Check if this file should be processed (debounce duplicate events)
     */
    private boolean shouldProcess(Item item) {
        String objectName = item.objectName();
        long now = System.currentTimeMillis();
        
        synchronized (lastProcessedTimes) {
            Long lastProcessed = lastProcessedTimes.get(objectName);
            
            if (lastProcessed != null && (now - lastProcessed) < DEBOUNCE_MILLIS) {
                monitor.debug("SKIPPING duplicate event for: " + objectName);
                return false;
            }
            
            lastProcessedTimes.put(objectName, now);
            
            // Clean up old entries (older than 5 minutes)
            lastProcessedTimes.entrySet().removeIf(entry -> (now - entry.getValue()) > 300000);
            
            return true;
        }
    }
    
    /**
     * Read MinIO object content as string
     */
    private String readObjectAsString(String objectName) throws Exception {
        try (InputStream stream = minioClient.getObject(GetObjectArgs.builder().bucket(bucketName).object(objectName).build())) {
            return new String(stream.readAllBytes());
        }
    }
    
    @NotNull
    private Stream<Item> openObjectStream() {
        return stream(new MinioObjectSpliterator(minioClient, bucketName, prefix, monitor), false);
    }
    
    @Override
    public void close() {
        monitor.info("Closing MinioFilesDataSource");
    }
    
    /**
     * Part representing a CSV file from MinIO to be transferred
     */
    private record MinioStreamingPart(Item item, MinioClient minioClient, String bucketName) implements Part {
        
        @Override
        public String name() {
            return item.objectName();
        }

        @Override
        public long size() {
            return item.size();
        }

        @Override
        public InputStream openStream() {
            try {
                return minioClient.getObject(GetObjectArgs.builder().bucket(bucketName).object(item.objectName()).build());
            } catch (Exception e) {
                throw new RuntimeException("Failed to open MinIO object: " + item.objectName(), e);
            }
        }
    }
    
    /**
     * Spliterator that polls MinIO bucket for new objects
     */
    private static class MinioObjectSpliterator extends Spliterators.AbstractSpliterator<Item> {

        private final MinioClient minioClient;
        private final String bucketName;
        private final String prefix;
        private final Monitor monitor;
        private final Map<String, Item> seenObjects = new HashMap<>();
        private boolean initialized = false;

        MinioObjectSpliterator(MinioClient minioClient, String bucketName, String prefix, Monitor monitor) {
            super(Long.MAX_VALUE, Spliterator.ORDERED);
            this.minioClient = minioClient;
            this.bucketName = bucketName;
            this.prefix = prefix;
            this.monitor = monitor;
        }

        @Override
        public boolean tryAdvance(Consumer<? super Item> action) {
            try {
                if (!initialized) {
                    // The first listing offers every object already in the bucket, so files stored before
                    // the transfer started (or while the connector was down) are sent too. The sink skips
                    // the ones the destination already holds with the same content.
                    monitor.info("MinIO data source: offering the files already in bucket '" + bucketName
                            + "' to the sink, which uploads those the destination does not have yet");
                    initialized = true;
                }

                // List objects in bucket
                Iterable<Result<Item>> results = minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).prefix(prefix).recursive(true).build());
                
                // Check for new or modified objects
                boolean foundNew = false;
                for (Result<Item> result : results) {
                    Item item = result.get();
                    String objectName = item.objectName();
                    
                    // Skip if it's a directory marker
                    if (objectName.endsWith("/")) {
                        continue;
                    }
                    
                    Item seenItem = seenObjects.get(objectName);
                    
                    // New object or modified object (different etag or size)
                    if (seenItem == null || !Objects.equals(seenItem.etag(), item.etag()) || seenItem.size() != item.size()) {
                        
                        monitor.debug("New/modified object detected: " + objectName);
                        seenObjects.put(objectName, item);
                        action.accept(item);
                        foundNew = true;
                    }
                }
                
                // If no new objects, wait before next poll
                if (!foundNew) {
                    Thread.sleep(POLL_INTERVAL_MS);
                }
                
                return true;
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("MinIO polling interrupted", e);
            } catch (Exception e) {
                monitor.severe("Error polling MinIO bucket", e);
                try {
                    Thread.sleep(POLL_INTERVAL_MS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                return true;
            }
        }
    }
}
