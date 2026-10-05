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

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.ErrorResponseException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Gives every dataset a random UUID directory in the data lake, and every data file a random
 * UUID name, instead of the names they have at the testbed.
 *
 * <p>A dataset's {@code metadata.json} and its data files arrive as separate parts (possibly in
 * different transfers), and all of them must end up in the same directory. The same file sent
 * again must also land on the same object, not a second copy. So the first time a source
 * directory, or a file within it, is seen a UUID is created, and the mapping is persisted in the
 * bucket as a small object under {@value #DATASET_PREFIX} or {@value #FILE_PREFIX}, so it
 * survives a connector restart. The objects have no file extension, so the s3-asset-monitor,
 * which only looks at {@code .csv} files and {@code metadata.json}, ignores them.
 */
public class DatasetDirectoryRegistry {

    static final String DATASET_PREFIX = ".datasets/";
    static final String FILE_PREFIX = ".files/";

    private final Map<String, String> cache = new ConcurrentHashMap<>();

    /**
     * The UUID directory for a source directory, creating and persisting a new one on first use.
     *
     * @param sourceId the dataset's identity at the testbed, e.g. {@code <experiment prefix>-<dir>}
     */
    public String directoryFor(MinioClient client, String bucket, String sourceId) throws Exception {
        return resolve(client, bucket, DATASET_PREFIX + sourceId);
    }

    /**
     * The UUID a source file is stored under (without its extension), creating and persisting a
     * new one the first time that file is seen, so re-sending it overwrites the same object.
     *
     * @param sourceId the dataset's identity at the testbed, as for {@link #directoryFor}
     * @param fileName the file's name at the testbed
     */
    public String fileIdFor(MinioClient client, String bucket, String sourceId, String fileName) throws Exception {
        return resolve(client, bucket, FILE_PREFIX + sourceId + "/" + fileName);
    }

    /** The UUID stored at {@code mappingKey}, created and stored there first if it does not exist. */
    private String resolve(MinioClient client, String bucket, String mappingKey) throws Exception {
        String cacheKey = bucket + "/" + mappingKey;
        String cached = cache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        synchronized (this) {
            cached = cache.get(cacheKey);
            if (cached != null) {
                return cached;
            }

            String id = read(client, bucket, mappingKey);
            if (id == null) {
                id = UUID.randomUUID().toString();
                byte[] body = id.getBytes(StandardCharsets.UTF_8);
                client.putObject(PutObjectArgs.builder()
                        .bucket(bucket)
                        .object(mappingKey)
                        .stream(new ByteArrayInputStream(body), body.length, -1)
                        .contentType("text/plain")
                        .build());
            }
            cache.put(cacheKey, id);
            return id;
        }
    }

    /** The stored UUID, or {@code null} when there is no mapping yet. */
    private String read(MinioClient client, String bucket, String key) throws Exception {
        try (InputStream in = client.getObject(GetObjectArgs.builder().bucket(bucket).object(key).build())) {
            String value = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
            // Fails loudly on a damaged mapping rather than silently starting a second directory.
            return UUID.fromString(value).toString();
        } catch (ErrorResponseException e) {
            String code = e.errorResponse().code();
            if ("NoSuchKey".equals(code) || "NoSuchObject".equals(code)) {
                return null;
            }
            throw e;
        }
    }
}
