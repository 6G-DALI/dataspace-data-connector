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
import org.eclipse.edc.spi.monitor.Monitor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Records, in the testbed's own bucket (the transfer's source), that a file reached the data lake.
 *
 * <p>For every file that was uploaded, an object named like the source file plus {@value #SUFFIX} is
 * written next to it ({@code exp-1/data.csv} gets {@code exp-1/data.csv.status}). It is written
 * when the upload is complete, and holds the UUID the data lake stores the file under, as plain
 * text and nothing else. A file without a status object has not been uploaded, or not completely.
 *
 * <p>The status objects sit in the same bucket as the files the transfer lists, so the sink skips
 * every name ending in {@value #SUFFIX} (see {@link #isStatusFile}): they are never uploaded.
 *
 * <p>Best effort: the credentials of the source address may be read-only, or the bucket
 * unreachable. A status that cannot be written is logged and never fails the upload it reports.
 */
public class SourceStatusWriter {

    /** The extension added to a source file's name; the sink never uploads a file that ends in it. */
    public static final String SUFFIX = ".status";

    private final MinioClient client;
    private final String bucket;
    private final Monitor monitor;

    public SourceStatusWriter(MinioClient client, String bucket, Monitor monitor) {
        this.client = client;
        this.bucket = bucket;
        this.monitor = monitor;
    }

    /** Whether a file name (or object key) is a status object written by this class. */
    public static boolean isStatusFile(String name) {
        return name != null && name.toLowerCase(Locale.ROOT).endsWith(SUFFIX);
    }

    /**
     * Write {@code <sourceObjectName>.status} holding {@code uuid}, unless it already holds exactly that.
     * Returns whether the object now holds the UUID. Never throws.
     *
     * @param sourceObjectName the object's key in the source bucket, as the part names it
     * @param uuid             the UUID generated for the file in the data lake
     */
    public boolean write(String sourceObjectName, String uuid) {
        String key = sourceObjectName + SUFFIX;
        try {
            if (uuid.equals(read(key))) {
                monitor.debug("Status '" + key + "' already holds " + uuid);
                return true;
            }
            byte[] body = uuid.getBytes(StandardCharsets.UTF_8);
            client.putObject(PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(key)
                    .stream(new ByteArrayInputStream(body), body.length, -1)
                    .contentType("text/plain")
                    .build());
            monitor.info("✓ Wrote '" + key + "' in source bucket '" + bucket + "': " + uuid);
            return true;
        } catch (Exception e) {
            monitor.warning("⚠ Could not write '" + key + "' in source bucket '" + bucket + "': " + e.getMessage());
            return false;
        }
    }

    /** The status object's content, or {@code null} when there is none. */
    private String read(String key) throws Exception {
        try (InputStream in = client.getObject(GetObjectArgs.builder().bucket(bucket).object(key).build())) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
        } catch (ErrorResponseException e) {
            String code = e.errorResponse().code();
            if ("NoSuchKey".equals(code) || "NoSuchObject".equals(code)) {
                return null;
            }
            throw e;
        }
    }
}
