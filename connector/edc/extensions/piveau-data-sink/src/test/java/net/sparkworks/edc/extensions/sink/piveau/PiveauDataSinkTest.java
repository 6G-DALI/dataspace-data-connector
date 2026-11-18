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

import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.ErrorResponse;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** What the sink uploads, what it leaves out, and the status object it writes for each file it uploaded. */
class PiveauDataSinkTest {

    private static final String DATASET_UUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final String FILE_UUID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

    private final MinioClient destination = mock(MinioClient.class);
    private final SourceStatusWriter status = mock(SourceStatusWriter.class);
    private final Monitor monitor = mock(Monitor.class);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @AfterEach
    void stop() {
        executor.shutdownNow();
    }

    /** UUIDs fixed in advance, so the test knows what the status objects must hold. */
    private static final class FixedIds extends DatasetDirectoryRegistry {
        @Override
        public String directoryFor(MinioClient client, String bucket, String sourceId) {
            return DATASET_UUID;
        }

        @Override
        public String fileIdFor(MinioClient client, String bucket, String sourceId, String fileName) {
            return FILE_UUID;
        }
    }

    private record Part(String name, byte[] content) implements DataSource.Part {
        @Override
        public InputStream openStream() {
            return new ByteArrayInputStream(content);
        }
    }

    private PiveauDataSink sink(String ignored, SourceStatusWriter statusWriter) {
        return new PiveauDataSink(destination, "lake-bucket", "", monitor, executor, null, null, null, null, "",
                new FixedIds(), IgnoredFileNames.parse(ignored), statusWriter);
    }

    private void transfer(PiveauDataSink sink, String... names) {
        List<DataSource.Part> parts = new ArrayList<>();
        for (String name : names) {
            parts.add(new Part(name, ("content of " + name).getBytes(StandardCharsets.UTF_8)));
        }
        DataSource source = mock(DataSource.class);
        when(source.openPartStream()).thenReturn(StreamResult.success(parts.stream()));
        assertThat(sink.transfer(source).join().succeeded()).isTrue();
    }

    /** Nothing in the data lake yet: statObject answers NoSuchKey, and a write returns a result. */
    private void emptyLake() throws Exception {
        var error = mock(ErrorResponse.class);
        when(error.code()).thenReturn("NoSuchKey");
        var noSuchKey = mock(ErrorResponseException.class);
        when(noSuchKey.errorResponse()).thenReturn(error);
        when(destination.statObject(any(StatObjectArgs.class))).thenThrow(noSuchKey);
        when(destination.getObject(any(io.minio.GetObjectArgs.class))).thenThrow(noSuchKey);
        when(destination.bucketExists(any())).thenReturn(true);
        when(destination.putObject(any(PutObjectArgs.class))).thenReturn(mock(ObjectWriteResponse.class));
    }

    /** The lake already holds a (different) metadata.json with this content. */
    private void lakeHolds(String storedJson) throws Exception {
        var stat = mock(io.minio.StatObjectResponse.class);
        when(stat.userMetadata()).thenReturn(java.util.Map.of());
        when(stat.etag()).thenReturn("not-an-md5-1");
        when(destination.statObject(any(StatObjectArgs.class))).thenReturn(stat);
        when(destination.getObject(any(io.minio.GetObjectArgs.class))).thenReturn(new io.minio.GetObjectResponse(
                okhttp3.Headers.of(), "lake-bucket", "", DATASET_UUID + "/metadata.json",
                new ByteArrayInputStream(storedJson.getBytes(StandardCharsets.UTF_8))));
        when(destination.bucketExists(any())).thenReturn(true);
        when(destination.putObject(any(PutObjectArgs.class))).thenReturn(mock(ObjectWriteResponse.class));
    }

    private void transferMetadata(String json) {
        DataSource source = mock(DataSource.class);
        when(source.openPartStream()).thenReturn(StreamResult.success(
                java.util.stream.Stream.<DataSource.Part>of(new Part("exp-1/metadata.json", json.getBytes(StandardCharsets.UTF_8)))));
        assertThat(sink("", status).transfer(source).join().succeeded()).isTrue();
    }

    private List<String> uploadedKeys() throws Exception {
        var args = ArgumentCaptor.forClass(PutObjectArgs.class);
        verify(destination, org.mockito.Mockito.atLeast(0)).putObject(args.capture());
        return args.getAllValues().stream().map(PutObjectArgs::object).toList();
    }

    @Test
    void aDataFileIsUploadedUnderItsUuidAndItsStatusHoldsThatUuid() throws Exception {
        emptyLake();

        transfer(sink("", status), "exp-1/data.csv");

        assertThat(uploadedKeys()).containsExactly(DATASET_UUID + "/" + FILE_UUID + ".csv");
        verify(status).write("exp-1/data.csv", FILE_UUID);
    }

    @Test
    void metadataJsonIsUploadedAndItsStatusHoldsTheDatasetDirectoryUuid() throws Exception {
        emptyLake();

        transfer(sink("", status), "exp-1/metadata.json");

        assertThat(uploadedKeys()).containsExactly(DATASET_UUID + "/metadata.json");
        verify(status).write("exp-1/metadata.json", DATASET_UUID);
    }

    @Test
    void aStatusFileIsNeverUploadedAndGetsNoStatusOfItsOwn() throws Exception {
        emptyLake();

        transfer(sink("", status), "exp-1/data.csv.status", "exp-1/metadata.json.status", "exp-1/DATA.CSV.STATUS");

        assertThat(uploadedKeys()).isEmpty();
        verify(status, never()).write(any(), any());
    }

    @Test
    void aFileInTheIgnoredListIsNeverUploadedAndGetsNoStatus() throws Exception {
        emptyLake();

        transfer(sink("Notes.csv, scratch.json", status), "exp-1/notes.csv", "exp-1/scratch.json", "exp-1/data.csv");

        assertThat(uploadedKeys()).containsExactly(DATASET_UUID + "/" + FILE_UUID + ".csv");   // only data.csv
        verify(status).write("exp-1/data.csv", FILE_UUID);
        verify(status, never()).write(eq("exp-1/notes.csv"), any());
        verify(status, never()).write(eq("exp-1/scratch.json"), any());
    }

    @Test
    void theStatusFilesInASourceBucketDoNotStopTheRealFilesFromGoingThrough() throws Exception {
        emptyLake();

        transfer(sink("", status), "exp-1/metadata.json", "exp-1/metadata.json.status", "exp-1/data.csv",
                "exp-1/data.csv.status");

        assertThat(uploadedKeys()).containsExactlyInAnyOrder(
                DATASET_UUID + "/metadata.json", DATASET_UUID + "/" + FILE_UUID + ".csv");
        verify(status).write("exp-1/metadata.json", DATASET_UUID);
        verify(status).write("exp-1/data.csv", FILE_UUID);
    }

    @Test
    void aFileTheLakeAlreadyHoldsIsNotUploadedAgainButStillGetsItsStatus() throws Exception {
        // the same content is already there: statObject answers with its MD5 as the ETag
        var stat = mock(io.minio.StatObjectResponse.class);
        when(stat.userMetadata()).thenReturn(java.util.Map.of());
        when(stat.etag()).thenReturn(PiveauDataSink.md5Hex(new ByteArrayInputStream(
                "content of exp-1/data.csv".getBytes(StandardCharsets.UTF_8))));
        when(destination.statObject(any(StatObjectArgs.class))).thenReturn(stat);
        when(destination.bucketExists(any())).thenReturn(true);

        transfer(sink("", status), "exp-1/data.csv");

        assertThat(uploadedKeys()).isEmpty();
        verify(status).write("exp-1/data.csv", FILE_UUID);          // files uploaded before status objects existed get one
    }

    @Test
    void withoutAStatusWriterTheSinkStillUploads() throws Exception {
        emptyLake();

        transfer(sink("", null), "exp-1/data.csv");

        assertThat(uploadedKeys()).containsExactly(DATASET_UUID + "/" + FILE_UUID + ".csv");
    }

    @Test
    void aFailingUploadWritesNoStatus() throws Exception {
        emptyLake();
        when(destination.putObject(any(PutObjectArgs.class))).thenThrow(new RuntimeException("lake down"));

        transfer(sink("", status), "exp-1/data.csv");

        verify(status, never()).write(any(), any());
    }

    @Test
    void aChangedMetadataWithAHigherRevisionIsSent() throws Exception {
        lakeHolds("{\"title\":\"Run\",\"revision\":1}");

        transferMetadata("{\"title\":\"Run 2\",\"revision\":2}");

        assertThat(uploadedKeys()).containsExactly(DATASET_UUID + "/metadata.json");
        verify(status).write("exp-1/metadata.json", DATASET_UUID);
    }

    @Test
    void aChangedMetadataWithTheSameRevisionIsNotSentButItsStatusIsWritten() throws Exception {
        lakeHolds("{\"title\":\"Run\",\"revision\":2}");

        transferMetadata("{\"title\":\"Run edited\",\"revision\":2}");

        assertThat(uploadedKeys()).isEmpty();
        verify(status).write("exp-1/metadata.json", DATASET_UUID);
    }

    @Test
    void aLowerRevisionIsNotSent() throws Exception {
        lakeHolds("{\"title\":\"Run\",\"revision\":5}");

        transferMetadata("{\"title\":\"Run\",\"revision\":4}");

        assertThat(uploadedKeys()).isEmpty();
    }

    @Test
    void aFirstRevisionReplacesAMetadataThatHadNone() throws Exception {
        lakeHolds("{\"title\":\"Run\"}");

        transferMetadata("{\"title\":\"Run\",\"revision\":1}");

        assertThat(uploadedKeys()).containsExactly(DATASET_UUID + "/metadata.json");
    }

    @Test
    void aChangeWithoutAnyRevisionIsNotSent() throws Exception {
        lakeHolds("{\"title\":\"Run\"}");

        transferMetadata("{\"title\":\"Run edited\"}");

        assertThat(uploadedKeys()).isEmpty();
    }

    @Test
    void revisionReadingToleratesTextNegativesAndGarbage() {
        assertThat(MetadataRevision.of("{\"revision\":\" 7 \"}".getBytes(StandardCharsets.UTF_8))).isEqualTo(7);
        assertThat(MetadataRevision.of("{\"revision\":-3}".getBytes(StandardCharsets.UTF_8))).isZero();
        assertThat(MetadataRevision.of("{\"revision\":\"two\"}".getBytes(StandardCharsets.UTF_8))).isZero();
        assertThat(MetadataRevision.of("{\"revision\":1.5}".getBytes(StandardCharsets.UTF_8))).isZero();
        assertThat(MetadataRevision.of("not json".getBytes(StandardCharsets.UTF_8))).isZero();
    }
}
