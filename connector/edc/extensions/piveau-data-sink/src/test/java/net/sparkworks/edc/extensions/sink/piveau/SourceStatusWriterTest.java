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
import io.minio.GetObjectResponse;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.ErrorResponse;
import okhttp3.Headers;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SourceStatusWriterTest {

    private static final String UUID = "0c955ae4-0be9-4f9e-8d77-57eb69ed58e1";

    private final MinioClient client = mock(MinioClient.class);
    private final Monitor monitor = mock(Monitor.class);
    private final SourceStatusWriter writer = new SourceStatusWriter(client, "testbed-bucket", monitor);

    private static ErrorResponseException noSuchKey() {
        var error = mock(ErrorResponse.class);
        when(error.code()).thenReturn("NoSuchKey");
        var exception = mock(ErrorResponseException.class);
        when(exception.errorResponse()).thenReturn(error);
        return exception;
    }

    private void statusIs(String content) throws Exception {
        when(client.getObject(any(GetObjectArgs.class))).thenReturn(new GetObjectResponse(
                Headers.of(), "testbed-bucket", "", "k", new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void aStatusFileIsAnyNameThatEndsInDotStatus() {
        assertThat(SourceStatusWriter.isStatusFile("data.csv.status")).isTrue();
        assertThat(SourceStatusWriter.isStatusFile("exp-1/DATA.CSV.STATUS")).isTrue();
        assertThat(SourceStatusWriter.isStatusFile("metadata.json.status")).isTrue();
        assertThat(SourceStatusWriter.isStatusFile("data.csv")).isFalse();
        assertThat(SourceStatusWriter.isStatusFile("status")).isFalse();
        assertThat(SourceStatusWriter.isStatusFile("data.status.csv")).isFalse();
        assertThat(SourceStatusWriter.isStatusFile(null)).isFalse();
    }

    @Test
    void theStatusObjectIsNamedAfterTheFileAndHoldsOnlyTheUuid() throws Exception {
        var missing = noSuchKey();                      // built first: a mock made inside when(...) breaks Mockito
        when(client.getObject(any(GetObjectArgs.class))).thenThrow(missing);

        assertThat(writer.write("exp-1/data.csv", UUID)).isTrue();

        var args = ArgumentCaptor.forClass(PutObjectArgs.class);
        verify(client).putObject(args.capture());
        assertThat(args.getValue().bucket()).isEqualTo("testbed-bucket");
        assertThat(args.getValue().object()).isEqualTo("exp-1/data.csv.status");
        assertThat(args.getValue().contentType()).isEqualTo("text/plain");
        assertThat(args.getValue().objectSize()).isEqualTo(UUID.length());          // the uuid and nothing else
    }

    @Test
    void aStatusThatAlreadyHoldsTheUuidIsNotWrittenAgain() throws Exception {
        statusIs(UUID + "\n");

        assertThat(writer.write("exp-1/data.csv", UUID)).isTrue();

        verify(client, never()).putObject(any(PutObjectArgs.class));
    }

    @Test
    void aStatusWithAnotherUuidIsReplaced() throws Exception {
        statusIs("11111111-1111-1111-1111-111111111111");

        assertThat(writer.write("exp-1/data.csv", UUID)).isTrue();

        verify(client).putObject(any(PutObjectArgs.class));
    }

    @Test
    void aStatusThatCannotBeWrittenIsLoggedAndNeverThrown() throws Exception {
        var missing = noSuchKey();                      // built first: a mock made inside when(...) breaks Mockito
        when(client.getObject(any(GetObjectArgs.class))).thenThrow(missing);
        when(client.putObject(any(PutObjectArgs.class))).thenThrow(new RuntimeException("Access Denied"));

        assertThat(writer.write("exp-1/data.csv", UUID)).isFalse();

        verify(monitor).warning(org.mockito.ArgumentMatchers.contains("exp-1/data.csv.status"));
    }
}
