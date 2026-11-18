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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The {@code revision} of a dataset's {@code metadata.json}: a whole number the testbed raises each time it wants the
 * catalogue entry updated. A metadata.json whose content changed but whose revision did not is not sent again.
 *
 * <p>A file without a revision, or with one that is not a whole number &ge; 0, counts as revision 0.
 */
final class MetadataRevision {
    static final String FIELD = "revision";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MetadataRevision() {
    }

    /** The revision in a metadata.json; 0 when absent, not a whole number, negative, or the JSON cannot be read. */
    static long of(byte[] json) {
        if (json == null) {
            return 0;
        }
        try {
            return of(MAPPER.readTree(json));
        } catch (Exception e) {
            return 0;
        }
    }

    static long of(JsonNode root) {
        if (root == null) {
            return 0;
        }
        JsonNode node = root.get(FIELD);
        if (node == null || node.isNull()) {
            return 0;
        }
        if (node.isIntegralNumber()) {
            return Math.max(node.asLong(), 0);
        }
        if (node.isTextual()) {
            try {
                return Math.max(Long.parseLong(node.asText().trim()), 0);
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }

    /** Whether {@code incoming} should replace {@code stored}: only a strictly higher revision does. */
    static boolean supersedes(long incoming, long stored) {
        return incoming > stored;
    }
}
