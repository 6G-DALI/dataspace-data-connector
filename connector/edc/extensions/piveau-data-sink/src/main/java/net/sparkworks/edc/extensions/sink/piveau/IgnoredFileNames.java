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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The file names the sink does not upload, from the connector's {@code edc.files.ignore} setting:
 * a comma-separated list such as {@code README.md, Thumbs.db, .DS_Store}.
 *
 * <p>A name matches a file's own name (the last segment of its path), as a whole and ignoring case.
 * Spaces around a name and empty entries are dropped.
 */
public final class IgnoredFileNames {

    /** The setting in the connector's .properties file. */
    public static final String SETTING = "edc.files.ignore";

    private final Set<String> names;

    private IgnoredFileNames(Set<String> names) {
        this.names = names;
    }

    /** Nothing is ignored. */
    public static IgnoredFileNames none() {
        return new IgnoredFileNames(Collections.emptySet());
    }

    /** The names in {@code commaSeparated}; {@code null} or blank gives {@link #none()}. */
    public static IgnoredFileNames parse(String commaSeparated) {
        if (commaSeparated == null || commaSeparated.isBlank()) {
            return none();
        }
        return new IgnoredFileNames(Arrays.stream(commaSeparated.split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .map(name -> name.toLowerCase(Locale.ROOT))
                .collect(Collectors.toCollection(LinkedHashSet::new)));
    }

    /** Whether a file with this name (a bare name or a path) is ignored. */
    public boolean ignores(String fileName) {
        if (fileName == null || names.isEmpty()) {
            return false;
        }
        int slash = Math.max(fileName.lastIndexOf('/'), fileName.lastIndexOf('\\'));
        return names.contains((slash >= 0 ? fileName.substring(slash + 1) : fileName).toLowerCase(Locale.ROOT));
    }

    public boolean isEmpty() {
        return names.isEmpty();
    }

    /** The names, lowercased, in the order they were given. */
    public Set<String> names() {
        return Collections.unmodifiableSet(names);
    }

    @Override
    public String toString() {
        return String.join(", ", names);
    }
}
