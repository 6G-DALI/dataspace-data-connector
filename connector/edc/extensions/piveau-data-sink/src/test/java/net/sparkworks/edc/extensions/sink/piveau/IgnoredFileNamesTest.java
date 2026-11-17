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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IgnoredFileNamesTest {

    @Test
    void theSettingIsEdcFilesIgnore() {
        assertThat(IgnoredFileNames.SETTING).isEqualTo("edc.files.ignore");
    }

    @Test
    void aCommaSeparatedListIsReadWithItsSpacesAndEmptyEntriesDropped() {
        var ignored = IgnoredFileNames.parse(" README.md, Thumbs.db ,, .DS_Store,");
        assertThat(List.copyOf(ignored.names())).containsExactly("readme.md", "thumbs.db", ".ds_store");
    }

    @Test
    void aNameMatchesAsAWholeAndIgnoringCase() {
        var ignored = IgnoredFileNames.parse("README.md,notes.txt");
        assertThat(ignored.ignores("readme.MD")).isTrue();
        assertThat(ignored.ignores("notes.txt")).isTrue();
        assertThat(ignored.ignores("notes.txt.bak")).isFalse();
        assertThat(ignored.ignores("my-notes.txt")).isFalse();
    }

    @Test
    void aPathIsMatchedOnItsFileName() {
        var ignored = IgnoredFileNames.parse("Thumbs.db");
        assertThat(ignored.ignores("exp-1/images/thumbs.db")).isTrue();
        assertThat(ignored.ignores("exp-1\\images\\Thumbs.db")).isTrue();
        assertThat(ignored.ignores("Thumbs.db/data.csv")).isFalse();
    }

    @Test
    void nothingIsIgnoredWithoutAList() {
        assertThat(IgnoredFileNames.parse(null).isEmpty()).isTrue();
        assertThat(IgnoredFileNames.parse("  ").isEmpty()).isTrue();
        assertThat(IgnoredFileNames.parse(" , ,").isEmpty()).isTrue();
        assertThat(IgnoredFileNames.none().ignores("data.csv")).isFalse();
        assertThat(IgnoredFileNames.parse("a.csv").ignores(null)).isFalse();
    }
}
