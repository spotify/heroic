/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.metric.bigtable.api;

import com.spotify.heroic.bigtable.com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import lombok.Data;
import lombok.RequiredArgsConstructor;

public interface RowFilter {
    static ColumnRange.Builder newColumnRangeBuilder(String family) {
        return new ColumnRange.Builder(family);
    }

    /**
     * Test if the current filter matches the given column qualifier.
     * <p>
     * This method is primarily used when testing.
     *
     * @param columnQualifier column qualifier to match
     * @return {@code true} if the qualifier matches
     */
    boolean matchesColumn(final ByteString columnQualifier);

    /**
     * Test if the current filter matches the given column family.
     * <p>
     * This method is primarily used when testing.
     *
     * @param familyName family to match
     * @return {@code true} if the family name matches
     */
    boolean matchesColumnFamily(final String familyName);

    /**
     * Build a filter that blocks all cells.
     *
     * @return A filter that blocks all cells.
     */
    static RowFilter blockAll() {
        return new BlockAll();
    }

    /**
     * Build a filter that only matches the latest cell in each column.
     *
     * @return A filter that only matches the latest cell in each column.
     */
    static RowFilter onlyLatestCell() {
        return new OnlyLatestCell();
    }

    /**
     * Apply all the given row filters.
     *
     * @param chain Filter to apply.
     * @return A filter that is the composition of all given row filters.
     */
    static RowFilter chain(final List<? extends RowFilter> chain) {
        return new Chain(chain);
    }

    com.google.bigtable.v2.RowFilter toPb();

    @Data
    class Chain implements RowFilter {
        private final List<? extends RowFilter> chain;

        @Override
        public boolean matchesColumn(final ByteString columnQualifier) {
            return chain.stream().allMatch(entry -> entry.matchesColumn(columnQualifier));
        }

        @Override
        public boolean matchesColumnFamily(final String familyName) {
            return chain.stream().allMatch(entry -> entry.matchesColumnFamily(familyName));
        }

        @Override
        public com.google.bigtable.v2.RowFilter toPb() {
            final com.google.bigtable.v2.RowFilter.Chain.Builder chain =
                com.google.bigtable.v2.RowFilter.Chain.newBuilder();
            this.chain.forEach(f -> chain.addFilters(f.toPb()));
            return com.google.bigtable.v2.RowFilter.newBuilder().setChain(chain.build()).build();
        }
    }

    @Data
    class ColumnRange implements RowFilter {
        private final String family;

        private final Optional<ByteString> startQualifierClosed;
        private final Optional<ByteString> startQualifierOpen;
        private final Optional<ByteString> endQualifierClosed;
        private final Optional<ByteString> endQualifierOpen;

        @Override
        public boolean matchesColumn(final ByteString columnQualifier) {
            if (!startQualifierClosed
                .map(sqc -> compareByteStrings(sqc, columnQualifier) <= 0)
                .orElse(true)) {
                return false;
            }

            if (!startQualifierOpen
                .map(sqo -> compareByteStrings(sqo, columnQualifier) < 0)
                .orElse(true)) {
                return false;
            }

            if (!endQualifierClosed
                .map(eqc -> compareByteStrings(eqc, columnQualifier) >= 0)
                .orElse(true)) {
                return false;
            }

            if (!endQualifierOpen
                .map(eqo -> compareByteStrings(eqo, columnQualifier) > 0)
                .orElse(true)) {
                return false;
            }

            return true;
        }

        @Override
        public boolean matchesColumnFamily(final String familyName) {
            return family.equals(familyName);
        }

        @Override
        public com.google.bigtable.v2.RowFilter toPb() {
            final com.google.bigtable.v2.ColumnRange.Builder builder =
                com.google.bigtable.v2.ColumnRange.newBuilder().setFamilyName(family);

            startQualifierClosed.ifPresent(builder::setStartQualifierClosed);
            startQualifierOpen.ifPresent(builder::setStartQualifierOpen);
            endQualifierClosed.ifPresent(builder::setEndQualifierClosed);
            endQualifierOpen.ifPresent(builder::setEndQualifierOpen);

            return com.google.bigtable.v2.RowFilter
                .newBuilder()
                .setColumnRangeFilter(builder.build())
                .build();
        }

        @RequiredArgsConstructor
        public static class Builder {
            private final String family;

            private Optional<ByteString> startQualifierClosed = Optional.empty();
            private Optional<ByteString> startQualifierOpen = Optional.empty();
            private Optional<ByteString> endQualifierClosed = Optional.empty();
            private Optional<ByteString> endQualifierOpen = Optional.empty();

            public Builder startQualifierClosed(final ByteString startQualifierClosed) {
                this.startQualifierClosed = Optional.of(startQualifierClosed);
                return this;
            }

            public Builder startQualifierOpen(final ByteString startQualifierOpen) {
                this.startQualifierOpen = Optional.of(startQualifierOpen);
                return this;
            }

            public Builder endQualifierClosed(final ByteString endQualifierClosed) {
                this.endQualifierClosed = Optional.of(endQualifierClosed);
                return this;
            }

            public Builder endQualifierOpen(final ByteString endQualifierOpen) {
                this.endQualifierOpen = Optional.of(endQualifierOpen);
                return this;
            }

            public ColumnRange build() {
                return new ColumnRange(family, startQualifierClosed, startQualifierOpen,
                    endQualifierClosed, endQualifierOpen);
            }
        }
    }

    @Data
    class OnlyLatestCell implements RowFilter {
        @Override
        public boolean matchesColumn(final ByteString columnQualifier) {
            return true;
        }

        @Override
        public boolean matchesColumnFamily(final String familyName) {
            return true;
        }

        @Override
        public com.google.bigtable.v2.RowFilter toPb() {
            return com.google.bigtable.v2.RowFilter
                .newBuilder()
                .setCellsPerColumnLimitFilter(1)
                .build();
        }
    }

    @Data
    class BlockAll implements RowFilter {
        @Override
        public boolean matchesColumn(final ByteString columnQualifier) {
            return false;
        }

        @Override
        public boolean matchesColumnFamily(final String familyName) {
            return false;
        }

        @Override
        public com.google.bigtable.v2.RowFilter toPb() {
            return com.google.bigtable.v2.RowFilter.newBuilder().setBlockAllFilter(true).build();
        }
    }

    static int compareByteStrings(final ByteString a, final ByteString b) {
        final ByteString.ByteIterator left = a.iterator();
        final ByteString.ByteIterator right = b.iterator();

        while (left.hasNext() && right.hasNext()) {
            final int c = Integer.compareUnsigned(left.nextByte(), right.nextByte());

            if (c != 0) {
                return c;
            }
        }

        if (left.hasNext()) {
            return 1;
        }

        if (right.hasNext()) {
            return -1;
        }

        return 0;
    }
}
