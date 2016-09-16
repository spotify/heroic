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

import com.google.protobuf.ByteString;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.Optional;

public interface RowFilter {
    static ColumnRange.Builder newColumnRangeBuilder(String family) {
        return new ColumnRange.Builder(family);
    }

    /**
     * Build a filter that blocks all cells.
     *
     * @return A filter that blocks all cells.
     */
    static RowFilter blockAll() {
        return new BlockAll();
    }

    /**
     * Apply all the given row filters.
     *
     * @param filters Filter to apply.
     * @return A filter that is the composition of all given row filters.
     */
    static RowFilter chain(final Iterable<? extends RowFilter> chain) {
        return new Chain(chain);
    }

    com.google.bigtable.v2.RowFilter toPb();

    @Data
    static class Chain implements RowFilter {
        private final Iterable<? extends RowFilter> chain;

        @Override
        public com.google.bigtable.v2.RowFilter toPb() {
            final com.google.bigtable.v2.RowFilter.Chain.Builder chain =
                com.google.bigtable.v2.RowFilter.Chain.newBuilder();
            this.chain.forEach(f -> chain.addFilters(f.toPb()));
            return com.google.bigtable.v2.RowFilter.newBuilder().setChain(chain.build()).build();
        }
    }

    @Data
    static class ColumnRange implements RowFilter {
        private final String family;

        private final Optional<ByteString> startQualifierClosed;
        private final Optional<ByteString> startQualifierOpen;
        private final Optional<ByteString> endQualifierClosed;
        private final Optional<ByteString> endQualifierOpen;

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
    public class BlockAll implements RowFilter {
        @Override
        public com.google.bigtable.v2.RowFilter toPb() {
            return com.google.bigtable.v2.RowFilter.newBuilder().setBlockAllFilter(true).build();
        }
    }
}
