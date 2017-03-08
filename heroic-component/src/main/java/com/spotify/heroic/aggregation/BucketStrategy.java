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

package com.spotify.heroic.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.spotify.heroic.common.DateRange;
import lombok.Data;
import lombok.RequiredArgsConstructor;

public interface BucketStrategy {
    long MAX_BUCKET_COUNT = 100000L;

    BucketStrategy END = new End();

    Mapping setup(DateRange range, long size, long extent);

    @JsonCreator
    static BucketStrategy create(final String strategy) {
        switch (strategy) {
            case "end":
                return END;
            default:
                throw new IllegalArgumentException(strategy);
        }
    }

    class End implements BucketStrategy {
        @JsonValue
        public String value() {
            return "end";
        }

        @Override
        public Mapping setup(final DateRange range, final long size, final long extent) {
            final long start = range.start() + size;
            final long count = (range.diff() + size) / size - 1;

            if (count < 1 || count > MAX_BUCKET_COUNT) {
                throw new IllegalArgumentException(String.format("range %s, size %d", range, size));
            }

            return new EndMapping(start, range.start(), size, extent, (int) count);
        }

        @RequiredArgsConstructor
        private static class EndMapping implements Mapping {
            private final long start;
            private final long offset;
            private final long size;
            private final long extent;
            private final int buckets;

            /**
             * Calculate the start and end index of the buckets that should be seeded for the given
             * timestamp.
             *
             * This guarantees that each timestamp ends up in the range (end - extent, end] for
             * any given bucket.
             *
             * @param timestamp timestamp to map
             * @return a start end and index
             */
            @Override
            public StartEnd map(final long timestamp) {
                /* adjust the timestamp to the number of buckets */
                final long adjusted = timestamp - offset;

                final int start = Math.max((int) ((adjusted - 1) / size), 0);
                final int end = Math.min((int) ((adjusted + extent - 1) / size), buckets);

                return new StartEnd(start, end);
            }

            @Override
            public long start() {
                return start;
            }

            @Override
            public int buckets() {
                return buckets;
            }
        }
    }

    interface Mapping {
        StartEnd map(final long timestamp);

        long start();

        int buckets();
    }

    /**
     * A start, and an end bucket (exclusive) selected.
     */
    @Data
    class StartEnd {
        private final int start;
        private final int end;
    }
}
