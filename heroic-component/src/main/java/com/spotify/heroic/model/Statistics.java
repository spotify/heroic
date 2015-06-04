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

package com.spotify.heroic.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public class Statistics {
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Builder {
        private Aggregator aggregator = Aggregator.EMPTY;
        private Row row = Row.EMPTY;
        private Cache cache = Cache.EMPTY;

        public Builder aggregator(Aggregator aggregator) {
            this.aggregator = aggregator;
            return this;
        }

        public Builder row(Row row) {
            this.row = row;
            return this;
        }

        public Builder cache(Cache cache) {
            this.cache = cache;
            return this;
        }

        public Statistics build() {
            return new Statistics(aggregator, row, cache);
        }
    }

    @Data
    public static final class Aggregator {
        public static final Aggregator EMPTY = new Aggregator(0, 0, 0);

        private final long sampleSize;
        private final long outOfBounds;

        /* aggregator performed a useless index scan. */
        private final long uselessScan;

        public Aggregator merge(Aggregator other) {
            return new Aggregator(this.sampleSize + other.sampleSize, this.outOfBounds + other.outOfBounds,
                    this.uselessScan + other.uselessScan);
        }

        @JsonCreator
        public static Aggregator create(@JsonProperty(value = "sampleSize", required = true) Integer sampleSize,
                @JsonProperty(value = "outOfBounds", required = true) Integer outOfBounds,
                @JsonProperty(value = "uselessScan", required = true) Integer uselessScan) {
            return new Aggregator(sampleSize, outOfBounds, uselessScan);
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Row {
        public static final Row EMPTY = new Row(0, 0);

        private final int successful;
        private final int failed;

        public Row merge(Row other) {
            return new Row(this.successful + other.successful, this.failed + other.failed);
        }

        @JsonCreator
        public static Row create(@JsonProperty(value = "successful", required = true) Integer successful,
                @JsonProperty(value = "failed", required = true) Integer failed) {
            return new Row(successful, failed);
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Cache {
        public static final Cache EMPTY = new Cache(0, 0, 0, 0);

        private final int hits;
        private final int conflicts;
        private final int cacheConflicts;
        private final int cachedNans;

        public Cache merge(Cache other) {
            return new Cache(this.hits + other.hits, this.conflicts + other.conflicts, this.cacheConflicts
                    + other.cacheConflicts, this.cachedNans + other.cachedNans);
        }

        @JsonCreator
        public static Cache create(@JsonProperty(value = "hits", required = true) Integer hits,
                @JsonProperty(value = "conflicts", required = true) Integer conflicts,
                @JsonProperty(value = "cacheConflicts", required = true) Integer cacheConflicts,
                @JsonProperty(value = "cachedNans", required = true) Integer cachedNans) {
            return new Cache(hits, conflicts, cacheConflicts, cachedNans);
        }
    }

    public static final Statistics EMPTY = new Statistics(Aggregator.EMPTY, Row.EMPTY, Cache.EMPTY);

    private final Aggregator aggregator;
    private final Row row;
    private final Cache cache;

    public Statistics merge(Statistics other) {
        return new Statistics(aggregator.merge(other.aggregator), row.merge(other.row), cache.merge(other.cache));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(final Statistics other) {
        return new Builder(other.aggregator, other.row, other.cache);
    }

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static Statistics create(@JsonProperty(value = "aggregator", required = true) Aggregator aggregator,
            @JsonProperty(value = "row", required = true) Row row,
            @JsonProperty(value = "cache", required = true) Cache cache,
            @JsonProperty(value = "rpc", required = false) Object ignore) {
        return new Statistics(aggregator, row, cache);
    }
}
