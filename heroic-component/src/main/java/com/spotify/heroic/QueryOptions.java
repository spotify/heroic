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

package com.spotify.heroic;

import static java.util.Optional.ofNullable;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.metric.QueryTrace;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class QueryOptions {
    public static final boolean DEFAULT_TRACING = false;

    public static final QueryOptions DEFAULTS = new QueryOptions(DEFAULT_TRACING, Optional.empty());

    // XXX: remove ones deployed everywhere.
    @Getter
    private final String type = "core";

    /**
     * Indicates if tracing is enabled.
     *
     * Traces queries will include a {@link QueryTrace} object that indicates detailed timings of
     * the query.
     *
     * @return {@code true} if tracing is enabled.
     */
    private final boolean tracing;

    /**
     * The number of entries to fetch for every batch.
     */
    private final Optional<Integer> fetchSize;

    @JsonCreator
    public QueryOptions(@JsonProperty("tracing") Boolean tracing,
            @JsonProperty("fetchSize") Optional<Integer> fetchSize) {
        this.tracing = ofNullable(tracing).orElse(DEFAULT_TRACING);
        this.fetchSize = fetchSize;
    }

    public boolean isTracing() {
        return tracing;
    }

    public Optional<Integer> getFetchSize() {
        return fetchSize;
    }

    public static QueryOptions defaults() {
        return DEFAULTS;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private boolean tracing = false;
        private Optional<Integer> fetchSize = Optional.empty();

        public Builder tracing(boolean tracing) {
            this.tracing = tracing;
            return this;
        }

        public Builder fetchSize(int fetchSize) {
            this.fetchSize = Optional.of(fetchSize);
            return this;
        }

        public QueryOptions build() {
            return new QueryOptions(tracing, fetchSize);
        }
    }
}
