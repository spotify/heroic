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

package com.spotify.heroic.elasticsearch.index;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;

import lombok.ToString;

@ToString
public class RotatingIndexMapping implements IndexMapping {
    public static final Duration DEFAULT_INTERVAL = Duration.of(7, TimeUnit.DAYS);
    public static final int DEFAULT_MAX_READ_INDICES = 2;
    public static final int DEFAULT_MAX_WRITE_INDICES = 1;
    public static final String DEFAULT_PATTERN = "heroic-%s";

    private final long interval;
    private final int maxReadIndices;
    private final int maxWriteIndices;
    private final String pattern;

    @JsonCreator
    public RotatingIndexMapping(@JsonProperty("interval") Duration interval,
            @JsonProperty("maxReadIndices") Integer maxReadIndices,
            @JsonProperty("maxWriteIndices") Integer maxWriteIndices, @JsonProperty("pattern") String pattern) {
        this.interval = Optional.fromNullable(interval).or(DEFAULT_INTERVAL).convert(TimeUnit.MILLISECONDS);
        this.maxReadIndices = verifyPositiveInt(Optional.fromNullable(maxReadIndices).or(DEFAULT_MAX_READ_INDICES),
                "maxReadIndices");
        this.maxWriteIndices = verifyPositiveInt(Optional.fromNullable(maxWriteIndices).or(DEFAULT_MAX_WRITE_INDICES),
                "maxWriteIndices");
        this.pattern = verifyPattern(Optional.fromNullable(pattern).or(DEFAULT_PATTERN));
    }

    private String verifyPattern(String pattern) {
        if (!pattern.contains("%s")) {
            throw new IllegalArgumentException("pattern '" + pattern + "' does not contain a string substitude '%s'");
        }

        return pattern;
    }

    private int verifyPositiveInt(int value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + "=" + value + "  is not a positive integer");
        }

        return value;
    }

    @Override
    public String template() {
        return String.format(pattern, "*");
    }

    private String[] indices(int maxIndices, long now) {
        long curr = now - (now % interval);
        final List<String> indices = new ArrayList<>();

        for (int i = 0; i < maxIndices; i++) {
            long date = curr - (interval * i);

            if (date < 0) {
                break;
            }

            indices.add(String.format(pattern, date));
        }

        return indices.toArray(new String[indices.size()]);
    }

    protected String[] readIndices(long now) throws NoIndexSelectedException {
        String[] indices = indices(maxReadIndices, now);

        if (indices.length == 0) {
            throw new NoIndexSelectedException();
        }

        return indices;
    }

    @Override
    public String[] readIndices(DateRange range) throws NoIndexSelectedException {
        return readIndices(System.currentTimeMillis());
    }

    protected String[] writeIndices(long now) {
        return indices(maxWriteIndices, now);
    }

    @Override
    public String[] writeIndices(DateRange range) {
        return writeIndices(System.currentTimeMillis());
    }

    @Override
    public DeleteByQueryRequestBuilder deleteByQuery(final Client client, final DateRange range, final String type)
            throws NoIndexSelectedException {
        return client.prepareDeleteByQuery(readIndices(range)).setIndicesOptions(options()).setTypes(type);
    }

    @Override
    public SearchRequestBuilder search(final Client client, final DateRange range, final String type)
            throws NoIndexSelectedException {
        return client.prepareSearch(readIndices(range)).setIndicesOptions(options()).setTypes(type);
    }

    @Override
    public CountRequestBuilder count(final Client client, final DateRange range, final String type)
            throws NoIndexSelectedException {
        return client.prepareCount(readIndices(range)).setIndicesOptions(options()).setTypes(type);
    }

    private IndicesOptions options() {
        return IndicesOptions.fromOptions(true, true, false, false);
    }

    public static Supplier<IndexMapping> defaultSupplier() {
        return new Supplier<IndexMapping>() {
            @Override
            public IndexMapping get() {
                return new RotatingIndexMapping(null, null, null, null);
            }
        };
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Duration interval;
        private Integer maxReadIndices;
        private Integer maxWriteIndices;
        private String pattern;

        public Builder interval(Duration interval) {
            this.interval = checkNotNull(interval, "interval");
            return this;
        }

        public Builder maxReadIndices(Integer maxReadIndices) {
            this.maxReadIndices = maxReadIndices;
            return this;
        }

        public Builder maxWriteIndices(Integer maxWriteIndices) {
            this.maxWriteIndices = maxWriteIndices;
            return this;
        }

        public Builder pattern(String pattern) {
            this.pattern = pattern;
            return this;
        }

        public RotatingIndexMapping build() {
            return new RotatingIndexMapping(interval, maxReadIndices, maxWriteIndices, pattern);
        }
    }
}
