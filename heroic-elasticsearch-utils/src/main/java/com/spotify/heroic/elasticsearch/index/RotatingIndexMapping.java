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

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import lombok.ToString;

import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.spotify.heroic.model.DateRange;

@ToString
public class RotatingIndexMapping implements IndexMapping {
    private static final String[] STRING_ARRAY = new String[0];

    public static final long DEFAULT_INTERVAL = 3600 * 24 * 7 * 1000;
    public static final String DEFAULT_PATTERN = "heroic-%s";
    public static final long DEFAULT_RETENTION = TimeUnit.MILLISECONDS.convert(28, TimeUnit.DAYS);

    private final long interval;
    private final String pattern;
    private final long retention;

    @JsonCreator
    public RotatingIndexMapping(@JsonProperty("interval") Long interval, @JsonProperty("pattern") String pattern,
            @JsonProperty("retention") Long retention) {
        this.interval = Optional.fromNullable(interval).or(DEFAULT_INTERVAL);
        this.pattern = verifyPattern(Optional.fromNullable(pattern).or(DEFAULT_PATTERN));
        this.retention = Optional.fromNullable(retention).or(DEFAULT_RETENTION);
    }

    private String verifyPattern(String pattern) {
        if (!pattern.contains("%s"))
            throw new IllegalArgumentException("pattern '" + pattern + "' does not contain a string substitude '%s'");

        return pattern;
    }

    @Override
    public String template() {
        return String.format(pattern, "*");
    }

    @Override
    public String[] indices(DateRange range) throws NoIndexSelectedException {
        final long now = System.currentTimeMillis();
        return indices(range, now);
    }

    protected String[] indices(DateRange range, long now) throws NoIndexSelectedException {
        // the first date that we allow.
        final long first = now - retention;

        final DateRange rounded = range.rounded(interval);

        final int size = (int) (rounded.diff() / interval) + 1;

        final ArrayList<String> indices = new ArrayList<String>();

        for (int i = 0; i < size; i++) {
            long date = rounded.getStart() + (i * interval);

            // skip date out of retention range.
            if (date < first)
                continue;

            indices.add(String.format(pattern, date));
        }

        if (indices.isEmpty())
            throw new NoIndexSelectedException();

        return indices.toArray(STRING_ARRAY);
    }

    @Override
    public DeleteByQueryRequestBuilder deleteByQuery(final Client client, final DateRange range, final String type)
            throws NoIndexSelectedException {
        return client.prepareDeleteByQuery(indices(range)).setIndicesOptions(options()).setTypes(type);
    }

    @Override
    public SearchRequestBuilder search(final Client client, final DateRange range, final String type)
            throws NoIndexSelectedException {
        return client.prepareSearch(indices(range)).setIndicesOptions(options()).setTypes(type);
    }

    @Override
    public CountRequestBuilder count(final Client client, final DateRange range, final String type)
            throws NoIndexSelectedException {
        return client.prepareCount(indices(range)).setIndicesOptions(options()).setTypes(type);
    }

    private IndicesOptions options() {
        return IndicesOptions.fromOptions(true, false, false, false);
    }

    public static Supplier<IndexMapping> defaultSupplier() {
        return new Supplier<IndexMapping>() {
            @Override
            public IndexMapping get() {
                return new RotatingIndexMapping(null, null, null);
            }
        };
    }
}