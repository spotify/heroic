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

package com.spotify.heroic.metric;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.spotify.heroic.common.Series;
import lombok.Data;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class SeriesValues {
    final SortedSet<String> keys;
    final Map<String, SortedSet<String>> tags;

    @JsonCreator
    public SeriesValues(
        @JsonProperty("keys") SortedSet<String> keys,
        @JsonProperty("tags") Map<String, SortedSet<String>> tags
    ) {
        this.keys = checkNotNull(keys, "keys");
        this.tags = checkNotNull(tags, "tags");
    }

    private static final Comparator<String> COMPARATOR = new Comparator<String>() {
        @Override
        public int compare(String a, String b) {
            if (a == null) {
                if (b == null) {
                    return 0;
                }

                return -1;
            }

            if (b == null) {
                return 1;
            }

            return a.compareTo(b);
        }
    };

    public static SeriesValues fromSeries(final Iterator<Series> series) {
        final SeriesValues.Builder builder = builder();

        while (series.hasNext()) {
            final Series s = series.next();
            builder.addKey(s.getKey());
            builder.addSingleTags(s.getTags());
        }

        return builder.build();
    }

    public static SeriesValues of(final String k, final String v) {
        return new SeriesValues(ImmutableSortedSet.of(), ImmutableMap.of());
    }

    public static SeriesValues empty() {
        return new SeriesValues(ImmutableSortedSet.of(), ImmutableMap.of());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        final SortedSet<String> keys = new TreeSet<>();
        final Map<String, SortedSet<String>> tags = new HashMap<>();

        public void addKey(final String key) {
            this.keys.add(key);
        }

        public void addSingleTags(final Map<String, String> tags) {
            for (final Map.Entry<String, String> e : tags.entrySet()) {
                SortedSet<String> values = this.tags.get(e.getKey());

                if (values == null) {
                    values = new TreeSet<String>(COMPARATOR);
                    this.tags.put(e.getKey(), values);
                }

                values.add(e.getValue());
            }
        }

        public void addKeys(final Collection<String> keys) {
            this.keys.addAll(keys);
        }

        public void addTags(final Map<String, SortedSet<String>> tags) {
            Set<Map.Entry<String, SortedSet<String>>> entries = tags.entrySet();

            for (final Map.Entry<String, SortedSet<String>> t : entries) {
                SortedSet<String> values = this.tags.get(t.getKey());

                if (values == null) {
                    values = new TreeSet<>();
                    this.tags.put(t.getKey(), values);
                }

                values.addAll(t.getValue());
            }
        }

        public void addSeriesValues(final SeriesValues series) {
            addKeys(series.getKeys());
            addTags(series.getTags());
        }

        public SeriesValues build() {
            return new SeriesValues(keys, tags);
        }
    }
}
