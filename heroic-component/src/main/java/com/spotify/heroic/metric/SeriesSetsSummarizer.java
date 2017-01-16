/*
 * Copyright (c) 2017 Spotify AB.
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.spotify.heroic.common.Histogram;
import com.spotify.heroic.common.Series;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.Data;

@Data
public class SeriesSetsSummarizer {
    private HashSet<String> uniqueKeys = new HashSet<>();
    private Multimap<String, String> tags = HashMultimap.create();
    private final Histogram.Builder seriesSize = Histogram.builder();

    public void add(Set<Series> series) {
        this.seriesSize.add(series.size());

        for (final Series s : series) {
            uniqueKeys.add(s.getKey());

            for (Map.Entry<String, String> e : s.getTags().entrySet()) {
                tags.put(e.getKey(), e.getValue());
            }
        }
    }

    public Summary end() {
        final Histogram.Builder tagsSize = Histogram.builder();

        for (final Map.Entry<String, Collection<String>> e : this.tags.asMap().entrySet()) {
            tagsSize.add(e.getValue().size());
        }

        return new Summary(uniqueKeys.size(), tagsSize.build(), seriesSize.build());
    }

    @Data
    public static class Summary {
        final long uniqueKeys;
        final Histogram tagsSize;
        final Histogram seriesSize;
    }
}
