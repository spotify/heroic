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

package com.spotify.heroic.metadata.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import eu.toolchain.async.Collector;

@Data
public class SearchResults {
    public static final SearchResults EMPTY = new SearchResults(new ArrayList<ScoredSeries>());
    private static final Comparator<ScoredSeries> SERIES_COMPARATOR = new Comparator<ScoredSeries>() {
        @Override
        public int compare(ScoredSeries a, ScoredSeries b) {
            // reverse, since we want to sort in descending order.
            return Float.compare(b.getScore(), a.getScore());
        }
    };

    private final List<ScoredSeries> series;

    public static Collector<SearchResults, SearchResults> reduce(final int limit) {
        return new Collector<SearchResults, SearchResults>() {
            @Override
            public SearchResults collect(Collection<SearchResults> results) throws Exception {
                final List<ScoredSeries> series = new ArrayList<>();

                for (final SearchResults r : results)
                    series.addAll(r.getSeries());

                Collections.sort(series, SERIES_COMPARATOR);

                return new SearchResults(series.subList(0, Math.min(limit, series.size())));
            }
        };
    }

    @JsonCreator
    public static SearchResults create(@JsonProperty("series") List<ScoredSeries> series) {
        return new SearchResults(series);
    }
}