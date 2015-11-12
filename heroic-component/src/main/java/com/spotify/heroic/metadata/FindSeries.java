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

package com.spotify.heroic.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.cluster.NodeRegistryEntry;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.NodeError;
import com.spotify.heroic.metric.RequestError;

import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;

@Data
public class FindSeries {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();
    public static final Set<Series> EMPTY_SERIES = new HashSet<Series>();
    public static final FindSeries EMPTY = new FindSeries(EMPTY_ERRORS, EMPTY_SERIES, 0, 0);

    private final List<RequestError> errors;
    private final Set<Series> series;
    private final int size;
    private final int duplicates;

    @RequiredArgsConstructor
    public static class SelfReducer implements Collector<FindSeries, FindSeries> {
        private final int limit;

        @Override
        public FindSeries collect(Collection<FindSeries> results) throws Exception {
            final List<RequestError> errors = new ArrayList<>();
            final Set<Series> series = new HashSet<Series>();
            int size = 0;
            int duplicates = 0;

            outer:
            for (final FindSeries result : results) {
                errors.addAll(result.errors);

                for (final Series s : result.series) {
                    if (series.add(s)) {
                        duplicates += 1;
                    }

                    if (series.size() >= limit) {
                        break outer;
                    }
                }

                duplicates += result.duplicates;
                size += result.size;
            }

            return new FindSeries(errors, series, size, duplicates);
        }
    };

    public static Collector<FindSeries, FindSeries> reduce(int limit) {
        return new SelfReducer(limit);
    }

    @JsonCreator
    public FindSeries(@JsonProperty("errors") List<RequestError> errors,
            @JsonProperty("series") Set<Series> series, @JsonProperty("size") int size,
            @JsonProperty("duplicates") int duplicates) {
        this.errors = Optional.fromNullable(errors).or(EMPTY_ERRORS);
        this.series = series;
        this.size = size;
        this.duplicates = duplicates;
    }

    public FindSeries(Set<Series> series, int size, int duplicates) {
        this(EMPTY_ERRORS, series, size, duplicates);
    }

    public static Transform<Throwable, ? extends FindSeries> nodeError(
            final NodeRegistryEntry node) {
        return new Transform<Throwable, FindSeries>() {
            @Override
            public FindSeries transform(Throwable e) throws Exception {
                final NodeMetadata m = node.getMetadata();
                final ClusterNode c = node.getClusterNode();
                return new FindSeries(
                        ImmutableList.<RequestError> of(
                                NodeError.fromThrowable(m.getId(), c.toString(), m.getTags(), e)),
                        EMPTY_SERIES, 0, 0);
            }
        };
    }

    public static Transform<Throwable, ? extends FindSeries> nodeError(
            final ClusterNode.Group group) {
        return new Transform<Throwable, FindSeries>() {
            @Override
            public FindSeries transform(Throwable e) throws Exception {
                final List<RequestError> errors =
                        ImmutableList.<RequestError> of(NodeError.fromThrowable(group.node(), e));
                return new FindSeries(errors, EMPTY_SERIES, 0, 0);
            }
        };
    }

    @JsonIgnore
    public boolean isEmpty() {
        return series.isEmpty();
    }
}
