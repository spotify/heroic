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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.cluster.ClusterShard;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardError;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Data
public class FindSeries {
    private final List<RequestError> errors;
    private final Set<Series> series;
    private final boolean limited;

    public static FindSeries of() {
        return new FindSeries(ImmutableList.of(), ImmutableSet.of(), false);
    }

    public static FindSeries of(final Set<Series> series, final boolean limited) {
        return new FindSeries(ImmutableList.of(), series, limited);
    }

    public static Collector<FindSeries, FindSeries> reduce(final OptionalLimit limit) {
        return results -> {
            final List<RequestError> errors = new ArrayList<>();
            final ImmutableSet.Builder<Series> series = ImmutableSet.builder();
            boolean limited = false;

            for (final FindSeries result : results) {
                errors.addAll(result.errors);
                series.addAll(result.series);
                limited |= result.limited;
            }

            final Set<Series> s = series.build();
            return new FindSeries(errors, limit.limitSet(s), limited || limit.isGreater(s.size()));
        };
    }

    public static Transform<Throwable, FindSeries> shardError(final ClusterShard shard) {
        return e -> new FindSeries(ImmutableList.of(ShardError.fromThrowable(shard, e)),
            ImmutableSet.of(), false);
    }

    @JsonIgnore
    public boolean isEmpty() {
        return series.isEmpty();
    }

    @Data
    public static class Request {
        private final Filter filter;
        private final DateRange range;
        private final OptionalLimit limit;
    }
}
