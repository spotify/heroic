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
import com.spotify.heroic.cluster.ClusterShardGroup;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardError;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
public class FindSeries {
    public static final FindSeries EMPTY =
        new FindSeries(ImmutableList.of(), ImmutableSet.of(), 0, 0);

    private final List<RequestError> errors;
    private final Set<Series> series;
    private final int size;
    private final int duplicates;

    public static FindSeries of(final Set<Series> series, final int size, final int duplicates) {
        return new FindSeries(ImmutableList.of(), series, size, duplicates);
    }

    public static Collector<FindSeries, FindSeries> reduce(OptionalLimit limit) {
        return results -> {
            final List<RequestError> errors = new ArrayList<>();
            final Set<Series> series = new HashSet<>();
            int size = 0;
            int duplicates = 0;

            outer:
            for (final FindSeries result : results) {
                errors.addAll(result.errors);

                duplicates += result.duplicates;
                size += result.size;

                for (final Series s : result.series) {
                    if (series.add(s)) {
                        duplicates += 1;
                    }

                    if (limit.isGreaterOrEqual(series.size())) {
                        break outer;
                    }
                }
            }

            return new FindSeries(errors, series, size, duplicates);
        };
    }

    public static Transform<Throwable, ? extends FindSeries> shardError(
        final ClusterShardGroup shard
    ) {
        return e -> new FindSeries(ImmutableList.of(ShardError.fromThrowable(shard, e)),
            ImmutableSet.of(), 0, 0);
    }

    @JsonIgnore
    public boolean isEmpty() {
        return series.isEmpty();
    }
}
