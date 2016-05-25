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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterShardGroup;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardError;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Data
public class CountSeries {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();
    public static final CountSeries EMPTY = new CountSeries(EMPTY_ERRORS, 0, false);

    private final List<RequestError> errors;
    private final boolean limited;
    private final long count;

    public static class SelfReducer implements Collector<CountSeries, CountSeries> {
        @Override
        public CountSeries collect(Collection<CountSeries> results) throws Exception {
            final List<RequestError> errors = new ArrayList<>();
            long count = 0;
            boolean limited = false;

            for (final CountSeries result : results) {
                errors.addAll(result.errors);
                count += result.count;
                limited &= result.limited;
            }

            return new CountSeries(errors, count, limited);
        }
    }

    private static final SelfReducer reducer = new SelfReducer();

    public static Collector<CountSeries, CountSeries> reduce() {
        return reducer;
    }

    @JsonCreator
    public CountSeries(
        @JsonProperty("errors") List<RequestError> errors, @JsonProperty("count") long count,
        @JsonProperty("limited") boolean limited
    ) {
        this.errors = Optional.fromNullable(errors).or(EMPTY_ERRORS);
        this.count = count;
        this.limited = limited;
    }

    public CountSeries(long count, boolean limited) {
        this(EMPTY_ERRORS, count, limited);
    }

    public static Transform<Throwable, CountSeries> shardError(
        final ClusterShardGroup shard
    ) {
        return e -> new CountSeries(ImmutableList.of(ShardError.fromThrowable(shard, e)), 0, false);
    }
}
