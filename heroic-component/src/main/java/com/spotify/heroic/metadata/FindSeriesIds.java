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
public class FindSeriesIds {
    private final List<RequestError> errors;
    private final Set<String> ids;
    private final boolean limited;

    public static FindSeriesIds of() {
        return new FindSeriesIds(ImmutableList.of(), ImmutableSet.of(), false);
    }

    public static FindSeriesIds of(final Set<String> ids, final boolean limited) {
        return new FindSeriesIds(ImmutableList.of(), ids, limited);
    }

    public static Collector<FindSeriesIds, FindSeriesIds> reduce(final OptionalLimit limit) {
        return results -> {
            final List<RequestError> errors = new ArrayList<>();
            final ImmutableSet.Builder<String> ids = ImmutableSet.builder();
            boolean limited = false;

            for (final FindSeriesIds result : results) {
                errors.addAll(result.errors);
                ids.addAll(result.ids);
                limited |= result.limited;
            }

            final Set<String> s = ids.build();
            return new FindSeriesIds(errors, limit.limitSet(s),
                limited || limit.isGreater(s.size()));
        };
    }

    public static Transform<Throwable, FindSeriesIds> shardError(final ClusterShard shard) {
        return e -> new FindSeriesIds(ImmutableList.of(ShardError.fromThrowable(shard, e)),
            ImmutableSet.of(), false);
    }

    @JsonIgnore
    public boolean isEmpty() {
        return ids.isEmpty();
    }

    @Data
    public static class Request {
        private final Filter filter;
        private final DateRange range;
        private final OptionalLimit limit;
    }
}
