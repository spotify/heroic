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

import com.google.common.collect.ImmutableList;
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

@Data
public class CountSeries {
    private final List<RequestError> errors;
    private final long count;
    private final boolean limited;

    public static CountSeries of() {
        return new CountSeries(ImmutableList.of(), 0, false);
    }

    public static CountSeries of(long count, boolean limited) {
        return new CountSeries(ImmutableList.of(), count, limited);
    }

    public static Collector<CountSeries, CountSeries> reduce() {
        return results -> {
            final List<RequestError> errors = new ArrayList<>();
            long count = 0;
            boolean limited = false;

            for (final CountSeries result : results) {
                errors.addAll(result.errors);
                count += result.count;
                limited |= result.limited;
            }

            return new CountSeries(errors, count, limited);
        };
    }

    public static Transform<Throwable, CountSeries> shardError(final ClusterShard shard) {
        return e -> new CountSeries(ImmutableList.of(ShardError.fromThrowable(shard, e)), 0, false);
    }

    @Data
    public static class Request {
        private final Filter filter;
        private final DateRange range;
        private final OptionalLimit limit;
    }
}
