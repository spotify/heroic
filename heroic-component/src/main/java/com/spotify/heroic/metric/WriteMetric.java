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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterShard;
import com.spotify.heroic.common.RequestTimer;
import com.spotify.heroic.common.Series;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;

import java.util.List;

@Data
public class WriteMetric {
    private final List<RequestError> errors;
    private final List<Long> times;

    public static WriteMetric of() {
        return new WriteMetric(ImmutableList.of(), ImmutableList.of());
    }

    private static WriteMetric of(final Long time) {
        return new WriteMetric(ImmutableList.of(), ImmutableList.of(time));
    }

    public static WriteMetric error(final RequestError error) {
        return new WriteMetric(ImmutableList.of(error), ImmutableList.of());
    }

    public static Transform<Throwable, WriteMetric> shardError(final ClusterShard c) {
        return e -> new WriteMetric(ImmutableList.of(ShardError.fromThrowable(c, e)),
            ImmutableList.of());
    }

    public static Collector<WriteMetric, WriteMetric> reduce() {
        return results -> {
            final ImmutableList.Builder<RequestError> errors = ImmutableList.builder();
            final ImmutableList.Builder<Long> times = ImmutableList.builder();

            for (final WriteMetric r : results) {
                errors.addAll(r.getErrors());
                times.addAll(r.getTimes());
            }

            return new WriteMetric(errors.build(), times.build());
        };
    }

    public static RequestTimer<WriteMetric> timer() {
        return new RequestTimer<>(WriteMetric::of);
    }

    @Data
    public static class Request {
        final Series series;
        final MetricCollection data;
    }
}
