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
import com.spotify.heroic.common.RequestTimer;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardError;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;

import java.util.List;

@Data
public class WriteMetadata {
    private final List<RequestError> errors;
    private final List<Long> times;

    public static WriteMetadata of() {
        return new WriteMetadata(ImmutableList.of(), ImmutableList.of());
    }

    private static WriteMetadata of(final long time) {
        return new WriteMetadata(ImmutableList.of(), ImmutableList.of(time));
    }

    public static Transform<Throwable, WriteMetadata> shardError(final ClusterShard c) {
        return e -> new WriteMetadata(ImmutableList.of(ShardError.fromThrowable(c, e)),
            ImmutableList.of());
    }

    public static Collector<WriteMetadata, WriteMetadata> reduce() {
        return requests -> {
            final ImmutableList.Builder<RequestError> errors = ImmutableList.builder();
            final ImmutableList.Builder<Long> times = ImmutableList.builder();

            for (final WriteMetadata r : requests) {
                errors.addAll(r.getErrors());
                times.addAll(r.getTimes());
            }

            return new WriteMetadata(errors.build(), times.build());
        };
    }

    public static RequestTimer<WriteMetadata> timer() {
        return new RequestTimer<>(WriteMetadata::of);
    }

    @Data
    public static class Request {
        private final Series series;
        private final DateRange range;
    }
}
