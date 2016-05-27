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
import com.spotify.heroic.cluster.ClusterShardGroup;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Data
public class WriteResult {
    public static final WriteResult EMPTY = new WriteResult(ImmutableList.of(), ImmutableList.of());

    private final List<RequestError> errors;
    private final List<Long> times;

    public static WriteResult of(final List<Long> times) {
        return new WriteResult(ImmutableList.of(), times);
    }

    public static WriteResult of(final Collection<Long> times) {
        return new WriteResult(ImmutableList.of(), ImmutableList.copyOf(times));
    }

    public static WriteResult of(long duration) {
        return of(ImmutableList.of(duration));
    }

    public static Collector<WriteResult, WriteResult> merger() {
        return results -> {
            if (results.isEmpty()) {
                return WriteResult.EMPTY;
            }

            if (results.size() == 1) {
                return results.iterator().next();
            }

            final List<RequestError> errors = new ArrayList<>();
            final List<Long> times = new ArrayList<>();

            for (final WriteResult r : results) {
                errors.addAll(r.errors);
                times.addAll(r.times);
            }

            return new WriteResult(errors, times);
        };
    }

    public static Transform<Throwable, WriteResult> shardError(final ClusterShardGroup shard) {
        return e -> new WriteResult(ImmutableList.of(ShardError.fromThrowable(shard, e)),
            ImmutableList.of());
    }
}
