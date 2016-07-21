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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
public class FindKeys {
    private final List<RequestError> errors;
    private final Set<String> keys;
    private final int size;
    private final int duplicates;

    public static FindKeys of() {
        return new FindKeys(ImmutableList.of(), ImmutableSet.of(), 0, 0);
    }

    public static FindKeys of(final Set<String> keys, int size, int duplicates) {
        return new FindKeys(ImmutableList.of(), keys, size, duplicates);
    }

    public static Collector<FindKeys, FindKeys> reduce() {
        return results -> {
            final List<RequestError> errors = new ArrayList<>();
            final Set<String> keys = new HashSet<>();
            int size = 0;
            int duplicates = 0;

            for (final FindKeys result : results) {
                errors.addAll(result.errors);

                for (final String k : result.keys) {
                    if (keys.add(k)) {
                        duplicates += 1;
                    }
                }

                duplicates += result.getDuplicates();
                size += result.getSize();
            }

            return new FindKeys(errors, keys, size, duplicates);
        };
    }

    public static Transform<Throwable, FindKeys> shardError(final ClusterShard shard) {
        return e -> new FindKeys(ImmutableList.of(ShardError.fromThrowable(shard, e)),
            ImmutableSet.of(), 0, 0);
    }

    @Data
    public static final class Request {
        private final Filter filter;
        private final DateRange range;
        private final OptionalLimit limit;
    }
}
