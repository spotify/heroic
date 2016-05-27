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

package com.spotify.heroic.suggest;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterShardGroup;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardError;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

@Data
public class TagValueSuggest {
    private final List<RequestError> errors;
    private final List<String> values;
    private final boolean limited;

    public static TagValueSuggest of(final List<String> values, final boolean limited) {
        return new TagValueSuggest(ImmutableList.of(), values, limited);
    }

    public static Collector<TagValueSuggest, TagValueSuggest> reduce(final OptionalLimit limit) {
        return groups -> {
            final List<RequestError> errors1 = new ArrayList<>();
            final SortedSet<String> values1 = new TreeSet<>();

            boolean limited1 = false;

            for (final TagValueSuggest g : groups) {
                errors1.addAll(g.errors);
                values1.addAll(g.values);
                limited1 = limited1 || g.limited;
            }

            limited1 = limited1 || limit.isGreater(values1.size());

            return new TagValueSuggest(errors1, limit.limitList(ImmutableList.copyOf(values1)),
                limited1);
        };
    }

    public static Transform<Throwable, TagValueSuggest> shardError(
        final ClusterShardGroup shard
    ) {
        return e -> new TagValueSuggest(ImmutableList.of(ShardError.fromThrowable(shard, e)),
            ImmutableList.of(), false);
    }
}
