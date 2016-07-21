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
import com.google.common.collect.ImmutableSortedSet;
import com.spotify.heroic.cluster.ClusterShard;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardError;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Data
public class TagSuggest {
    private final List<RequestError> errors;
    private final List<Suggestion> suggestions;

    public static TagSuggest of() {
        return new TagSuggest(ImmutableList.of(), ImmutableList.of());
    }

    public static TagSuggest of(List<Suggestion> suggestions) {
        return new TagSuggest(ImmutableList.of(), suggestions);
    }

    public static Collector<TagSuggest, TagSuggest> reduce(final OptionalLimit limit) {
        return groups -> {
            final List<RequestError> errors1 = new ArrayList<>();
            final Map<Pair<String, String>, Float> suggestions1 = new HashMap<>();

            for (final TagSuggest g : groups) {
                errors1.addAll(g.errors);

                for (final Suggestion s : g.suggestions) {
                    final Pair<String, String> key = Pair.of(s.getKey(), s.getValue());
                    final Float old = suggestions1.put(key, s.getScore());

                    // prefer higher score if available.
                    if (old != null && s.score < old) {
                        suggestions1.put(key, old);
                    }
                }
            }

            final List<Suggestion> values = ImmutableList.copyOf(ImmutableSortedSet.copyOf(
                suggestions1
                    .entrySet()
                    .stream()
                    .map(e -> new Suggestion(e.getValue(), e.getKey().getLeft(),
                        e.getKey().getRight()))
                    .iterator()));

            return new TagSuggest(errors1, limit.limitList(values));
        };
    }

    @Data
    public static final class Suggestion implements Comparable<Suggestion> {
        private final float score;
        private final String key;
        private final String value;

        @Override
        public int compareTo(final Suggestion o) {
            final int s = -Float.compare(score, o.score);

            if (s != 0) {
                return s;
            }

            int k = key.compareTo(o.key);

            if (k != 0) {
                return k;
            }

            return value.compareTo(o.value);
        }
    }

    public static Transform<Throwable, TagSuggest> shardError(final ClusterShard shard) {
        return e -> new TagSuggest(ImmutableList.of(ShardError.fromThrowable(shard, e)),
            ImmutableList.of());
    }

    @Data
    public static class Request {
        private final Filter filter;
        private final DateRange range;
        private final OptionalLimit limit;
        private final MatchOptions options;
        private final Optional<String> key;
        private final Optional<String> value;
    }
}
