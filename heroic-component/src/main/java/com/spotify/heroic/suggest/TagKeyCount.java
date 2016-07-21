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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Data
public class TagKeyCount {
    private final List<RequestError> errors;
    private final List<Suggestion> suggestions;
    private final boolean limited;

    public static TagKeyCount of(final List<Suggestion> suggestions, final boolean limited) {
        return new TagKeyCount(ImmutableList.of(), suggestions, limited);
    }

    public static Collector<TagKeyCount, TagKeyCount> reduce(
        final OptionalLimit limit, final OptionalLimit exactLimit
    ) {
        return groups -> {
            final List<RequestError> errors1 = new ArrayList<>();
            final HashMap<String, Suggestion> suggestions = new HashMap<>();
            boolean limited = false;

            for (final TagKeyCount g : groups) {
                errors1.addAll(g.errors);

                for (final Suggestion s : g.suggestions) {
                    final Suggestion replaced = suggestions.put(s.key, s);

                    if (replaced == null) {
                        continue;
                    }

                    final Optional<Set<String>> exactValues;

                    if (s.exactValues.isPresent() && replaced.exactValues.isPresent()) {
                        exactValues = Optional.<Set<String>>of(ImmutableSet.copyOf(
                            Iterables.concat(s.exactValues.get(),
                                replaced.exactValues.get()))).filter(
                            set -> !exactLimit.isGreater(set.size()));
                    } else {
                        exactValues = Optional.empty();
                    }

                    suggestions.put(s.key,
                        new Suggestion(s.key, s.count + replaced.count, exactValues));
                }

                limited = limited || g.limited;
            }

            final List<Suggestion> list = new ArrayList<>(suggestions.values());
            Collections.sort(list);

            return new TagKeyCount(errors1, limit.limitList(list),
                limited || limit.isGreater(list.size()));
        };
    }

    @Data
    public static final class Suggestion implements Comparable<Suggestion> {
        private final String key;
        private final long count;
        private final Optional<Set<String>> exactValues;

        @Override
        public int compareTo(final Suggestion o) {
            final int k = key.compareTo(o.key);

            if (k != 0) {
                return k;
            }

            return Long.compare(count(), o.count());
        }

        public long count() {
            return exactValues.map(s -> (long) s.size()).orElse(count);
        }
    }

    public static Transform<Throwable, TagKeyCount> shardError(final ClusterShard shard) {
        return e -> new TagKeyCount(ImmutableList.of(ShardError.fromThrowable(shard, e)),
            ImmutableList.of(), false);
    }

    @Data
    public static class Request {
        private final Filter filter;
        private final DateRange range;
        private final OptionalLimit limit;
        private final OptionalLimit exactLimit;
    }
}
