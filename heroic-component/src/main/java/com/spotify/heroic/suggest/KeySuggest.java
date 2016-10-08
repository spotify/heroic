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
import com.spotify.heroic.cluster.ClusterShard;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardError;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Data
public class KeySuggest {
    private final List<RequestError> errors;
    private final List<Suggestion> suggestions;

    public static KeySuggest of(final List<Suggestion> suggestions) {
        return new KeySuggest(ImmutableList.of(), suggestions);
    }

    public static Collector<KeySuggest, KeySuggest> reduce(final OptionalLimit limit) {
        return results -> {
            final List<RequestError> errors1 = new ArrayList<>();
            final Map<String, Suggestion> suggestions1 = new HashMap<>();

            for (final KeySuggest r : results) {
                errors1.addAll(r.errors);

                for (final Suggestion s : r.suggestions) {
                    Suggestion alt = suggestions1.get(s.key);

                    if (alt == null) {
                        suggestions1.put(s.key, s);
                        continue;
                    }

                    if (alt.score < s.score) {
                        suggestions1.put(s.key, s);
                    }
                }
            }

            final List<Suggestion> list = new ArrayList<>(suggestions1.values());
            Collections.sort(list, Suggestion.COMPARATOR);

            return new KeySuggest(errors1, limit.limitList(list));
        };
    }

    public static Transform<Throwable, KeySuggest> shardError(final ClusterShard shard) {
        return e -> new KeySuggest(ImmutableList.of(ShardError.fromThrowable(shard, e)),
            ImmutableList.of());
    }

    @Data
    public static final class Suggestion {
        @NonNull
        private final float score;
        @NonNull
        private final String key;

        private static final Comparator<Suggestion> COMPARATOR = new Comparator<Suggestion>() {
            @Override
            public int compare(Suggestion a, Suggestion b) {
                final int score = Float.compare(b.score, a.score);

                if (score != 0) {
                    return score;
                }

                return compareKey(a, b);
            }

            private int compareKey(Suggestion a, Suggestion b) {
                if (a.key == null && b.key == null) {
                    return 0;
                }

                if (a.key == null) {
                    return 1;
                }

                if (b.key == null) {
                    return -1;
                }

                return a.key.compareTo(b.key);
            }
        };
    }

    @Data
    public static class Request {
        private final Filter filter;
        private final DateRange range;
        private final OptionalLimit limit;
        private final MatchOptions options;
        private final Optional<String> key;
    }
}
