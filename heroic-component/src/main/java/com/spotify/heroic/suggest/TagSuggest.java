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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterShardGroup;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardError;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

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
            final HashMap<Pair<String, String>, Suggestion> suggestions1 = new HashMap<>();

            for (final TagSuggest g : groups) {
                errors1.addAll(g.errors);

                for (final Suggestion s : g.suggestions) {
                    final Pair<String, String> key = Pair.of(s.key, s.value);

                    final Suggestion replaced = suggestions1.put(key, s);

                    if (replaced == null) {
                        continue;
                    }

                    // prefer higher score if available.
                    if (s.score < replaced.score) {
                        suggestions1.put(key, replaced);
                    }
                }
            }

            final List<Suggestion> results = new ArrayList<>(suggestions1.values());
            Collections.sort(results, Suggestion.COMPARATOR);

            return new TagSuggest(errors1, limit.limitList(results));
        };
    }

    @Data
    @EqualsAndHashCode(of = {"key", "value"})
    public static final class Suggestion {
        private final float score;
        private final String key;
        private final String value;

        @JsonCreator
        public Suggestion(
            @JsonProperty("score") Float score, @JsonProperty("key") String key,
            @JsonProperty("value") String value
        ) {
            this.score = checkNotNull(score, "score");
            this.key = checkNotNull(key, "key");
            this.value = value;
        }

        // sort suggestions descending by score.
        private static final Comparator<Suggestion> COMPARATOR = new Comparator<Suggestion>() {
            @Override
            public int compare(Suggestion a, Suggestion b) {
                final int s = Float.compare(b.score, a.score);

                if (s != 0) {
                    return s;
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

    public static Transform<Throwable, TagSuggest> shardError(
        final ClusterShardGroup shard
    ) {
        return e -> new TagSuggest(ImmutableList.of(ShardError.fromThrowable(shard, e)),
            ImmutableList.of());
    }
}
