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
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardError;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class TagKeyCount {
    private final List<RequestError> errors;
    private final List<Suggestion> suggestions;
    private final boolean limited;

    public static TagKeyCount of(final List<Suggestion> suggestions, final boolean limited) {
        return new TagKeyCount(ImmutableList.of(), suggestions, limited);
    }

    public static Collector<TagKeyCount, TagKeyCount> reduce(final OptionalLimit limit) {
        return groups -> {
            final List<RequestError> errors1 = new ArrayList<>();
            final HashMap<String, Suggestion> suggestions1 = new HashMap<>();
            boolean limited1 = false;

            for (final TagKeyCount g : groups) {
                errors1.addAll(g.errors);

                for (final Suggestion s : g.suggestions) {
                    final Suggestion replaced = suggestions1.put(s.key, s);

                    if (replaced == null) {
                        continue;
                    }

                    suggestions1.put(s.key, new Suggestion(s.key, s.count + replaced.count));
                }

                limited1 = limited1 || g.limited;
            }

            final List<Suggestion> list = new ArrayList<>(suggestions1.values());
            Collections.sort(list, Suggestion.COMPARATOR);

            return new TagKeyCount(errors1, limit.limitList(list),
                limited1 || limit.isGreater(list.size()));
        };
    }

    @Data
    @EqualsAndHashCode(of = {"key"})
    public static final class Suggestion {
        private final String key;
        private final long count;

        @JsonCreator
        public Suggestion(@JsonProperty("key") String key, @JsonProperty("count") Long count) {
            this.key = checkNotNull(key, "value must not be null");
            this.count = checkNotNull(count, "count must not be null");
        }

        // sort suggestions descending by score.
        private static final Comparator<Suggestion> COMPARATOR = new Comparator<Suggestion>() {
            @Override
            public int compare(Suggestion a, Suggestion b) {
                final int s = Long.compare(b.count, a.count);

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

    public static Transform<Throwable, TagKeyCount> shardError(final ClusterShardGroup shard) {
        return e -> new TagKeyCount(ImmutableList.of(ShardError.fromThrowable(shard, e)),
            ImmutableList.of(), false);
    }

    @Data
    public static class Request {
        private final Filter filter;
        private final DateRange range;
        private final OptionalLimit limit;
    }
}
