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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

@Data
public class TagValuesSuggest {
    private final List<RequestError> errors;
    private final List<Suggestion> suggestions;
    private final boolean limited;

    public static TagValuesSuggest of(List<Suggestion> suggestions, boolean limited) {
        return new TagValuesSuggest(ImmutableList.of(), suggestions, limited);
    }

    public static Collector<TagValuesSuggest, TagValuesSuggest> reduce(
        final OptionalLimit limit, final OptionalLimit groupLimit
    ) {
        return groups -> {
            final ImmutableList.Builder<RequestError> errors = ImmutableList.builder();
            final Map<String, MidFlight> midflights = new HashMap<>();
            boolean limited1 = false;

            for (final TagValuesSuggest g : groups) {
                errors.addAll(g.getErrors());

                for (final Suggestion s : g.suggestions) {
                    MidFlight m = midflights.get(s.key);

                    if (m == null) {
                        m = new MidFlight();
                        midflights.put(s.key, m);
                    }

                    m.values.addAll(s.values);
                    m.limited = m.limited || s.limited;
                }

                limited1 = limited1 || g.limited;
            }

            final SortedSet<Suggestion> suggestions1 = new TreeSet<>();

            for (final Map.Entry<String, MidFlight> e : midflights.entrySet()) {
                final String key = e.getKey();
                final MidFlight m = e.getValue();

                final boolean sLimited = m.limited || groupLimit.isGreater(m.values.size());

                suggestions1.add(
                    new Suggestion(key, groupLimit.limitSortedSet(m.values), sLimited));
            }

            return new TagValuesSuggest(errors.build(),
                limit.limitList(ImmutableList.copyOf(suggestions1)),
                limited1 || limit.isGreater(suggestions1.size()));
        };
    }

    @Data
    public static final class Suggestion implements Comparable<Suggestion> {
        private final String key;
        private final SortedSet<String> values;
        private final boolean limited;

        @Override
        public int compareTo(final Suggestion o) {
            final int v = -Integer.compare(values.size(), o.values.size());

            if (v != 0) {
                return v;
            }

            final int k = key.compareTo(o.key);

            if (k != 0) {
                return k;
            }

            final Iterator<String> left = values.iterator();
            final Iterator<String> right = o.values.iterator();

            while (left.hasNext()) {
                final String l = left.next();

                if (!right.hasNext()) {
                    return -1;
                }

                final String r = right.next();

                final int kv = l.compareTo(r);

                if (kv != 0) {
                    return kv;
                }
            }

            if (right.hasNext()) {
                return 1;
            }

            return Boolean.compare(limited, o.limited);
        }
    }

    private static class MidFlight {
        private final SortedSet<String> values = new TreeSet<>();
        private boolean limited;
    }

    public static Transform<Throwable, TagValuesSuggest> shardError(final ClusterShard shard) {
        return e -> new TagValuesSuggest(ImmutableList.of(ShardError.fromThrowable(shard, e)),
            ImmutableList.of(), false);
    }

    @Data
    public static class Request {
        private final Filter filter;
        private final DateRange range;
        private final OptionalLimit limit;
        private final OptionalLimit groupLimit;
        private final List<String> exclude;
    }
}
