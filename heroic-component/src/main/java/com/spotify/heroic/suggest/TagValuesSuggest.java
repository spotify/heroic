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
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.cluster.NodeRegistryEntry;
import com.spotify.heroic.metric.NodeError;
import com.spotify.heroic.metric.RequestError;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class TagValuesSuggest {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();
    public static final List<Suggestion> EMPTY_SUGGESTIONS = new ArrayList<>();

    private final List<RequestError> errors;
    private final List<Suggestion> suggestions;
    private final boolean limited;

    @JsonCreator
    public TagValuesSuggest(
        @JsonProperty("errors") List<RequestError> errors,
        @JsonProperty("suggestions") List<Suggestion> suggestions,
        @JsonProperty("limited") Boolean limited
    ) {
        this.errors = checkNotNull(errors, "errors");
        this.suggestions = checkNotNull(suggestions, "suggestions");
        this.limited = checkNotNull(limited, "limited");
    }

    public TagValuesSuggest(List<Suggestion> suggestions, boolean limited) {
        this(EMPTY_ERRORS, suggestions, limited);
    }

    public static Collector<TagValuesSuggest, TagValuesSuggest> reduce(
        final int limit, final int groupLimit
    ) {
        return new Collector<TagValuesSuggest, TagValuesSuggest>() {
            @Override
            public TagValuesSuggest collect(Collection<TagValuesSuggest> groups) throws Exception {
                final Map<String, MidFlight> midflights = new HashMap<>();
                boolean limited = false;

                for (final TagValuesSuggest g : groups) {
                    for (final Suggestion s : g.suggestions) {
                        MidFlight m = midflights.get(s.key);

                        if (m == null) {
                            m = new MidFlight();
                            midflights.put(s.key, m);
                        }

                        m.values.addAll(s.values);
                        m.limited = m.limited || s.limited;
                    }

                    limited = limited || g.limited;
                }

                final ArrayList<Suggestion> suggestions = new ArrayList<>(midflights.size());

                for (final Map.Entry<String, MidFlight> e : midflights.entrySet()) {
                    final String key = e.getKey();
                    final MidFlight m = e.getValue();

                    final ImmutableList<String> values = ImmutableList.copyOf(m.values);
                    final boolean sLimited = m.limited || values.size() >= groupLimit;

                    suggestions.add(
                        new Suggestion(key, values.subList(0, Math.min(values.size(), groupLimit)),
                            sLimited));
                }

                limited = limited || suggestions.size() >= limit;
                return new TagValuesSuggest(
                    suggestions.subList(0, Math.min(suggestions.size(), limit)), limited);
            }
        };
    }

    public static Transform<Throwable, ? extends TagValuesSuggest> nodeError(
        final NodeRegistryEntry node
    ) {
        return new Transform<Throwable, TagValuesSuggest>() {
            @Override
            public TagValuesSuggest transform(Throwable e) throws Exception {
                final NodeMetadata m = node.getMetadata();
                final ClusterNode c = node.getClusterNode();
                return new TagValuesSuggest(ImmutableList.<RequestError>of(
                    NodeError.fromThrowable(m.getId(), c.toString(), m.getTags(), e)),
                    EMPTY_SUGGESTIONS, false);
            }
        };
    }

    @Data
    public static final class Suggestion {
        private final String key;
        private final List<String> values;
        private final boolean limited;

        @JsonCreator
        public Suggestion(
            @JsonProperty("key") String key, @JsonProperty("values") List<String> values,
            @JsonProperty("limited") Boolean limited
        ) {
            this.key = checkNotNull(key, "key");
            this.values = ImmutableList.copyOf(checkNotNull(values, "values"));
            this.limited = checkNotNull(limited, "limited");
        }

        // sort suggestions descending by score.
        public static final Comparator<Suggestion> COMPARATOR = new Comparator<Suggestion>() {
            @Override
            public int compare(Suggestion a, Suggestion b) {
                final int v = Integer.compare(b.values.size(), a.values.size());

                if (v != 0) {
                    return v;
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

    private static class MidFlight {
        private final TreeSet<String> values = new TreeSet<>();
        private boolean limited;
    }

    public static Transform<Throwable, ? extends TagValuesSuggest> nodeError(
        final ClusterNode.Group group
    ) {
        return new Transform<Throwable, TagValuesSuggest>() {
            @Override
            public TagValuesSuggest transform(Throwable e) throws Exception {
                final List<RequestError> errors =
                    ImmutableList.<RequestError>of(NodeError.fromThrowable(group.node(), e));
                return new TagValuesSuggest(errors, EMPTY_SUGGESTIONS, false);
            }
        };
    }
}
