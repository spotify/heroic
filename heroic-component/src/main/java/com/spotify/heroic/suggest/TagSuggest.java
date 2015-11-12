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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;

import org.apache.commons.lang3.tuple.Pair;

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

@Data
public class TagSuggest {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();
    public static final List<Suggestion> EMPTY_SUGGESTIONS = new ArrayList<>();

    private final List<RequestError> errors;
    private final List<Suggestion> suggestions;

    @JsonCreator
    public TagSuggest(@JsonProperty("errors") List<RequestError> errors,
            @JsonProperty("suggestions") List<Suggestion> suggestions) {
        this.errors = checkNotNull(errors, "errors");
        this.suggestions = checkNotNull(suggestions, "suggestions");
    }

    public TagSuggest(List<Suggestion> suggestions) {
        this(EMPTY_ERRORS, suggestions);
    }

    public static Collector<TagSuggest, TagSuggest> reduce(final int limit) {
        return new Collector<TagSuggest, TagSuggest>() {
            @Override
            public TagSuggest collect(Collection<TagSuggest> groups) throws Exception {
                final List<RequestError> errors = new ArrayList<>();
                final HashMap<Pair<String, String>, Suggestion> suggestions = new HashMap<>();

                for (final TagSuggest g : groups) {
                    errors.addAll(g.errors);

                    for (final Suggestion s : g.suggestions) {
                        final Pair<String, String> key = Pair.of(s.key, s.value);

                        final Suggestion replaced = suggestions.put(key, s);

                        if (replaced == null) {
                            continue;
                        }

                        // prefer higher score if available.
                        if (s.score < replaced.score) {
                            suggestions.put(key, replaced);
                        }
                    }
                }

                final List<Suggestion> results = new ArrayList<>(suggestions.values());
                Collections.sort(results, Suggestion.COMPARATOR);

                return new TagSuggest(errors, results.subList(0, Math.min(results.size(), limit)));
            }
        };
    }

    public static Transform<Throwable, ? extends TagSuggest> nodeError(
            final NodeRegistryEntry node) {
        return new Transform<Throwable, TagSuggest>() {
            @Override
            public TagSuggest transform(Throwable e) throws Exception {
                final NodeMetadata m = node.getMetadata();
                final ClusterNode c = node.getClusterNode();
                return new TagSuggest(
                        ImmutableList.<RequestError> of(
                                NodeError.fromThrowable(m.getId(), c.toString(), m.getTags(), e)),
                        EMPTY_SUGGESTIONS);
            }
        };
    }

    @Data
    @EqualsAndHashCode(of = { "key", "value" })
    public static final class Suggestion {
        private final float score;
        private final String key;
        private final String value;

        @JsonCreator
        public Suggestion(@JsonProperty("score") Float score, @JsonProperty("key") String key,
                @JsonProperty("value") String value) {
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

    public static Transform<Throwable, ? extends TagSuggest> nodeError(
            final ClusterNode.Group group) {
        return new Transform<Throwable, TagSuggest>() {
            @Override
            public TagSuggest transform(Throwable e) throws Exception {
                final List<RequestError> errors =
                        ImmutableList.<RequestError> of(NodeError.fromThrowable(group.node(), e));
                return new TagSuggest(errors, EMPTY_SUGGESTIONS);
            }
        };
    }

    static final TagSuggest empty = new TagSuggest(EMPTY_SUGGESTIONS);

    public static TagSuggest empty() {
        return empty;
    }
}
