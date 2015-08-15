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
import java.util.Map;

import lombok.Data;

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
public class KeySuggest {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();
    public static final List<Suggestion> EMPTY_SUGGESTIONS = new ArrayList<>();

    private final List<RequestError> errors;
    private final List<Suggestion> suggestions;

    @JsonCreator
    public KeySuggest(@JsonProperty("errors") List<RequestError> errors,
            @JsonProperty("suggestions") List<Suggestion> suggestions) {
        this.errors = checkNotNull(errors, "errors");
        this.suggestions = checkNotNull(suggestions, "suggestions");
    }

    public KeySuggest(List<Suggestion> suggestions) {
        this(EMPTY_ERRORS, suggestions);
    }

    public static Collector<KeySuggest, KeySuggest> reduce(final int limit) {
        return new Collector<KeySuggest, KeySuggest>() {
            @Override
            public KeySuggest collect(Collection<KeySuggest> results) throws Exception {
                final List<RequestError> errors = new ArrayList<>();
                final Map<String, Suggestion> suggestions = new HashMap<>();

                for (final KeySuggest r : results) {
                    errors.addAll(r.errors);

                    for (final Suggestion s : r.suggestions) {
                        Suggestion alt = suggestions.get(s.key);

                        if (alt == null) {
                            suggestions.put(s.key, s);
                            continue;
                        }

                        if (alt.score < s.score)
                            suggestions.put(s.key, s);
                    }
                }

                final List<Suggestion> list = new ArrayList<>(suggestions.values());
                Collections.sort(list, Suggestion.COMPARATOR);

                return new KeySuggest(errors, list.subList(0, Math.min(list.size(), limit)));
            }
        };
    }

    public static Transform<Throwable, ? extends KeySuggest> nodeError(final NodeRegistryEntry node) {
        return new Transform<Throwable, KeySuggest>() {
            @Override
            public KeySuggest transform(Throwable e) throws Exception {
                final NodeMetadata m = node.getMetadata();
                final ClusterNode c = node.getClusterNode();
                return new KeySuggest(ImmutableList.<RequestError> of(NodeError.fromThrowable(m.getId(), c.toString(),
                        m.getTags(), e)), EMPTY_SUGGESTIONS);
            }
        };
    }

    @Data
    public static final class Suggestion {
        private final float score;
        private final String key;

        @JsonCreator
        public Suggestion(@JsonProperty("score") Float score, @JsonProperty("key") String key) {
            this.score = checkNotNull(score, "score must not be null");
            this.key = checkNotNull(key, "value must not be null");
        }

        private static Comparator<Suggestion> COMPARATOR = new Comparator<Suggestion>() {
            @Override
            public int compare(Suggestion a, Suggestion b) {
                final int score = Float.compare(b.score, a.score);

                if (score != 0)
                    return score;

                return compareKey(a, b);
            }

            private int compareKey(Suggestion a, Suggestion b) {
                if (a.key == null && b.key == null)
                    return 0;

                if (a.key == null)
                    return 1;

                if (b.key == null)
                    return -1;

                return a.key.compareTo(b.key);
            }
        };
    }

    public static Transform<Throwable, ? extends KeySuggest> nodeError(final ClusterNode.Group group) {
        return new Transform<Throwable, KeySuggest>() {
            @Override
            public KeySuggest transform(Throwable e) throws Exception {
                final List<RequestError> errors = ImmutableList.<RequestError> of(NodeError.fromThrowable(group.node(),
                        e));
                return new KeySuggest(errors, EMPTY_SUGGESTIONS);
            }
        };
    }
}
