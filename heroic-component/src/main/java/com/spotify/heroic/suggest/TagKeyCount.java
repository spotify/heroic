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
public class TagKeyCount {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();
    public static final List<Suggestion> EMPTY_SUGGESTIONS = new ArrayList<>();

    private final List<RequestError> errors;
    private final List<Suggestion> suggestions;
    private final boolean limited;

    @JsonCreator
    public TagKeyCount(@JsonProperty("errors") List<RequestError> errors,
            @JsonProperty("suggestions") List<Suggestion> suggestions, @JsonProperty("limited") boolean limited) {
        this.errors = checkNotNull(errors, "errors");
        this.suggestions = checkNotNull(suggestions, "suggestions");
        this.limited = checkNotNull(limited, "limited");
    }

    public TagKeyCount(List<Suggestion> suggestions, boolean limited) {
        this(EMPTY_ERRORS, suggestions, limited);
    }

    public static Collector<TagKeyCount, TagKeyCount> reduce(final int limit) {
        return new Collector<TagKeyCount, TagKeyCount>() {
            @Override
            public TagKeyCount collect(Collection<TagKeyCount> groups) throws Exception {
                final List<RequestError> errors = new ArrayList<>();
                final HashMap<String, Suggestion> suggestions = new HashMap<>();
                boolean limited = false;

                for (final TagKeyCount g : groups) {
                    errors.addAll(g.errors);

                    for (final Suggestion s : g.suggestions) {
                        final Suggestion replaced = suggestions.put(s.key, s);

                        if (replaced == null)
                            continue;

                        suggestions.put(s.key, new Suggestion(s.key, s.count + replaced.count));
                    }

                    limited = limited || g.limited;
                }

                final List<Suggestion> list = new ArrayList<>(suggestions.values());
                Collections.sort(list, Suggestion.COMPARATOR);

                limited = limited || list.size() >= limit;
                return new TagKeyCount(errors, list.subList(0, Math.min(list.size(), limit)), limited);
            }
        };
    }

    public static Transform<Throwable, ? extends TagKeyCount> nodeError(final NodeRegistryEntry node) {
        return new Transform<Throwable, TagKeyCount>() {
            @Override
            public TagKeyCount transform(Throwable e) throws Exception {
                final NodeMetadata m = node.getMetadata();
                final ClusterNode c = node.getClusterNode();
                return new TagKeyCount(ImmutableList.<RequestError> of(NodeError.fromThrowable(m.getId(), c.toString(),
                        m.getTags(), e)), EMPTY_SUGGESTIONS, false);
            }
        };
    }

    @Data
    @EqualsAndHashCode(of = { "key" })
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

                if (s != 0)
                    return s;

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

    public static Transform<Throwable, ? extends TagKeyCount> nodeError(final ClusterNode.Group group) {
        return new Transform<Throwable, TagKeyCount>() {
            @Override
            public TagKeyCount transform(Throwable e) throws Exception {
                final List<RequestError> errors = ImmutableList.<RequestError> of(NodeError.fromThrowable(group.node(),
                        e));
                return new TagKeyCount(errors, EMPTY_SUGGESTIONS, false);
            }
        };
    }
}
