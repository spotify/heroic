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

package com.spotify.heroic.suggest.model;

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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.metric.model.NodeError;
import com.spotify.heroic.metric.model.RequestError;

import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;

@Data
public class TagKeyCount {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();
    public static final List<Suggestion> EMPTY_SUGGESTIONS = new ArrayList<>();
    public static final TagKeyCount EMPTY = new TagKeyCount(EMPTY_ERRORS, EMPTY_SUGGESTIONS);

    // sort suggestions descending by score.
    public static final Comparator<Suggestion> COMPARATOR = new Comparator<Suggestion>() {
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

    private final List<RequestError> errors;
    private final List<Suggestion> suggestions;

    @JsonCreator
    public TagKeyCount(@JsonProperty("errors") List<RequestError> errors,
            @JsonProperty("suggestions") List<Suggestion> suggestions) {
        this.errors = Optional.fromNullable(errors).or(EMPTY_ERRORS);
        this.suggestions = suggestions;
    }

    public TagKeyCount(List<Suggestion> suggestions) {
        this(EMPTY_ERRORS, suggestions);
    }

    public TagKeyCount merge(TagKeyCount other, int limit) {
        final List<RequestError> errors = new ArrayList<>(this.errors);
        errors.addAll(other.errors);

        return new TagKeyCount(errors, mergeSuggestions(other, limit));
    }

    private List<Suggestion> mergeSuggestions(TagKeyCount other, int limit) {
        final HashMap<String, Suggestion> suggestions = new HashMap<>(this.suggestions.size()
                + other.suggestions.size());

        for (final Suggestion s : this.suggestions) {
            suggestions.put(s.key, s);
        }

        for (final Suggestion s : other.suggestions) {
            final Suggestion replaced = suggestions.put(s.key, s);

            if (replaced == null)
                continue;

            suggestions.put(s.key, new Suggestion(s.key, s.count + replaced.count));
        }

        final List<Suggestion> results = new ArrayList<>(suggestions.values());

        Collections.sort(results, COMPARATOR);

        return results.subList(0, Math.min(suggestions.size(), limit));
    }

    public static Collector<TagKeyCount, TagKeyCount> reduce(final int limit) {
        return new Collector<TagKeyCount, TagKeyCount>() {
            @Override
            public TagKeyCount collect(Collection<TagKeyCount> results) throws Exception {
                TagKeyCount result = EMPTY;

                for (final TagKeyCount r : results)
                    result = r.merge(result, limit);

                return result;
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
    }

    public static Transform<Throwable, ? extends TagKeyCount> nodeError(final NodeRegistryEntry node) {
        return new Transform<Throwable, TagKeyCount>() {
            @Override
            public TagKeyCount transform(Throwable e) throws Exception {
                final NodeMetadata m = node.getMetadata();
                final ClusterNode c = node.getClusterNode();
                return new TagKeyCount(ImmutableList.<RequestError> of(NodeError.fromThrowable(m.getId(), c.toString(),
                        m.getTags(), e)), EMPTY_SUGGESTIONS);
            }
        };
    }
}