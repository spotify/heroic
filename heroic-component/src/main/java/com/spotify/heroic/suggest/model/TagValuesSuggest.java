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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import lombok.Data;
import lombok.RequiredArgsConstructor;

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
public class TagValuesSuggest {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();
    public static final List<Suggestion> EMPTY_SUGGESTIONS = new ArrayList<>();
    public static final boolean DEFAULT_LIMITED = false;

    public static final TagValuesSuggest EMPTY = new TagValuesSuggest(EMPTY_ERRORS, EMPTY_SUGGESTIONS, DEFAULT_LIMITED);

    // sort suggestions descending by score.
    public static final Comparator<Suggestion> COMPARATOR = new Comparator<Suggestion>() {
        @Override
        public int compare(Suggestion a, Suggestion b) {
            final int v = Integer.compare(b.values.size(), a.values.size());

            if (v != 0)
                return v;

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
    private final boolean limited;

    @JsonCreator
    public TagValuesSuggest(@JsonProperty("errors") List<RequestError> errors,
            @JsonProperty("suggestions") List<Suggestion> suggestions, @JsonProperty("limited") Boolean limited) {
        this.errors = Optional.fromNullable(errors).or(EMPTY_ERRORS);
        this.suggestions = suggestions;
        this.limited = limited;
    }

    public TagValuesSuggest(List<Suggestion> suggestions, boolean limited) {
        this(EMPTY_ERRORS, suggestions, limited);
    }

    public TagValuesSuggest merge(TagValuesSuggest other, int limit, int groupLimit) {
        final List<RequestError> errors = new ArrayList<>(this.errors);
        errors.addAll(other.errors);

        final MergeResults merge = mergeSuggestions(other, limit, groupLimit);
        return new TagValuesSuggest(errors, merge.getSuggestions(), merge.limited || this.limited || other.limited);
    }

    private MergeResults mergeSuggestions(TagValuesSuggest other, int limit, int groupLimit) {
        final Map<String, MidFlight> result = new HashMap<>(this.suggestions.size() + other.suggestions.size());

        for (final Suggestion s : this.suggestions) {
            result.put(s.key, new MidFlight(new TreeSet<>(s.values), s.limited));
        }

        for (final Suggestion s : other.suggestions) {
            final MidFlight midFlight = result.get(s.key);

            if (midFlight == null) {
                result.put(s.key, new MidFlight(new TreeSet<>(s.values), s.limited));
            } else {
                result.put(s.key, midFlight.merge(s));
            }
        }

        final ArrayList<Suggestion> suggestions = new ArrayList<>(result.size());

        for (final Map.Entry<String, MidFlight> e : result.entrySet()) {
            final MidFlight midFlight = e.getValue();
            final ImmutableList<String> mergedValues = ImmutableList.copyOf(midFlight.values);
            final ImmutableList<String> values = mergedValues.subList(0, Math.min(midFlight.values.size(), groupLimit));
            final boolean limited = midFlight.limited || mergedValues.size() > groupLimit;

            suggestions.add(new Suggestion(e.getKey(), values, limited));
        }

        return new MergeResults(suggestions.subList(0, Math.min(suggestions.size(), limit)), suggestions.size() > limit);
    }

    @Data
    private static class MergeResults {
        private final List<Suggestion> suggestions;
        private final boolean limited;
    }

    @RequiredArgsConstructor
    private static class MidFlight {
        private final TreeSet<String> values;
        private final boolean limited;

        public MidFlight merge(Suggestion s) {
            final TreeSet<String> values = new TreeSet<>(this.values);
            values.addAll(s.values);
            return new MidFlight(values, limited || s.limited);
        }
    }

    public static Collector<TagValuesSuggest, TagValuesSuggest> reduce(final int limit, final int groupLimit) {
        return new Collector<TagValuesSuggest, TagValuesSuggest>() {
            @Override
            public TagValuesSuggest collect(Collection<TagValuesSuggest> results) throws Exception {
                TagValuesSuggest result = EMPTY;

                for (final TagValuesSuggest r : results)
                    result = r.merge(result, limit, groupLimit);

                return result;
            }
        };
    }

    @Data
    public static final class Suggestion {
        private final String key;
        private final List<String> values;
        private final boolean limited;

        @JsonCreator
        public Suggestion(@JsonProperty("key") String key, @JsonProperty("values") List<String> values,
                @JsonProperty("limited") Boolean limited) {
            this.key = checkNotNull(key, "value must not be null");
            this.values = ImmutableList.copyOf(checkNotNull(values, "values must not be null"));
            this.limited = checkNotNull(limited, "limited must not be null");
        }
    }

    public static Transform<Throwable, ? extends TagValuesSuggest> nodeError(final NodeRegistryEntry node) {
        return new Transform<Throwable, TagValuesSuggest>() {
            @Override
            public TagValuesSuggest transform(Throwable e) throws Exception {
                final NodeMetadata m = node.getMetadata();
                final ClusterNode c = node.getClusterNode();
                return new TagValuesSuggest(ImmutableList.<RequestError> of(NodeError.fromThrowable(m.getId(),
                        c.toString(), m.getTags(), e)), EMPTY_SUGGESTIONS, DEFAULT_LIMITED);
            }
        };
    }
}