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

package com.spotify.heroic.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.cluster.NodeRegistryEntry;
import com.spotify.heroic.metric.NodeError;
import com.spotify.heroic.metric.RequestError;

import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;

@Data
public class FindTags {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();
    public static final Map<String, Set<String>> EMPTY_TAGS = new HashMap<>();

    public static final FindTags EMPTY = new FindTags(EMPTY_TAGS, 0);

    private final List<RequestError> errors;
    private final Map<String, Set<String>> tags;
    private final int size;

    /**
     * Handle that tags is a deeply nested structure and copy it up until the closest immutable type.
     */
    private static void updateTags(final Map<String, Set<String>> data, final Map<String, Set<String>> add) {
        for (final Map.Entry<String, Set<String>> entry : add.entrySet()) {
            Set<String> entries = data.get(entry.getKey());

            if (entries == null) {
                entries = new HashSet<String>();
                data.put(entry.getKey(), entries);
            }

            entries.addAll(entry.getValue());
        }
    }

    public static class SelfReducer implements Collector<FindTags, FindTags> {
        @Override
        public FindTags collect(Collection<FindTags> results) throws Exception {
            final List<RequestError> errors = new ArrayList<>();
            final HashMap<String, Set<String>> tags = new HashMap<String, Set<String>>();
            int size = 0;

            for (final FindTags r : results) {
                errors.addAll(r.errors);
                updateTags(tags, r.tags);
                size += r.getSize();
            }

            return new FindTags(errors, tags, size);
        }
    }

    private static final SelfReducer reducer = new SelfReducer();

    public static Collector<FindTags, FindTags> reduce() {
        return reducer;
    }

    @JsonCreator
    public FindTags(@JsonProperty("errors") List<RequestError> errors,
            @JsonProperty("tags") Map<String, Set<String>> tags, @JsonProperty("size") int size) {
        this.errors = Optional.fromNullable(errors).or(EMPTY_ERRORS);
        this.tags = tags;
        this.size = size;
    }

    public FindTags(Map<String, Set<String>> tags, int size) {
        this(EMPTY_ERRORS, tags, size);
    }

    public static Transform<Throwable, ? extends FindTags> nodeError(final NodeRegistryEntry node) {
        return new Transform<Throwable, FindTags>() {
            @Override
            public FindTags transform(Throwable e) throws Exception {
                final NodeMetadata m = node.getMetadata();
                final ClusterNode c = node.getClusterNode();
                return new FindTags(ImmutableList.<RequestError> of(NodeError.fromThrowable(m.getId(), c.toString(),
                        m.getTags(), e)), EMPTY_TAGS, 0);
            }
        };
    }

    public static Transform<Throwable, ? extends FindTags> nodeError(final ClusterNode.Group group) {
        return new Transform<Throwable, FindTags>() {
            @Override
            public FindTags transform(Throwable e) throws Exception {
                final List<RequestError> errors = ImmutableList.<RequestError> of(NodeError.fromThrowable(group.node(),
                        e));
                return new FindTags(errors, EMPTY_TAGS, 0);
            }
        };
    }
}
