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
import java.util.HashSet;
import java.util.List;
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
public class FindKeys {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();
    public static final Set<String> EMPTY_KEYS = new HashSet<String>();

    public static final FindKeys EMPTY = new FindKeys(EMPTY_ERRORS, EMPTY_KEYS, 0, 0);

    private final List<RequestError> errors;
    private final Set<String> keys;
    private final int size;
    private final int duplicates;

    public static class SelfReducer implements Collector<FindKeys, FindKeys> {
        @Override
        public FindKeys collect(Collection<FindKeys> results) throws Exception {
            final List<RequestError> errors = new ArrayList<>();
            final Set<String> keys = new HashSet<>();
            int size = 0;
            int duplicates = 0;

            for (final FindKeys result : results) {
                errors.addAll(result.errors);

                for (final String k : result.keys) {
                    if (keys.add(k)) {
                        duplicates += 1;
                    }
                }

                duplicates += result.getDuplicates();
                size += result.getSize();
            }

            return new FindKeys(errors, keys, size, duplicates);
        }
    }

    private static final SelfReducer reducer = new SelfReducer();

    public static Collector<FindKeys, FindKeys> reduce() {
        return reducer;
    }

    @JsonCreator
    public FindKeys(@JsonProperty("errors") List<RequestError> errors, @JsonProperty("keys") Set<String> keys,
            @JsonProperty("size") int size, @JsonProperty("duplicates") int duplicates) {
        this.errors = Optional.fromNullable(errors).or(EMPTY_ERRORS);
        this.keys = keys;
        this.size = size;
        this.duplicates = duplicates;
    }

    public FindKeys(Set<String> keys, int size, int duplicates) {
        this(EMPTY_ERRORS, keys, size, duplicates);
    }

    public static Transform<Throwable, ? extends FindKeys> nodeError(final NodeRegistryEntry node) {
        return new Transform<Throwable, FindKeys>() {
            @Override
            public FindKeys transform(Throwable e) throws Exception {
                final NodeMetadata m = node.getMetadata();
                final ClusterNode c = node.getClusterNode();
                return new FindKeys(ImmutableList.<RequestError> of(NodeError.fromThrowable(m.getId(), c.toString(),
                        m.getTags(), e)), EMPTY_KEYS, 0, 0);
            }
        };
    }

    public static Transform<Throwable, ? extends FindKeys> nodeError(final ClusterNode.Group group) {
        return new Transform<Throwable, FindKeys>() {
            @Override
            public FindKeys transform(Throwable e) throws Exception {
                final List<RequestError> errors = ImmutableList.<RequestError> of(NodeError.fromThrowable(group.node(),
                        e));
                return new FindKeys(errors, EMPTY_KEYS, 0, 0);
            }
        };
    }
}
