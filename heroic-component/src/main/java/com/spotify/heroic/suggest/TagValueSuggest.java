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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.cluster.NodeRegistryEntry;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.metric.NodeError;
import com.spotify.heroic.metric.RequestError;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

@Data
public class TagValueSuggest {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<RequestError>();
    public static final List<String> EMPTY_VALUES = new ArrayList<String>();

    private final List<RequestError> errors;
    private final List<String> values;
    private final boolean limited;

    @JsonCreator
    public TagValueSuggest(
        @JsonProperty("errors") List<RequestError> errors,
        @JsonProperty("values") List<String> values, @JsonProperty("limited") Boolean limited
    ) {
        this.errors = Optional.fromNullable(errors).or(EMPTY_ERRORS);
        this.values = Optional.fromNullable(values).or(EMPTY_VALUES);
        this.limited = Optional.fromNullable(limited).or(false);
    }

    public TagValueSuggest(List<String> values, boolean limited) {
        this(EMPTY_ERRORS, values, limited);
    }

    public static Collector<TagValueSuggest, TagValueSuggest> reduce(final OptionalLimit limit) {
        return groups -> {
            final List<RequestError> errors1 = new ArrayList<>();
            final SortedSet<String> values1 = new TreeSet<>();

            boolean limited1 = false;

            for (final TagValueSuggest g : groups) {
                errors1.addAll(g.errors);
                values1.addAll(g.values);
                limited1 = limited1 || g.limited;
            }

            limited1 = limited1 || limit.isGreaterOrEqual(values1.size());

            return new TagValueSuggest(errors1, limit.limitList(ImmutableList.copyOf(values1)),
                limited1);
        };
    }

    public static Transform<Throwable, ? extends TagValueSuggest> nodeError(
        final NodeRegistryEntry node
    ) {
        return e -> {
            final NodeMetadata m = node.getMetadata();
            final ClusterNode c = node.getClusterNode();
            return new TagValueSuggest(ImmutableList.<RequestError>of(
                NodeError.fromThrowable(m.getId(), c.toString(), m.getTags(), e)), EMPTY_VALUES,
                false);
        };
    }

    public static Transform<Throwable, ? extends TagValueSuggest> nodeError(
        final ClusterNode.Group group
    ) {
        return e -> {
            final List<RequestError> errors1 =
                ImmutableList.<RequestError>of(NodeError.fromThrowable(group.node(), e));
            return new TagValueSuggest(errors1, EMPTY_VALUES, false);
        };
    }
}
