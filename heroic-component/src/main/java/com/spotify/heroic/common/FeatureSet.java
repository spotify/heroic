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

package com.spotify.heroic.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableSet;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A set of enabled and disabled features.
 */
@Data
public class FeatureSet {
    private final Set<Feature> enabled;
    private final Set<Feature> disabled;

    @JsonCreator
    public static FeatureSet create(final Set<String> features) {
        final ImmutableSet.Builder<Feature> enabled = ImmutableSet.builder();
        final ImmutableSet.Builder<Feature> disabled = ImmutableSet.builder();

        for (final String feature : features) {
            if (feature.startsWith("-")) {
                disabled.add(Feature.create(feature.substring(1)));
            } else {
                enabled.add(Feature.create(feature));
            }
        }

        return new FeatureSet(enabled.build(), disabled.build());
    }

    @JsonValue
    public List<String> value() {
        final List<String> features = new ArrayList<>();

        for (final Feature feature : enabled) {
            features.add(feature.id());
        }

        for (final Feature feature : disabled) {
            features.add("-" + feature.id());
        }

        return features;
    }

    /**
     * Combine this feature set with another.
     *
     * @param other Other set to combine with.
     * @return a new feature set.
     */
    public FeatureSet combine(final FeatureSet other) {
        final Set<Feature> enabled = new HashSet<>(this.enabled);
        enabled.addAll(other.enabled);

        final Set<Feature> disabled = new HashSet<>(this.disabled);
        disabled.addAll(other.disabled);

        return new FeatureSet(enabled, disabled);
    }

    /**
     * Create an empty feature set.
     *
     * @return a new feature set.
     */
    public static FeatureSet empty() {
        return new FeatureSet(ImmutableSet.of(), ImmutableSet.of());
    }

    /**
     * Create a new feature set with the given features enabled.
     *
     * @param features Features to enable in the new set.
     * @return a new feature set.
     */
    public static FeatureSet of(final Feature... features) {
        return new FeatureSet(ImmutableSet.copyOf(features), ImmutableSet.of());
    }
}
