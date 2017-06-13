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

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A container for a set of features that provides convenience methods for accessing them.
 */
@Data
public class Features {
    /**
     * Default set of features.
     */
    public static final Features DEFAULT = Features.create(ImmutableSet.<Feature>builder()
        .add(Feature.SHIFT_RANGE)
        .add(Feature.END_BUCKET)
        .build());

    private final Set<Feature> features;

    public boolean hasFeature(final Feature feature) {
        return features.contains(feature);
    }

    /**
     * Apply the given feature set.
     *
     * @param featureSet Feature set to apply.
     * @return A new Feature with the given set applied.
     */
    public Features applySet(final FeatureSet featureSet) {
        final Set<Feature> features = new HashSet<>(this.features);
        features.addAll(featureSet.getEnabled());
        features.removeAll(featureSet.getDisabled());
        return new Features(features);
    }

    @JsonCreator
    public static Features create(final Set<Feature> features) {
        return new Features(features);
    }

    @JsonValue
    public Set<Feature> value() {
        return features;
    }

    /**
     * Run the given operation if feature is set, or another operation if it is not.
     *
     * @param feature Feature to check for.
     * @param isSet Operation to run if feature is set.
     * @param isNotSet Operation to run if feature is not set.
     * @param <T> type to return from operation.
     * @return The returned value from the matching operation.
     */
    public <T> T withFeature(
        final Feature feature, final Supplier<T> isSet, final Supplier<T> isNotSet
    ) {
        return hasFeature(feature) ? isSet.get() : isNotSet.get();
    }

    /**
     * Create an empty set of enabled features.
     *
     * @return A new feature set.
     */
    public static Features empty() {
        return new Features(ImmutableSet.of());
    }

    public static Features of(final Feature... features) {
        return new Features(ImmutableSet.copyOf(features));
    }
}
