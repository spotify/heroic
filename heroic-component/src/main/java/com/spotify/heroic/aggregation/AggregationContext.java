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

package com.spotify.heroic.aggregation;

import static com.spotify.heroic.common.Optionals.firstPresent;

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.Duration;
import java.util.Optional;
import java.util.Set;

public class AggregationContext {
    private final Optional<Duration> size;
    private final Optional<Duration> extent;
    private final Duration defaultSize;
    private final Duration defaultExtent;
    private final Set<String> requiredTags;

    @java.beans.ConstructorProperties({ "size", "extent", "defaultSize", "defaultExtent",
                                        "requiredTags" })
    public AggregationContext(final Optional<Duration> size, final Optional<Duration> extent,
                              final Duration defaultSize, final Duration defaultExtent,
                              final Set<String> requiredTags) {
        this.size = size;
        this.extent = extent;
        this.defaultSize = defaultSize;
        this.defaultExtent = defaultExtent;
        this.requiredTags = requiredTags;
    }

    /**
     * Get the size that is currently configured in the context.
     *
     * @return The currently configured size.
     */
    public Optional<Duration> size() {
        return size;
    }

    /**
     * Get extent that is currently configured in the context.
     *
     * @return The currently configured extent.
     */
    public Optional<Duration> extent() {
        return extent;
    }

    public Duration defaultSize() {
        return defaultSize;
    }

    public Duration defaultExtent() {
        return defaultExtent;
    }

    public Set<String> requiredTags() {
        return requiredTags;
    }

    public AggregationContext withRequiredTags(final Set<String> requiredTags) {
        return new AggregationContext(size, extent, defaultSize, defaultExtent, requiredTags);
    }

    public AggregationContext withOptions(
        final Optional<Duration> size, final Optional<Duration> extent
    ) {
        return new AggregationContext(firstPresent(size, this.size),
            firstPresent(extent, this.extent), defaultSize, defaultExtent, requiredTags);
    }

    public static AggregationContext defaultInstance(final Duration duration) {
        return new AggregationContext(Optional.empty(), Optional.empty(), duration, duration,
            ImmutableSet.of());
    }
}
