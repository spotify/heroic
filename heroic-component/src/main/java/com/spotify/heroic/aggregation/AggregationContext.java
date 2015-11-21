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

import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.Duration;

public interface AggregationContext {
    /**
     * Get the size that is currently configured in the context.
     *
     * @return The currently configured size.
     */
    Optional<Duration> size();

    /**
     * Get extent that is currently configured in the context.
     *
     * @return The currently configured extent.
     */
    Optional<Duration> extent();

    Duration defaultSize();

    Duration defaultExtent();

    default Set<String> requiredTags() {
        return ImmutableSet.of();
    }

    static AggregationContext withRequiredTags(final AggregationContext context,
            final Set<String> tags) {
        return new AggregationContext() {
            @Override
            public Optional<Duration> size() {
                return context.size();
            }

            @Override
            public Optional<Duration> extent() {
                return context.extent();
            }

            @Override
            public Duration defaultSize() {
                return context.defaultSize();
            }

            @Override
            public Duration defaultExtent() {
                return context.defaultExtent();
            }

            @Override
            public Set<String> requiredTags() {
                return tags;
            }
        };
    }
}
