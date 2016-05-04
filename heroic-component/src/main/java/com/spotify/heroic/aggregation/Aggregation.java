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
import java.util.function.Function;

/**
 * Describes an aggregation.
 * <p>
 * Aggregations are responsible for down-sampling time-series data into more compact representations
 * that are easier to reason about.
 * <p>
 * All members must be fully thread-safe. This class describes and contains the configuration for a
 * given aggregation, in order to perform one you would use the {@link #apply(AggregationContext)}
 * method which will return an instance of the current aggregation.
 *
 * @author udoprog
 * @see AggregationInstance
 */
public interface Aggregation extends Function<AggregationContext, AggregationInstance> {
    /**
     * Get the size of the aggregation.
     * <p>
     * The size is the space of time between subsequent samples.
     *
     * @return The size if available, otherwise an empty result.
     */
    Optional<Long> size();

    /**
     * Get the extent of the aggregation.
     * <p>
     * The extent is the space of time in milliseconds that an aggregation takes samples.
     *
     * @return The extent if available, otherwise an empty result.
     */
    Optional<Long> extent();

    /**
     * Convert to Heroic DSL.
     *
     * @return A DSL version of the current aggregation.
     */
    String toDSL();
}
