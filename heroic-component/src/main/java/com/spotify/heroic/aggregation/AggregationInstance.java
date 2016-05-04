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

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.DateRange;

import java.util.List;
import java.util.Set;

/**
 * An instance of an aggregation.
 * <p>
 * Contains the materialized configuration of a given aggregation. All members must be fully
 * thread-safe.
 *
 * @author udoprog
 * @see Aggregation
 */
public interface AggregationInstance {
    public static final String SAMPLE_SIZE = "Aggregation.sampleSize";

    /**
     * Estimate number of points this aggregation will produce.
     *
     * @param range Range to perform aggregation over.
     * @return Number of datapoints required for aggregation, or {@code -1} if not known.
     */
    long estimate(DateRange range);

    /**
     * Get the cadence of the current aggregation.
     * <p>
     * The cadence indicates the interval in milliseconds that samples can be expected.
     *
     * @return The cadence of the resulting aggregation, or {@code 0} if this is unknown.
     */
    long cadence();

    /**
     * Traverse the possible aggregations and build the necessary graph out of them.
     */
    AggregationTraversal session(List<AggregationState> states, DateRange range);

    /**
     * Get the distributed aggregation that is relevant for this aggregation.
     * <p>
     * A distributed aggregation instance is suitable for performing a sub-aggregation in order to
     * allow non-destructive recombination of the results.
     * <p>
     * Consider distributing an average aggregation, if done naively this would cause a large loss
     * in precision because the following does not hold true {@code avg(A, B) != avg(avg(A),
     * avg(B))}
     * <p>
     * Instead the average aggregation can designate another type of aggregation as its
     * intermediate, which preserves the information from each sub-aggregation in a non-destructive
     * form. This can be accomplished by using an aggregation that outputs the sum and the count of
     * all seen values.
     *
     * @return The distributed aggregation for the current aggregation.
     */
    default AggregationInstance distributed() {
        return this;
    }

    /**
     * Build a reducer for the given aggregation.
     * <p>
     * A reducer is responsible for taking a set of distributed sub-aggregations, and
     * non-destructively combine them into a complete result.
     *
     * @return A reducer for the current aggregation.
     */
    ReducerSession reducer(final DateRange range);

    /**
     * Build a combiner for the given aggregation.
     * <p>
     * A combiner organizes how distributed sub-aggregations are re-combined. Specific aggregations
     * have different methods of recombining results that are more or less efficient.
     *
     * @return An aggregation combiner.
     */
    default AggregationCombiner combiner(final DateRange range) {
        return AggregationCombiner.DEFAULT;
    }

    /**
     * Get a set of required tags.
     * <p>
     * This is used to elide a set of required tags that needs to be forwarded for a certain
     * aggregation.
     *
     * @return Return the set of required tags for this aggregation.
     */
    default Set<String> requiredTags() {
        return ImmutableSet.of();
    }
}
