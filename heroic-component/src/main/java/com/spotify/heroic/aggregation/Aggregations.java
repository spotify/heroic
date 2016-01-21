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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public abstract class Aggregations {
    /**
     * Creates an aggregation chain.
     *
     * An empty chain is the same as an instance of {@link EmptyInstance}. A chain with a single
     * entry will return that single item. More than one entry will construct a new instance of
     * {@link AggregationChain}.
     *
     * @param input The input chain.
     * @return A new aggregation for the given chain.
     */
    public static Aggregation chain(final Optional<? extends Iterable<Aggregation>> input) {
        return input.flatMap(Aggregations::chain).orElse(Empty.INSTANCE);
    }

    /**
     * Same as {@link #chain(Optional)}, but takes an iterable.
     *
     * @param input Iterable to build chain out of.
     * @return An empty, or an aggregation.
     */
    public static Optional<Aggregation> chain(Iterable<Aggregation> input) {
        final Iterator<Aggregation> it = input.iterator();

        if (!it.hasNext()) {
            return Optional.empty();
        }

        final Aggregation first = it.next();

        if (!it.hasNext()) {
            return Optional.of(first);
        }

        final List<Aggregation> chain = new ArrayList<>();
        chain.add(first);

        while (it.hasNext()) {
            chain.add(it.next());
        }

        return Optional.of(new Chain(chain));
    }

    /**
     * Same as {@link #chain(Iterable)}, but with arrays.
     *
     * @param aggregations The input chain.
     * @return A new aggregation for the given chain.
     */
    public static AggregationInstance chain(AggregationInstance... aggregations) {
        if (aggregations.length == 0) {
            return EmptyInstance.INSTANCE;
        }

        if (aggregations.length == 1) {
            return aggregations[0];
        }

        return new ChainInstance(Arrays.asList(aggregations));
    }

    public static AggregationInstance empty() {
        return EmptyInstance.INSTANCE;
    }
}
