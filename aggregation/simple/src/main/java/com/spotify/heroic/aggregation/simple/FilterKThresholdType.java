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

package com.spotify.heroic.aggregation.simple;

import java.util.Comparator;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public enum FilterKThresholdType {
    // @formatter:off
    ABOVE((v, k) -> (v > k), ((Comparator<Double>) Double::compare).reversed()),
    BELOW((v, k) -> (v < k), Double::compare);
    // @formatter:off

    private final Comparator<Double> comparator;
    private final BiFunction<Double, Double, Boolean> predicate;

    FilterKThresholdType(BiFunction<Double, Double, Boolean> predicate,
                         Comparator<Double> comparator) {
        this.predicate = predicate;
        this.comparator = comparator;
    }

    public boolean predicate(double v, double k) {
        return predicate.apply(v, k);
    }

    public Optional<Double> findExtreme(Stream<Double> metrics) {
        return metrics.min(comparator);
    }
}

