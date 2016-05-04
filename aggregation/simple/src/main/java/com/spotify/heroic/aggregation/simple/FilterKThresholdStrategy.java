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

import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.Point;
import lombok.Data;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FilterKThresholdStrategy implements FilterStrategy {
    private final FilterKThresholdType filterType;
    private final double k;

    public FilterKThresholdStrategy(FilterKThresholdType filterType, double k) {
        this.filterType = filterType;
        this.k = k;
    }

    @Override
    public <T> List<T> filter(List<FilterableMetrics<T>> metrics) {
        return metrics
            .stream()
            .map(m -> new Extreme<>(m, filterType))
            .filter(m -> m.getValue().map(v -> filterType.predicate(v, k)).orElse(false))
            .map(Extreme::getFilterableMetrics)
            .map(FilterableMetrics::getData)
            .collect(Collectors.toList());
    }

    public double getK() {
        return k;
    }

    @Data
    private static class Extreme<T> {
        private final FilterableMetrics<T> filterableMetrics;
        private final Optional<Double> value;

        public Extreme(FilterableMetrics<T> filterableMetrics, FilterKThresholdType filterType) {
            this.filterableMetrics = filterableMetrics;
            this.value = findExtreme(filterType, filterableMetrics.getMetricSupplier().get());
        }

        private Optional<Double> findExtreme(
            FilterKThresholdType filterType, MetricCollection metrics
        ) {
            final Stream<Double> stream =
                metrics.getDataAs(Point.class).stream().map(Point::getValue);

            return filterType.findExtreme(stream);
        }
    }
}
