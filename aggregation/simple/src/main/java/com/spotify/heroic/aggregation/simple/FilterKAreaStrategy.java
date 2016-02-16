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
import java.util.stream.Collectors;

public class FilterKAreaStrategy implements FilterStrategy {

    private final FilterKAreaType filterType;
    private final long k;

    public FilterKAreaStrategy(FilterKAreaType filterType, long k) {
        this.filterType = filterType;
        this.k = k;
    }

    @Override
    public <T> List<T> filter(List<FilterableMetrics<T>> metrics) {
        return metrics
            .stream()
            .map(Area<T>::new)
            .sorted((a, b) -> filterType.compare(a.getValue(), b.getValue()))
            .limit(k)
            .map(Area::getFilterableMetrics)
            .map(FilterableMetrics::getData)
            .collect(Collectors.toList());
    }

    public Long getK() {
        return k;
    }

    @Data
    private class Area<T> {
        private final FilterableMetrics<T> filterableMetrics;
        private final double value;

        public Area(FilterableMetrics<T> filterableMetrics) {
            this.filterableMetrics = filterableMetrics;
            this.value = computeArea(filterableMetrics.getMetricSupplier().get());
        }

        private Double computeArea(MetricCollection metrics) {
            return metrics
                .getDataAs(Point.class)
                .stream()
                .map(Point::getValue)
                .reduce(0D, (a, b) -> a + b);
        }
    }
}
