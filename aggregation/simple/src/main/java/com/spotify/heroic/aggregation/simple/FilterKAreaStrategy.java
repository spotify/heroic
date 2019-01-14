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

import com.spotify.heroic.ObjectHasher;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.Point;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;

/**
 * This filter strategy calculates the area under the graphs of the time series and
 * selects the time series with either the biggest (TopK) or smallest (BottomK) area.
 * <p>
 * Time series without any data points are disregarded and never part of the result.
 */
@Data
public class FilterKAreaStrategy implements FilterStrategy {
    private final FilterKAreaType filterType;
    private final long k;

    @Override
    public <T> List<T> filter(List<FilterableMetrics<T>> metrics) {
        return metrics
            .stream()
            .filter(m -> m.getMetricSupplier().get().size() > 0)
            .map(Area::new)
            .sorted((a, b) -> filterType.compare(a.getValue(), b.getValue()))
            .limit(k)
            .map(Area::getFilterableMetrics)
            .map(FilterableMetrics::getData)
            .collect(Collectors.toList());
    }

    @Override
    public void hashTo(final ObjectHasher hasher) {
        hasher.putObject(getClass(), () -> {
            hasher.putField("filterType", filterType, hasher.enumValue());
            hasher.putField("k", k, hasher.longValue());
        });
    }

    @Data
    private class Area<T> {
        private final FilterableMetrics<T> filterableMetrics;
        private final double value;

        public Area(FilterableMetrics<T> filterableMetrics) {
            this.filterableMetrics = filterableMetrics;
            this.value = computeArea(filterableMetrics.getMetricSupplier().get());
        }

        private double computeArea(MetricCollection metricCollection) {
            final List<Point> metrics = metricCollection.getDataAs(Point.class);

            double area = 0;
            for (int i = 1; i < metrics.size(); i++) {
                area += PointPairArea.computeArea(metrics.get(i - 1), metrics.get(i));
            }

            return area;
        }
    }
}
