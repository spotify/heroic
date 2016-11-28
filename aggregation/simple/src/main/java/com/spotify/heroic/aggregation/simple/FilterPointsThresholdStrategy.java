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
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import lombok.Data;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Data
public class FilterPointsThresholdStrategy implements MetricMappingStrategy {
    private final FilterKThresholdType filterType;
    private final double threshold;

    @Override
    public MetricCollection apply(MetricCollection metrics) {
        if (metrics.getType() == MetricType.POINT) {
            return MetricCollection.build(
                MetricType.POINT,
                filterWithThreshold(metrics.getDataAs(Point.class))
            );
        } else {
            return metrics;
        }
    }

    private List<Point> filterWithThreshold(List<Point> points) {
        return points.stream()
            .filter(point -> filterType.predicate(point.getValue(), threshold))
            .collect(toList());
    }
}
