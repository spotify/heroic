/*
 * Copyright (c) 2020 Spotify AB.
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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.tdunning.math.stats.TDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class TdigestStatInstanceHelper {
    public static Metric computePercentile(final TDigest tDigest,
                                           final long timestamp,
                                           final double[] quantiles) {
        List<Point> values = new ArrayList<>();
        values.add(new Point(timestamp, tDigest.getMin()));
        values.add(new Point(timestamp, tDigest.getMax()));
        Arrays.stream(quantiles).forEachOrdered(q -> values
            .add(new Point(timestamp, tDigest.quantile(q))));

        MetricCollection metricCollection = MetricCollection.points(values);
        return new MetricGroup(timestamp, ImmutableList.of(metricCollection));
    }
}
