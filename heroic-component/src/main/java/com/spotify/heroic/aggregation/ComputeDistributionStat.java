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

package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.TdigestPoint;
import com.tdunning.math.stats.TDigest;

public class ComputeDistributionStat {

    public static Point computePercentile(final TdigestPoint point,
                                          final Percentile percentile) {
        final long timestamp = point.getTimestamp();
        final TDigest datasketch = point.value();
        return (datasketch == null || datasketch.size() == 0L) ? new Point(timestamp, Double.NaN) :
            new Point(timestamp, datasketch.quantile(percentile.quantile));
    }

    public static Point computePercentile(final TDigest datasketch,
                                          final long timestamp,
                                          final Percentile percentile) {
        return (datasketch == null || datasketch.size() == 0L) ? new Point(timestamp, Double.NaN) :
            new Point(timestamp, datasketch.quantile(percentile.quantile));
    }

    public static class Percentile {
        public double getQuantile() {
            return quantile;
        }

        public String getName() {
            return name;
        }

        private final double quantile;
        private final String name;

        public Percentile(final String name, final double quantile) {
            this.name = name;
            this.quantile = quantile;
        }

        static final ImmutableList<Percentile> DEFAULT = ImmutableList.of(
            new Percentile("P99", 0.99),
            new Percentile("P50", 0.50),
            new Percentile("P75", 0.75));
    }
}
