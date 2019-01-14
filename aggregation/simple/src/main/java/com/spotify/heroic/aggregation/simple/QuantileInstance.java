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

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.ObjectHasher;
import com.spotify.heroic.aggregation.BucketAggregationInstance;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import java.beans.ConstructorProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class QuantileInstance extends BucketAggregationInstance<QuantileBucket> {
    private final double q;
    private final double error;

    @ConstructorProperties({"size", "extent", "q", "error"})
    public QuantileInstance(
        final long size, final long extent, final double q, double error
    ) {
        super(size, extent, ImmutableSet.of(MetricType.POINT), MetricType.POINT);
        this.q = q;
        this.error = error;
    }

    @Override
    protected QuantileBucket buildBucket(long timestamp) {
        return new QuantileBucket(timestamp, q, error);
    }

    @Override
    protected Metric build(QuantileBucket bucket) {
        final double value = bucket.value();

        if (Double.isNaN(value)) {
            return Metric.invalid();
        }

        return new Point(bucket.timestamp(), value);
    }

    @Override
    protected void bucketHashTo(final ObjectHasher hasher) {
        hasher.putField("q", q, hasher.doubleValue());
        hasher.putField("error", error, hasher.doubleValue());
    }
}
