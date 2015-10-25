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

import com.google.common.base.Optional;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationQuery;
import com.spotify.heroic.aggregation.SamplingQuery;

public abstract class SamplingAggregationQuery implements AggregationQuery {
    private final Optional<Long> size;
    private final Optional<Long> extent;

    public SamplingAggregationQuery(final SamplingQuery sampling) {
        size = sampling.getSize();
        extent = sampling.getExtent();
    }

    @Override
    public Aggregation build(AggregationContext context) {
        final long s = size.or(context.size()).or(context::defaultSize);
        final long e = extent.or(context.extent()).or(context::defaultExtent);
        return build(context, s, e);
    }

    protected abstract Aggregation build(AggregationContext context, long size, long extent);
}