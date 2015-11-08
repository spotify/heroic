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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.spotify.heroic.aggregation.AbstractAggregationDSL;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.SamplingQuery;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.grammar.Value;

public abstract class SamplingAggregationDSL<T> extends AbstractAggregationDSL {
    public SamplingAggregationDSL(AggregationFactory factory) {
        super(factory);
    }

    @Override
    public Aggregation build(final List<Value> args, final Map<String, Value> keywords) {
        final Optional<Duration> size = parseDuration(keywords, "size");
        final Optional<Duration> extent = parseDuration(keywords, "extent");
        final SamplingQuery sampling = new SamplingQuery(size, extent);
        return buildWith(args, keywords, sampling);
    }

    protected abstract Aggregation buildWith(List<Value> args, Map<String, Value> keywords, final SamplingQuery sampling);
}
