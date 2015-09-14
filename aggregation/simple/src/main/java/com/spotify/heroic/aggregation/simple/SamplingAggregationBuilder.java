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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.spotify.heroic.aggregation.AbstractAggregationBuilder;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.grammar.Value;

public abstract class SamplingAggregationBuilder<T> extends AbstractAggregationBuilder<T> {
    public SamplingAggregationBuilder(AggregationFactory factory) {
        super(factory);
    }

    @Override
    public T build(AggregationContext context, List<Value> args, Map<String, Value> keywords) {
        final LinkedList<Value> a = new LinkedList<>(args);
        final long size = parseDiffMillis(a, keywords, "size").or(context.size()).or(context::defaultSize);
        final long extent = parseDiffMillis(a, keywords, "extent").or(context.extent()).or(context::defaultExtent);
        return buildWith(args, keywords, size, extent);
    }

    protected abstract T buildWith(List<Value> args, Map<String, Value> keywords, final long size, final long extent);
}
