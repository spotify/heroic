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
import java.util.concurrent.TimeUnit;

import com.spotify.heroic.aggregation.AggregationBuilder;
import com.spotify.heroic.grammar.DiffValue;
import com.spotify.heroic.grammar.Value;
import com.spotify.heroic.model.Sampling;

public abstract class SamplingAggregationBuilder<T> implements AggregationBuilder<T> {
    private static final long DEFAULT_SIZE = TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES);

    @Override
    public T build(List<Value> args, Map<String, Value> keywords) {
        final long size;
        final long extent;

        if (args.size() > 0) {
            size = args.get(0).cast(DiffValue.class).toMilliseconds();
        } else {
            if (keywords.containsKey("sampling")) {
                size = keywords.get("sampling").cast(DiffValue.class).toMilliseconds();
            } else {
                size = DEFAULT_SIZE;
            }
        }

        if (args.size() > 1) {
            extent = args.get(1).cast(DiffValue.class).toMilliseconds();
        } else {
            if (keywords.containsKey("extent")) {
                extent = keywords.get("extent").cast(DiffValue.class).toMilliseconds();
            } else {
                extent = size;
            }
        }

        return buildWith(new Sampling(size, extent), keywords);
    }

    protected abstract T buildWith(Sampling sampling, Map<String, Value> keywords);
}
