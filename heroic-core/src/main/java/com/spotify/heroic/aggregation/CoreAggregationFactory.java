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

package com.spotify.heroic.aggregation;

import com.spotify.heroic.grammar.ListValue;
import com.spotify.heroic.grammar.Value;
import lombok.RequiredArgsConstructor;

import java.util.Map;

@RequiredArgsConstructor
public class CoreAggregationFactory implements AggregationFactory {
    final Map<String, AggregationDSL> builderMap;

    @Override
    public Aggregation build(String name, ListValue args, Map<String, Value> keywords) {
        final AggregationDSL builder = builderMap.get(name);

        if (builder == null) {
            throw new MissingAggregation(name);
        }

        final AggregationArguments a = new AggregationArguments(args.getList(), keywords);

        final Aggregation aggregation;

        try {
            aggregation = builder.build(a);
        } catch (final Exception e) {
            throw new RuntimeException(name + ": " + e.getMessage(), e);
        }

        // throw an exception unless all provided arguments have been consumed.
        a.throwUnlessEmpty(name);
        return aggregation;
    }
}
