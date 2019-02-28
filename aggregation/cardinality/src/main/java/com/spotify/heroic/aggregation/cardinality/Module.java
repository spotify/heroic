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

package com.spotify.heroic.aggregation.cardinality;

import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.aggregation.AggregationArguments;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.AggregationRegistry;
import com.spotify.heroic.aggregation.SamplingAggregationDSL;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.dagger.LoadingComponent;
import com.spotify.heroic.grammar.Expression;

import java.util.Optional;

public class Module implements HeroicModule {
    @Override
    public Runnable setup(LoadingComponent loading) {
        final AggregationRegistry c = loading.aggregationRegistry();
        final AggregationFactory factory = loading.aggregationFactory();

        return () -> {
            c.register(CardinalityAggregation.NAME, CardinalityAggregation.class,
                CardinalityInstance.class,
                new SamplingAggregationDSL<CardinalityAggregation>(factory) {
                    @Override
                    protected CardinalityAggregation buildWith(
                        final AggregationArguments args,
                        final Optional<Duration> size,
                        final Optional<Duration> extent
                    ) {
                        final CardinalityMethod method = args
                            .positionalOrKeyword("method", Expression.class)
                            .map(CardinalityMethod.Companion::fromExpression)
                            .orElse(null);

                        return new CardinalityAggregation(
                            null, size.orElse(null), extent.orElse(null), method);
                    }
                });

            c.registerInstance(DistributedCardinalityInstance.NAME,
                DistributedCardinalityInstance.class);
        };
    }
}
