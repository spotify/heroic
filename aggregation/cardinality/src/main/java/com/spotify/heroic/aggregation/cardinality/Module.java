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
import dagger.Component;
import eu.toolchain.serializer.SerializerFramework;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Optional;

public class Module implements HeroicModule {
    @Override
    public Entry setup(LoadingComponent loading) {
        return DaggerModule_C.builder().loadingComponent(loading).build().entry();
    }

    @Component(dependencies = LoadingComponent.class)
    interface C {
        E entry();
    }

    static class E implements Entry {
        private final AggregationRegistry c;
        private final SerializerFramework s;
        private final AggregationFactory factory;

        @Inject
        public E(
            AggregationRegistry c, @Named("common") SerializerFramework s,
            AggregationFactory factory
        ) {
            this.c = c;
            this.s = s;
            this.factory = factory;
        }

        @Override
        public void setup() {
            c.register(CardinalityAggregation.NAME, CardinalityAggregation.class,
                CardinalityInstance.class,
                new SamplingAggregationDSL<CardinalityAggregation>(factory) {
                    @Override
                    protected CardinalityAggregation buildWith(
                        final AggregationArguments args, final Optional<Duration> size,
                        final Optional<Duration> extent
                    ) {
                        final Optional<CardinalityMethod> method = args
                            .getNext("method", Expression.class)
                            .map(CardinalityMethod::fromExpression);

                        return new CardinalityAggregation(Optional.empty(), size, extent, method);
                    }
                });
        }
    }
}
