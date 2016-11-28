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

import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationArguments;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.AggregationRegistry;
import com.spotify.heroic.aggregation.SamplingAggregationDSL;
import com.spotify.heroic.aggregation.SamplingQuery;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.dagger.LoadingComponent;
import com.spotify.heroic.grammar.DoubleExpression;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.grammar.IntegerExpression;
import dagger.Component;
import eu.toolchain.serializer.SerializerFramework;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Optional;

public class Module implements HeroicModule {
    @Override
    public Runnable setup(LoadingComponent loading) {
        return DaggerModule_C.builder().loadingComponent(loading).build().entry();
    }

    @Component(dependencies = LoadingComponent.class)
    interface C {
        E entry();
    }

    static class E implements Runnable {
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
        public void run() {
            /* example aggregation, if used only returns zeroes. */
            c.register(Template.NAME, Template.class, TemplateInstance.class,
                samplingBuilder(Template::new));

            c.register(Spread.NAME, Spread.class, SpreadInstance.class,
                samplingBuilder(Spread::new));

            c.register(Sum.NAME, Sum.class, SumInstance.class, samplingBuilder(Sum::new));

            c.register(Average.NAME, Average.class, AverageInstance.class,
                samplingBuilder(Average::new));

            c.register(Min.NAME, Min.class, MinInstance.class, samplingBuilder(Min::new));

            c.register(Max.NAME, Max.class, MaxInstance.class, samplingBuilder(Max::new));

            c.register(StdDev.NAME, StdDev.class, StdDevInstance.class,
                samplingBuilder(StdDev::new));

            c.register(Count.NAME, Count.class, CountInstance.class, samplingBuilder(Count::new));

            c.register(GroupUnique.NAME, GroupUnique.class, GroupUniqueInstance.class,
                samplingBuilder(GroupUnique::new));

            c.register(Quantile.NAME, Quantile.class, QuantileInstance.class,
                new SamplingAggregationDSL<Quantile>(factory) {
                    @Override
                    protected Quantile buildWith(
                        final AggregationArguments args, final Optional<Duration> size,
                        final Optional<Duration> extent
                    ) {
                        final Optional<Double> q = args
                            .positionalOrKeyword("q", DoubleExpression.class)
                            .map(DoubleExpression::getValue);
                        final Optional<Double> error = args
                            .positionalOrKeyword("error", DoubleExpression.class)
                            .map(DoubleExpression::getValue);
                        return new Quantile(Optional.empty(), size, extent, q, error);
                    }
                });

            c.register(Delta.NAME, Delta.class, DeltaInstance.class, args -> new Delta());

            c.register(TopK.NAME, TopK.class, TopKInstance.class,
                args -> new TopK(fetchK(args, IntegerExpression.class).getValue(),
                    Optional.empty()));

            c.register(BottomK.NAME, BottomK.class, BottomKInstance.class,
                args -> new BottomK(fetchK(args, IntegerExpression.class).getValue(),
                    Optional.empty()));

            c.register(AboveK.NAME, AboveK.class, AboveKInstance.class,
                args -> new AboveK(fetchK(args, DoubleExpression.class).getValue(),
                    Optional.empty()));

            c.register(BelowK.NAME, BelowK.class, BelowKInstance.class,
                args -> new BelowK(fetchK(args, DoubleExpression.class).getValue(),
                    Optional.empty()));

            c.register(PointsAbove.NAME, PointsAbove.class, PointsAboveInstance.class,
                args -> new PointsAbove(fetchK(args, DoubleExpression.class).getValue()));

            c.register(PointsBelow.NAME, PointsBelow.class, PointsBelowInstance.class,
                args -> new PointsBelow(fetchK(args, DoubleExpression.class).getValue()));
        }

        private <T extends Expression> T fetchK(AggregationArguments args, Class<T> doubleClass) {
            return args
                .positional(doubleClass)
                .orElseThrow(() -> new IllegalArgumentException("missing required argument 'k'"));
        }

        private <T extends Aggregation> SamplingAggregationDSL<T> samplingBuilder(
            SamplingBuilder<T> builder
        ) {
            return new SamplingAggregationDSL<T>(factory) {
                @Override
                protected T buildWith(
                    final AggregationArguments args, final Optional<Duration> size,
                    final Optional<Duration> extent
                ) {
                    return builder.apply(Optional.empty(), size, extent);
                }
            };
        }
    }

    interface SamplingBuilder<T> {
        T apply(
            Optional<SamplingQuery> sampling, Optional<Duration> size, Optional<Duration> extent
        );
    }
}
