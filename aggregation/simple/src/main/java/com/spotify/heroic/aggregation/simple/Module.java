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

import java.io.IOException;
import java.util.Optional;
import java.util.function.BiFunction;

import javax.inject.Inject;
import javax.inject.Named;

import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationArguments;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.BucketAggregationInstance;
import com.spotify.heroic.aggregation.SamplingQuery;
import com.spotify.heroic.common.Duration;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;

public class Module implements HeroicModule {
    @Override
    public Entry setup() {
        return new Entry() {
            @Inject
            private HeroicContext ctx;

            @Inject
            @Named("common")
            private SerializerFramework s;

            @Inject
            private AggregationFactory factory;

            @Override
            public void setup() {
                /* example aggregation, if used only returns zeroes. */
                ctx.aggregation(Template.NAME, TemplateInstance.class, Template.class,
                        samplingSerializer(TemplateInstance::new), samplingBuilder(Template::new));

                ctx.aggregation(Spread.NAME, SpreadInstance.class, Spread.class,
                        samplingSerializer(SpreadInstance::new), samplingBuilder(Spread::new));

                ctx.aggregation(Sum.NAME, SumInstance.class, Sum.class,
                        samplingSerializer(SumInstance::new), samplingBuilder(Sum::new));

                ctx.aggregation(Average.NAME, AverageInstance.class, Average.class,
                        samplingSerializer(AverageInstance::new), samplingBuilder(Average::new));

                ctx.aggregation(Min.NAME, MinInstance.class, Min.class,
                        samplingSerializer(MinInstance::new), samplingBuilder(Min::new));

                ctx.aggregation(Max.NAME, MaxInstance.class, Max.class,
                        samplingSerializer(MaxInstance::new), samplingBuilder(Max::new));

                ctx.aggregation(StdDev.NAME, StdDevInstance.class, StdDev.class,
                        samplingSerializer(StdDevInstance::new), samplingBuilder(StdDev::new));

                ctx.aggregation(CountUnique.NAME, CountUniqueInstance.class, CountUnique.class,
                        samplingSerializer(CountUniqueInstance::new),
                        samplingBuilder(CountUnique::new));

                ctx.aggregation(Count.NAME, CountInstance.class, Count.class,
                        samplingSerializer(CountInstance::new), samplingBuilder(Count::new));

                ctx.aggregation(GroupUnique.NAME, GroupUniqueInstance.class, GroupUnique.class,
                        samplingSerializer(GroupUniqueInstance::new),
                        samplingBuilder(GroupUnique::new));

                ctx.aggregation(Quantile.NAME, QuantileInstance.class, Quantile.class,
                        new Serializer<QuantileInstance>() {
                    final Serializer<Double> fixedDouble = s.fixedDouble();
                    final Serializer<Long> fixedLong = s.fixedLong();

                    @Override
                    public void serialize(SerialWriter buffer, QuantileInstance value)
                            throws IOException {
                        fixedLong.serialize(buffer, value.getSize());
                        fixedLong.serialize(buffer, value.getExtent());
                        fixedDouble.serialize(buffer, value.getQ());
                        fixedDouble.serialize(buffer, value.getError());
                    }

                    @Override
                    public QuantileInstance deserialize(SerialReader buffer) throws IOException {
                        final long size = fixedLong.deserialize(buffer);
                        final long extent = fixedLong.deserialize(buffer);
                        final double q = fixedDouble.deserialize(buffer);
                        final double error = fixedDouble.deserialize(buffer);
                        return new QuantileInstance(size, extent, q, error);
                    }
                }, new SamplingAggregationDSL<Quantile>(factory) {
                    @Override
                    protected Quantile buildWith(final AggregationArguments args,
                            final Optional<Duration> size, final Optional<Duration> extent) {
                        final Optional<Double> q =
                                args.getNext("q", Long.class).map(v -> ((double) v) / 100.0);
                        final Optional<Double> error =
                                args.getNext("error", Long.class).map(v -> ((double) v) / 100.0);
                        return new Quantile(Optional.empty(), size, extent, q, error);
                    }
                });
            }

            private <T extends BucketAggregationInstance<?>> Serializer<T> samplingSerializer(
                    BiFunction<Long, Long, T> builder) {
                final Serializer<Long> fixedLong = s.fixedLong();

                return new Serializer<T>() {
                    @Override
                    public void serialize(SerialWriter buffer, T value) throws IOException {
                        fixedLong.serialize(buffer, value.getSize());
                        fixedLong.serialize(buffer, value.getExtent());
                    }

                    @Override
                    public T deserialize(SerialReader buffer) throws IOException {
                        final long size = fixedLong.deserialize(buffer);
                        final long extent = fixedLong.deserialize(buffer);
                        return builder.apply(size, extent);
                    }
                };
            }

            private <T extends Aggregation> SamplingAggregationDSL<T> samplingBuilder(
                    SamplingBuilder<T> builder) {
                return new SamplingAggregationDSL<T>(factory) {
                    @Override
                    protected T buildWith(final AggregationArguments args,
                            final Optional<Duration> size, final Optional<Duration> extent) {
                        return builder.apply(Optional.empty(), size, extent);
                    }
                };
            }
        };
    }

    interface SamplingBuilder<T> {
        T apply(Optional<SamplingQuery> sampling, Optional<Duration> size,
                Optional<Duration> extent);
    }
}
