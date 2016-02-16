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
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationRegistry;
import com.spotify.heroic.aggregation.AggregationSerializer;
import com.spotify.heroic.aggregation.BucketAggregationInstance;
import com.spotify.heroic.aggregation.SamplingQuery;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.dagger.LoadingComponent;
import dagger.Component;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.Optional;
import java.util.function.BiFunction;

public class Module implements HeroicModule {
    @Override
    public Entry setup(LoadingComponent loading) {
        return DaggerModule_C.builder().loadingComponent(loading).build().entry();
    }

    @Component(dependencies = LoadingComponent.class)
    interface C {
        E entry();
    }

    static class E implements HeroicModule.Entry {
        private final AggregationRegistry c;
        private final SerializerFramework s;
        private final AggregationSerializer aggregation;
        private final AggregationFactory factory;

        @Inject
        public E(
            AggregationRegistry c, @Named("common") SerializerFramework s,
            AggregationSerializer aggregation, AggregationFactory factory
        ) {
            this.c = c;
            this.s = s;
            this.aggregation = aggregation;
            this.factory = factory;
        }

        // @formatter:off
        @Override
        public void setup() {
            /* example aggregation, if used only returns zeroes. */
            c.register(Template.NAME, Template.class, TemplateInstance.class,
                samplingSerializer(TemplateInstance::new), samplingBuilder(Template::new));

            c.register(Spread.NAME, Spread.class, SpreadInstance.class,
                samplingSerializer(SpreadInstance::new), samplingBuilder(Spread::new));

            c.register(Sum.NAME, Sum.class, SumInstance.class, samplingSerializer(SumInstance::new),
                samplingBuilder(Sum::new));

            c.register(Average.NAME, Average.class, AverageInstance.class,
                samplingSerializer(AverageInstance::new), samplingBuilder(Average::new));

            c.register(Min.NAME, Min.class, MinInstance.class, samplingSerializer(MinInstance::new),
                samplingBuilder(Min::new));

            c.register(Max.NAME, Max.class, MaxInstance.class, samplingSerializer(MaxInstance::new),
                samplingBuilder(Max::new));

            c.register(StdDev.NAME, StdDev.class, StdDevInstance.class,
                samplingSerializer(StdDevInstance::new), samplingBuilder(StdDev::new));

            c.register(CountUnique.NAME, CountUnique.class, CountUniqueInstance.class,
                samplingSerializer(CountUniqueInstance::new), samplingBuilder(CountUnique::new));

            c.register(Count.NAME, Count.class, CountInstance.class,
                samplingSerializer(CountInstance::new), samplingBuilder(Count::new));

            c.register(GroupUnique.NAME, GroupUnique.class, GroupUniqueInstance.class,
                samplingSerializer(GroupUniqueInstance::new), samplingBuilder(GroupUnique::new));

            c.register(Quantile.NAME, Quantile.class, QuantileInstance.class,
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
                    protected Quantile buildWith(
                        final AggregationArguments args, final Optional<Duration> size,
                        final Optional<Duration> extent
                    ) {
                        final Optional<Double> q =
                            args.getNext("q", Long.class).map(v -> ((double) v) / 100.0);
                        final Optional<Double> error =
                            args.getNext("error", Long.class).map(v -> ((double) v) / 100.0);
                        return new Quantile(Optional.empty(), size, extent, q, error);
                    }
                });

            c.register(TopK.NAME, TopK.class, TopKInstance.class,
                new FilterSerializer<TopKInstance>(aggregation) {
                    final Serializer<Long> fixedLong = s.fixedLong();

                    @Override
                    protected void serializeNext(SerialWriter buffer, TopKInstance value)
                        throws IOException {
                        fixedLong.serialize(buffer, value.getK());
                    }

                    @Override
                    protected TopKInstance deserializeNext(SerialReader buffer,
                                                           AggregationInstance of)
                        throws IOException {
                        return new TopKInstance(fixedLong.deserialize(buffer), of);
                    }
                },
                new FilterAggregationBuilder<TopK>(factory) {
                    @Override
                    protected TopK buildAggregation(AggregationArguments args, Aggregation of) {
                        return new TopK(fetchK(args, Long.class), of);

                    }
                });

            c.register(BottomK.NAME, BottomK.class, BottomKInstance.class,
                new FilterSerializer<BottomKInstance>(aggregation) {
                    final Serializer<Long> fixedLong = s.fixedLong();

                    @Override
                    protected void serializeNext(SerialWriter buffer, BottomKInstance value)
                        throws IOException {
                        fixedLong.serialize(buffer, value.getK());
                    }

                    @Override
                    protected BottomKInstance deserializeNext(SerialReader buffer,
                                                              AggregationInstance of)
                        throws IOException {
                        return new BottomKInstance(fixedLong.deserialize(buffer), of);
                    }
                },
                new FilterAggregationBuilder<BottomK>(factory) {
                    @Override
                    protected BottomK buildAggregation(AggregationArguments args,
                                                       Aggregation of) {
                        return new BottomK(fetchK(args, Long.class), of);

                    }
                });

            c.register(AboveK.NAME, AboveK.class, AboveKInstance.class,
                new FilterSerializer<AboveKInstance>(aggregation) {
                    final Serializer<Double> fixedDouble = s.fixedDouble();

                    @Override
                    protected void serializeNext(SerialWriter buffer, AboveKInstance value)
                        throws IOException {
                        fixedDouble.serialize(buffer, value.getK());
                    }

                    @Override
                    protected AboveKInstance deserializeNext(SerialReader buffer,
                                                             AggregationInstance of)
                        throws IOException {
                        return new AboveKInstance(fixedDouble.deserialize(buffer), of);
                    }
                },
                new FilterAggregationBuilder<AboveK>(factory) {
                    @Override
                    protected AboveK buildAggregation(AggregationArguments args,
                                                      Aggregation of) {
                        return new AboveK(fetchK(args, Double.class), of);
                    }
                });

            c.register(BelowK.NAME, BelowK.class, BelowKInstance.class,
                new FilterSerializer<BelowKInstance>(aggregation) {
                    final Serializer<Double> fixedDouble = s.fixedDouble();

                    @Override
                    protected void serializeNext(SerialWriter buffer, BelowKInstance value)
                        throws IOException {
                        fixedDouble.serialize(buffer, value.getK());
                    }

                    @Override
                    protected BelowKInstance deserializeNext(SerialReader buffer,
                                                             AggregationInstance of)
                        throws IOException {
                        return new BelowKInstance(fixedDouble.deserialize(buffer), of);
                    }
                },
                new FilterAggregationBuilder<BelowK>(factory) {
                    @Override
                    protected BelowK buildAggregation(AggregationArguments args,
                                                      Aggregation of) {
                        return new BelowK(fetchK(args, Double.class), of);
                    }
                });
            }
        // @formatter:on

        private <T extends Number> T fetchK(AggregationArguments args, Class<T> doubleClass) {
            return args
                .positional(doubleClass)
                .orElseThrow(() -> new IllegalArgumentException("missing required argument 'k'"));
        }

        private <T extends BucketAggregationInstance<?>> Serializer<T> samplingSerializer(
            BiFunction<Long, Long, T> builder
        ) {
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
