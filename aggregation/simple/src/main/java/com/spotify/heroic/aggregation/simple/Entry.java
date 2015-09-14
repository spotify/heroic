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
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import javax.inject.Inject;
import javax.inject.Named;

import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.grammar.Value;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;

public class Entry implements HeroicModule {
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
        ctx.aggregation(TemplateAggregation.NAME, TemplateAggregation.class, TemplateAggregationQuery.class,
                samplingSerializer(TemplateAggregation::new), samplingBuilder(TemplateAggregation::new));

        ctx.aggregation(SpreadAggregation.NAME, SpreadAggregation.class, SpreadAggregationQuery.class,
                samplingSerializer(SpreadAggregation::new), samplingBuilder(SpreadAggregation::new));

        ctx.aggregation(SumAggregation.NAME, SumAggregation.class, SumAggregationQuery.class,
                samplingSerializer(SumAggregation::new), samplingBuilder(SumAggregation::new));

        ctx.aggregation(AverageAggregation.NAME, AverageAggregation.class, AverageAggregationQuery.class,
                samplingSerializer(AverageAggregation::new), samplingBuilder(AverageAggregation::new));

        ctx.aggregation(MinAggregation.NAME, MinAggregation.class, MinAggregationQuery.class,
                samplingSerializer(MinAggregation::new), samplingBuilder(MinAggregation::new));

        ctx.aggregation(MaxAggregation.NAME, MaxAggregation.class, MaxAggregationQuery.class,
                samplingSerializer(MaxAggregation::new), samplingBuilder(MaxAggregation::new));

        ctx.aggregation(StdDevAggregation.NAME, StdDevAggregation.class, StdDevAggregationQuery.class,
                samplingSerializer(StdDevAggregation::new), samplingBuilder(StdDevAggregation::new));

        ctx.aggregation(CountUniqueAggregation.NAME, CountUniqueAggregation.class, CountUniqueAggregationQuery.class,
                samplingSerializer(CountUniqueAggregation::new), samplingBuilder(CountUniqueAggregation::new));

        ctx.aggregation(CountAggregation.NAME, CountAggregation.class, CountAggregationQuery.class,
                samplingSerializer(CountAggregation::new), samplingBuilder(CountAggregation::new));

        ctx.aggregation(GroupUniqueAggregation.NAME, GroupUniqueAggregation.class, GroupUniqueAggregationQuery.class,
                samplingSerializer(GroupUniqueAggregation::new), samplingBuilder(GroupUniqueAggregation::new));

        ctx.aggregation(QuantileAggregation.NAME, QuantileAggregation.class, QuantileAggregationQuery.class,
                new Serializer<QuantileAggregation>() {
                    final Serializer<Double> fixedDouble = s.fixedDouble();
                    final Serializer<Long> fixedLong = s.fixedLong();

                    @Override
                    public void serialize(SerialWriter buffer, QuantileAggregation value) throws IOException {
                        fixedLong.serialize(buffer, value.getSize());
                        fixedLong.serialize(buffer, value.getExtent());
                        fixedDouble.serialize(buffer, value.getQ());
                        fixedDouble.serialize(buffer, value.getError());
                    }

                    @Override
                    public QuantileAggregation deserialize(SerialReader buffer) throws IOException {
                        final long size = fixedLong.deserialize(buffer);
                        final long extent = fixedLong.deserialize(buffer);
                        final double q = fixedDouble.deserialize(buffer);
                        final double error = fixedDouble.deserialize(buffer);
                        return new QuantileAggregation(size, extent, q, error);
                    }
                }, new SamplingAggregationBuilder<QuantileAggregation>(factory) {
                    @Override
                    protected QuantileAggregation buildWith(List<Value> args, Map<String, Value> keywords,
                            final long size, final long extent) {
                        double q = QuantileAggregationQuery.DEFAULT_QUANTILE;
                        double error = QuantileAggregationQuery.DEFAULT_ERROR;

                        if (keywords.containsKey("q")) {
                            q = ((double) keywords.get("q").cast(Long.class)) / 100;
                        }

                        if (keywords.containsKey("error")) {
                            error = ((double) keywords.get("error").cast(Long.class)) / 100;
                        }

                        return new QuantileAggregation(size, extent, q, error);
                    }
                });
    }

    private <T extends BucketAggregation<?>> Serializer<T> samplingSerializer(BiFunction<Long, Long, T> builder) {
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

    private <T extends Aggregation> SamplingAggregationBuilder<T> samplingBuilder(BiFunction<Long, Long, T> builder) {
        return new SamplingAggregationBuilder<T>(factory) {
            @Override
            protected T buildWith(List<Value> args, Map<String, Value> keywords, final long size, final long extent) {
                return builder.apply(size, extent);
            }
        };
    }
}
