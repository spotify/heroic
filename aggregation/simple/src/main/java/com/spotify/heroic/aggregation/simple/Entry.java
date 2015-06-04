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
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;

import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.grammar.Value;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.SamplingSerializer;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;

public class Entry implements HeroicModule {
    @Inject
    private SamplingSerializer resolutionSerializer;

    @Inject
    private HeroicContext ctx;

    @Inject
    @Named("common")
    private SerializerFramework s;

    @Override
    public void setup() {
        final Serializer<Double> doubleS = s.doubleNumber();

        /* example aggregation, if used only returns zeroes. */
        ctx.aggregation(TemplateAggregation.NAME, TemplateAggregation.class, TemplateAggregationQuery.class,
                new Serializer<TemplateAggregation>() {
                    @Override
                    public void serialize(SerialWriter buffer, TemplateAggregation value) throws IOException {
                        resolutionSerializer.serialize(buffer, value.getSampling());
                    }

                    @Override
                    public TemplateAggregation deserialize(SerialReader buffer) throws IOException {
                        final Sampling sampling = resolutionSerializer.deserialize(buffer);
                        return new TemplateAggregation(sampling);
                    }
                }, new SamplingAggregationBuilder<TemplateAggregation>() {
                    @Override
                    protected TemplateAggregation buildWith(Sampling sampling, Map<String, Value> keywords) {
                        return new TemplateAggregation(sampling);
                    }
                });

        ctx.aggregation(SumAggregation.NAME, SumAggregation.class, SumAggregationQuery.class,
                new Serializer<SumAggregation>() {
                    @Override
                    public void serialize(SerialWriter buffer, SumAggregation value) throws IOException {
                        resolutionSerializer.serialize(buffer, value.getSampling());
                    }

                    @Override
                    public SumAggregation deserialize(SerialReader buffer) throws IOException {
                        final Sampling sampling = resolutionSerializer.deserialize(buffer);
                        return new SumAggregation(sampling);
                    }
                }, new SamplingAggregationBuilder<SumAggregation>() {
                    @Override
                    protected SumAggregation buildWith(Sampling sampling, Map<String, Value> keywords) {
                        return new SumAggregation(sampling);
                    }
                });

        ctx.aggregation(AverageAggregation.NAME, AverageAggregation.class, AverageAggregationQuery.class,
                new Serializer<AverageAggregation>() {
                    @Override
                    public void serialize(SerialWriter buffer, AverageAggregation value) throws IOException {
                        resolutionSerializer.serialize(buffer, value.getSampling());
                    }

                    @Override
                    public AverageAggregation deserialize(SerialReader buffer) throws IOException {
                        final Sampling sampling = resolutionSerializer.deserialize(buffer);
                        return new AverageAggregation(sampling);
                    }
                }, new SamplingAggregationBuilder<AverageAggregation>() {
                    @Override
                    protected AverageAggregation buildWith(Sampling sampling, Map<String, Value> keywords) {
                        return new AverageAggregation(sampling);
                    }
                });

        ctx.aggregation(MinAggregation.NAME, MinAggregation.class, MinAggregationQuery.class,
                new Serializer<MinAggregation>() {
                    @Override
                    public void serialize(SerialWriter buffer, MinAggregation value) throws IOException {
                        resolutionSerializer.serialize(buffer, value.getSampling());
                    }

                    @Override
                    public MinAggregation deserialize(SerialReader buffer) throws IOException {
                        final Sampling sampling = resolutionSerializer.deserialize(buffer);
                        return new MinAggregation(sampling);
                    }
                }, new SamplingAggregationBuilder<MinAggregation>() {
                    @Override
                    protected MinAggregation buildWith(Sampling sampling, Map<String, Value> keywords) {
                        return new MinAggregation(sampling);
                    }
                });

        ctx.aggregation(MaxAggregation.NAME, MaxAggregation.class, MaxAggregationQuery.class,
                new Serializer<MaxAggregation>() {
                    @Override
                    public void serialize(SerialWriter buffer, MaxAggregation value) throws IOException {
                        resolutionSerializer.serialize(buffer, value.getSampling());
                    }

                    @Override
                    public MaxAggregation deserialize(SerialReader buffer) throws IOException {
                        final Sampling sampling = resolutionSerializer.deserialize(buffer);
                        return new MaxAggregation(sampling);
                    }
                }, new SamplingAggregationBuilder<MaxAggregation>() {
                    @Override
                    protected MaxAggregation buildWith(Sampling sampling, Map<String, Value> keywords) {
                        return new MaxAggregation(sampling);
                    }
                });

        ctx.aggregation(StdDevAggregation.NAME, StdDevAggregation.class, StdDevAggregationQuery.class,
                new Serializer<StdDevAggregation>() {
                    @Override
                    public void serialize(SerialWriter buffer, StdDevAggregation value) throws IOException {
                        resolutionSerializer.serialize(buffer, value.getSampling());
                    }

                    @Override
                    public StdDevAggregation deserialize(SerialReader buffer) throws IOException {
                        final Sampling sampling = resolutionSerializer.deserialize(buffer);
                        return new StdDevAggregation(sampling);
                    }
                }, new SamplingAggregationBuilder<StdDevAggregation>() {
                    @Override
                    protected StdDevAggregation buildWith(Sampling sampling, Map<String, Value> keywords) {
                        return new StdDevAggregation(sampling);
                    }
                });

        ctx.aggregation(QuantileAggregation.NAME, QuantileAggregation.class, QuantileAggregationQuery.class,
                new Serializer<QuantileAggregation>() {
                    @Override
                    public void serialize(SerialWriter buffer, QuantileAggregation value) throws IOException {
                        resolutionSerializer.serialize(buffer, value.getSampling());
                        doubleS.serialize(buffer, value.getQ());
                        doubleS.serialize(buffer, value.getError());
                    }

                    @Override
                    public QuantileAggregation deserialize(SerialReader buffer) throws IOException {
                        final Sampling sampling = resolutionSerializer.deserialize(buffer);
                        final double q = doubleS.deserialize(buffer);
                        final double error = doubleS.deserialize(buffer);
                        return new QuantileAggregation(sampling, q, error);
                    }
                }, new SamplingAggregationBuilder<QuantileAggregation>() {
                    @Override
                    protected QuantileAggregation buildWith(Sampling sampling, Map<String, Value> keywords) {
                        double q = QuantileAggregationQuery.DEFAULT_QUANTILE;
                        double error = QuantileAggregationQuery.DEFAULT_ERROR;

                        if (keywords.containsKey("q")) {
                            q = ((double) keywords.get("q").cast(Long.class)) / 100;
                        }

                        if (keywords.containsKey("error")) {
                            error = ((double) keywords.get("error").cast(Long.class)) / 100;
                        }

                        return new QuantileAggregation(sampling, q, error);
                    }
                });

        ctx.aggregation(CountUniqueAggregation.NAME, CountUniqueAggregation.class, CountUniqueAggregationQuery.class,
                new Serializer<CountUniqueAggregation>() {
                    @Override
                    public void serialize(SerialWriter buffer, CountUniqueAggregation value) throws IOException {
                        resolutionSerializer.serialize(buffer, value.getSampling());
                    }

                    @Override
                    public CountUniqueAggregation deserialize(SerialReader buffer) throws IOException {
                        final Sampling sampling = resolutionSerializer.deserialize(buffer);
                        return new CountUniqueAggregation(sampling);
                    }
                }, new SamplingAggregationBuilder<CountUniqueAggregation>() {
                    @Override
                    protected CountUniqueAggregation buildWith(Sampling sampling, Map<String, Value> keywords) {
                        return new CountUniqueAggregation(sampling);
                    }
                });

        ctx.aggregation(CountAggregation.NAME, CountAggregation.class, CountAggregationQuery.class,
                new Serializer<CountAggregation>() {
                    @Override
                    public void serialize(SerialWriter buffer, CountAggregation value) throws IOException {
                        resolutionSerializer.serialize(buffer, value.getSampling());
                    }

                    @Override
                    public CountAggregation deserialize(SerialReader buffer) throws IOException {
                        final Sampling sampling = resolutionSerializer.deserialize(buffer);
                        return new CountAggregation(sampling);
                    }
                }, new SamplingAggregationBuilder<CountAggregation>() {
                    @Override
                    protected CountAggregation buildWith(Sampling sampling, Map<String, Value> keywords) {
                        return new CountAggregation(sampling);
                    }
                });
    }
}
