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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.grammar.AggregationValue;
import com.spotify.heroic.grammar.Value;

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
            private AggregationSerializer aggregation;

            @Inject
            private AggregationFactory factory;

            @Inject
            @Named("common")
            private SerializerFramework s;

            @Override
            public void setup() {
                final Serializer<List<String>> list = s.list(s.string());
                final Serializer<List<Aggregation>> aggregations = s.list(aggregation);

                ctx.aggregation(EmptyAggregation.NAME, EmptyAggregation.class, EmptyAggregationQuery.class,
                        new Serializer<EmptyAggregation>() {
                    @Override
                    public void serialize(SerialWriter buffer, EmptyAggregation value) throws IOException {
                    }

                    @Override
                    public EmptyAggregation deserialize(SerialReader buffer) throws IOException {
                        return EmptyAggregation.INSTANCE;
                    }
                }, new AggregationBuilder<EmptyAggregation>() {
                    @Override
                    public EmptyAggregation build(AggregationContext context, List<Value> args,
                            Map<String, Value> keywords) {
                        return EmptyAggregation.INSTANCE;
                    }
                });

                ctx.aggregation(GroupAggregation.NAME, GroupAggregation.class, GroupAggregationQuery.class,
                        new GroupingAggregationSerializer<GroupAggregation>(list, aggregation) {
                    @Override
                    protected GroupAggregation build(List<String> of, Aggregation each) {
                        return new GroupAggregation(of, each);
                    }
                }, new GroupingAggregationBuilder<GroupAggregation>(factory) {
                    @Override
                    protected GroupAggregation build(List<String> over, Aggregation each) {
                        return new GroupAggregation(over, each);
                    }
                });

                ctx.aggregation(CollapseAggregation.NAME, CollapseAggregation.class, CollapseAggregationQuery.class,
                        new GroupingAggregationSerializer<CollapseAggregation>(list, aggregation) {
                    @Override
                    protected CollapseAggregation build(List<String> of, Aggregation each) {
                        return new CollapseAggregation(of, each);
                    }
                }, new GroupingAggregationBuilder<CollapseAggregation>(factory) {
                    @Override
                    protected CollapseAggregation build(List<String> over, Aggregation each) {
                        return new CollapseAggregation(over, each);
                    }
                });

                ctx.aggregation(ChainAggregation.NAME, ChainAggregation.class, ChainAggregationQuery.class,
                        new Serializer<ChainAggregation>() {
                    @Override
                    public void serialize(SerialWriter buffer, ChainAggregation value) throws IOException {
                        aggregations.serialize(buffer, value.getChain());
                    }

                    @Override
                    public ChainAggregation deserialize(SerialReader buffer) throws IOException {
                        final List<Aggregation> chain = aggregations.deserialize(buffer);
                        return new ChainAggregation(chain);
                    }
                }, new AbstractAggregationBuilder<ChainAggregation>(factory) {
                    @Override
                    public ChainAggregation build(AggregationContext context, List<Value> args,
                            Map<String, Value> keywords) {
                        final List<Aggregation> aggregations = new ArrayList<>();

                        for (final Value v : args) {
                            aggregations.addAll(flatten(context, v));
                        }

                        return new ChainAggregation(aggregations);
                    }
                });

                ctx.aggregation(PartitionAggregation.NAME, PartitionAggregation.class, PartitionAggregationQuery.class,
                        new Serializer<PartitionAggregation>() {
                    @Override
                    public void serialize(SerialWriter buffer, PartitionAggregation value) throws IOException {
                        aggregations.serialize(buffer, value.getChildren());
                    }

                    @Override
                    public PartitionAggregation deserialize(SerialReader buffer) throws IOException {
                        final List<Aggregation> children = aggregations.deserialize(buffer);
                        return new PartitionAggregation(children);
                    }
                }, new AbstractAggregationBuilder<PartitionAggregation>(factory) {
                    @Override
                    public PartitionAggregation build(AggregationContext context, List<Value> args,
                            Map<String, Value> keywords) {
                        final ImmutableList.Builder<Aggregation> aggregations = ImmutableList.builder();

                        for (final Value v : args) {
                            aggregations.addAll(flatten(context, v));
                        }

                        return new PartitionAggregation(aggregations.build());
                    }
                });

                ctx.aggregation("opts", Aggregation.class, OptionsAggregationQuery.class, aggregation,
                        new AbstractAggregationBuilder<Aggregation>(factory) {
                    @Override
                    public Aggregation build(AggregationContext context, List<Value> args,
                            Map<String, Value> keywords) {
                        final Deque<Value> a = new LinkedList<>(args);
                        final Optional<Long> size = parseDiffMillis(a, keywords, "size");
                        final Optional<Long> extent = parseDiffMillis(a, keywords, "extent");
                        final OptionsContext c = new OptionsContext(context, size, extent);
                        return extracted(keywords, a, c);
                    }

                    private Aggregation extracted(Map<String, Value> keywords, final Deque<Value> a,
                            final OptionsContext c) {
                        final AggregationValue aggregation;

                        if (!a.isEmpty()) {
                            aggregation = a.removeFirst().cast(AggregationValue.class);
                        } else {
                            if (!keywords.containsKey("aggregation")) {
                                throw new IllegalArgumentException("Missing aggregation argument");
                            }

                            aggregation = keywords.get("aggregation").cast(AggregationValue.class);
                        }

                        return factory.build(c, aggregation.getName(), aggregation.getArguments(),
                                aggregation.getKeywordArguments());
                    }
                });
            }
        };
    }
}