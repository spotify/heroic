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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Named;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.common.Duration;
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
                final Serializer<List<AggregationInstance>> aggregations = s.list(aggregation);

                ctx.aggregation(Empty.NAME, EmptyInstance.class, Empty.class,
                        new Serializer<EmptyInstance>() {
                    @Override
                    public void serialize(SerialWriter buffer, EmptyInstance value) throws IOException {
                    }

                    @Override
                    public EmptyInstance deserialize(SerialReader buffer) throws IOException {
                        return EmptyInstance.INSTANCE;
                    }
                }, (args, keywords) -> Empty.INSTANCE);

                ctx.aggregation(Group.NAME, GroupInstance.class, Group.class,
                        new GroupingAggregationSerializer<GroupInstance>(list, aggregation) {
                    @Override
                    protected GroupInstance build(List<String> of, AggregationInstance each) {
                        return new GroupInstance(of, each);
                    }
                }, new GroupingAggregationBuilder(factory) {
                    @Override
                    protected Aggregation build(List<String> over, Aggregation each) {
                        return new Group(over, each);
                    }
                });

                ctx.aggregation(Collapse.NAME, CollapseInstance.class, Collapse.class,
                        new GroupingAggregationSerializer<CollapseInstance>(list, aggregation) {
                    @Override
                    protected CollapseInstance build(List<String> of, AggregationInstance each) {
                        return new CollapseInstance(of, each);
                    }
                }, new GroupingAggregationBuilder(factory) {
                    @Override
                    protected Aggregation build(List<String> over, Aggregation each) {
                        return new Collapse(over, each);
                    }
                });

                ctx.aggregation(Chain.NAME, ChainInstance.class, Chain.class,
                        new Serializer<ChainInstance>() {
                    @Override
                    public void serialize(SerialWriter buffer, ChainInstance value) throws IOException {
                        aggregations.serialize(buffer, value.getChain());
                    }

                    @Override
                    public ChainInstance deserialize(SerialReader buffer) throws IOException {
                        final List<AggregationInstance> chain = aggregations.deserialize(buffer);
                        return new ChainInstance(chain);
                    }
                }, new AbstractAggregationDSL(factory) {
                    @Override
                    public Aggregation build(List<Value> args, Map<String, Value> keywords) {
                        final List<Aggregation> aggregations = new ArrayList<>();

                        for (final Value v : args) {
                            aggregations.addAll(flatten(v));
                        }

                        return new Chain(aggregations);
                    }
                });

                ctx.aggregation(Partition.NAME, PartitionInstance.class, Partition.class,
                        new Serializer<PartitionInstance>() {
                    @Override
                    public void serialize(SerialWriter buffer, PartitionInstance value) throws IOException {
                        aggregations.serialize(buffer, value.getChildren());
                    }

                    @Override
                    public PartitionInstance deserialize(SerialReader buffer) throws IOException {
                        final List<AggregationInstance> children = aggregations.deserialize(buffer);
                        return new PartitionInstance(children);
                    }
                }, new AbstractAggregationDSL(factory) {
                    @Override
                    public Aggregation build(List<Value> args, Map<String, Value> keywords) {
                        final ImmutableList.Builder<Aggregation> aggregations = ImmutableList.builder();

                        for (final Value v : args) {
                            aggregations.addAll(flatten(v));
                        }

                        return new Partition(aggregations.build());
                    }
                });

                ctx.aggregation(Options.NAME, AggregationInstance.class, Options.class, aggregation,
                        new AbstractAggregationDSL(factory) {
                    @Override
                    public Aggregation build(List<Value> args,
                            Map<String, Value> keywords) {
                        final Optional<Duration> size = parseDuration(keywords, "size");
                        final Optional<Duration> extent = parseDuration(keywords, "extent");
                        final SamplingQuery sampling = new SamplingQuery(size, extent);

                        final Aggregation child = extractAggregation(args, keywords);
                        return new Options(sampling, child);
                    }

                    private Aggregation extractAggregation(final List<Value> args, final Map<String, Value> keywords) {
                        if (!args.isEmpty()) {
                            return args.iterator().next().cast(AggregationValue.class).build(factory);
                        }

                        if (!keywords.containsKey("aggregation")) {
                            throw new IllegalArgumentException("Missing aggregation argument");
                        }

                        return keywords.get("aggregation").cast(AggregationValue.class).build(factory);
                    }
                });
            }
        };
    }
}