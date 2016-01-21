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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.grammar.AggregationValue;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Named;

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
                final Serializer<Optional<List<String>>> list = s.optional(s.list(s.string()));
                final Serializer<List<AggregationInstance>> aggregations = s.list(aggregation);

                ctx.aggregation(Empty.NAME, EmptyInstance.class, Empty.class,
                        new Serializer<EmptyInstance>() {
                    @Override
                    public void serialize(SerialWriter buffer, EmptyInstance value)
                            throws IOException {
                    }

                    @Override
                    public EmptyInstance deserialize(SerialReader buffer) throws IOException {
                        return EmptyInstance.INSTANCE;
                    }
                }, args -> Empty.INSTANCE);

                ctx.aggregation(Group.NAME, GroupInstance.class, Group.class,
                        new GroupingAggregationSerializer<GroupInstance>(list, aggregation) {
                    @Override
                    protected GroupInstance build(Optional<List<String>> of,
                            AggregationInstance each) {
                        return new GroupInstance(of, each);
                    }
                }, new GroupingAggregationBuilder(factory) {
                    @Override
                    protected Aggregation build(Optional<List<String>> over,
                            Optional<Aggregation> each) {
                        return new Group(over, each);
                    }
                });

                ctx.aggregation(Collapse.NAME, CollapseInstance.class, Collapse.class,
                        new GroupingAggregationSerializer<CollapseInstance>(list, aggregation) {
                    @Override
                    protected CollapseInstance build(Optional<List<String>> of,
                            AggregationInstance each) {
                        return new CollapseInstance(of, each);
                    }
                }, new GroupingAggregationBuilder(factory) {
                    @Override
                    protected Aggregation build(Optional<List<String>> over,
                            Optional<Aggregation> each) {
                        return new Collapse(over, each);
                    }
                });

                ctx.aggregation(Chain.NAME, ChainInstance.class, Chain.class,
                        new Serializer<ChainInstance>() {
                    @Override
                    public void serialize(SerialWriter buffer, ChainInstance value)
                            throws IOException {
                        aggregations.serialize(buffer, value.getChain());
                    }

                    @Override
                    public ChainInstance deserialize(SerialReader buffer) throws IOException {
                        final List<AggregationInstance> chain = aggregations.deserialize(buffer);
                        return new ChainInstance(chain);
                    }
                }, new AbstractAggregationDSL(factory) {
                    @Override
                    public Aggregation build(final AggregationArguments args) {
                        final List<Aggregation> chain =
                                ImmutableList.copyOf(args.takeArguments(AggregationValue.class)
                                        .stream().map(this::asAggregation).iterator());

                        return new Chain(chain);
                    }
                });

                ctx.aggregation(Partition.NAME, PartitionInstance.class, Partition.class,
                        new Serializer<PartitionInstance>() {
                    @Override
                    public void serialize(SerialWriter buffer, PartitionInstance value)
                            throws IOException {
                        aggregations.serialize(buffer, value.getChildren());
                    }

                    @Override
                    public PartitionInstance deserialize(SerialReader buffer) throws IOException {
                        final List<AggregationInstance> children = aggregations.deserialize(buffer);
                        return new PartitionInstance(children);
                    }
                }, new AbstractAggregationDSL(factory) {
                    @Override
                    public Aggregation build(final AggregationArguments args) {
                        final List<Aggregation> children =
                                ImmutableList.copyOf(args.takeArguments(AggregationValue.class)
                                        .stream().map(this::asAggregation).iterator());
                        return new Partition(children);
                    }
                });

                ctx.aggregation(Options.NAME, AggregationInstance.class, Options.class, aggregation,
                        new AbstractAggregationDSL(factory) {
                    @Override
                    public Aggregation build(final AggregationArguments args) {
                        final Optional<Aggregation> child =
                                args.getNext("aggregation", AggregationValue.class)
                                        .map(this::asAggregation);

                        final Optional<Duration> size = args.keyword("size", Duration.class);
                        final Optional<Duration> extent = args.keyword("extent", Duration.class);

                        final Optional<SamplingQuery> sampling;

                        if (size.isPresent() || extent.isPresent()) {
                            sampling = Optional.of(new SamplingQuery(size, extent));
                        } else {
                            sampling = Optional.empty();
                        }

                        return new Options(sampling, child);
                    }
                });
            }
        };
    }
}
