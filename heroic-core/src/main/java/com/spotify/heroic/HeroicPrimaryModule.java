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

package com.spotify.heroic;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.inject.Named;
import javax.inject.Singleton;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationSerializer;
import com.spotify.heroic.aggregation.CoreAggregationRegistry;
import com.spotify.heroic.common.CollectingTypeListener;
import com.spotify.heroic.common.IsSubclassOf;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.common.TypeNameMixin;
import com.spotify.heroic.filter.FilterJsonDeserializer;
import com.spotify.heroic.filter.FilterJsonDeserializerImpl;
import com.spotify.heroic.filter.FilterJsonSerializer;
import com.spotify.heroic.filter.FilterJsonSerializerImpl;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.EventSerialization;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricCollectionSerialization;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.MetricGroupSerialization;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.MetricTypeSerialization;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.PointSerialization;
import com.spotify.heroic.metric.Spread;
import com.spotify.heroic.metric.SpreadSerialization;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.ShellTaskDefinition;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.statistics.HeroicReporter;

import eu.toolchain.async.AsyncFramework;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class HeroicPrimaryModule extends AbstractModule {
    private final HeroicCoreInstance instance;
    private final Set<LifeCycle> lifeCycles;
    private final InetSocketAddress bindAddress;
    private final boolean enableCors;
    private final Optional<String> corsAllowOrigin;

    private final boolean setupService;
    private final HeroicReporter reporter;
    private final Optional<HeroicStartupPinger> pinger;

    @Provides
    @Singleton
    HeroicShellTasks tasks(AsyncFramework async, HeroicCoreInstance injector) throws Exception {
        final List<ShellTaskDefinition> commands = Tasks.available();
        return new HeroicShellTasks(commands, setupTasks(commands, injector), async);
    }

    private SortedMap<String, ShellTask> setupTasks(final List<ShellTaskDefinition> commands,
            final HeroicCoreInstance injector) throws Exception {
        final SortedMap<String, ShellTask> tasks = new TreeMap<>();

        for (final ShellTaskDefinition def : commands) {
            final ShellTask instance = def.setup(injector);

            for (final String n : def.names()) {
                tasks.put(n, instance);
            }
        }

        return tasks;
    }

    @Provides
    @Singleton
    public HeroicCoreInstance instance() {
        return instance;
    }

    @Provides
    @Singleton
    public HeroicReporter reporter() {
        return reporter;
    }

    @Provides
    @Singleton
    public Set<LifeCycle> lifecycles() {
        return lifeCycles;
    }

    @Provides
    @Singleton
    @Named("bindAddress")
    public InetSocketAddress bindAddress() {
        return bindAddress;
    }

    @Provides
    @Singleton
    @Named("enableCors")
    public boolean enableCors() {
        return enableCors;
    }

    @Provides
    @Singleton
    @Named("corsAllowOrigin")
    public Optional<String> corsAllowOrigin() {
        return corsAllowOrigin;
    }

    @Provides
    @Singleton
    @Named(HeroicCore.APPLICATION_JSON_INTERNAL)
    @Inject
    public ObjectMapper internalMapper(FilterJsonSerializer serializer,
            FilterJsonDeserializer deserializer, AggregationSerializer aggregationSerializer) {
        final SimpleModule module = new SimpleModule("custom");

        final FilterJsonSerializerImpl serializerImpl = (FilterJsonSerializerImpl) serializer;
        final FilterJsonDeserializerImpl deserializerImpl =
                (FilterJsonDeserializerImpl) deserializer;
        final CoreAggregationRegistry aggregationRegistry =
                (CoreAggregationRegistry) aggregationSerializer;

        deserializerImpl.configure(module);
        serializerImpl.configure(module);
        aggregationRegistry.configure(module);

        final ObjectMapper mapper = new ObjectMapper();

        mapper.addMixIn(AggregationInstance.class, TypeNameMixin.class);
        mapper.addMixIn(Aggregation.class, TypeNameMixin.class);

        mapper.registerModule(module);
        mapper.registerModule(serializerModule());
        mapper.registerModule(new Jdk8Module().configureAbsentsAsNulls(true));
        mapper.registerModule(HeroicLoadingModule.serialization());

        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        return mapper;
    }

    @Provides
    @Singleton
    @Named(HeroicCore.APPLICATION_JSON)
    @Inject
    public ObjectMapper jsonMapper(
            @Named(HeroicCore.APPLICATION_JSON_INTERNAL) ObjectMapper mapper) {
        return mapper;
    }

    @Override
    protected void configure() {
        bind(QueryManager.class).to(CoreQueryManager.class).in(Scopes.SINGLETON);

        if (setupService) {
            bind(HeroicServer.class).in(Scopes.SINGLETON);
        }

        if (pinger.isPresent()) {
            bind(HeroicStartupPinger.class).toInstance(pinger.get());
        }

        bindListener(new IsSubclassOf(LifeCycle.class),
                new CollectingTypeListener<LifeCycle>(lifeCycles));
    }

    public static SimpleModule serializerModule() {
        final SimpleModule module = new SimpleModule("serializers");

        module.addSerializer(Point.class, new PointSerialization.Serializer());
        module.addDeserializer(Point.class, new PointSerialization.Deserializer());

        module.addSerializer(Event.class, new EventSerialization.Serializer());
        module.addDeserializer(Event.class, new EventSerialization.Deserializer());

        module.addSerializer(Spread.class, new SpreadSerialization.Serializer());
        module.addDeserializer(Spread.class, new SpreadSerialization.Deserializer());

        module.addSerializer(MetricGroup.class, new MetricGroupSerialization.Serializer());
        module.addDeserializer(MetricGroup.class, new MetricGroupSerialization.Deserializer());

        module.addSerializer(MetricCollection.class,
                new MetricCollectionSerialization.Serializer());
        module.addDeserializer(MetricCollection.class,
                new MetricCollectionSerialization.Deserializer());

        module.addSerializer(MetricType.class, new MetricTypeSerialization.Serializer());
        module.addDeserializer(MetricType.class, new MetricTypeSerialization.Deserializer());

        return module;
    }
}
