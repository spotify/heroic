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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.aggregation.AggregationRegistry;
import com.spotify.heroic.common.CollectingTypeListener;
import com.spotify.heroic.common.IsSubclassOf;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.common.ServiceInfo;
import com.spotify.heroic.filter.FilterRegistry;
import com.spotify.heroic.grammar.CoreQueryParser;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.jetty.JettyServerConnector;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.ShellTaskDefinition;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.statistics.HeroicReporter;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.inject.Named;
import javax.inject.Singleton;

import eu.toolchain.async.AsyncFramework;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class HeroicPrimaryModule extends AbstractModule {
    private final HeroicCoreInstance instance;
    private final Set<LifeCycle> lifeCycles;
    private final InetSocketAddress bindAddress;
    private final boolean enableCors;
    private final Optional<String> corsAllowOrigin;
    private final Set<String> features;
    private final List<JettyServerConnector> connectors;

    private final boolean setupService;
    private final HeroicReporter reporter;
    private final Optional<HeroicStartupPinger> pinger;

    private final String service;
    private final String version;

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
    @Named("features")
    public Set<String> features() {
        return features;
    }

    @Provides
    @Singleton
    public ServiceInfo service() {
        return new ServiceInfo(service, version);
    }

    @Provides
    @Singleton
    public List<JettyServerConnector> connectors() {
        return connectors;
    }

    @Provides
    @Singleton
    @Named(HeroicCore.APPLICATION_JSON_INTERNAL)
    @Inject
    public ObjectMapper internalMapper(FilterRegistry filterRegistry, QueryParser parser,
            AggregationRegistry aggregation) {
        final ObjectMapper m = HeroicMappers.json();

        /* configuration determined at runtime, unsuitable for testing */
        m.registerModule(filterRegistry.module(parser));
        m.registerModule(aggregation.module());

        return m;
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
        bind(QueryParser.class).to(CoreQueryParser.class).in(Scopes.SINGLETON);
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
}
