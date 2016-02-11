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

package com.spotify.heroic.metric.astyanax;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.statistics.MetricBackendReporter;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

@Data
public final class AstyanaxMetricModule implements MetricModule {
    public static final Set<String> DEFAULT_SEEDS = ImmutableSet.of("localhost");
    public static final String DEFAULT_KEYSPACE = "heroic";
    public static final String DEFAULT_GROUP = "heroic";
    public static final int DEFAULT_MAX_CONNECTIONS_PER_HOST = 50;

    private final Optional<String> id;
    private final Groups groups;
    private final String keyspace;
    private final Set<String> seeds;
    private final int maxConnectionsPerHost;
    private final ReadWriteThreadPools.Config pools;

    @JsonCreator
    public AstyanaxMetricModule(
        @JsonProperty("id") Optional<String> id, @JsonProperty("seeds") Optional<Set<String>> seeds,
        @JsonProperty("keyspace") Optional<String> keyspace,
        @JsonProperty("maxConnectionsPerHost") Optional<Integer> maxConnectionsPerHost,
        @JsonProperty("groups") Optional<Groups> groups,
        @JsonProperty("pools") Optional<ReadWriteThreadPools.Config> pools
    ) {
        this.id = id;
        this.groups = groups.orElseGet(Groups::empty).or(DEFAULT_GROUP);
        this.keyspace = keyspace.orElse(DEFAULT_KEYSPACE);
        this.seeds = seeds.orElse(DEFAULT_SEEDS);
        this.maxConnectionsPerHost = maxConnectionsPerHost.orElse(DEFAULT_MAX_CONNECTIONS_PER_HOST);
        this.pools = pools.orElseGet(ReadWriteThreadPools.Config::buildDefault);
    }

    @Override
    public Exposed module(PrimaryComponent primary, Depends depends, String id) {
        return DaggerAstyanaxMetricModule_C
            .builder()
            .primaryComponent(primary)
            .depends(depends)
            .m(new M())
            .build();
    }

    @Override
    public Optional<String> id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("heroic#%d", i);
    }

    @AstyanaxScope
    @Component(modules = M.class, dependencies = {PrimaryComponent.class, Depends.class})
    interface C extends Exposed {
        @Override
        AstyanaxBackend backend();

        @Override
        LifeCycle life();
    }

    @Module
    class M {
        @Provides
        @AstyanaxScope
        public ReadWriteThreadPools pools(AsyncFramework async, MetricBackendReporter reporter) {
            return pools.construct(async, reporter.newThreadPool());
        }

        @Provides
        @AstyanaxScope
        public Groups groups() {
            return groups;
        }

        @Provides
        @AstyanaxScope
        public Managed<Context> context(final AsyncFramework async) {
            return async.managed(new ManagedSetup<Context>() {
                @Override
                public AsyncFuture<Context> construct() {
                    return async.call(new Callable<Context>() {
                        public Context call() throws Exception {
                            final AstyanaxConfiguration config = new AstyanaxConfigurationImpl()
                                .setCqlVersion("3.0.0")
                                .setTargetCassandraVersion("2.0");

                            final String seeds = buildSeeds();

                            final AstyanaxContext<Keyspace> ctx = new AstyanaxContext.Builder()
                                .withConnectionPoolConfiguration(
                                    new ConnectionPoolConfigurationImpl("HeroicConnectionPool")
                                        .setPort(9160)
                                        .setMaxConnsPerHost(maxConnectionsPerHost)
                                        .setSeeds(seeds))
                                .forKeyspace(keyspace)
                                .withAstyanaxConfiguration(config)
                                .buildKeyspace(ThriftFamilyFactory.getInstance());

                            ctx.start();
                            final Keyspace keyspace = ctx.getClient();
                            return new Context(ctx, keyspace);
                        }
                    });
                }

                private String buildSeeds() {
                    return StringUtils.join(seeds, ",");
                }

                @Override
                public AsyncFuture<Void> destruct(final Context ctx) {
                    return async.call(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            ctx.context.shutdown();
                            return null;
                        }
                    });
                }
            });
        }

        @Provides
        @AstyanaxScope
        LifeCycle life(LifeCycleManager manager, AstyanaxBackend backend) {
            return manager.build(backend);
        }
    }
}
