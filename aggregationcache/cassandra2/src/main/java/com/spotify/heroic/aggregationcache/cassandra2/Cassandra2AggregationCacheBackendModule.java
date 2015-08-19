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

package com.spotify.heroic.aggregationcache.cassandra2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import javax.inject.Inject;
import javax.inject.Singleton;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.spotify.heroic.aggregationcache.AggregationCacheBackend;
import com.spotify.heroic.aggregationcache.AggregationCacheBackendModule;
import com.spotify.heroic.aggregationcache.CacheKey;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.statistics.AggregationCacheReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import eu.toolchain.serializer.SerializerFramework;

@RequiredArgsConstructor
public class Cassandra2AggregationCacheBackendModule implements AggregationCacheBackendModule {
    public static final int DEFAULT_THREADS = 20;
    public static final String DEFAULT_KEYSPACE = "aggregations";
    public static final int DEFAULT_MAX_CONNECTIONS_PER_HOST = 20;

    private final String seeds;
    private final String keyspace;
    private final int maxConnectionsPerHost;
    private final ReadWriteThreadPools.Config pools;

    @JsonCreator
    public Cassandra2AggregationCacheBackendModule(@JsonProperty("seeds") String seeds,
            @JsonProperty("keyspace") String keyspace,
            @JsonProperty("maxConnectionsPerHost") Integer maxConnectionsPerHost,
            @JsonProperty("pools") ReadWriteThreadPools.Config pools) {
        this.seeds = Preconditions.checkNotNull(seeds, "seeds must be defined");
        this.keyspace = Optional.fromNullable(DEFAULT_KEYSPACE).or(DEFAULT_KEYSPACE);
        this.maxConnectionsPerHost = Optional.fromNullable(maxConnectionsPerHost).or(DEFAULT_MAX_CONNECTIONS_PER_HOST);
        this.pools = Optional.fromNullable(pools).or(ReadWriteThreadPools.Config.provideDefault());
    }

    @Override
    public Module module() {
        return new PrivateModule() {
            @Provides
            @Singleton
            public AggregationCacheBackendReporter reporter(AggregationCacheReporter reporter) {
                return reporter.newAggregationCacheBackend();
            }

            @Provides
            @Singleton
            public ReadWriteThreadPools pools(AsyncFramework async, AggregationCacheBackendReporter reporter) {
                return pools.construct(async, reporter.newThreadPool());
            }

            @Provides
            @Singleton
            public Managed<Context> context(final AsyncFramework async) {
                return async.managed(new ManagedSetup<Context>() {
                    @Override
                    public AsyncFuture<Context> construct() {
                        return async.call(new Callable<Context>() {
                            @Override
                            public Context call() throws Exception {
                                final AstyanaxConfiguration config = new AstyanaxConfigurationImpl().setCqlVersion(
                                        "3.0.0").setTargetCassandraVersion("2.0");

                                final AstyanaxContext<Keyspace> ctx = new AstyanaxContext.Builder()
                                        .withConnectionPoolConfiguration(
                                                new ConnectionPoolConfigurationImpl("HeroicConnectionPool")
                                                        .setPort(9160).setMaxConnsPerHost(maxConnectionsPerHost)
                                                        .setSeeds(seeds)).forKeyspace(keyspace)
                                        .withAstyanaxConfiguration(config)
                                        .buildKeyspace(ThriftFamilyFactory.getInstance());
                                ctx.start();
                                final Keyspace client = ctx.getClient();
                                return new Context(ctx, client);
                            }
                        });
                    }

                    @Override
                    public AsyncFuture<Void> destruct(final Context context) {
                        return async.call(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                context.context.shutdown();
                                return null;
                            }
                        });
                    }
                });
            }

            @Provides
            @Singleton
            @Inject
            public Serializer<CacheKey> cacheKeySerializer(final eu.toolchain.serializer.Serializer<CacheKey> source,
                    @Named("common") final SerializerFramework s) {
                return new AbstractSerializer<CacheKey>() {
                    @Override
                    public ByteBuffer toByteBuffer(CacheKey obj) {
                        // the serializer framework returns a read-only object, while Astyanax expects a mutable one to
                        // access ByteBuffer#array().
                        final ByteBuffer readOnly;

                        try {
                            readOnly = s.serialize(source, obj);
                        } catch (IOException e) {
                            throw new RuntimeException("serialization failed", e);
                        }

                        final ByteBuffer result = ByteBuffer.allocate(readOnly.capacity());
                        result.put(readOnly);
                        result.rewind();
                        return result;
                    }

                    @Override
                    public CacheKey fromByteBuffer(ByteBuffer byteBuffer) {
                        try {
                            return s.deserialize(source, byteBuffer);
                        } catch (IOException e) {
                            throw new RuntimeException("de-serialization failed", e);
                        }
                    }
                };
            }

            @Override
            protected void configure() {
                bind(AggregationCacheBackend.class).toInstance(new Cassandra2AggregationCacheBackend());
                expose(AggregationCacheBackend.class);
            }
        };
    }
}
