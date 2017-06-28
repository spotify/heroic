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

package com.spotify.heroic.cache.memcached;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.spotify.folsom.BinaryMemcacheClient;
import com.spotify.folsom.ConnectFuture;
import com.spotify.folsom.MemcacheClient;
import com.spotify.folsom.MemcacheClientBuilder;
import com.spotify.heroic.cache.CacheComponent;
import com.spotify.heroic.cache.CacheModule;
import com.spotify.heroic.cache.CacheScope;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import eu.toolchain.async.ResolvableFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.inject.Named;
import lombok.NoArgsConstructor;

@Module
public class MemcachedCacheModule implements CacheModule {
    public static final String DEFAULT_ADDRESS = "localhost:11211";

    private final List<String> addresses;

    public MemcachedCacheModule(final List<String> addresses) {
        this.addresses = addresses;
    }

    @Override
    public CacheComponent module(PrimaryComponent primary) {
        return DaggerMemcachedCacheModule_C
            .builder()
            .primaryComponent(primary)
            .memcachedCacheModule(this)
            .build();
    }

    @CacheScope
    @Component(modules = MemcachedCacheModule.class, dependencies = PrimaryComponent.class)
    interface C extends CacheComponent {
        @Override
        MemcachedQueryCache queryCache();

        @Named("cache")
        LifeCycle cacheLife();
    }

    @Provides
    @CacheScope
    public Managed<MemcacheClient<byte[]>> memcacheClient(final AsyncFramework async) {
        return async.managed(new ManagedSetup<MemcacheClient<byte[]>>() {
            @Override
            public AsyncFuture<MemcacheClient<byte[]>> construct() throws Exception {
                final List<HostAndPort> addresses = new ArrayList<>();

                for (final String address : MemcachedCacheModule.this.addresses) {
                    addresses.add(HostAndPort.fromString(address));
                }

                final BinaryMemcacheClient<byte[]> client = MemcacheClientBuilder
                    .newByteArrayClient()
                    .withAddresses(addresses)
                    .connectBinary();

                final ResolvableFuture<MemcacheClient<byte[]>> future = async.future();

                Futures.addCallback(ConnectFuture.connectFuture(client),
                    new FutureCallback<Void>() {
                        @Override
                        public void onSuccess(@Nullable final Void result) {
                            future.resolve(client);
                        }

                        @Override
                        public void onFailure(final Throwable cause) {
                            future.fail(cause);
                        }
                    });

                return future;
            }

            @Override
            public AsyncFuture<Void> destruct(
                final MemcacheClient<byte[]> value
            ) throws Exception {
                return async.call(() -> {
                    value.shutdown();
                    return null;
                });
            }
        });
    }

    @Provides
    @Named("cache")
    @CacheScope
    public LifeCycle life(
        final LifeCycleRegistry registry, final Managed<MemcacheClient<byte[]>> client
    ) {
        return () -> {
            registry.start(client::start);
            registry.stop(client::stop);
        };
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor
    public static class Builder implements CacheModule.Builder {
        private Optional<List<String>> addresses = Optional.empty();

        @JsonCreator
        public Builder(@JsonProperty("addresses") final Optional<List<String>> addresses) {
            this.addresses = addresses;
        }

        public Builder addresses(final List<String> addresses) {
            this.addresses = Optional.of(addresses);
            return this;
        }

        @Override
        public CacheModule build() {
            final List<String> addresses =
                this.addresses.orElseGet(() -> Collections.singletonList(DEFAULT_ADDRESS));
            return new MemcachedCacheModule(addresses);
        }
    }
}
