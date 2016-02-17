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

package com.spotify.heroic.consumer.collectd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.GrokProcessor;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.ingestion.IngestionComponent;
import com.spotify.heroic.ingestion.IngestionGroup;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Named;
import java.net.InetAddress;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
@Data
public class CollectdConsumerModule implements ConsumerModule {
    public static final int DEFAULT_PORT = 25826;

    private final Optional<String> id;
    private final Optional<String> host;
    private final Optional<Integer> port;
    private final Optional<GrokProcessor> hostProcessor;
    private final CollectdTypes types;

    @Override
    public Exposed module(
        PrimaryComponent primary, IngestionComponent ingestion, Depends depends, String id
    ) {
        return DaggerCollectdConsumerModule_C
            .builder()
            .primaryComponent(primary)
            .ingestionComponent(ingestion)
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
        return String.format("collectd#%d", i);
    }

    public static Builder builder() {
        return new Builder();
    }

    @CollectdScope
    @Component(modules = M.class,
        dependencies = {
            PrimaryComponent.class, IngestionComponent.class, ConsumerModule.Depends.class
        })
    interface C extends ConsumerModule.Exposed {
        @Override
        CollectdConsumer consumer();

        @Override
        LifeCycle consumerLife();
    }

    @Module
    class M {
        @Provides
        LifeCycle consumerLife(LifeCycleManager manager, CollectdConsumer consumer) {
            return manager.build(consumer);
        }

        @Provides
        @Named("consuming")
        @CollectdScope
        AtomicInteger consuming() {
            return new AtomicInteger();
        }

        @Provides
        @Named("total")
        @CollectdScope
        AtomicInteger total() {
            return new AtomicInteger();
        }

        @Provides
        @Named("errors")
        @CollectdScope
        AtomicLong errors() {
            return new AtomicLong();
        }

        @Provides
        @Named("consumed")
        @CollectdScope
        LongAdder consumed() {
            return new LongAdder();
        }

        @Provides
        @CollectdScope
        Managed<Server> connection(
            final AsyncFramework async, final IngestionManager ingestionManager
        ) {
            return async.managed(new ManagedSetup<Server>() {
                @Override
                public AsyncFuture<Server> construct() {
                    final IngestionGroup ingestion = ingestionManager.useDefaultGroup();

                    if (ingestion.isEmpty()) {
                        log.warn("No backends are part of the selected ingestion group");
                    }

                    final CollectdChannelHandler handler =
                        new CollectdChannelHandler(async, ingestion, hostProcessor, types);

                    final InetAddress h = host.map(host -> {
                        try {
                            return InetAddress.getByName(host);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }).orElseGet(InetAddress::getLoopbackAddress);

                    final int p = port.orElse(DEFAULT_PORT);

                    log.info("Setting up on {}:{}", h, p);
                    return Server.setup(async, handler, h, p);
                }

                @Override
                public AsyncFuture<Void> destruct(final Server value) {
                    log.info("Shutting down");
                    return value.shutdown();
                }
            });
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder implements ConsumerModule.Builder {
        private Optional<String> id = Optional.empty();
        private Optional<String> host = Optional.empty();
        private Optional<Integer> port = Optional.empty();
        private Optional<GrokProcessor> hostProcessor = Optional.empty();
        private Optional<CollectdTypes> types = Optional.empty();

        @JsonCreator
        public Builder(
            @JsonProperty("id") Optional<String> id, @JsonProperty("host") Optional<String> host,
            @JsonProperty("port") Optional<Integer> port,
            @JsonProperty("hostPattern") Optional<GrokProcessor> hostPattern,
            @JsonProperty("types") Optional<CollectdTypes> types
        ) {
            this.id = id;
            this.host = host;
            this.port = port;
            this.hostProcessor = hostPattern;
            this.types = types;
        }

        public Builder id(String id) {
            this.id = Optional.of(id);
            return this;
        }

        public Builder host(String host) {
            this.host = Optional.of(host);
            return this;
        }

        public Builder port(int port) {
            this.port = Optional.of(port);
            return this;
        }

        public Builder hostProcessor(GrokProcessor hostProcessor) {
            this.hostProcessor = Optional.of(hostProcessor);
            return this;
        }

        public Builder types(CollectdTypes types) {
            this.types = Optional.of(types);
            return this;
        }

        @Override
        public ConsumerModule build() {
            // @formatter:off
            return new CollectdConsumerModule(
                id,
                host,
                port,
                hostProcessor,
                types.orElseGet(CollectdTypes::supplyDefault)
            );
            // @formatter:on
        }
    }
}
