/*
 * Copyright (c) 2018 Spotify AB.
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

package com.spotify.heroic.consumer.pubsub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.ReflectionUtils;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.consumer.DaggerConsumerSchema_Depends;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.ingestion.IngestionComponent;
import com.spotify.heroic.ingestion.IngestionGroup;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.statistics.ConsumerReporter;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import javax.inject.Named;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class PubSubConsumerModule implements ConsumerModule {
    private static final int DEFAULT_THREADS_PER_SUBSCRIPTION = 8;
    private static final Long DEFAULT_MAX_OUTSTANDING_ELEMENT_COUNT = 20_000L;
    private static final Long DEFAULT_MAX_OUTSTANDING_REQUEST_BYTES = 1_000_000_000L;
    // 20MB API maximum message size.
    private static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 20 * 1024 * 1024;
    private static final Long DEFAULT_KEEP_ALIVE = 300L;

    private final Optional<String> id;
    private final int threads;
    private final ConsumerSchema schema;
    private final String projectId;
    private final String topicId;
    private final String subscriptionId;
    private final Long maxOutstandingElementCount;
    private final Long maxOutstandingRequestBytes;
    private final int maxInboundMessageSize;
    private final Long keepAlive;

    @Override
    public Exposed module(
        PrimaryComponent primary, IngestionComponent ingestion, Depends depends, String id
    ) {
        return DaggerPubSubConsumerModule_C
            .builder()
            .primaryComponent(primary)
            .ingestionComponent(ingestion)
            .depends(depends)
            .m(new M(primary, depends))
            .build();
    }

    @PubSubScope
    @Component(modules = M.class, dependencies = {
        PrimaryComponent.class, IngestionComponent.class, ConsumerModule.Depends.class
    })
    interface C extends ConsumerModule.Exposed {
        @Override
        PubSubConsumer consumer();

        @Override
        LifeCycle consumerLife();
    }

    @Module
    class M {
        private final PrimaryComponent primary;
        private final ConsumerModule.Depends depends;

        @java.beans.ConstructorProperties({ "primary", "depends" })
        public M(final PrimaryComponent primary, final Depends depends) {
            this.primary = primary;
            this.depends = depends;
        }

        @Provides
        @Named("consuming")
        @PubSubScope
        AtomicInteger consuming() {
            return new AtomicInteger();
        }

        @Provides
        @Named("total")
        @PubSubScope
        AtomicInteger total() {
            return new AtomicInteger();
        }

        @Provides
        @Named("errors")
        @PubSubScope
        AtomicLong errors() {
            return new AtomicLong();
        }

        @Provides
        @Named("consumed")
        @PubSubScope
        LongAdder consumed() {
            return new LongAdder();
        }

        @Provides
        @Named("subscriptionId")
        @PubSubScope
        String subscriptionId() {
            return subscriptionId;
        }

        @Provides
        @Named("topicId")
        @PubSubScope
        String topicId() {
            return topicId;
        }

        @Provides
        @Named("projectId")
        @PubSubScope
        String projectId() {
            return projectId;
        }

        @Provides
        @Named("maxOutstandingElementCount")
        @PubSubScope
        Long maxOutstandingElementCount() {
            return maxOutstandingElementCount;
        }

        @Provides
        @Named("maxOutstandingRequestBytes")
        @PubSubScope
        Long maxOutstandingRequestBytes() {
            return maxOutstandingRequestBytes;
        }

        @Provides
        @Named("maxInboundMessageSize")
        @PubSubScope
        int maxInboundMessageSize() {
            return maxInboundMessageSize;
        }

        @Provides
        @Named("keepAlive")
        @PubSubScope
        Long keepAlive() {
            return keepAlive;
        }

        @Provides
        @PubSubScope
        ConsumerSchema.Consumer consumer(final IngestionManager ingestionManager) {
            final IngestionGroup ingestion = ingestionManager.useDefaultGroup();

            if (ingestion.isEmpty()) {
                throw new IllegalStateException("No backends are part of the ingestion group");
            }

            final ConsumerSchema.Depends dep = DaggerConsumerSchema_Depends
                .builder()
                .primaryComponent(primary)
                .depends(depends)
                .dependsModule(new ConsumerSchema.DependsModule(ingestion))
                .build();
            final ConsumerSchema.Exposed exposed = schema.setup(dep);
            return exposed.consumer();
        }

        @Provides
        @PubSubScope
        public Managed<Connection> connection(
            final AsyncFramework async,
            final ConsumerReporter reporter,
            final ConsumerSchema.Consumer consumer,
            @Named("maxOutstandingElementCount") Long maxOutstandingElementCount,
            @Named("maxOutstandingRequestBytes") Long maxOutstandingRequestBytes,
            @Named("maxInboundMessageSize") int maxInboundMessageSize,
            @Named("keepAlive") Long keepAlive,
            @Named("consuming") AtomicInteger consuming,
            @Named("total") AtomicInteger total,
            @Named("errors") AtomicLong errors,
            @Named("consumed") LongAdder consumed
        ) {
            return async.managed(new ManagedSetup<Connection>() {
                @Override
                public AsyncFuture<Connection> construct() {
                    return async.call(() -> {
                        log.info("project:{}, topic:{}, subscription:{}",
                                 projectId, topicId, subscriptionId);
                        final Connection connection = new Connection(
                            consumer, reporter, errors, consumed,
                            projectId, topicId, subscriptionId, threads,
                            maxOutstandingElementCount, maxOutstandingRequestBytes,
                            maxInboundMessageSize, keepAlive);
                        connection.setEmulatorOptions();

                        // Create topics/subscriptions if they don't exist
                        connection.createTopic();
                        connection.createSubscription();

                        return connection.start();
                    });
                }

                @Override
                public AsyncFuture<Void> destruct(final Connection connection) {
                    connection.shutdown();
                    total.set(0);

                    return async.resolved();
                }
            });
        }

        @Provides
        @PubSubScope
        LifeCycle consumerLife(LifeCycleManager manager, PubSubConsumer consumer) {
            return manager.build(consumer);
        }
    }

    @Override
    public Optional<String> id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("pubsub#%d", i);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements ConsumerModule.Builder {
        private Optional<String> id = Optional.empty();
        private Optional<ConsumerSchema> schema = Optional.empty();
        private Optional<Integer> threads = Optional.empty();
        private Optional<String> projectId = Optional.empty();
        private Optional<String> topicId = Optional.empty();
        private Optional<String> subscriptionId = Optional.empty();
        private Optional<Long> maxOutstandingElementCount = Optional.empty();
        private Optional<Long> maxOutstandingRequestBytes = Optional.empty();
        private Optional<Integer> maxInboundMessageSize = Optional.empty();
        private Optional<Long> keepAlive = Optional.empty();

        @JsonCreator
        public Builder(
            @JsonProperty("id") Optional<String> id,
            @JsonProperty("threadsPerSubscription") Optional<Integer> threads,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("project") Optional<String> projectId,
            @JsonProperty("topic") Optional<String> topicId,
            @JsonProperty("subscription") Optional<String> subscriptionId,
            @JsonProperty("maxOutstandingElementCount") Optional<Long> maxOutstandingElementCount,
            @JsonProperty("maxOutstandingRequestBytes") Optional<Long> maxOutstandingRequestBytes,
            @JsonProperty("maxInboundMessageSize") Optional<Integer> maxInboundMessageSize,
            @JsonProperty("keepAlive") Optional<Long> keepAlive
        ) {
            this.id = id;
            this.threads = threads;
            this.schema = schema.map(s -> ReflectionUtils.buildInstance(s, ConsumerSchema.class));
            this.projectId = projectId;
            this.topicId = topicId;
            this.subscriptionId = subscriptionId;
            this.maxOutstandingElementCount = maxOutstandingElementCount;
            this.maxOutstandingRequestBytes = maxOutstandingRequestBytes;
            this.maxInboundMessageSize = maxInboundMessageSize;
            this.keepAlive = keepAlive;
        }

        private Builder() {
        }

        public Builder id(String id) {
            this.id = Optional.of(id);
            return this;
        }

        public Builder threads(int threads) {
            this.threads = Optional.of(threads);
            return this;
        }

        public Builder schema(Class<? extends ConsumerSchema> schemaClass) {
            this.schema = Optional.of(ReflectionUtils.buildInstance(schemaClass));
            return this;
        }

        public Builder schema(String schemaClass) {
            this.schema =
                Optional.of(ReflectionUtils.buildInstance(schemaClass, ConsumerSchema.class));
            return this;
        }

        public Builder projectId(String projectId) {
            this.projectId = Optional.of(projectId);
            return this;
        }

        public Builder topicId(String topicId) {
            this.topicId = Optional.of(topicId);
            return this;
        }

        public Builder subscriptionId(String subscriptionId) {
            this.subscriptionId = Optional.of(subscriptionId);
            return this;
        }

        public Builder maxOutstandingElementCount(Long count) {
            this.maxOutstandingElementCount = Optional.of(count);
            return this;
        }

        public Builder maxOutstandingRequestBytes(Long bytes) {
            this.maxOutstandingRequestBytes = Optional.of(bytes);
            return this;
        }

        public Builder maxInboundMessageSize(Integer size) {
            this.maxInboundMessageSize = Optional.of(size);
            return this;
        }

        public Builder keepAlive(Long keepAlive) {
            this.keepAlive = Optional.of(keepAlive);
            return this;
        }

        @Override
        public ConsumerModule build() {
            if (!schema.isPresent()) {
                throw new RuntimeException("Schema is not defined");
            }

            return new PubSubConsumerModule(
                id,
                threads.orElse(DEFAULT_THREADS_PER_SUBSCRIPTION),
                schema.get(),
                projectId.get(),
                topicId.get(),
                subscriptionId.get(),
                maxOutstandingElementCount.orElse(DEFAULT_MAX_OUTSTANDING_ELEMENT_COUNT),
                maxOutstandingRequestBytes.orElse(DEFAULT_MAX_OUTSTANDING_REQUEST_BYTES),
                maxInboundMessageSize.orElse(DEFAULT_MAX_INBOUND_MESSAGE_SIZE),
                keepAlive.orElse(DEFAULT_KEEP_ALIVE)
            );
        }
    }
}
