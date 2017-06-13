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

package com.spotify.heroic.consumer.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import com.spotify.heroic.time.Clock;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import javax.inject.Named;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class KafkaConsumerModule implements ConsumerModule {
    public static final int DEFAULT_THREADS_PER_TOPIC = 2;
    private static final Boolean DEFAULT_TRANSACTIONAL = false;
    public static final long DEFAULT_COMMIT_INTERVAL = TimeUnit.SECONDS.toMillis(30);
    private static final long COMMIT_INITIAL_DELAY = TimeUnit.SECONDS.toMillis(2);
    private static final String AUTO_COMMIT_ENABLE = "auto.commit.enable";

    private final Optional<String> id;
    private final List<String> topics;
    private final int threads;
    private final Map<String, String> config;
    private final ConsumerSchema schema;
    private final Boolean transactional;
    private final long transactionCommitInterval;
    private final Optional<KafkaConnection> fakeKafkaConnection;

    @Override
    public Exposed module(
        PrimaryComponent primary, IngestionComponent ingestion, Depends depends, String id
    ) {
        return DaggerKafkaConsumerModule_C
            .builder()
            .primaryComponent(primary)
            .ingestionComponent(ingestion)
            .depends(depends)
            .m(new M(primary, depends))
            .build();
    }

    @KafkaScope
    @Component(modules = M.class, dependencies = {
        PrimaryComponent.class, IngestionComponent.class, ConsumerModule.Depends.class
    })
    interface C extends ConsumerModule.Exposed {
        @Override
        KafkaConsumer consumer();

        @Override
        LifeCycle consumerLife();
    }

    @RequiredArgsConstructor
    @Module
    class M {
        private final PrimaryComponent primary;
        private final ConsumerModule.Depends depends;

        @Provides
        @Named("consuming")
        @KafkaScope
        AtomicInteger consuming() {
            return new AtomicInteger();
        }

        @Provides
        @Named("total")
        @KafkaScope
        AtomicInteger total() {
            return new AtomicInteger();
        }

        @Provides
        @Named("errors")
        @KafkaScope
        AtomicLong errors() {
            return new AtomicLong();
        }

        @Provides
        @Named("consumed")
        @KafkaScope
        LongAdder consumed() {
            return new LongAdder();
        }

        @Provides
        @Named("config")
        @KafkaScope
        Map<String, String> config() {
            return config;
        }

        @Provides
        @Named("topics")
        @KafkaScope
        List<String> topics() {
            return topics;
        }

        @Provides
        @Named("transactional")
        @KafkaScope
        Boolean transactional() {
            return transactional;
        }

        @Provides
        @Named("transactionCommitInterval")
        @KafkaScope
        Long transactionCommitInterval() {
            return transactionCommitInterval;
        }

        @Provides
        @Named("fakeKafkaConnection")
        @KafkaScope
        Optional<KafkaConnection> fakeKafkaConnection() {
            return fakeKafkaConnection;
        }

        @Provides
        @KafkaScope
        ConsumerSchema.Consumer consumer(final IngestionManager ingestionManager) {
            // XXX: make target group configurable?
            final IngestionGroup ingestion = ingestionManager.useDefaultGroup();

            if (ingestion.isEmpty()) {
                throw new IllegalStateException("No backends are part of the ingestion group");
            }

            final ConsumerSchema.Depends d = DaggerConsumerSchema_Depends
                .builder()
                .primaryComponent(primary)
                .depends(depends)
                .dependsModule(new ConsumerSchema.DependsModule(ingestion))
                .build();

            final ConsumerSchema.Exposed exposed = schema.setup(d);
            return exposed.consumer();
        }

        @Provides
        @KafkaScope
        public Managed<Connection> connection(
            final AsyncFramework async, final Clock clock, final ConsumerReporter reporter,
            final ConsumerSchema.Consumer consumer, @Named("consuming") AtomicInteger consuming,
            @Named("total") AtomicInteger total, @Named("errors") AtomicLong errors,
            @Named("consumed") LongAdder consumed,
            @Named("transactional") final Boolean transactional,
            @Named("transactionCommitInterval") final Long transactionCommitInterval,
            @Named("fakeKafkaConnection") final Optional<KafkaConnection> fakeKafkaConnection
        ) {
            return async.managed(new ManagedSetup<Connection>() {
                @Override
                public AsyncFuture<Connection> construct() {
                    return async.call(() -> {
                        log.info("Starting");
                        KafkaConnection kafkaConnection =
                            fakeKafkaConnection.orElseGet(() -> createKafkaConnection());

                        final Map<String, Integer> streamsMap = makeStreams();

                        final Map<String, List<KafkaStream<byte[]>>> streams =
                            kafkaConnection.createMessageStreams(streamsMap);

                        final AtomicLong nextOffsetsCommitTS = new AtomicLong(
                            clock.currentTimeMillis() +
                                Math.min(COMMIT_INITIAL_DELAY, transactionCommitInterval));

                        final List<ConsumerThread> threads =
                            buildThreads(async, clock, reporter, streams, consumer, consuming,
                                errors, consumed, transactional, transactionCommitInterval,
                                nextOffsetsCommitTS);

                        // Report the wanted count of threads before starting the threads below
                        reporter.reportConsumerThreadsWanted(threads.size());

                        total.set(threads.size());

                        final Connection connection =
                            new Connection(async, reporter, kafkaConnection, threads);
                        ConsumerThreadCoordinator coordinator = connection;

                        for (final ConsumerThread thread : threads) {
                            thread.setCoordinator(coordinator);
                            thread.start();
                        }

                        return connection;
                    });
                }

                @Override
                public AsyncFuture<Void> destruct(final Connection value) {
                    final List<AsyncFuture<Void>> shutdown = ImmutableList.copyOf(
                        value.getThreads().stream().map(ConsumerThread::shutdown).iterator());

                    value.getConnection().shutdown();

                    total.set(0);

                    return async.collectAndDiscard(shutdown);
                }

                /* private */

                private KafkaConnection createKafkaConnection() {
                    final Properties properties = new Properties();
                    properties.putAll(config);

                    if (transactional) {
                        final String autoCommitEnable = properties.getProperty(AUTO_COMMIT_ENABLE);
                        if (autoCommitEnable != null && autoCommitEnable.equals("true")) {
                            throw new IllegalArgumentException(
                                "'transactional' and " + AUTO_COMMIT_ENABLE +
                                    " are mutually exclusive");
                        }
                    }

                    final ConsumerConfig config = new ConsumerConfig(properties);
                    final ConsumerConnector connector =
                        Consumer.createJavaConsumerConnector(config);

                    final RealKafkaConnection kafkaConnection = new RealKafkaConnection(connector);

                    return kafkaConnection;
                }

                private Map<String, Integer> makeStreams() {
                    final Map<String, Integer> streamsMap = new HashMap<String, Integer>();

                    for (final String topic : topics) {
                        streamsMap.put(topic, threads);
                    }

                    return streamsMap;
                }
            });
        }

        @Provides
        @KafkaScope
        LifeCycle life(LifeCycleManager manager, KafkaConsumer consumer) {
            return manager.build(consumer);
        }
    }

    private List<ConsumerThread> buildThreads(
        final AsyncFramework async, final Clock clock, final ConsumerReporter reporter,
        final Map<String, List<KafkaStream<byte[]>>> streams,
        final ConsumerSchema.Consumer consumer, final AtomicInteger consuming,
        final AtomicLong errors, final LongAdder consumed, final boolean enablePeriodicCommit,
        final long periodicCommitInterval, final AtomicLong nextOffsetsCommitTS
    ) {
        final List<ConsumerThread> threads = new ArrayList<>();

        final Set<Map.Entry<String, List<KafkaStream<byte[]>>>> entries = streams.entrySet();

        for (final Map.Entry<String, List<KafkaStream<byte[]>>> entry : entries) {
            final String topic = entry.getKey();
            final List<KafkaStream<byte[]>> list = entry.getValue();

            int count = 0;

            for (final KafkaStream<byte[]> stream : list) {
                final String name = String.format("%s:%d", topic, count++);

                threads.add(
                    new ConsumerThread(async, clock, name, reporter, stream, consumer, consuming,
                        errors, consumed, enablePeriodicCommit, periodicCommitInterval,
                        nextOffsetsCommitTS));
            }
        }

        return threads;
    }

    @Override
    public Optional<String> id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("kafka#%d", i);
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder implements ConsumerModule.Builder {
        private Optional<String> id = Optional.empty();
        private Optional<List<String>> topics = Optional.empty();
        private Optional<Integer> threads = Optional.empty();
        private Optional<Map<String, String>> config = Optional.empty();
        private Optional<ConsumerSchema> schema = Optional.empty();
        private Optional<Boolean> transactional = Optional.empty();
        private Optional<Long> transactionCommitInterval = Optional.empty();
        private Optional<KafkaConnection> fakeKafkaConnection = Optional.empty();

        @JsonCreator
        public Builder(
            @JsonProperty("id") Optional<String> id,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("topics") Optional<List<String>> topics,
            @JsonProperty("threadsPerTopic") Optional<Integer> threads,
            @JsonProperty("config") Optional<Map<String, String>> config,
            @JsonProperty("transactional") Optional<Boolean> transactional,
            @JsonProperty("transactionCommitInterval") Optional<Long> transactionCommitInterval
        ) {
            this.id = id;
            this.threads = threads;
            this.topics = topics;
            this.config = config;
            this.schema = schema.map(s -> ReflectionUtils.buildInstance(s, ConsumerSchema.class));
            this.transactional = transactional;
            this.transactionCommitInterval = transactionCommitInterval;
        }

        public Builder id(String id) {
            this.id = Optional.of(id);
            return this;
        }

        public Builder topics(List<String> topics) {
            this.topics = Optional.of(topics);
            return this;
        }

        public Builder threads(int threads) {
            this.threads = Optional.of(threads);
            return this;
        }

        public Builder config(Map<String, String> config) {
            this.config = Optional.of(config);
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

        public Builder transactional(Boolean enabled) {
            this.transactional = Optional.of(enabled);
            return this;
        }

        public Builder transactionCommitInterval(long ms) {
            this.transactionCommitInterval = Optional.of(ms);
            return this;
        }

        public Builder fakeKafkaConnection(KafkaConnection fakeKafkaConnection) {
            this.fakeKafkaConnection = Optional.of(fakeKafkaConnection);
            return this;
        }

        @Override
        public ConsumerModule build() {
            if (topics.map(Collection::isEmpty).orElse(true)) {
                throw new RuntimeException("No topics are defined");
            }

            if (!schema.isPresent()) {
                throw new RuntimeException("Schema is not defined");
            }

            // @formatter:off
            return new KafkaConsumerModule(
                id,
                topics.get(),
                threads.orElse(DEFAULT_THREADS_PER_TOPIC),
                config.orElseGet(ImmutableMap::of),
                schema.get(),
                transactional.orElse(DEFAULT_TRANSACTIONAL),
                transactionCommitInterval.orElse(DEFAULT_COMMIT_INTERVAL),
                fakeKafkaConnection
            );
            // @formatter:on
        }
    }
}
