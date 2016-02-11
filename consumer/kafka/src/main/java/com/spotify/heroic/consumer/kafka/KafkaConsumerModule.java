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
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
@Data
public class KafkaConsumerModule implements ConsumerModule {
    public static final int DEFAULT_THREADS_PER_TOPIC = 2;

    private final Optional<String> id;
    private final List<String> topics;
    private final int threads;
    private final Map<String, String> config;
    private final ConsumerSchema schema;

    @Override
    public Out module(PrimaryComponent primary, IngestionComponent ingestion, In in, String id) {
        return DaggerKafkaConsumerModule_C
            .builder()
            .primaryComponent(primary)
            .ingestionComponent(ingestion)
            .in(in)
            .m(new M())
            .build();
    }

    @KafkaScope
    @Component(modules = M.class,
        dependencies = {PrimaryComponent.class, IngestionComponent.class, ConsumerModule.In.class})
    interface C extends ConsumerModule.Out {
        @Override
        KafkaConsumer consumer();

        @Override
        LifeCycle consumerLife();
    }

    @Module
    class M {
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
        @KafkaScope
        public Managed<Connection> connection(
            final AsyncFramework async, final IngestionManager ingestionManager,
            final ConsumerReporter reporter, @Named("consuming") AtomicInteger consuming,
            @Named("total") AtomicInteger total, @Named("errors") AtomicLong errors,
            @Named("consumed") LongAdder consumed
        ) {
            return async.managed(new ManagedSetup<Connection>() {
                @Override
                public AsyncFuture<Connection> construct() {
                    // XXX: make target group configurable?
                    final IngestionGroup ingestion = ingestionManager.useDefaultGroup();

                    if (ingestion.isEmpty()) {
                        throw new IllegalStateException(
                            "No backends are part of the ingestion group");
                    }

                    return async.call(() -> {
                        log.info("Starting");
                        final Properties properties = new Properties();
                        properties.putAll(config);

                        final ConsumerConfig config = new ConsumerConfig(properties);
                        final ConsumerConnector connector =
                            kafka.consumer.Consumer.createJavaConsumerConnector(config);

                        final Map<String, Integer> streamsMap = makeStreams();

                        final Map<String, List<KafkaStream<byte[], byte[]>>> streams =
                            connector.createMessageStreams(streamsMap);

                        final List<ConsumerThread> threads =
                            buildThreads(async, reporter, ingestion, streams, consuming, errors,
                                consumed);

                        for (final ConsumerThread t : threads) {
                            t.start();
                        }

                        total.set(threads.size());
                        return new Connection(connector, threads);
                    });
                }

                @Override
                public AsyncFuture<Void> destruct(final Connection value) {
                    value.getConnector().shutdown();

                    total.set(0);

                    final List<AsyncFuture<Void>> shutdown = ImmutableList.copyOf(
                        value.getThreads().stream().map(ConsumerThread::shutdown).iterator());

                    return async.collectAndDiscard(shutdown);
                }

                /* private */

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
        final AsyncFramework async, final ConsumerReporter reporter, final IngestionGroup ingestion,
        final Map<String, List<KafkaStream<byte[], byte[]>>> streams, AtomicInteger consuming,
        AtomicLong errors, LongAdder consumed
    ) {
        final List<ConsumerThread> threads = new ArrayList<>();

        final Set<Map.Entry<String, List<KafkaStream<byte[], byte[]>>>> entries =
            streams.entrySet();

        for (final Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : entries) {
            final String topic = entry.getKey();
            final List<KafkaStream<byte[], byte[]>> list = entry.getValue();

            int count = 0;

            for (final KafkaStream<byte[], byte[]> stream : list) {
                final String name = String.format("%s:%d", topic, count++);

                threads.add(
                    new ConsumerThread(async, ingestion, name, reporter, stream, schema, consuming,
                        errors, consumed));
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

        @JsonCreator
        public Builder(
            @JsonProperty("id") Optional<String> id,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("topics") Optional<List<String>> topics,
            @JsonProperty("threadsPerTopic") Optional<Integer> threads,
            @JsonProperty("config") Optional<Map<String, String>> config
        ) {
            this.id = id;
            this.threads = threads;
            this.topics = topics;
            this.config = config;
            this.schema = schema.map(s -> ReflectionUtils.buildInstance(s, ConsumerSchema.class));
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

        public Builder schema(Class<ConsumerSchema> schemaClass) {
            this.schema = Optional.of(ReflectionUtils.buildInstance(schemaClass));
            return this;
        }

        public Builder schema(String schemaClass) {
            this.schema =
                Optional.of(ReflectionUtils.buildInstance(schemaClass, ConsumerSchema.class));
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
                schema.get()
            );
            // @formatter:on
        }
    }
}
