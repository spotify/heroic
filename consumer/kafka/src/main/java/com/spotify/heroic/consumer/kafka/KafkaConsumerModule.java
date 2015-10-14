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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.common.ReflectionUtils;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.statistics.ConsumerReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import eu.toolchain.async.ResolvableFuture;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
    public Module module(final Key<Consumer> key, final ConsumerReporter reporter) {
        final AtomicInteger consuming = new AtomicInteger();
        final AtomicInteger total = new AtomicInteger();
        final AtomicLong errors = new AtomicLong();

        return new PrivateModule() {
            @Provides
            public Managed<Connection> connection(final AsyncFramework async, final Consumer consumer) {
                return async.managed(new ManagedSetup<Connection>() {
                    /**
                     * Latch that will be set when we want to shut down.
                     */
                    private final CountDownLatch stopSignal = new CountDownLatch(1);

                    @Override
                    public AsyncFuture<Connection> construct() {
                        return async.call(new Callable<Connection>() {
                            @Override
                            public Connection call() throws Exception {
                                log.info("Starting");
                                final Properties properties = new Properties();
                                properties.putAll(config);

                                final ConsumerConfig config = new ConsumerConfig(properties);
                                final ConsumerConnector connector = kafka.consumer.Consumer
                                        .createJavaConsumerConnector(config);

                                final Map<String, Integer> streamsMap = makeStreams();

                                final Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector
                                        .createMessageStreams(streamsMap);

                                final List<ConsumerThread> threads = buildThreads(reporter, consumer, streams);

                                for (final ConsumerThread t : threads)
                                    t.start();

                                total.set(threads.size());
                                return new Connection(connector, threads);
                            }
                        });
                    }

                    @Override
                    public AsyncFuture<Void> destruct(final Connection value) {
                        return async.call(shutdownConnector(value)).lazyTransform(new LazyTransform<Void, Void>() {
                            @Override
                            public AsyncFuture<Void> transform(Void arg0) throws Exception {
                                final List<AsyncFuture<Void>> shutdown = new ArrayList<>();

                                for (final ConsumerThread t : value.getThreads())
                                    shutdown.add(t.stopFuture);

                                total.set(0);
                                return async.collectAndDiscard(shutdown);
                            }
                        });
                    }

                    private Callable<Void> shutdownConnector(final Connection value) {
                        stopSignal.countDown();

                        return new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                // tell threads to shut down.

                                value.getConnector().shutdown();
                                log.info("Waiting for all threads to shut down");

                                return null;
                            }
                        };
                    }

                    /* private */

                    private Map<String, Integer> makeStreams() {
                        final Map<String, Integer> streamsMap = new HashMap<String, Integer>();

                        for (final String topic : topics)
                            streamsMap.put(topic, threads);

                        return streamsMap;
                    }

                    private List<ConsumerThread> buildThreads(final ConsumerReporter reporter, final Consumer consumer,
                            final Map<String, List<KafkaStream<byte[], byte[]>>> streams) {
                        final List<ConsumerThread> threads = new ArrayList<>();

                        for (final Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : streams.entrySet()) {
                            final String topic = entry.getKey();
                            final List<KafkaStream<byte[], byte[]>> list = entry.getValue();

                            int count = 0;

                            for (final KafkaStream<byte[], byte[]> stream : list) {
                                final String name = String.format("%s:%d", topic, count++);
                                final ResolvableFuture<Void> stopFuture = async.future();

                                threads.add(new ConsumerThread(name, reporter, stream, consumer, schema, consuming,
                                        errors, stopSignal, stopFuture));
                            }
                        }

                        return threads;
                    }
                });
            }

            @Override
            protected void configure() {
                bind(ConsumerReporter.class).toInstance(reporter);
                bind(Consumer.class).toInstance(new KafkaConsumer(consuming, total, errors));
                bind(key).to(Consumer.class).in(Scopes.SINGLETON);
                expose(key);
            }
        };
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

    @NoArgsConstructor(access=AccessLevel.PRIVATE)
    @AllArgsConstructor(access=AccessLevel.PRIVATE)
    public static class Builder implements ConsumerModule.Builder {
        private Optional<String> id = Optional.empty();
        private Optional<List<String>> topics = Optional.empty();
        private Optional<Integer> threads = Optional.empty();
        private Optional<Map<String, String>> config = Optional.empty();
        private Optional<ConsumerSchema> schema = Optional.empty();

        @JsonCreator
        public Builder(@JsonProperty("id") String id, @JsonProperty("schema") String schema,
                @JsonProperty("topics") List<String> topics, @JsonProperty("threadsPerTopic") Integer threads,
                @JsonProperty("config") Map<String, String> config) {
            this.id = Optional.ofNullable(id);
            this.threads = Optional.ofNullable(threads);
            this.topics = Optional.ofNullable(topics);
            this.config = Optional.ofNullable(config);
            this.schema = Optional.ofNullable(schema).map(s -> ReflectionUtils.buildInstance(s, ConsumerSchema.class));
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

        public Builder schema(String schemaClass) {
            this.schema = Optional.of(ReflectionUtils.buildInstance(schemaClass, ConsumerSchema.class));
            return this;
        }

        @Override
        public ConsumerModule.Builder merge(final ConsumerModule.Builder u) {
            final Builder o = (Builder) u;

            // @formatter:off
            return new Builder(
                o.id.isPresent() ? o.id : id,
                o.topics.isPresent() ? o.topics : topics,
                o.threads.isPresent() ? o.threads : threads,
                o.config.isPresent() ? o.config : config,
                o.schema.isPresent() ? o.schema : schema
            );
            // @formatter:on
        }

        @Override
        public ConsumerModule build() {
            if (topics.map(Collection::isEmpty).orElse(true))
                throw new RuntimeException("No topics are defined");

            if (!schema.isPresent())
                throw new RuntimeException("Schema is not defined");

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
