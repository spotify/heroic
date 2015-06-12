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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.utils.ReflectionUtils;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import eu.toolchain.async.ResolvableFuture;

@Slf4j
public class KafkaConsumerModule implements ConsumerModule {
    public static final int DEFAULT_THREADS_PER_TOPIC = 2;
    public static final Map<String, String> DEFAULT_CONFIG = new HashMap<String, String>();

    private final String id;
    private final int threads;
    private final List<String> topics;
    private final Map<String, String> config;
    private final ConsumerSchema schema;

    private final AtomicInteger consuming = new AtomicInteger();
    private final AtomicInteger total = new AtomicInteger();
    private final AtomicLong errors = new AtomicLong();

    @JsonCreator
    public KafkaConsumerModule(@JsonProperty("id") String id,
            @JsonProperty("threadsPerTopic") Integer threads, @JsonProperty("topics") List<String> topics,
            @JsonProperty("config") Map<String, String> config, @JsonProperty("schema") String schema) {
        checkArgument(topics != null && !topics.isEmpty(), "'topics' must be defined and non-empty", topics);

        this.id = id;
        this.threads = Optional.fromNullable(threads).or(DEFAULT_THREADS_PER_TOPIC);
        this.topics = topics;
        this.config = Optional.fromNullable(config).or(DEFAULT_CONFIG);
        this.schema = ReflectionUtils.buildInstance(checkNotNull(schema), ConsumerSchema.class);
    }

    @Override
    public Module module(final Key<Consumer> key, final ConsumerReporter reporter) {
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
    public String id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("kafka#%d", i);
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String id;
        private Integer threads;
        private List<String> topics;
        private Map<String, String> config;
        private String schema;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder threads(int threads) {
            this.threads = threads;
            return this;
        }

        public Builder topics(List<String> topics) {
            this.topics = topics;
            return this;
        }

        public Builder config(Map<String, String> config) {
            this.config = config;
            return this;
        }

        public Builder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public KafkaConsumerModule build() {
            return new KafkaConsumerModule(id, threads, topics, config, schema);
        }
    }
}
