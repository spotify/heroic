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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.Managed;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@KafkaScope
public class KafkaConsumer implements Consumer, LifeCycles {
    private final AsyncFramework async;
    private final Managed<Connection> connection;

    private final AtomicInteger consuming;
    private final AtomicInteger total;
    private final AtomicLong errors;
    private final LongAdder consumed;
    private final List<String> topics;
    private final Map<String, String> config;

    @Inject
    public KafkaConsumer(
        AsyncFramework async, Managed<Connection> connection,
        @Named("consuming") AtomicInteger consuming, @Named("total") AtomicInteger total,
        @Named("errors") AtomicLong errors, @Named("consumed") LongAdder consumed,
        @Named("topics") List<String> topics, @Named("config") Map<String, String> config
    ) {
        this.async = async;
        this.connection = connection;
        this.consuming = consuming;
        this.total = total;
        this.errors = errors;
        this.consumed = consumed;
        this.topics = topics;
        this.config = config;
    }

    @Override
    public void register(LifeCycleRegistry registry) {
        registry.start(connection::start);
        registry.stop(connection::stop);
    }

    @Override
    public boolean isReady() {
        return connection.isReady();
    }

    @Override
    public Statistics getStatistics() {
        final long consuming = this.consuming.get();
        final long total = this.total.get();
        final long errors = this.errors.get();
        final long consumed = this.consumed.sum();

        return Statistics.of(
            ImmutableMap.<String, Long>of(CONSUMING, consuming, TOTAL, total, ERRORS, errors,
                CONSUMED, consumed));
    }

    @Override
    public AsyncFuture<Void> pause() {
        // pause all threads
        return connection.doto(c -> async.collectAndDiscard(ImmutableList.copyOf(
            c.getThreads().stream().map(ConsumerThread::pauseConsumption).iterator())));
    }

    @Override
    public AsyncFuture<Void> resume() {
        // resume all threads
        return connection.doto(c -> async.collectAndDiscard(ImmutableList.copyOf(
            c.getThreads().stream().map(ConsumerThread::resumeConsumption).iterator())));
    }

    @Override
    public String toString() {
        final Borrowed<Connection> b = connection.borrow();

        if (!b.isValid()) {
            return String.format("KafkaConsumer(non-configured, topics=%s, config=%s)", topics,
                config);
        }

        try {
            final Connection c = b.get();
            final int threads = c.getThreads().size();
            final int paused = c.getThreads().stream().mapToInt(t -> t.isPaused() ? 1 : 0).sum();
            return String.format(
                "KafkaConsumer(configured, topics=%s, config=%s, threads=%d, " + "paused=%d)",
                topics, config, threads, paused);
        } finally {
            b.release();
        }
    }
}
