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

import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.google.common.collect.ImmutableMap;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import javax.inject.Inject;
import javax.inject.Named;

@PubSubScope
public class PubSubConsumer implements Consumer, LifeCycles {
    private final AsyncFramework async;
    private final Managed<Connection> connection;

    private final AtomicInteger consuming;
    private final AtomicInteger total;
    private final AtomicLong errors;
    private final LongAdder consumed;

    @Inject
    PubSubConsumer(
        AsyncFramework async, Managed<Connection> connection,
        @Named("consuming") AtomicInteger consuming, @Named("total") AtomicInteger total,
        @Named("errors") AtomicLong errors, @Named("consumed") LongAdder consumed
    ) {
        this.async = async;
        this.connection = connection;
        this.consuming = consuming;
        this.total = total;
        this.errors = errors;
        this.consumed = consumed;
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
            ImmutableMap.of(CONSUMING, consuming, TOTAL, total, ERRORS, errors,
                CONSUMED, consumed));
    }

    @Override
    public AsyncFuture<Void> pause() {
        throw new RuntimeException("Pausing not implemented for PubSub consumer");
    }

    @Override
    public AsyncFuture<Void> resume() {
        return null;
    }

    @Override
    public String toString() {
        return "PubSubConsumer";
    }
}
