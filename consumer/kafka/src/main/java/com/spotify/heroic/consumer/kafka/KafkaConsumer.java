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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.spotify.heroic.common.BackendGroupException;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString(of={"connection"})
public class KafkaConsumer implements Consumer {
    @Inject
    private IngestionManager ingestion;

    @Inject
    private AsyncFramework async;

    @Inject
    private Managed<Connection> connection;

    private final AtomicInteger consuming;
    private final AtomicInteger total;
    private final AtomicLong errors;
    private final LongAdder consumed;

    public KafkaConsumer(AtomicInteger consuming, AtomicInteger total, AtomicLong errors, LongAdder consumed) {
        this.consuming = consuming;
        this.total = total;
        this.errors = errors;
        this.consumed = consumed;
    }

    @Override
    public AsyncFuture<Void> start() {
        return connection.start();
    }

    @Override
    public AsyncFuture<Void> stop() {
        return connection.stop();
    }

    @Override
    public boolean isReady() {
        return connection.isReady();
    }

    @Override
    public AsyncFuture<WriteResult> write(WriteMetric write) {
        try {
            return ingestion.write(null, write);
        } catch (BackendGroupException e) {
            log.error("Unable to find group to write to", e);
            // still counts as a valid write.
            return async.resolved(null);
        }
    }

    @Override
    public Statistics getStatistics() {
        final long consuming = this.consuming.get();
        final long total = this.total.get();
        final long errors = this.errors.get();
        final long consumed = this.consumed.sum();

        return Statistics.of(
                ImmutableMap.<String, Long> of(CONSUMING, consuming, TOTAL, total, ERRORS, errors, CONSUMED, consumed));
    }
}
