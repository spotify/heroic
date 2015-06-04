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

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.google.inject.Inject;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.exceptions.BackendGroupException;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.statistics.ConsumerReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;

@Slf4j
@ToString
public class KafkaConsumer implements Consumer {
    @Inject
    private IngestionManager ingestion;

    @Inject
    private ConsumerReporter reporter;

    @Inject
    private AsyncFramework async;

    @Inject
    private Managed<Connection> connection;

    private final AtomicInteger consuming;
    private final AtomicInteger total;
    private final AtomicLong errors;

    public KafkaConsumer(AtomicInteger consuming, AtomicInteger total, AtomicLong errors) {
        this.consuming = consuming;
        this.total = total;
        this.errors = errors;
    }

    @Override
    public AsyncFuture<Void> start() throws Exception {
        return connection.start();
    }

    @Override
    public AsyncFuture<Void> stop() throws Exception {
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
        int consuming = this.consuming.get();
        int total = this.total.get();
        final boolean ok = consuming == total;
        return new Statistics(ok, errors.get(), consuming, total);
    }
}
