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

import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.HeroicTimer;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class Connection implements ConsumerThreadCoordinator {
    private final AsyncFramework async;
    private final ConsumerReporter reporter;

    private final ConsumerConnector connector;
    private final List<ConsumerThread> threads;

    private final AtomicReference<Boolean> pleaseCommitConsumerOffsets = new AtomicReference<>();

    private HeroicTimer.Context wholeOperationTimer;
    private HeroicTimer.Context writeCompletionTimer;

    public AsyncFuture<Void> pause() {
        final List<AsyncFuture<Void>> perThread = new ArrayList<>();
        for (final ConsumerThread t : threads) {
            perThread.add(t.pauseConsumption());
        }
        return async.collectAndDiscard(perThread);
    }

    public AsyncFuture<Void> resume() {
        final List<AsyncFuture<Void>> perThread = new ArrayList<>();
        for (final ConsumerThread t : threads) {
            perThread.add(t.resumeConsumption());
        }
        return async.collectAndDiscard(perThread);
    }

    public void prepareToCommitConsumerOffsets() {
        synchronized (pleaseCommitConsumerOffsets) {
            log.info("Consumer offsets commit: Preparing");
            wholeOperationTimer = reporter.reportConsumerCommitOperation();
            writeCompletionTimer = reporter.reportConsumerCommitPhase1();
            pleaseCommitConsumerOffsets.set(true);
            pause();
        }
    }

    public boolean isPreparingToCommitConsumerOffsets() {
        return !(pleaseCommitConsumerOffsets.get() == null ||
            !pleaseCommitConsumerOffsets.get().booleanValue());
    }

    public void commitConsumerOffsets() {
        if (pleaseCommitConsumerOffsets.get() == null) {
            return;
        }
        // Verify that all threads have 0 outstanding consumption requests
        for (final ConsumerThread t : threads) {
            if (t.getNumOutstandingRequests() > 0) {
                return;
            }
        }

        synchronized (pleaseCommitConsumerOffsets) {
            final Boolean valueRef = pleaseCommitConsumerOffsets.get();
            if (valueRef == null || !valueRef.booleanValue()) {
                // pleaseCommitConsumerOffsets changed value while we tried to take the lock
                return;
            }

            writeCompletionTimer.stop();
            writeCompletionTimer = null;
            log.info("Consumer offsets commit: All writes complete");

            /* 1) We've asked threads to pause. The ones that are relevant (actively processing
             * work) will at this point be paused.
             * 2) There's no more outstanding writes
             * == This is a good time to commit offsets. */

            final HeroicTimer.Context offsetsCommitTimer = reporter.reportConsumerCommitPhase2();

            // Commit
            connector.commitOffsets();

            offsetsCommitTimer.stop();
            log.info("Consumer offsets commit: Done committing");

            resume();
            pleaseCommitConsumerOffsets.set(null);
            wholeOperationTimer.stop();
            wholeOperationTimer = null;
        }
    }
}
