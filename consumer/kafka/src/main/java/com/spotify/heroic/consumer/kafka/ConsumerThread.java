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

import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.consumer.ConsumerSchemaValidationException;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.time.Clock;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ConsumerThread extends Thread {
    private static final long RETRY_INITIAL_SLEEP = 5;
    private static final long RETRY_MAX_SLEEP = 40;

    private final AsyncFramework async;
    private final Clock clock;
    private final String name;
    private final ConsumerReporter reporter;
    private final KafkaStream<byte[]> stream;
    private final ConsumerSchema.Consumer schema;
    private final AtomicInteger active;
    private final AtomicLong errors;
    private final LongAdder consumed;
    // use a latch as a signal so that we can block on it instead of Thread#sleep (or similar) which
    // would be a pain to
    // interrupt.
    private final CountDownLatch shouldStop = new CountDownLatch(1);

    protected final ResolvableFuture<Void> hasStopped;

    private volatile AtomicReference<CountDownLatch> shouldPause = new AtomicReference<>();

    private final boolean enablePeriodicCommit;

    private final AtomicLong outstandingConsumptionRequests = new AtomicLong(0);

    private final long periodicCommitInterval;
    // Timestamp specifying when the next consumer commit should happen
    private final AtomicLong nextOffsetsCommitTSGlobal;
    // Thread-local copy of the above timestamp
    private long nextOffsetsCommitTSThreadLocal;

    @Setter
    private ConsumerThreadCoordinator coordinator;

    public ConsumerThread(
        final AsyncFramework async, final Clock clock, final String name,
        final ConsumerReporter reporter, final KafkaStream<byte[]> stream,
        final ConsumerSchema.Consumer schema, final AtomicInteger active, final AtomicLong errors,
        final LongAdder consumed, final boolean enablePeriodicCommit,
        final long periodicCommitInterval, final AtomicLong nextOffsetsCommitTSGlobal
    ) {
        super(String.format("%s: %s", ConsumerThread.class.getCanonicalName(), name));

        this.async = async;
        this.clock = clock;
        this.name = name;
        this.reporter = reporter;
        this.stream = stream;
        this.schema = schema;
        this.active = active;
        this.errors = errors;
        this.consumed = consumed;
        this.enablePeriodicCommit = enablePeriodicCommit;

        this.periodicCommitInterval = periodicCommitInterval;
        this.nextOffsetsCommitTSGlobal = nextOffsetsCommitTSGlobal;
        this.nextOffsetsCommitTSThreadLocal = this.nextOffsetsCommitTSGlobal.get();
        this.coordinator = null;

        this.hasStopped = async.future();
    }

    @Override
    public void run() {
        log.info("{}: Starting thread", name);

        threadIsStarting();

        try {
            guardedRun();
        } catch (final Throwable e) {
            log.error("{}: Error in thread", name, e);
            threadIsStopping();
            hasStopped.fail(e);
            return;
        }

        log.info("{}: Stopping thread", name);
        threadIsStopping();
        hasStopped.resolve(null);
        return;
    }

    private void threadIsStarting() {
        active.incrementAndGet();
        reporter.reportConsumerThreadsIncrement();
    }

    private void threadIsStopping() {
        active.decrementAndGet();
        reporter.reportConsumerThreadsDecrement();
    }

    public AsyncFuture<Void> pauseConsumption() {
        final CountDownLatch old = this.shouldPause.getAndSet(new CountDownLatch(1));

        if (old != null) {
            old.countDown();
        }

        return async.resolved();
    }

    public AsyncFuture<Void> resumeConsumption() {
        final CountDownLatch old = this.shouldPause.getAndSet(null);

        if (old != null) {
            old.countDown();
        }

        return async.resolved();
    }

    public boolean isPausing() {
        return this.shouldPause.get() != null;
    }

    public long getNumOutstandingRequests() {
        return outstandingConsumptionRequests.get();
    }

    public AsyncFuture<Void> shutdown() {
        shouldStop.countDown();

        final CountDownLatch old = this.shouldPause.getAndSet(null);

        if (old != null) {
            old.countDown();
        }

        return hasStopped;
    }

    private void guardedRun() throws Exception {
        for (final byte[] messageBody : stream.messageIterable()) {
            if (shouldStop.getCount() == 0 || messageBody == null) {
                // Kafka will send a null message when connection is closing
                break;
            }

            consumeOneWithRetry(messageBody);

            maybePause();
            if (shouldStop.getCount() == 0) {
                break;
            }
        }
    }

    private void maybePause() throws InterruptedException {
        CountDownLatch p = shouldPause.get();

        if (p == null) {
            return;
        }

        log.info("Pausing");

        /* block on stop signal while shouldPause, re-check since multiple calls to
         * {#link #pauseConsumption()} might swap it */
        while (p != null && shouldStop.getCount() > 0) {
            p.await();
            p = shouldPause.get();
        }

        log.info("Resuming");
    }

    private void consumeOneWithRetry(final byte[] body) throws InterruptedException {
        long sleep = RETRY_INITIAL_SLEEP;

        while (shouldStop.getCount() > 0) {
            final boolean retry = consumeOne(body);

            if (retry) {
                handleRetry(sleep);
                sleep = Math.min(sleep * 2, RETRY_MAX_SLEEP);
                continue;
            }

            break;
        }
    }

    private boolean consumeOne(final byte[] body) {
        try {
            /* We have read something. This is a good time to check if we should prepare to commit.
             * Why is it a good time? Because if we pause now, then send off one more consumption
             * request, then we know that there will be _at least one_ write being finished sometime
             * soon so we can use onFinished() to do the commit. */
            maybePrepareToCommitConsumerOffsets();

            final FutureReporter.Context consumptionContext = reporter.reportConsumption();

            // Actually consume
            final AsyncFuture<Void> future = schema.consume(body);

            if (enablePeriodicCommit) {
                outstandingConsumptionRequests.incrementAndGet();

                future.onFinished(() -> {
                    long value = outstandingConsumptionRequests.decrementAndGet();
                    if (value == 0) {
                        // If applicable, commit consumer offsets
                        coordinator.commitConsumerOffsets();
                    }
                });
            }

            future.onDone(consumptionContext);

            reporter.reportMessageSize(body.length);
            consumed.increment();

            return false;
        } catch (final ConsumerSchemaValidationException e) {
            /* these messages should be ignored */
            reporter.reportConsumerSchemaError();
            return false;
        } catch (final Exception e) {
            errors.incrementAndGet();
            log.error("{}: Failed to consume", name, e);
            reporter.reportMessageError();
            return true;
        }
    }

    /* There's a timestamp, nextOffsetsCommitTSGlobal, saying when we should commit consumer offsets
     * the next time. This method looks at a thread local cached copy of that timestamp, to make
     * this check as fast as possible. When the cached copy says that we should commit, we check the
     * actual nextOffsetsCommitTSGlobal to see if the value has changed (if some other thread has
     * already done a commit).
     */
    private boolean maybePrepareToCommitConsumerOffsets() {
        if (!enablePeriodicCommit) {
            return false;
        }

        final Long currTS = clock.currentTimeMillis();

        if (nextOffsetsCommitTSThreadLocal > currTS) {
            return false;
        }
        /* Thread local cached copy of nextOffsetsCommitTSGlobal says that it's time. Double check
         * with the actual nextOffsetsCommitTSGlobal. */

        synchronized (nextOffsetsCommitTSGlobal) {
            // We now have exclusive access to nextOffsetsCommitTSGlobal
            final Long nextOffsetsCommitTSGlobalValue = nextOffsetsCommitTSGlobal.get();
            nextOffsetsCommitTSThreadLocal = nextOffsetsCommitTSGlobalValue;

            if (nextOffsetsCommitTSGlobalValue > currTS) {
                return false;
            }

            final Long nextTS = currTS + periodicCommitInterval;
            nextOffsetsCommitTSGlobal.set(nextTS);
            nextOffsetsCommitTSThreadLocal = nextTS;

            /* This will ask all threads to pause and will set us up for committing when all writes
             * are done. */
            coordinator.prepareToCommitConsumerOffsets();
            return true;
        }
    }

    private void handleRetry(final long sleep) throws InterruptedException {
        log.info("{}: Retrying in {} second(s)", name, sleep);

        /* decrementing the number of active active consumers indicates an error to the consumer
         * module. This makes sure that the status of the service is set to as 'failing'. */
        active.decrementAndGet();
        shouldStop.await(sleep, TimeUnit.SECONDS);
        active.incrementAndGet();
    }
}
