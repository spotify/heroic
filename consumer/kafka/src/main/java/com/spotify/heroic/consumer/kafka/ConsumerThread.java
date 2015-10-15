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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.consumer.ConsumerSchemaValidationException;
import com.spotify.heroic.statistics.ConsumerReporter;

import eu.toolchain.async.ResolvableFuture;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ConsumerThread extends Thread {
    private static final long INITIAL_SLEEP = 5;
    private static final long MAX_SLEEP = 40;

    private final String name;
    private final ConsumerReporter reporter;
    private final KafkaStream<byte[], byte[]> stream;
    private final Consumer consumer;
    private final ConsumerSchema schema;
    private final AtomicInteger active;
    private final AtomicLong errors;
    private final LongAdder consumed;

    // use a latch as a signal so that we can block on it (instead of Thread#sleep).
    private final CountDownLatch stopSignal;
    protected final ResolvableFuture<Void> stopFuture;

    public ConsumerThread(final String name, final ConsumerReporter reporter, final KafkaStream<byte[], byte[]> stream,
            final Consumer consumer, final ConsumerSchema schema, final AtomicInteger active, final AtomicLong errors,
            final LongAdder consumed, final CountDownLatch stopSignal, final ResolvableFuture<Void> stopFuture) {
        super(String.format("%s: %s", ConsumerThread.class.getCanonicalName(), name));

        this.name = name;
        this.reporter = reporter;
        this.stream = stream;
        this.consumer = consumer;
        this.schema = schema;
        this.active = active;
        this.errors = errors;
        this.consumed = consumed;
        this.stopSignal = stopSignal;
        this.stopFuture = stopFuture;
    }

    @Override
    public void run() {
        log.info("{}: Starting thread", name);

        active.incrementAndGet();

        try {
            guardedRun();
        } catch (final Throwable e) {
            log.error("{}: Error in thread", name, e);
            active.decrementAndGet();
            stopFuture.fail(e);
            return;
        }

        log.info("{}: Stopping thread", name);
        active.decrementAndGet();
        stopFuture.resolve(null);
        return;
    }

    private void guardedRun() throws Exception {
        for (final MessageAndMetadata<byte[], byte[]> m : stream) {
            if (stopSignal.getCount() == 0)
                break;

            final byte[] body = m.message();
            retryUntilSuccessful(body);
        }
    }

    private void retryUntilSuccessful(final byte[] body) throws InterruptedException {
        long sleep = INITIAL_SLEEP;

        while (stopSignal.getCount() > 0) {
            final boolean retry = consumeOne(body);

            if (retry) {
                handleRetry(sleep);
                sleep = Math.min(sleep * 2, MAX_SLEEP);
                continue;
            }

            break;
        }
    }

    private boolean consumeOne(final byte[] body) {
        try {
            schema.consume(consumer, body);
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

    private void handleRetry(long sleep) throws InterruptedException {
        log.info("{}: Retrying in {} second(s)", name, sleep);

        /* decrementing the number of active active consumers indicates an error to the consumer module. This makes sure
         * that the status of the service is set to as 'failing'. */
        active.decrementAndGet();
        stopSignal.await(sleep, TimeUnit.SECONDS);
        active.incrementAndGet();
    }
}