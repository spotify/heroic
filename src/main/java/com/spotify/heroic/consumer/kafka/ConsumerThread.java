package com.spotify.heroic.consumer.kafka;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.HeroicLifeCycle;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.consumer.exceptions.SchemaValidationException;
import com.spotify.heroic.statistics.ConsumerReporter;

@RequiredArgsConstructor
@Slf4j
public final class ConsumerThread implements Runnable {
    private static final long INITIAL_SLEEP = 5;
    private static final long MAX_SLEEP = 40;

    private final HeroicLifeCycle lifecycle;
    private final ConsumerReporter reporter;
    private final String topic;
    private final KafkaStream<byte[], byte[]> stream;
    private final Consumer consumer;
    private final ConsumerSchema schema;
    private final AtomicInteger consuming;
    private final AtomicLong errors;
    private final CountDownLatch shutdownLatch;

    @Override
    public void run() {
        try {
            lifecycle.awaitStartup();
        } catch (final InterruptedException e) {
            log.error("Startup interrupted");
            return;
        }

        consuming.incrementAndGet();

        try {
            guardedRun();
        } catch (final Exception e) {
            log.error("Failed to consume message", e);
        }

        consuming.decrementAndGet();
    }

    private void guardedRun() throws Exception {
        log.info("{}: Started consuming.", topic);

        for (final MessageAndMetadata<byte[], byte[]> m : stream) {
            if (shutdownLatch.getCount() == 0)
                break;

            final byte[] body = m.message();
            retryUntilSuccessful(body);
        }

        log.info("{}: Stopped consuming.", topic);
    }

    private void retryUntilSuccessful(final byte[] body)
            throws InterruptedException {
        long sleep = INITIAL_SLEEP;

        while (shutdownLatch.getCount() > 0) {
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
            return false;
        } catch (final SchemaValidationException e) {
            /* these messages should be ignored */
            reporter.reportConsumerSchemaError();
            return false;
        } catch (final Exception e) {
            errors.incrementAndGet();
            log.error("{}: Failed to consume", topic, e);
            reporter.reportMessageError();
            return true;
        }
    }

    private void handleRetry(long sleep) throws InterruptedException {
        log.info("{}: Retrying in {} second(s)", topic, sleep);

        /*
         * decrementing the number of active consumers indicates an error to the
         * consumer module. This makes sure that the status of the service is
         * set to as 'failing'.
         */
        consuming.decrementAndGet();
        shutdownLatch.await(sleep, TimeUnit.SECONDS);
        consuming.incrementAndGet();
    }
}