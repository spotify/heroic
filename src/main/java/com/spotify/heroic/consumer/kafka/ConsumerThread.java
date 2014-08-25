package com.spotify.heroic.consumer.kafka;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.consumer.exceptions.SchemaValidationException;
import com.spotify.heroic.statistics.ConsumerReporter;

@RequiredArgsConstructor
@Slf4j
public final class ConsumerThread implements Runnable {
    private final ConsumerReporter reporter;
    private final String topic;
    private final KafkaStream<byte[], byte[]> stream;
    private final Consumer consumer;
    private final ConsumerSchema schema;

    @Override
    public void run() {
        try {
            guardedRun();
        } catch (final Exception e) {
            log.error("Failed to consume message", e);
        }
    }

    private void guardedRun() throws Exception {
        log.info("Consuming from topic: {}", topic);

        for (final MessageAndMetadata<byte[], byte[]> m : stream) {
            final byte[] body = m.message();
            reporter.reportMessageSize(body.length);

            try {
                schema.consume(consumer, body);
            } catch (final SchemaValidationException e) {
                reporter.reportConsumerSchemaError();
            } catch (final Exception e) {
                log.error("Failed to consume", e);
                reporter.reportMessageError();
            }
        }
    }
}