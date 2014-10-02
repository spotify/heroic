package com.spotify.heroic.consumer.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;
import javax.inject.Named;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.HeroicLifeCycle;
import com.spotify.heroic.concurrrency.ThreadPool;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.consumer.exceptions.WriteException;
import com.spotify.heroic.ingestion.FatalIngestionException;
import com.spotify.heroic.ingestion.IngestionException;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.statistics.ConsumerReporter;

@NoArgsConstructor
@Slf4j
@ToString
public class KafkaConsumer implements Consumer {
    @Inject
    @Named("topics")
    private List<String> topics;

    @Inject
    @Named("config")
    private Map<String, String> config;

    @Inject
    @Named("threads")
    private int threads;

    @Inject
    private ConsumerSchema schema;

    @Inject
    private IngestionManager ingestion;

    @Inject
    private HeroicLifeCycle lifecycle;

    @Inject
    private ConsumerReporter reporter;

    @Inject
    private ThreadPool consumer;

    /**
     * Total number of threads which are still consuming.
     */
    private final AtomicInteger consuming = new AtomicInteger(0);

    /**
     * Total number of threads that should be consuming.
     */
    private final AtomicInteger total = new AtomicInteger(0);

    /**
     * Total number of errors encountered.
     */
    private final AtomicLong errors = new AtomicLong(0);

    /**
     * Latch that will be set when we want to shut down.
     */
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private volatile boolean running = false;

    private ConsumerConnector connector;

    @Override
    public synchronized void start() throws Exception {
        if (running)
            throw new IllegalStateException("Kafka consumer already running");

        log.info("Starting");

        final Properties properties = new Properties();
        properties.putAll(config);

        final ConsumerConfig config = new ConsumerConfig(properties);
        connector = kafka.consumer.Consumer.createJavaConsumerConnector(config);

        final Map<String, Integer> streamsMap = makeStreamsMap();

        consuming.set(0);
        total.set(consumer.getThreadPoolSize());

        final Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector
                .createMessageStreams(streamsMap);

        for (final Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : streams
                .entrySet()) {
            final String topic = entry.getKey();
            final List<KafkaStream<byte[], byte[]>> list = entry.getValue();

            for (final KafkaStream<byte[], byte[]> stream : list) {
                consumer.get()
                        .execute(
                                new ConsumerThread(lifecycle, reporter, topic,
                                        stream, this, schema, consuming,
                                        errors, shutdownLatch));
            }
        }

        this.running = true;
    }

    @Override
    public synchronized void stop() throws Exception {
        if (!running)
            throw new IllegalStateException("Kafka consumer not running");

        this.running = false;

        // disconnect streams.
        connector.shutdown();
        // tell sleeping threads to wake up.
        shutdownLatch.countDown();
        // shut down executor.
        consumer.stop();
    }

    @Override
    public boolean isReady() {
        return running;
    }

    private Map<String, Integer> makeStreamsMap() {
        final Map<String, Integer> streamsMap = new HashMap<String, Integer>();

        for (final String topic : topics) {
            streamsMap.put(topic, threads);
        }

        return streamsMap;
    }

    @Override
    public void write(WriteMetric write) throws WriteException {
        try {
            ingestion.write(write);
        } catch (final IngestionException e) {
            log.error("Invalid write: {}", write, e);
        } catch (final FatalIngestionException e) {
            throw new WriteException("Failed to write metric", e);
        }
    }

    @Override
    public Statistics getStatistics() {
        final boolean ok = consuming.get() == total.get();
        return new Statistics(ok, errors.get());
    }
}
