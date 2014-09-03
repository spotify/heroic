package com.spotify.heroic.consumer.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.ApplicationLifecycle;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.consumer.exceptions.WriteException;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.metrics.MetricFormatException;
import com.spotify.heroic.metrics.error.BufferEnqueueException;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.yaml.ConfigContext;
import com.spotify.heroic.yaml.ConfigUtils;
import com.spotify.heroic.yaml.ValidationException;

@RequiredArgsConstructor
@Slf4j
public class KafkaConsumer implements Consumer {
    @Data
    public static class YAML implements Consumer.YAML {
        public static final String TYPE = "!kafka-consumer";

        private String schema = null;
        private List<String> topics = new ArrayList<String>();
        private int threadCount = 2;
        private Map<String, String> config = new HashMap<String, String>();

        @Override
        public Consumer build(ConfigContext ctx, ConsumerReporter reporter)
                throws ValidationException {
            final List<String> topics = ConfigUtils.notEmpty(
                    ctx.extend("topics"), this.topics);
            final ConsumerSchema schema = ConfigUtils.instance(
                    ctx.extend("schema"), this.schema, ConsumerSchema.class);
            return new KafkaConsumer(topics, threadCount, config, reporter,
                    schema);
        }
    }

    @Inject
    private MetricBackendManager metric;

    @Inject
    private ApplicationLifecycle lifecycle;

    private final List<String> topics;
    private final int threadCount;
    private final Map<String, String> config;
    private final ConsumerReporter reporter;
    private final ConsumerSchema schema;

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

    private ExecutorService executor;
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

        executor = Executors.newFixedThreadPool(topics.size() * threadCount);

        consuming.set(0);
        total.set(topics.size() * threadCount);

        final Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector
                .createMessageStreams(streamsMap);

        for (final Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : streams
                .entrySet()) {
            final String topic = entry.getKey();
            final List<KafkaStream<byte[], byte[]>> list = entry.getValue();

            for (final KafkaStream<byte[], byte[]> stream : list) {
                executor.execute(new ConsumerThread(lifecycle, reporter, topic,
                        stream, this, schema, consuming, errors, shutdownLatch));
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
        executor.shutdown();

        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            log.info("Waiting for executor service was interrupted");
        }
    }

    @Override
    public boolean isReady() {
        return running;
    }

    private Map<String, Integer> makeStreamsMap() {
        final Map<String, Integer> streamsMap = new HashMap<String, Integer>();

        for (final String topic : topics) {
            streamsMap.put(topic, threadCount);
        }

        return streamsMap;
    }

    @Override
    public void write(WriteMetric write) throws WriteException {
        try {
            metric.bufferWrite(null, write);
        } catch (InterruptedException | BufferEnqueueException e) {
            throw new WriteException("Failed to write metric", e);
        } catch (final MetricFormatException e) {
            log.error("Invalid write: {}", write, e);
        }
    }

    @Override
    public Statistics getStatistics() {
        final boolean ok = consuming.get() == total.get();
        return new Statistics(ok, errors.get());
    }
}
