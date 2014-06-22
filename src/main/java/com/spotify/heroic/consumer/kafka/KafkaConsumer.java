package com.spotify.heroic.consumer.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.inject.Inject;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.yaml.Utils;
import com.spotify.heroic.yaml.ValidationException;

@RequiredArgsConstructor
@Slf4j
public class KafkaConsumer implements Consumer {
    public static class YAML implements Consumer.YAML {
        public static final String TYPE = "!kafka-consumer";

        @Getter
        @Setter
        private String schema = null;

        @Getter
        @Setter
        private List<String> topics = new ArrayList<String>();

        @Getter
        @Setter
        private int threadCount = 2;

        @Getter
        @Setter
        private Map<String, String> config = new HashMap<String, String>();

        @Override
        public Consumer build(String context, ConsumerReporter reporter)
                throws ValidationException {
            final List<String> topics = Utils.notEmpty(context + ".topics",
                    this.topics);
            final ConsumerSchema schema = Utils.instance(context + ".schema",
                    this.schema, ConsumerSchema.class);
            return new KafkaConsumer(topics, threadCount, config, reporter,
                    schema);
        }
    }

    private final List<String> topics;
    private final int threadCount;
    private final Map<String, String> config;
    private final ConsumerReporter reporter;
    private final ConsumerSchema schema;

    @Inject
    private MetadataBackendManager metadata;

    @Inject
    private MetricBackendManager metric;

    private Executor executor;
    private ConsumerConnector connector;

    @Override
    public void start() throws Exception {
        log.info("Starting");

        final Properties properties = new Properties();
        properties.putAll(config);

        final ConsumerConfig config = new ConsumerConfig(properties);
        connector = kafka.consumer.Consumer.createJavaConsumerConnector(config);

        final Map<String, Integer> streamsMap = makeStreamsMap();

        executor = Executors.newFixedThreadPool(topics.size() * threadCount);

        final Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector
                .createMessageStreams(streamsMap);

        for (final Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : streams
                .entrySet()) {
            final String topic = entry.getKey();
            final List<KafkaStream<byte[], byte[]>> list = entry.getValue();

            for (final KafkaStream<byte[], byte[]> stream : list) {
                executor.execute(new ConsumerThread(reporter, topic, stream,
                        this, schema));
            }
        }
    }

    private Map<String, Integer> makeStreamsMap() {
        final Map<String, Integer> streamsMap = new HashMap<String, Integer>();

        for (final String topic : topics) {
            streamsMap.put(topic, threadCount);
        }

        return streamsMap;
    }

    @Override
    public void stop() throws Exception {
        connector.shutdown();
    }

    @Override
    public MetadataBackendManager getMetadataManager() {
        return metadata;
    }

    @Override
    public MetricBackendManager getMetricBackendManager() {
        return metric;
    }
}
