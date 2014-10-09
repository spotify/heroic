package com.spotify.heroic.consumer.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Named;
import javax.inject.Singleton;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.concurrrency.ThreadPool;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerConfig;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.utils.Reflection;

@RequiredArgsConstructor
public class KafkaConsumerConfig implements ConsumerConfig {
    public static final int DEFAULT_THREADS = 2;

    private final String id;
    private final List<String> topics;
    private final int threads;
    private final Map<String, String> config;
    private final ConsumerSchema schema;

    @JsonCreator
    public static KafkaConsumerConfig create(@JsonProperty("id") String id, @JsonProperty("schema") String schema,
            @JsonProperty("topics") List<String> topics, @JsonProperty("threads") Integer threads,
            @JsonProperty("config") Map<String, String> config) {
        if (threads == null)
            threads = DEFAULT_THREADS;

        if (schema == null)
            throw new RuntimeException("'schema' not defined");

        final ConsumerSchema schemaClass = Reflection.buildInstance(schema, ConsumerSchema.class);

        if (topics == null || topics.isEmpty())
            throw new RuntimeException("'topics' must be defined and non-empty");

        if (config == null)
            config = new HashMap<String, String>();

        return new KafkaConsumerConfig(id, topics, threads, config, schemaClass);
    }

    @Override
    public Module module(final Key<Consumer> key, final ConsumerReporter reporter) {
        return new PrivateModule() {
            @Provides
            @Singleton
            public ConsumerReporter reporter() {
                return reporter;
            }

            @Provides
            @Singleton
            @Named("topics")
            public List<String> topics() {
                return topics;
            }

            @Provides
            @Singleton
            @Named("threads")
            public int threads() {
                return threads;
            }

            @Provides
            @Singleton
            public ThreadPool threadPool() {
                return ThreadPool.create("consumers", reporter.newThreadPool(), threads * topics.size(), threads
                        * topics.size());
            }

            @Provides
            @Named("config")
            public Map<String, String> config() {
                return config;
            }

            @Provides
            public ConsumerSchema schema() {
                return schema;
            }

            @Override
            protected void configure() {
                bind(key).to(KafkaConsumer.class).in(Scopes.SINGLETON);
                expose(key);
            }
        };
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("kafka#%d", i);
    }
}
