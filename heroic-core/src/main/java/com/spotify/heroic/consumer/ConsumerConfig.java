package com.spotify.heroic.consumer;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Key;
import com.google.inject.Module;
import com.spotify.heroic.consumer.kafka.KafkaConsumerConfig;
import com.spotify.heroic.statistics.ConsumerReporter;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = KafkaConsumerConfig.class, name = "kafka") })
public interface ConsumerConfig {
    public Module module(Key<Consumer> key, ConsumerReporter consumerReporter);

    public String id();

    public String buildId(int i);
}
