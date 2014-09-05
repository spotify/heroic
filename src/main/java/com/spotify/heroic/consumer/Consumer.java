package com.spotify.heroic.consumer;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.consumer.exceptions.WriteException;
import com.spotify.heroic.consumer.kafka.KafkaConsumer;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metrics.model.WriteMetric;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = KafkaConsumer.class, name = "kafka") })
public interface Consumer extends LifeCycle {
    @Data
    public static class Statistics {
        private final boolean ok;
        private final long errors;
    }

    public void write(WriteMetric entry) throws WriteException,
            InterruptedException;

    public Statistics getStatistics();
}
