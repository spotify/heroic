package com.spotify.heroic.consumer.kafka;

import java.util.List;

import kafka.javaapi.consumer.ConsumerConnector;
import lombok.Data;

@Data
public class Connection {
    private final ConsumerConnector connector;
    private final List<ConsumerThread> threads;
}