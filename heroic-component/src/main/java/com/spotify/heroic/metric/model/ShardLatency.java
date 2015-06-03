package com.spotify.heroic.metric.model;

import java.util.Map;

import lombok.Data;

@Data
public class ShardLatency {
    private final Map<String, String> tags;
    private final long latency;
}