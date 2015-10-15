package com.spotify.heroic.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import lombok.Data;

@Data
public class Statistics {
    static final Statistics EMPTY = new Statistics(ImmutableMap.of());

    private final Map<String, Long> counters;

    @JsonCreator
    public Statistics(@JsonProperty("counters") Map<String, Long> counters) {
        this.counters = checkNotNull(counters, "counters");
    }

    public Statistics merge(Statistics other) {
        final ImmutableMap.Builder<String, Long> counters = ImmutableMap.builder();
        final Set<String> keys = ImmutableSet.<String> builder().addAll(this.counters.keySet())
                .addAll(other.counters.keySet()).build();

        for (final String k : keys) {
            counters.put(k, this.counters.getOrDefault(k, 0l) + other.counters.getOrDefault(k, 0l));
        }

        return new Statistics(counters.build());
    }

    public static Statistics of(Map<String, Long> samples) {
        return new Statistics(ImmutableMap.copyOf(samples));
    }

    public static Statistics of(String k1, long v1) {
        return new Statistics(ImmutableMap.of(k1, v1));
    }

    public static Statistics of(String k1, long v1, String k2, long v2) {
        return new Statistics(ImmutableMap.of(k1, v1, k2, v2));
    }

    public static Statistics of(String k1, long v1, String k2, long v2, String k3, long v3) {
        return new Statistics(ImmutableMap.of(k1, v1, k2, v2, k3, v3));
    }

    public long get(final String key, final long defaultValue) {
        return counters.getOrDefault(key, defaultValue);
    }

    public static Statistics empty() {
        return EMPTY;
    }
}