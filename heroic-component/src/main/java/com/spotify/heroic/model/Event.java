package com.spotify.heroic.model;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;

import com.google.common.collect.ImmutableMap;

@Data
@AllArgsConstructor
public class Event implements TimeData {
    private static final Map<String, Object> EMPTY_PAYLOAD = ImmutableMap.of();

    private final long timestamp;
    private final Map<String, Object> payload;

    public Event(long timestamp) {
        this(timestamp, EMPTY_PAYLOAD);
    }

    @Override
    public int compareTo(TimeData o) {
        return Long.compare(timestamp, o.getTimestamp());
    }

    @Override
    public int hash() {
        return payload.hashCode();
    }

    public boolean valid() {
        return true;
    }
}
