package com.spotify.heroic.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString(of = { "timestamp", "value" })
@EqualsAndHashCode(of = { "timestamp", "value" })
public class DataPoint implements Comparable<DataPoint> {
    @Getter
    private final long timestamp;

    @Getter
    private final double value;

    public DataPoint(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public DataPoint(long timestamp, long value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public int compareTo(DataPoint o) {
        return Long.compare(timestamp, o.timestamp);
    }
}
