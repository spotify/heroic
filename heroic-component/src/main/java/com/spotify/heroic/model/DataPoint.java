package com.spotify.heroic.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class DataPoint implements TimeData {
    private final long timestamp;
    private final double value;

    @Override
    public int compareTo(TimeData o) {
        return Long.compare(timestamp, o.getTimestamp());
    }

    @Override
    public int hash() {
        return 0;
    }

    @Override
    public boolean valid() {
        return !Double.isNaN(value);
    }
}