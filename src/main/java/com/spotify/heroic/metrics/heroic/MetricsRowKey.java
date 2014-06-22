package com.spotify.heroic.metrics.heroic;

import lombok.Data;
import lombok.Getter;

import com.spotify.heroic.model.TimeSerie;

@Data
public class MetricsRowKey {
    public static final long MAX_WIDTH = 4294967296L;
    public static final long MAX_BITSET = 0xffffffffL;

    @Getter
    private final TimeSerie timeSerie;
    @Getter
    private final long base;

    /**
     * Get the time bucket associated with the specified date.
     * 
     * @param date
     * @return The bucket for the specified date.
     */
    public static long getTimeBucket(long date) {
        return date - (date % MAX_WIDTH);
    }
}