package com.spotify.heroic.metrics.heroic;

import java.util.HashMap;

import lombok.Data;
import lombok.Getter;

import com.spotify.heroic.model.TimeSerie;

@Data
class DataPointsRowKey {
    public static final long MAX_WIDTH = 1814400000L;
    private static final HashMap<String, String> EMPTY_TAGS = new HashMap<String, String>();

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