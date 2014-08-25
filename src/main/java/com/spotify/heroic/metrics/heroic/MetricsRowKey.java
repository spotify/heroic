package com.spotify.heroic.metrics.heroic;

import lombok.Data;
import lombok.Getter;

import com.spotify.heroic.model.Series;

@Data
public class MetricsRowKey {
    /**
     * This constant represents the maximum row width of the metrics column
     * family. It equals the amount of numbers that can be represented by
     * Integer. Since the column name is the timestamp offset, having an integer
     * as column offset indicates that we can fit about 49 days of data into one
     * row. We do not assume that Integers are 32 bits. This makes it possible
     * to work even if it's not 32 bits.
     */
    public static final long MAX_WIDTH = (long) Integer.MAX_VALUE
            - (long) Integer.MIN_VALUE + 1;

    @Getter
    private final Series series;
    @Getter
    private final long base;
}
