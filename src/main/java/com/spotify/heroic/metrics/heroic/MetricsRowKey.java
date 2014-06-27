package com.spotify.heroic.metrics.heroic;

import lombok.Data;
import lombok.Getter;

import com.spotify.heroic.model.TimeSerie;

@Data
public class MetricsRowKey {
    public static final long MAX_WIDTH = (long) Integer.MAX_VALUE
            - (long) Integer.MIN_VALUE + 1; // This makes it platform
                                            // independent, rather than
                                            // calculating it with pow(2, 32)
    @Getter
    private final TimeSerie timeSerie;
    @Getter
    private final long base;
}