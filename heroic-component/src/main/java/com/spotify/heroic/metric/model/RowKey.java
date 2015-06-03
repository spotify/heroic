package com.spotify.heroic.metric.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.Series;

/**
 * Opaque type designating a row key for any type of metric backend.
 */
@Data
public class RowKey {
    private static final long DEFAULT_BASE = 0;

    private final Series series;
    private final long base;

    @JsonCreator
    public static RowKey create(@JsonProperty("series") Series series, @JsonProperty("base") Long base) {
        if (series == null)
            series = Series.EMPTY;

        if (base == null)
            base = DEFAULT_BASE;

        return new RowKey(series, base);
    }
}
