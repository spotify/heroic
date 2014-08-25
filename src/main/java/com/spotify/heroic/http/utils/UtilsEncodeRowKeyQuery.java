package com.spotify.heroic.http.utils;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.metrics.heroic.MetricsRowKey;
import com.spotify.heroic.model.Series;

@Data
public class UtilsEncodeRowKeyQuery {
    private final Series series;
    private final long base;

    @JsonCreator
    public static UtilsEncodeRowKeyQuery create(
            @JsonProperty("series") Series series,
            @JsonProperty("base") Long base) {
        if (base == null) {
            final long now = System.currentTimeMillis();
            base = now - now % MetricsRowKey.MAX_WIDTH;
        }

        return new UtilsEncodeRowKeyQuery(series, base);
    }
}
