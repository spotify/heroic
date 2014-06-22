package com.spotify.heroic.http.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.metrics.heroic.MetricsRowKey;
import com.spotify.heroic.model.TimeSerie;

@Data
public class EncodeRowKeyRequest {
    private final TimeSerie timeSerie;
    private final long base;

    @JsonCreator
    public static EncodeRowKeyRequest create(
            @JsonProperty("timeSerie") TimeSerie timeSerie,
            @JsonProperty("base") Long base) {
        if (base == null) {
            final long now = System.currentTimeMillis();
            base = now - now % MetricsRowKey.MAX_WIDTH;
        }

        return new EncodeRowKeyRequest(timeSerie, base);
    }
}
