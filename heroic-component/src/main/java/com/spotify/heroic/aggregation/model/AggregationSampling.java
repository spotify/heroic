package com.spotify.heroic.aggregation.model;

import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.utils.TimeUtils;

@Data
public class AggregationSampling {
    private static final TimeUnit DEFAULT_UNIT = TimeUnit.MINUTES;
    public static final long DEFAULT_VALUE = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

    private final long size;
    private final long extent;

    @JsonCreator
    public static AggregationSampling create(@JsonProperty("unit") String unitName, @JsonProperty("value") Long inputSize,
            @JsonProperty("extent") Long inputExtent) {
        final TimeUnit unit = TimeUtils.parseUnitName(unitName, DEFAULT_UNIT);
        final long size = TimeUtils.parseSize(inputSize, unit, DEFAULT_VALUE);
        final long extent = TimeUtils.parseExtent(inputExtent, unit, size);
        return new AggregationSampling(size, extent);
    }

    public Sampling build() {
        return new Sampling(size, extent);
    }
}
