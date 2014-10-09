package com.spotify.heroic.http.query;

import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.spotify.heroic.model.Sampling;

@Data
public class QuerySampling {
    private static final TimeUnit DEFAULT_UNIT = TimeUnit.MINUTES;
    public static final long DEFAULT_VALUE = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

    private final long size;
    private final long extent;

    @JsonCreator
    public static QuerySampling create(@JsonProperty("unit") String unitName, @JsonProperty("value") Long inputSize,
            @JsonProperty("extent") Long inputExtent) {
        final TimeUnit unit = QueryUtils.parseUnitName(unitName, DEFAULT_UNIT);
        final long size = QueryUtils.parseSize(inputSize, unit, DEFAULT_VALUE);
        final long extent = QueryUtils.parseExtent(inputExtent, unit, size);
        return new QuerySampling(size, extent);
    }

    public Sampling makeSampling() {
        return new Sampling(size, extent);
    }
}
