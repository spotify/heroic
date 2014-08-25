package com.spotify.heroic.http.query;

import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.Sampling;

@Data
public class QuerySampling {
    private static final TimeUnit DEFAULT_UNIT = TimeUnit.MINUTES;
    public static final long DEFAULT_VALUE = TimeUnit.MILLISECONDS.convert(10,
            TimeUnit.MINUTES);

    private final long size;
    private final long extent;

    @JsonCreator
    public static QuerySampling create(@JsonProperty("unit") String unitName,
            @JsonProperty("value") Long inputSize,
            @JsonProperty("extent") Long inputExtent) {
        final TimeUnit unit = parseUnitName(unitName);
        final long size = parseSize(inputSize, unit);
        final long extent = parseExtent(inputExtent, unit, size);
        return new QuerySampling(size, extent);
    }

    private static TimeUnit parseUnitName(String unitName) {
        if (unitName == null)
            return DEFAULT_UNIT;

        final TimeUnit first = TimeUnit.valueOf(unitName.toUpperCase());

        if (first != null)
            return first;

        return DEFAULT_UNIT;
    }

    private static long parseSize(Long inputSize, final TimeUnit unit) {
        final long size;

        if (inputSize == null) {
            size = DEFAULT_VALUE;
        } else {
            size = TimeUnit.MILLISECONDS.convert(inputSize, unit);
        }

        if (size <= 0)
            throw new IllegalArgumentException(
                    "'size' must be a positive value");

        return size;
    }

    private static long parseExtent(Long inputExtent, final TimeUnit unit,
            final long size) {
        final long extent;

        if (inputExtent == null) {
            extent = size;
        } else {
            extent = TimeUnit.MILLISECONDS.convert(inputExtent, unit);
        }

        if (extent <= 0)
            throw new IllegalArgumentException(
                    "'extent' must be a positive value");

        return extent;
    }

    public Sampling makeSampling() {
        return new Sampling(size, extent);
    }
}
