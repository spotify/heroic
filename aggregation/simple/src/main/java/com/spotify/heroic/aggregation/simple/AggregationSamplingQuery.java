package com.spotify.heroic.aggregation.simple;

import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.utils.TimeUtils;

@Data
public class AggregationSamplingQuery {
    private static final TimeUnit DEFAULT_UNIT = TimeUnit.MINUTES;
    private static final long DEFAULT_VALUE = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

    public static final Supplier<AggregationSamplingQuery> DEFAULT_SUPPLIER = new Supplier<AggregationSamplingQuery>() {
        @Override
        public AggregationSamplingQuery get() {
            return new AggregationSamplingQuery(DEFAULT_VALUE, DEFAULT_VALUE);
        }
    };

    private final long size;
    private final long extent;

    @JsonCreator
    public static AggregationSamplingQuery create(@JsonProperty("unit") String unitName,
            @JsonProperty("value") Long inputSize, @JsonProperty("extent") Long inputExtent) {
        final TimeUnit unit = TimeUtils.parseUnitName(unitName, DEFAULT_UNIT);
        final long size = TimeUtils.parseSize(inputSize, unit, DEFAULT_VALUE);
        final long extent = TimeUtils.parseExtent(inputExtent, unit, size);
        return new AggregationSamplingQuery(size, extent);
    }

    public Sampling build() {
        return new Sampling(size, extent);
    }
}
