package com.spotify.heroic.aggregation.simple;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.spotify.heroic.aggregation.AggregationQuery;

@Data
public class QuantileAggregationQuery implements AggregationQuery<QuantileAggregation> {
    public static final double DEFAULT_QUANTILE = 0.5;
    public static final double DEFAULT_ERROR = 0.01;

    private final AggregationSamplingQuery sampling;
    private final double q;
    private final double error;

    @JsonCreator
    public QuantileAggregationQuery(@JsonProperty("sampling") AggregationSamplingQuery sampling,
            @JsonProperty("q") Double q, @JsonProperty("error") Double error) {
        this.sampling = Optional.fromNullable(sampling).or(AggregationSamplingQuery.DEFAULT_SUPPLIER);
        this.q = Optional.fromNullable(q).or(DEFAULT_QUANTILE);
        this.error = Optional.fromNullable(error).or(DEFAULT_ERROR);
    }

    @Override
    public QuantileAggregation build() {
        return new QuantileAggregation(sampling.build(), q, error);
    }
}