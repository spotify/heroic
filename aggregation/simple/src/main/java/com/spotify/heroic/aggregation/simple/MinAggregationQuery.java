package com.spotify.heroic.aggregation.simple;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.spotify.heroic.aggregation.AggregationQuery;

@Data
public class MinAggregationQuery implements AggregationQuery<MinAggregation> {
    private final AggregationSamplingQuery sampling;

    @JsonCreator
    public MinAggregationQuery(@JsonProperty("sampling") AggregationSamplingQuery sampling) {
        this.sampling = Optional.fromNullable(sampling).or(AggregationSamplingQuery.DEFAULT_SUPPLIER);
    }

    @Override
    public MinAggregation build() {
        return new MinAggregation(sampling.build());
    }
}