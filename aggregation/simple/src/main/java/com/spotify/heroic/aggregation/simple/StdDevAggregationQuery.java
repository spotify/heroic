package com.spotify.heroic.aggregation.simple;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.spotify.heroic.aggregation.AggregationQuery;

@Data
public class StdDevAggregationQuery implements AggregationQuery<StdDevAggregation> {
    private final AggregationSamplingQuery sampling;

    @JsonCreator
    public StdDevAggregationQuery(@JsonProperty("sampling") AggregationSamplingQuery sampling) {
        this.sampling = Optional.fromNullable(sampling).or(AggregationSamplingQuery.DEFAULT_SUPPLIER);
    }

    @Override
    public StdDevAggregation build() {
        return new StdDevAggregation(sampling.build());
    }
}