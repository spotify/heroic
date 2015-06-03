package com.spotify.heroic.aggregation.simple;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.spotify.heroic.aggregation.AggregationQuery;

@Data
public class CountUniqueAggregationQuery implements AggregationQuery<CountUniqueAggregation> {
    private final AggregationSamplingQuery sampling;

    @JsonCreator
    public CountUniqueAggregationQuery(@JsonProperty("sampling") AggregationSamplingQuery sampling) {
        this.sampling = Optional.fromNullable(sampling).or(AggregationSamplingQuery.DEFAULT_SUPPLIER);
    }

    @Override
    public CountUniqueAggregation build() {
        return new CountUniqueAggregation(sampling.build());
    }
}