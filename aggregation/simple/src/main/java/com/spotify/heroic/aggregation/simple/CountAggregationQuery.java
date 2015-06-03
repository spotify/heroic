package com.spotify.heroic.aggregation.simple;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.spotify.heroic.aggregation.AggregationQuery;

@Data
public class CountAggregationQuery implements AggregationQuery<CountAggregation> {
    private final AggregationSamplingQuery sampling;

    @JsonCreator
    public CountAggregationQuery(@JsonProperty("sampling") AggregationSamplingQuery sampling) {
        this.sampling = Optional.fromNullable(sampling).or(AggregationSamplingQuery.DEFAULT_SUPPLIER);
    }

    @Override
    public CountAggregation build() {
        return new CountAggregation(sampling.build());
    }
}