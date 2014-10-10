package com.spotify.heroic.aggregation.simple;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.model.QueryAggregation;
import com.spotify.heroic.aggregation.model.QuerySampling;

@Data
@JsonTypeName("max")
public class MaxAggregationQuery implements QueryAggregation {
    private final QuerySampling sampling;

    @Override
    public Aggregation build() {
        return new MaxAggregation(sampling.build());
    }

    @JsonCreator
    public static MaxAggregationQuery create(@JsonProperty(value = "sampling", required = true) QuerySampling sampling) {
        return new MaxAggregationQuery(sampling);
    }
}