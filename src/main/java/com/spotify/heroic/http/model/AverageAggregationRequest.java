package com.spotify.heroic.http.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AverageAggregation;

@Data
public class AverageAggregationRequest implements AggregationRequest {
    private final SamplingRequest sampling;

    @Override
    public Aggregation makeAggregation() {
        return new AverageAggregation(sampling.makeSampling());
    }

    @JsonCreator
    public static AverageAggregationRequest create(
            @JsonProperty(value = "sampling", required = true) SamplingRequest sampling) {
        return new AverageAggregationRequest(sampling);
    }
}
