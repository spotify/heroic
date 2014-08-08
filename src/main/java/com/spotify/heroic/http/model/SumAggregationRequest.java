package com.spotify.heroic.http.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.SumAggregation;

@Data
public class SumAggregationRequest implements AggregationRequest {
    private final SamplingRequest sampling;

    @Override
    public Aggregation makeAggregation() {
        return new SumAggregation(sampling.makeSampling());
    }

    @JsonCreator
    public static SumAggregationRequest create(
            @JsonProperty(value = "sampling", required = true) SamplingRequest sampling) {
        return new SumAggregationRequest(sampling);
    }
}
