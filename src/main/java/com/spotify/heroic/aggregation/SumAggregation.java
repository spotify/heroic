package com.spotify.heroic.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Sampling;

@JsonSerialize
public class SumAggregation extends BucketAggregation {
    public SumAggregation(Sampling sampling) {
        super(sampling);
    }

    @JsonCreator
    public static SumAggregation create(
            @JsonProperty("sampling") Sampling sampling) {
        return new SumAggregation(sampling);
    }

    @Override
    protected DataPoint build(long timestamp, long count, double value, float p) {
        return new DataPoint(timestamp, value, p);
    }
}