package com.spotify.heroic.aggregation.simple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Sampling;

public class AverageAggregation extends BucketAggregation {
    public AverageAggregation(Sampling sampling) {
        super(sampling);
    }

    @JsonCreator
    public static AverageAggregation create(@JsonProperty("sampling") Sampling sampling) {
        return new AverageAggregation(sampling);
    }

    @Override
    protected DataPoint build(long timestamp, long count, double value) {
        return new DataPoint(timestamp, value / count);
    }
}