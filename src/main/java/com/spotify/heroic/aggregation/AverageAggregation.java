package com.spotify.heroic.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregator.AverageAggregator;
import com.spotify.heroic.aggregator.SumBucketAggregator;
import com.spotify.heroic.model.Resolution;

public class AverageAggregation extends SumBucketAggregation {
    @JsonCreator
    public AverageAggregation(Resolution sampling) {
        super(sampling);
    }

    @JsonCreator
    public static AverageAggregation create(@JsonProperty("sampling") Resolution sampling) {
        return new AverageAggregation(sampling);
    }

    @Override
    public SumBucketAggregator build() {
        return new AverageAggregator(this, getSampling());
    }
}