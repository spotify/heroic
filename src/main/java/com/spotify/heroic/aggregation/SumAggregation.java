package com.spotify.heroic.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.spotify.heroic.aggregator.SumAggregator;
import com.spotify.heroic.model.Resolution;

@JsonSerialize
public class SumAggregation extends SumBucketAggregation {
    public SumAggregation(Resolution sampling) {
        super(sampling);
    }

    @JsonCreator
    public static SumAggregation create(@JsonProperty("sampling") Resolution sampling) {
        return new SumAggregation(sampling);
    }

    @Override
    public SumAggregator build() {
        return new SumAggregator(this, getSampling());
    }
}