package com.spotify.heroic.aggregation.simple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Sampling;

@JsonSerialize
@JsonTypeName("min")
public class MinAggregation extends BucketAggregation<MinBucket> {
    public MinAggregation(Sampling sampling) {
        super(sampling);
    }

    @JsonCreator
    public static MinAggregation create(@JsonProperty("sampling") Sampling sampling) {
        return new MinAggregation(sampling);
    }

    @Override
    protected MinBucket buildBucket(long timestamp) {
        return new MinBucket(timestamp);
    }

    @Override
    protected DataPoint build(MinBucket bucket) {
        return new DataPoint(bucket.timestamp(), bucket.value());
    }
}