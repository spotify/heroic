package com.spotify.heroic.aggregation.simple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Sampling;

@JsonSerialize
@JsonTypeName("max")
public class MaxAggregation extends BucketAggregation<MaxBucket> {
    public MaxAggregation(Sampling sampling) {
        super(sampling);
    }

    @JsonCreator
    public static MaxAggregation create(@JsonProperty("sampling") Sampling sampling) {
        return new MaxAggregation(sampling);
    }

    @Override
    protected MaxBucket buildBucket(long timestamp) {
        return new MaxBucket(timestamp);
    }

    @Override
    protected DataPoint build(MaxBucket bucket) {
        return new DataPoint(bucket.timestamp(), bucket.value());
    }
}