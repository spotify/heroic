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
public class StdDevAggregation extends BucketAggregation<StdDevBucket> {
    public StdDevAggregation(Sampling sampling) {
        super(sampling);
    }

    @JsonCreator
    public static StdDevAggregation create(@JsonProperty("sampling") Sampling sampling) {
        return new StdDevAggregation(sampling);
    }

    @Override
    protected StdDevBucket buildBucket(long timestamp) {
        return new StdDevBucket(timestamp);
    }

    @Override
    protected DataPoint build(StdDevBucket bucket) {
        return new DataPoint(bucket.timestamp(), bucket.value());
    }
}