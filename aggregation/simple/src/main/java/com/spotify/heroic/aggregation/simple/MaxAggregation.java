package com.spotify.heroic.aggregation.simple;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Sampling;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true, of = { "NAME" })
public class MaxAggregation extends BucketAggregation<DataPoint, DataPoint, MaxBucket> {
    public static final String NAME = "max";

    public MaxAggregation(Sampling sampling) {
        super(sampling, DataPoint.class, DataPoint.class);
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