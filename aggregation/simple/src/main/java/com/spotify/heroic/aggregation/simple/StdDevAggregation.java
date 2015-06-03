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
public class StdDevAggregation extends BucketAggregation<DataPoint, DataPoint, StdDevBucket> {
    public static final String NAME = "stddev";

    public StdDevAggregation(Sampling sampling) {
        super(sampling, DataPoint.class, DataPoint.class);
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