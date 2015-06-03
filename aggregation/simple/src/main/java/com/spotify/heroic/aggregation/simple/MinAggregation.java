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
public class MinAggregation extends BucketAggregation<DataPoint, DataPoint, MinBucket> {
    public static final String NAME = "min";

    public MinAggregation(Sampling sampling) {
        super(sampling, DataPoint.class, DataPoint.class);
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