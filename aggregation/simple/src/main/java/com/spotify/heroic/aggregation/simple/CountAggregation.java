package com.spotify.heroic.aggregation.simple;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.TimeData;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true, of = { "NAME" })
public class CountAggregation extends BucketAggregation<TimeData, DataPoint, CountBucket> {
    public static final String NAME = "count";

    public CountAggregation(Sampling sampling) {
        super(sampling, TimeData.class, DataPoint.class);
    }

    @JsonCreator
    public static CountAggregation create(@JsonProperty("sampling") Sampling sampling) {
        return new CountAggregation(sampling);
    }

    @Override
    protected CountBucket buildBucket(long timestamp) {
        return new CountBucket(timestamp);
    }

    @Override
    protected DataPoint build(CountBucket bucket) {
        final long count = bucket.count();

        if (count == 0)
            return new DataPoint(bucket.timestamp(), Double.NaN);

        return new DataPoint(bucket.timestamp(), count);
    }
}