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
public class CountUniqueAggregation extends BucketAggregation<TimeData, DataPoint, CountUniqueBucket> {
    public static final String NAME = "count-unique";

    public CountUniqueAggregation(Sampling sampling) {
        super(sampling, TimeData.class, DataPoint.class);
    }

    @JsonCreator
    public static CountUniqueAggregation create(@JsonProperty("sampling") Sampling sampling) {
        return new CountUniqueAggregation(sampling);
    }

    @Override
    protected CountUniqueBucket buildBucket(long timestamp) {
        return new CountUniqueBucket(timestamp);
    }

    @Override
    protected DataPoint build(CountUniqueBucket bucket) {
        final long count = bucket.count();

        if (count == 0)
            return new DataPoint(bucket.timestamp(), Double.NaN);

        return new DataPoint(bucket.timestamp(), count);
    }
}