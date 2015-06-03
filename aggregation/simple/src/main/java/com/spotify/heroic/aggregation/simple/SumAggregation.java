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
public class SumAggregation extends BucketAggregation<DataPoint, DataPoint, SumBucket> {
    public static final String NAME = "sum";

    public SumAggregation(Sampling sampling) {
        super(sampling, DataPoint.class, DataPoint.class);
    }

    @JsonCreator
    public static SumAggregation create(@JsonProperty("sampling") Sampling sampling) {
        return new SumAggregation(sampling);
    }

    @Override
    protected SumBucket buildBucket(long timestamp) {
        return new SumBucket(timestamp);
    }

    @Override
    protected DataPoint build(SumBucket bucket) {
        final long count = bucket.count();

        if (count == 0)
            return new DataPoint(bucket.timestamp(), Double.NaN);

        return new DataPoint(bucket.timestamp(), bucket.value());
    }
}