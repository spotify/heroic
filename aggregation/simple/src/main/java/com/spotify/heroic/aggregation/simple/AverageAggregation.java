package com.spotify.heroic.aggregation.simple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Sampling;

@JsonTypeName("average")
public class AverageAggregation extends BucketAggregation<SumBucket> {
    public AverageAggregation(Sampling sampling) {
        super(sampling);
    }

    @JsonCreator
    public static AverageAggregation create(@JsonProperty("sampling") Sampling sampling) {
        return new AverageAggregation(sampling);
    }

    @Override
    protected SumBucket buildBucket(long timestamp) {
        return new SumBucket(timestamp);
    }

    @Override
    protected DataPoint build(SumBucket bucket) {
        final long count = bucket.count();

        if (count == 0) {
            return new DataPoint(bucket.timestamp(), Double.NaN);
        }

        return new DataPoint(bucket.timestamp(), bucket.value() / count);
    }
}