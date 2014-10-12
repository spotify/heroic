package com.spotify.heroic.aggregation.simple;

import lombok.Getter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Sampling;

@JsonSerialize
@JsonTypeName("min")
public class QuantileAggregation extends BucketAggregation<QuantileBucket> {
    @Getter
    private final double q;

    public QuantileAggregation(Sampling sampling, double q) {
        super(sampling);
        this.q = q;
    }

    @JsonCreator
    public static QuantileAggregation create(@JsonProperty("sampling") Sampling sampling, @JsonProperty("q") Double q) {
        if (q == null)
            throw new IllegalArgumentException("'q' is required");

        return new QuantileAggregation(sampling, q);
    }

    @Override
    protected QuantileBucket buildBucket(long timestamp) {
        return new QuantileBucket(timestamp, q);
    }

    @Override
    protected DataPoint build(QuantileBucket bucket) {
        return new DataPoint(bucket.timestamp(), bucket.value());
    }
}