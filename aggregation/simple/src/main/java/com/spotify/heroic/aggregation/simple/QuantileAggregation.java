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

    @Getter
    private final double error;

    public QuantileAggregation(Sampling sampling, double q, double error) {
        super(sampling);
        this.q = q;
        this.error = error;
    }

    @JsonCreator
    public static QuantileAggregation create(@JsonProperty("sampling") Sampling sampling, @JsonProperty("q") Double q,
            @JsonProperty("error") Double error) {
        if (q == null)
            throw new IllegalArgumentException("'q' is required");

        if (error == null)
            throw new IllegalArgumentException("'error' is required");

        if (!(0 < error && error <= 1.0))
            throw new IllegalArgumentException("'error' must be a value between 0 and 1 (inclusive).");

        return new QuantileAggregation(sampling, q, error);
    }

    @Override
    protected QuantileBucket buildBucket(long timestamp) {
        return new QuantileBucket(timestamp, q, error);
    }

    @Override
    protected DataPoint build(QuantileBucket bucket) {
        return new DataPoint(bucket.timestamp(), bucket.value());
    }
}