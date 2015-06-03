package com.spotify.heroic.aggregation.simple;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Sampling;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true, of = { "NAME", "q", "error" })
public class QuantileAggregation extends BucketAggregation<DataPoint, DataPoint, QuantileBucket> {
    public static final String NAME = "quantile";

    @Getter
    private final double q;

    @Getter
    private final double error;

    public QuantileAggregation(Sampling sampling, double q, double error) {
        super(sampling, DataPoint.class, DataPoint.class);
        this.q = q;
        this.error = error;
    }

    @JsonCreator
    public static QuantileAggregation create(@JsonProperty("sampling") Sampling sampling, @JsonProperty("q") Double q,
            @JsonProperty("error") Double error) {
        if (q == null)
            throw new RuntimeException("'q' is required");

        if (error == null)
            throw new RuntimeException("'error' is required");

        if (!(0 < error && error <= 1.0))
            throw new RuntimeException("'error' must be a value between 0 and 1 (inclusive).");

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