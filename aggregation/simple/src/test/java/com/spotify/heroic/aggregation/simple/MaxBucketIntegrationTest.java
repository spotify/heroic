package com.spotify.heroic.aggregation.simple;

import java.util.Collection;
import java.util.function.DoubleBinaryOperator;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.DoubleBucket;

public class MaxBucketIntegrationTest extends ValueBucketIntegrationTest {
    public MaxBucketIntegrationTest() {
        super(Double.NEGATIVE_INFINITY, new DoubleBinaryOperator() {
            @Override
            public double applyAsDouble(double left, double right) {
                return Math.max(left, right);
            }
        });
    }

    @Override
    public Collection<DoubleBucket> buckets() {
        return ImmutableList.<DoubleBucket> of(new MaxBucket(0L), new StripedMaxBucket(0L));
    }
}
