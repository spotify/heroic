package com.spotify.heroic.aggregation.simple;

import java.util.Collection;
import java.util.function.DoubleBinaryOperator;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.DoubleBucket;

public class MinBucketIntegrationTest extends ValueBucketIntegrationTest {
    public MinBucketIntegrationTest() {
        super(Double.POSITIVE_INFINITY, new DoubleBinaryOperator() {
            @Override
            public double applyAsDouble(double left, double right) {
                return Math.min(left, right);
            }
        });
    }

    @Override
    public Collection<DoubleBucket> buckets() {
        return ImmutableList.<DoubleBucket> of(new MinBucket(0l), new StripedMinBucket(0l));
    }
}