package com.spotify.heroic.aggregation.cardinality;

public class HyperLogLogCardinalityBucketTest extends AbstractCardinalityBucketTest {
    @Override
    protected double allowedError() {
        return 0.9D;
    }

    @Override
    protected CardinalityBucket setupBucket(final long timestamp) {
        return new HyperLogLogCardinalityBucket(42, true, 0.01D);
    }
}
