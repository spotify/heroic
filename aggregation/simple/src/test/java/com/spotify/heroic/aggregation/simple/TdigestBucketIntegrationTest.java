package com.spotify.heroic.aggregation.simple;


import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.aggregation.TDigestBucket;
import java.util.Collection;
import java.util.List;

public class TdigestBucketIntegrationTest extends ValueBucketIntegrationTest {


    public TdigestBucketIntegrationTest() {
        super(Double.NEGATIVE_INFINITY, null);
    }

    @Override
    public Collection<DoubleBucket> buckets() {
        return List.of();
    }

    @Override
    public Collection<? extends TDigestBucket> tDigestBuckets(){
        return ImmutableList.<TDigestBucket>of(new TdigestMergingBucket(0L));
    }
}
