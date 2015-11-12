package com.spotify.heroic.aggregation;

import com.spotify.heroic.metric.Metric;

public interface BucketConsumer<B extends Bucket, M extends Metric> {
    void apply(B bucket, M metric);
}