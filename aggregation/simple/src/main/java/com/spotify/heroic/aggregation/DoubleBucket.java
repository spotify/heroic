package com.spotify.heroic.aggregation;

import com.spotify.heroic.metric.Metric;

public interface DoubleBucket<T extends Metric> extends Bucket<T> {
    double value();
}