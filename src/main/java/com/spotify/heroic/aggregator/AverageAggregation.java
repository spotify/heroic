package com.spotify.heroic.aggregator;

import com.spotify.heroic.query.DateRange;

public class AverageAggregation extends SumBucketAggregation {
    @Override
    public SumBucketAggregator build(DateRange range) {
        return new AverageAggregator(range, getSampling());
    }
}