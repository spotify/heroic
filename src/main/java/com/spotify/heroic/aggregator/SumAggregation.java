package com.spotify.heroic.aggregator;

import com.spotify.heroic.query.DateRange;

public class SumAggregation extends SumBucketAggregation {
    @Override
    public SumBucketAggregator build(DateRange range) {
        return new SumAggregator(range, getSampling());
    }
}