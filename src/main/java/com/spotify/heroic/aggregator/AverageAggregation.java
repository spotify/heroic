package com.spotify.heroic.aggregator;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.query.DateRange;

public class AverageAggregation extends SumBucketAggregation {
    public AverageAggregation() {
        super();
    }

    public AverageAggregation(Composite composite) {
        super(composite);
    }

    @Override
    public SumBucketAggregator build(DateRange range) {
        return new AverageAggregator(range, getSampling());
    }
}