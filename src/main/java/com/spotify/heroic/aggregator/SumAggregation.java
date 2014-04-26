package com.spotify.heroic.aggregator;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.query.DateRange;

public class SumAggregation extends SumBucketAggregation {
    public SumAggregation() {
        super();
    }

    public SumAggregation(Composite composite) {
        super(composite);
    }

    @Override
    public SumBucketAggregator build(DateRange range) {
        return new SumAggregator(range, getSampling());
    }
}