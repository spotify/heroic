package com.spotify.heroic.aggregator;

import com.netflix.astyanax.model.Composite;

public class SumAggregation extends SumBucketAggregation {
    public SumAggregation() {
        super();
    }

    public SumAggregation(Composite composite) {
        super(composite);
    }

    @Override
    public SumBucketAggregator build() {
        return new SumAggregator(this, getSampling());
    }
}