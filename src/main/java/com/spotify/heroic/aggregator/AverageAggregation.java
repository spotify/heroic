package com.spotify.heroic.aggregator;

import com.netflix.astyanax.model.Composite;

public class AverageAggregation extends SumBucketAggregation {
    public AverageAggregation() {
        super();
    }

    public AverageAggregation(Composite composite) {
        super(composite);
    }

    @Override
    public SumBucketAggregator build() {
        return new AverageAggregator(this, getSampling());
    }
}