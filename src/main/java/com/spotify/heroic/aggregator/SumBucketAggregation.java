package com.spotify.heroic.aggregator;

import com.netflix.astyanax.model.Composite;

public abstract class SumBucketAggregation extends Aggregation {
    @Override
    public abstract SumBucketAggregator build();

    public SumBucketAggregation() {
        super();
    }

    public SumBucketAggregation(Composite composite) {
        super(composite);
    }

    @Override
    public void serializeRest(Composite composite) {
    }
}