package com.spotify.heroic.aggregator;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.query.DateRange;

public abstract class SumBucketAggregation extends Aggregation {
    @Override
    public abstract SumBucketAggregator build(DateRange range);

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