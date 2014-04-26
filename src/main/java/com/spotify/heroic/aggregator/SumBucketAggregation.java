package com.spotify.heroic.aggregator;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.model.Resolution;
import com.spotify.heroic.model.ResolutionSerializer;
import com.spotify.heroic.query.DateRange;

@ToString(of = { "sampling" })
public abstract class SumBucketAggregation implements Aggregation {
    @Getter
    @Setter
    private Resolution sampling;

    @Override
    public abstract SumBucketAggregator build(DateRange range);

    public SumBucketAggregation() {
        this.sampling = Resolution.DEFAULT_RESOLUTION;
    }

    public SumBucketAggregation(Composite composite) {
        this.sampling = composite.get(1, ResolutionSerializer.get());
    }

    @Override
    public void serialize(Composite composite) {
        composite.addComponent(sampling, ResolutionSerializer.get());
    }
}