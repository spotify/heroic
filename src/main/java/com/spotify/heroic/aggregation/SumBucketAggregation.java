package com.spotify.heroic.aggregation;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.model.Resolution;
import com.spotify.heroic.model.ResolutionSerializer;

@ToString(of={"sampling"})
@EqualsAndHashCode(of={"sampling"})
public abstract class SumBucketAggregation implements Aggregation {
    private static final ResolutionSerializer resolutionSerializer = ResolutionSerializer.get();

    @Getter
    private final Resolution sampling;

    public long getWidth() {
        return sampling.getWidth();
    }

    public SumBucketAggregation(Resolution sampling) {
        this.sampling = sampling;
    }

    public SumBucketAggregation(Composite composite) {
        this.sampling = composite.get(0, resolutionSerializer);
    }

    public void serialize(Composite composite) {
        composite.addComponent(sampling, resolutionSerializer);
    }
}