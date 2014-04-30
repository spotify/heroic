package com.spotify.heroic.aggregator;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.model.Resolution;
import com.spotify.heroic.model.ResolutionSerializer;

@ToString(of = { "sampling" })
@EqualsAndHashCode(of = { "sampling" })
public abstract class Aggregation {
    @Getter
    @Setter
    private Resolution sampling;

    public Aggregation() {
        this.sampling = Resolution.DEFAULT_RESOLUTION;
    }

    public Aggregation(Composite composite) {
        this.sampling = composite.get(1, ResolutionSerializer.get());
    }

    public long getWidth() {
        return sampling.getWidth();
    }

    public void serialize(Composite composite) {
        composite.addComponent(sampling, ResolutionSerializer.get());
        serializeRest(composite);
    }

    public abstract Aggregator build();

    public abstract void serializeRest(Composite composite);
}