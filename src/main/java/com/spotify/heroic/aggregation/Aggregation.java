package com.spotify.heroic.aggregation;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.aggregator.Aggregator;

public interface Aggregation {
    public void serialize(Composite composite);

    public long getWidth();

    public Aggregator build();
}