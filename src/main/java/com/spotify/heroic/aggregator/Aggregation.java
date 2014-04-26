package com.spotify.heroic.aggregator;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.query.DateRange;

public interface Aggregation {
    public Aggregator build(DateRange range);

    public void serialize(Composite composite);
}