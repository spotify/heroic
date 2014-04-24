package com.spotify.heroic.aggregator;

import com.spotify.heroic.query.DateRange;

public interface Aggregation {
    public Aggregator build(DateRange range);
}