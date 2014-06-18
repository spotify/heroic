package com.spotify.heroic.aggregation;

import com.spotify.heroic.aggregator.Aggregator;

public interface Aggregation {
    public long getWidth();
    public Aggregator build();
}