package com.spotify.heroic.aggregation;

import com.google.common.base.Optional;
import com.spotify.heroic.common.Sampling;

public interface AggregationContext {
    public Optional<Sampling> getSampling();
}