package com.spotify.heroic.aggregation;

import com.google.common.base.Optional;
import com.spotify.heroic.common.Sampling;

public class DefaultAggregationContext implements AggregationContext {
    @Override
    public Optional<Sampling> getSampling() {
        return Optional.absent();
    }
}