package com.spotify.heroic.aggregation;

import com.google.common.base.Optional;
import com.spotify.heroic.common.Sampling;

import lombok.Data;

@Data
class OptionsContext implements AggregationContext {
    private final AggregationContext parent;
    private final Optional<Sampling> sampling;

    @Override
    public Optional<Sampling> getSampling() {
        return sampling.or(parent.getSampling());
    }
}