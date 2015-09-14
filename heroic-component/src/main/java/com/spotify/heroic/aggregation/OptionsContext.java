package com.spotify.heroic.aggregation;

import com.google.common.base.Optional;

import lombok.Data;

@Data
class OptionsContext implements AggregationContext {
    private final AggregationContext parent;
    private final Optional<Long> size;
    private final Optional<Long> extent;

    @Override
    public Optional<Long> size() {
        return size.or(parent.size());
    }

    @Override
    public Optional<Long> extent() {
        return extent.or(parent.extent());
    }

    @Override
    public long defaultSize() {
        return parent.defaultSize();
    }

    @Override
    public long defaultExtent() {
        return parent.defaultExtent();
    }
}