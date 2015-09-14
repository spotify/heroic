package com.spotify.heroic.aggregation;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;

public class DefaultAggregationContext implements AggregationContext {
    public static final long DEFAULT_SIZE = TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES);

    @Override
    public Optional<Long> size() {
        return Optional.absent();
    }

    @Override
    public Optional<Long> extent() {
        return Optional.absent();
    }

    @Override
    public long defaultSize() {
        return DEFAULT_SIZE;
    }

    @Override
    public long defaultExtent() {
        return defaultSize();
    }
}