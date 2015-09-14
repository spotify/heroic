package com.spotify.heroic.aggregation.simple;

import com.google.common.base.Optional;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationQuery;
import com.spotify.heroic.aggregation.SamplingQuery;

public abstract class SamplingAggregationQuery implements AggregationQuery {
    private final Optional<Long> size;
    private final Optional<Long> extent;

    public SamplingAggregationQuery(final SamplingQuery sampling) {
        size = sampling.getSize();
        extent = sampling.getExtent();
    }

    @Override
    public Aggregation build(AggregationContext context) {
        final long s = size.or(context.size()).or(context::defaultSize);
        final long e = extent.or(context.extent()).or(context::defaultExtent);
        return build(context, s, e);
    }

    protected abstract Aggregation build(AggregationContext context, long size, long extent);
}