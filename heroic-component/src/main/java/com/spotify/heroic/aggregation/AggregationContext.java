package com.spotify.heroic.aggregation;

import com.google.common.base.Optional;

public interface AggregationContext {
    /**
     * Get the size that is currently configured in the context.
     * @return The currently configured size.
     */
    public Optional<Long> size();

    /**
     * Get extent that is currently configured in the context.
     * @return The currently configured extent.
     */
    public Optional<Long> extent();

    public long defaultSize();

    public long defaultExtent();
}