package com.spotify.heroic.aggregationcache;

import com.google.inject.Module;

/**
 * Is used to query for pre-aggregated cached time series.
 *
 * @author udoprog
 */
public interface AggregationCacheBackendModule {
    public Module module();
}
