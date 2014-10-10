package com.spotify.heroic.aggregationcache;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Module;

/**
 * Is used to query for pre-aggregated cached time series.
 *
 * @author udoprog
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface AggregationCacheBackendModule {
    public Module module();
}
