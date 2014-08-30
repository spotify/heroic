package com.spotify.heroic.statistics;

import com.spotify.heroic.yaml.ConfigContext;

public interface AggregationCacheReporter {
    CallbackReporter.Context reportGet();

    CallbackReporter.Context reportPut();

    /**
     * Report that the specific count of missing slices was detected.
     *
     * @param size
     */
    void reportGetMiss(int size);

    AggregationCacheBackendReporter newAggregationCacheBackend(
            ConfigContext context);
}
