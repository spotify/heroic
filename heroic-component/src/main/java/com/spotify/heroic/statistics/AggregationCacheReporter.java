package com.spotify.heroic.statistics;

public interface AggregationCacheReporter {
    FutureReporter.Context reportGet();

    FutureReporter.Context reportPut();

    /**
     * Report that the specific count of missing slices was detected.
     *
     * @param size
     */
    void reportGetMiss(int size);

    AggregationCacheBackendReporter newAggregationCacheBackend();
}
