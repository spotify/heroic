package com.spotify.heroic.statistics;


public interface AggregationCacheReporter {
    CallbackReporter.Context reportGet();

    CallbackReporter.Context reportPut();

    /**
     * Report that the specific count of missing slices was detected.
     *
     * @param size
     */
    void reportGetMiss(int size);
}
