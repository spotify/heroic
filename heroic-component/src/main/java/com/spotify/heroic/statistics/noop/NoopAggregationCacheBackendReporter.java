package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.statistics.ThreadPoolReporter;

public class NoopAggregationCacheBackendReporter implements AggregationCacheBackendReporter {
    private NoopAggregationCacheBackendReporter() {
    }

    @Override
    public ThreadPoolReporter newThreadPool() {
        return NoopThreadPoolReporter.get();
    }

    private static final NoopAggregationCacheBackendReporter instance = new NoopAggregationCacheBackendReporter();

    public static NoopAggregationCacheBackendReporter get() {
        return instance;
    }
}
