package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.FutureReporter.Context;

public class NoopAggregationCacheReporter implements AggregationCacheReporter {
    private NoopAggregationCacheReporter() {
    }

    @Override
    public Context reportGet() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportPut() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public void reportGetMiss(int size) {
    }

    @Override
    public AggregationCacheBackendReporter newAggregationCacheBackend() {
        return NoopAggregationCacheBackendReporter.get();
    }

    private static final NoopAggregationCacheReporter instance = new NoopAggregationCacheReporter();

    public static NoopAggregationCacheReporter get() {
        return instance;
    }
}
