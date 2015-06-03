package com.spotify.heroic.statistics;

public interface MetricBackendReporter {
    FutureReporter.Context reportWriteBatch();

    FutureReporter.Context reportFetch();

    ThreadPoolReporter newThreadPool();
}