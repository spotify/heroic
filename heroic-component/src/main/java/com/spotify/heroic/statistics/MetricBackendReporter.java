package com.spotify.heroic.statistics;

public interface MetricBackendReporter {
    FutureReporter.Context reportWriteBatch();

    ThreadPoolReporter newThreadPool();
}
