package com.spotify.heroic.statistics;

public interface MetricBackendReporter {
    CallbackReporter.Context reportWriteBatch();

    ThreadPoolReporter newThreadPool();
}
