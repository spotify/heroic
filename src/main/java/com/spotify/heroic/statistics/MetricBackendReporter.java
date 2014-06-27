package com.spotify.heroic.statistics;

public interface MetricBackendReporter {
    CallbackReporter.Context reportWriteBatch();

    ThreadPoolsReporter newThreadPoolsReporter();
}
