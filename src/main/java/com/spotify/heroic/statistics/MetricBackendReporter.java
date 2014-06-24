package com.spotify.heroic.statistics;

public interface MetricBackendReporter {
    CallbackReporter.Context reportWriteBatch();

    void newWriteThreadPool(final ThreadPoolProvider provider);
}
