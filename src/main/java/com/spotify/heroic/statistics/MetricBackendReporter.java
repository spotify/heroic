package com.spotify.heroic.statistics;


public interface MetricBackendReporter {
    CallbackReporter.Context reportSingleWrite();

    CallbackReporter.Context reportWriteBatch();
}
