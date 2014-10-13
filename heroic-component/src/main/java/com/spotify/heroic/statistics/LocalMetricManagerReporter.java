package com.spotify.heroic.statistics;

public interface LocalMetricManagerReporter {
    FutureReporter.Context reportQueryMetrics();

    FutureReporter.Context reportFindSeries();

    FutureReporter.Context reportWrite();

    FutureReporter.Context reportFlushWrites();

    MetricBackendReporter newBackend(String id);
}
