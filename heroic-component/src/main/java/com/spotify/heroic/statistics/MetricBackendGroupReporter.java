package com.spotify.heroic.statistics;

public interface MetricBackendGroupReporter {
    FutureReporter.Context reportQuery();

    FutureReporter.Context reportWrite();

    FutureReporter.Context reportWriteBatch();

    FutureReporter.Context reportQueryMetrics();

    FutureReporter.Context reportFindSeries();
}