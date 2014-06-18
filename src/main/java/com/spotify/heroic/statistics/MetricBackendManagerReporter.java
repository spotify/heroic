package com.spotify.heroic.statistics;



public interface MetricBackendManagerReporter {
    CallbackReporter.Context reportGetAllRows();

    CallbackReporter.Context reportQueryMetrics();

    CallbackReporter.Context reportStreamMetrics();

    CallbackReporter.Context reportStreamMetricsChunk();

    CallbackReporter.Context reportFindRowGroups();
}
