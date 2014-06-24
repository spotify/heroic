package com.spotify.heroic.statistics.semantic;

import com.spotify.heroic.statistics.CallbackReporter;
import com.spotify.heroic.statistics.CallbackReporter.Context;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticMetricBackendManagerReporter implements
        MetricBackendManagerReporter {
    private final CallbackReporter getAllRows;
    private final CallbackReporter queryMetrics;
    private final CallbackReporter streamMetrics;
    private final CallbackReporter streamMetricsChunk;
    private final CallbackReporter findRowGroups;
    private final CallbackReporter write;

    public SemanticMetricBackendManagerReporter(SemanticMetricRegistry registry) {
        final MetricId id = MetricId.build("metric-backend-manager");
        this.getAllRows = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "get-all-rows"));
        this.queryMetrics = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "query-metrics"));
        this.streamMetrics = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "stream-metrics"));
        this.streamMetricsChunk = new SemanticCallbackReporter(registry,
                id.tagged("operation", "stream-metrics-chunk"));
        this.findRowGroups = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "find-row-groups"));
        this.write = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "write"));
    }

    @Override
    public CallbackReporter.Context reportGetAllRows() {
        return getAllRows.setup();
    }

    @Override
    public CallbackReporter.Context reportQueryMetrics() {
        return queryMetrics.setup();
    }

    @Override
    public CallbackReporter.Context reportStreamMetrics() {
        return streamMetrics.setup();
    }

    @Override
    public CallbackReporter.Context reportStreamMetricsChunk() {
        return streamMetricsChunk.setup();
    }

    @Override
    public Context reportFindTimeSeries() {
        return findRowGroups.setup();
    }

    @Override
    public Context reportWrite() {
        return write.setup();
    }
}
