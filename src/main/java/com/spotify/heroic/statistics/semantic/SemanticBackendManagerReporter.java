package com.spotify.heroic.statistics.semantic;

import com.spotify.heroic.statistics.BackendManagerReporter;
import com.spotify.heroic.statistics.HeroicTimer;
import com.spotify.heroic.statistics.HeroicTimer.Context;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticBackendManagerReporter implements BackendManagerReporter {
    private final HeroicTimer getAllRows;
    private final HeroicTimer queryMetrics;
    private final HeroicTimer streamMetrics;
    private final HeroicTimer streamMetricsChunk;

    public SemanticBackendManagerReporter(SemanticMetricRegistry registry, String context) {
        final MetricId id = MetricId.build("backend-manager").tagged("context", context);
        this.getAllRows = new SemanticHeroicTimer(registry.timer(id.resolve("get-all-rows")));
        this.queryMetrics = new SemanticHeroicTimer(registry.timer(id.resolve("query-metrics")));
        this.streamMetrics = new SemanticHeroicTimer(registry.timer(id.resolve("stream-metrics")));
        this.streamMetricsChunk = new SemanticHeroicTimer(registry.timer(id.resolve("stream-metrics-chunk")));
    }

    @Override
    public HeroicTimer.Context timeGetAllRows() {
        return getAllRows.time();
    }

    @Override
    public Context timeQueryMetrics() {
        return queryMetrics.time();
    }

    @Override
    public Context timeStreamMetrics() {
        return streamMetrics.time();
    }

    @Override
    public Context timeStreamMetricsChunk() {
        return streamMetricsChunk.time();
    }
}
