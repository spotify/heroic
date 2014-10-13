package com.spotify.heroic.statistics.semantic;

import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.LocalMetricManagerReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticLocalMetricManagerReporter implements LocalMetricManagerReporter {
    private static final String COMPONENT = "local-metric-manager";

    private final SemanticMetricRegistry registry;

    private final FutureReporter queryMetrics;
    private final FutureReporter findSeries;
    private final FutureReporter write;
    private final FutureReporter flushWrites;

    public SemanticLocalMetricManagerReporter(SemanticMetricRegistry registry) {
        this.registry = registry;

        final MetricId id = MetricId.build().tagged("component", COMPONENT);

        this.queryMetrics = new SemanticFutureReporter(registry, id.tagged("what", "query-metrics", "unit", Units.READ));
        this.findSeries = new SemanticFutureReporter(registry, id.tagged("what", "find-series", "unit", Units.LOOKUP));
        this.write = new SemanticFutureReporter(registry, id.tagged("what", "write", "unit", Units.WRITE));
        this.flushWrites = new SemanticFutureReporter(registry, id.tagged("what", "flush-writes", "unit", Units.WRITE));
    }

    @Override
    public FutureReporter.Context reportQueryMetrics() {
        return queryMetrics.setup();
    }

    @Override
    public FutureReporter.Context reportFindSeries() {
        return findSeries.setup();
    }

    @Override
    public FutureReporter.Context reportWrite() {
        return write.setup();
    }

    @Override
    public FutureReporter.Context reportFlushWrites() {
        return flushWrites.setup();
    }

    @Override
    public MetricBackendReporter newBackend(String id) {
        return new SemanticMetricBackendReporter(registry, id);
    }
}
