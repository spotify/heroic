package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.MetricBackendsReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticMetricBackendsReporter implements MetricBackendsReporter {
    private static final String COMPONENT = "metric-backends";

    private final FutureReporter query;
    private final FutureReporter write;

    public SemanticMetricBackendsReporter(SemanticMetricRegistry registry) {
        final MetricId id = MetricId.build().tagged("component", COMPONENT);

        this.query = new SemanticFutureReporter(registry, id.tagged("what", "query", "unit", Units.READ));
        this.write = new SemanticFutureReporter(registry, id.tagged("what", "write", "unit", Units.WRITE));
    }

    @Override
    public FutureReporter.Context reportQuery() {
        return query.setup();
    }

    @Override
    public FutureReporter.Context reportWrite() {
        return write.setup();
    }
}