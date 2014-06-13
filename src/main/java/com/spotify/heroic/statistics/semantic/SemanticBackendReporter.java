package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.codahale.metrics.Histogram;
import com.spotify.heroic.statistics.BackendReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticBackendReporter implements BackendReporter {
    private final Histogram rowCount;

    public SemanticBackendReporter(SemanticMetricRegistry registry, String context) {
        final MetricId id = MetricId.build("backend").tagged("context", context);
        this.rowCount = registry.histogram(id.resolve("row-count"));
    }

    @Override
    public void reportRowCount(long rows) {
        rowCount.update(rows);
    }
}
