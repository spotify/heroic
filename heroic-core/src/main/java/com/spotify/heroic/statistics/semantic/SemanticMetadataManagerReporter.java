package com.spotify.heroic.statistics.semantic;

import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.FutureReporter.Context;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticMetadataManagerReporter implements LocalMetadataManagerReporter {
    private static final String COMPONENT = "metadata-backend-manager";

    private final SemanticMetricRegistry registry;

    private final FutureReporter refresh;
    private final FutureReporter findTags;
    private final FutureReporter findTimeSeries;
    private final FutureReporter findKeys;

    private final MetricId id;

    public SemanticMetadataManagerReporter(SemanticMetricRegistry registry) {
        this.id = MetricId.build().tagged("component", COMPONENT);

        this.registry = registry;

        refresh = new SemanticFutureReporter(registry, id.tagged("what", "refresh", "unit", Units.REFRESH));
        findTags = new SemanticFutureReporter(registry, id.tagged("what", "find-tags", "unit", Units.LOOKUP));
        findTimeSeries = new SemanticFutureReporter(registry, id.tagged("what", "find-time-series", "unit",
                Units.LOOKUP));
        findKeys = new SemanticFutureReporter(registry, id.tagged("what", "find-keys", "unit", Units.LOOKUP));
    }

    @Override
    public FutureReporter.Context reportRefresh() {
        return refresh.setup();
    }

    @Override
    public Context reportFindTags() {
        return findTags.setup();
    }

    @Override
    public Context reportFindTimeSeries() {
        return findTimeSeries.setup();
    }

    @Override
    public Context reportFindKeys() {
        return findKeys.setup();
    }

    @Override
    public LocalMetadataBackendReporter newMetadataBackend(String id) {
        return new SemanticLocalMetadataBackendReporter(registry, this.id.tagged("backend", id));
    }
}
