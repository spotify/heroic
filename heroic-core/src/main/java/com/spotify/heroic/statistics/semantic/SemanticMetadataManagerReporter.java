package com.spotify.heroic.statistics.semantic;

import com.spotify.heroic.statistics.CallbackReporter;
import com.spotify.heroic.statistics.CallbackReporter.Context;
import com.spotify.heroic.statistics.MetadataManagerReporter;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticMetadataManagerReporter implements MetadataManagerReporter {
    private static final String COMPONENT = "metadata-backend-manager";

    private final SemanticMetricRegistry registry;

    private final CallbackReporter refresh;
    private final CallbackReporter findTags;
    private final CallbackReporter findTimeSeries;
    private final CallbackReporter findKeys;

    private final MetricId id;

    public SemanticMetadataManagerReporter(SemanticMetricRegistry registry) {
        this.id = MetricId.build().tagged("component", COMPONENT);

        this.registry = registry;

        refresh = new SemanticCallbackReporter(registry, id.tagged("what", "refresh", "unit", Units.REFRESH));
        findTags = new SemanticCallbackReporter(registry, id.tagged("what", "find-tags", "unit", Units.LOOKUP));
        findTimeSeries = new SemanticCallbackReporter(registry, id.tagged("what", "find-time-series", "unit",
                Units.LOOKUP));
        findKeys = new SemanticCallbackReporter(registry, id.tagged("what", "find-keys", "unit", Units.LOOKUP));
    }

    @Override
    public CallbackReporter.Context reportRefresh() {
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
    public MetadataBackendReporter newMetadataBackend(String id) {
        return new SemanticMetadataBackendReporter(registry, this.id.tagged("backend", id));
    }
}
