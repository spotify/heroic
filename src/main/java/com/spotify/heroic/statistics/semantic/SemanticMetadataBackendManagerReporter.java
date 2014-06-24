package com.spotify.heroic.statistics.semantic;

import com.spotify.heroic.statistics.CallbackReporter;
import com.spotify.heroic.statistics.CallbackReporter.Context;
import com.spotify.heroic.statistics.MetadataBackendManagerReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticMetadataBackendManagerReporter implements
        MetadataBackendManagerReporter {
    private final CallbackReporter refresh;
    private final CallbackReporter findTags;
    private final CallbackReporter findTimeSeries;
    private final CallbackReporter findKeys;

    public SemanticMetadataBackendManagerReporter(
            SemanticMetricRegistry registry) {
        final MetricId id = MetricId.build("metadata-backend-manager");
        refresh = new SemanticCallbackReporter(registry, id.tagged("operation",
                "refresh"));
        findTags = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "find-tags"));
        findTimeSeries = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "find-time-series"));
        findKeys = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "find-keys"));
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
}
