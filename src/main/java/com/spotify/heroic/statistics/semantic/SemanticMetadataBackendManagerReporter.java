package com.spotify.heroic.statistics.semantic;

import com.spotify.heroic.statistics.CallbackReporter;
import com.spotify.heroic.statistics.CallbackReporter.Context;
import com.spotify.heroic.statistics.MetadataBackendManagerReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticMetadataBackendManagerReporter implements
        MetadataBackendManagerReporter {
    private static final String COMPONENT = "metadata-backend-manager";

    private final CallbackReporter refresh;
    private final CallbackReporter findTags;
    private final CallbackReporter findTimeSeries;
    private final CallbackReporter findKeys;

    public SemanticMetadataBackendManagerReporter(
            SemanticMetricRegistry registry) {
        final MetricId id = MetricId.build().tagged("component", COMPONENT);

        refresh = new SemanticCallbackReporter(registry, id.tagged("what",
                "refresh", "unit", Units.REFRESHES));
        findTags = new SemanticCallbackReporter(registry, id.tagged("what",
                "find-tags", "unit", Units.LOOKUPS));
        findTimeSeries = new SemanticCallbackReporter(registry, id.tagged(
                "what", "find-time-series", "unit", Units.LOOKUPS));
        findKeys = new SemanticCallbackReporter(registry, id.tagged("what",
                "find-keys", "unit", Units.LOOKUPS));
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
