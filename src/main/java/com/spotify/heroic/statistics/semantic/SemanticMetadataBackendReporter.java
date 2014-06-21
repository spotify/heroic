package com.spotify.heroic.statistics.semantic;

import com.spotify.heroic.statistics.CallbackReporter;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticMetadataBackendReporter implements MetadataBackendReporter {
    private final CallbackReporter refresh;
    private final CallbackReporter findTags;
    private final CallbackReporter findTimeSeries;
    private final CallbackReporter findKeys;
    private final CallbackReporter write;

    public SemanticMetadataBackendReporter(SemanticMetricRegistry registry,
            String context) {
        final MetricId id = MetricId.build("metadata-backend").tagged(
                "context", context);
        refresh = new SemanticCallbackReporter(registry, id.tagged("operation",
                "refresh"));
        findTags = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "find-tags"));
        findTimeSeries = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "find-time-series"));
        findKeys = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "find-keys"));
        write = new SemanticCallbackReporter(registry, id.tagged("operation",
                "write"));
    }

    @Override
    public CallbackReporter.Context reportRefresh() {
        return refresh.setup();
    }

    @Override
    public CallbackReporter.Context reportFindTags() {
        return findTags.setup();
    }

    @Override
    public CallbackReporter.Context reportFindTimeSeries() {
        return findTimeSeries.setup();
    }

    @Override
    public CallbackReporter.Context reportFindKeys() {
        return findKeys.setup();
    }

    @Override
    public CallbackReporter.Context reportWrite() {
        return write.setup();
    }
}
