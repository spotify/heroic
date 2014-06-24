package com.spotify.heroic.statistics.semantic;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.spotify.heroic.statistics.CallbackReporter;
import com.spotify.heroic.statistics.CallbackReporter.Context;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.statistics.ThreadPoolProvider;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticMetadataBackendReporter implements MetadataBackendReporter {
    private final SemanticMetricRegistry registry;
    private final MetricId id;

    private final CallbackReporter refresh;
    private final CallbackReporter findTags;
    private final CallbackReporter findTagKeys;
    private final CallbackReporter findTimeSeries;
    private final CallbackReporter findKeys;
    private final CallbackReporter write;

    private final Meter writeCacheHit;
    private final Meter writeCacheMiss;

    public SemanticMetadataBackendReporter(SemanticMetricRegistry registry,
            String context) {
        this.registry = registry;
        this.id = MetricId.build("metadata-backend").tagged("context", context);

        refresh = new SemanticCallbackReporter(registry, id.tagged("operation",
                "refresh"));
        findTags = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "find-tags"));
        findTagKeys = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "find-tag-keys"));
        findTimeSeries = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "find-time-series"));
        findKeys = new SemanticCallbackReporter(registry, id.tagged(
                "operation", "find-keys"));
        write = new SemanticCallbackReporter(registry, id.tagged("operation",
                "write"));

        final MetricId cache = id.resolve("write-cache");
        writeCacheHit = registry.meter(cache.tagged("what", "hit"));
        writeCacheMiss = registry.meter(cache.tagged("what", "miss"));
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
    public Context reportFindTagKeys() {
        return findTagKeys.setup();
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

    @Override
    public void reportWriteCacheHit() {
        writeCacheHit.mark();
    }

    @Override
    public void reportWriteCacheMiss() {
        writeCacheMiss.mark();
    }

    @Override
    public void newWriteThreadPool(final ThreadPoolProvider provider) {
        final MetricId id = this.id.resolve("write-thread-pool");

        registry.register(id.tagged("what", "size", "unit", "count"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return provider.getQueueSize();
                    }
                });
    }
}
