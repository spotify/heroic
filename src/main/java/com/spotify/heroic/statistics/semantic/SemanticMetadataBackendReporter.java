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
    private static final String COMPONENT = "metadata-backend";

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
        this.id = MetricId.build().tagged("context", context, "component",
                COMPONENT);

        refresh = new SemanticCallbackReporter(registry, id.tagged("what",
                "refresh", "unit", Units.REFRESHES));
        findTags = new SemanticCallbackReporter(registry, id.tagged("what",
                "find-tags", "unit", Units.LOOKUPS));
        findTagKeys = new SemanticCallbackReporter(registry, id.tagged("what",
                "find-tag-keys", "unit", Units.LOOKUPS));
        findTimeSeries = new SemanticCallbackReporter(registry, id.tagged(
                "what", "find-time-series", "unit", Units.LOOKUPS));
        findKeys = new SemanticCallbackReporter(registry, id.tagged("what",
                "find-keys", "unit", Units.LOOKUPS));
        write = new SemanticCallbackReporter(registry, id.tagged("what",
                "write", "unit", Units.WRITES));
        writeCacheHit = registry.meter(id.tagged("what", "write-cache", "unit",
                Units.HITS));
        writeCacheMiss = registry.meter(id.tagged("what", "write-cache",
                "unit", Units.MISSES));
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
        registry.register(
                id.tagged("what", "write-thread-pool-size", "unit", Units.SIZE),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return provider.getQueueSize();
                    }
                });
    }
}
