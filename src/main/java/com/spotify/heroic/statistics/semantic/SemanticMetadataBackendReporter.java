package com.spotify.heroic.statistics.semantic;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
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

    private final Meter writeSuccess;
    private final Meter writeFailure;

    private final Meter writeCacheHit;
    private final Meter writeCacheMiss;

    private final Histogram writeBatchDuration;

    public SemanticMetadataBackendReporter(SemanticMetricRegistry registry,
            String context) {
        this.registry = registry;
        this.id = MetricId.build().tagged("context", context, "component",
                COMPONENT);

        refresh = new SemanticCallbackReporter(registry, id.tagged("what",
                "refresh", "unit", Units.REFRESH));
        findTags = new SemanticCallbackReporter(registry, id.tagged("what",
                "find-tags", "unit", Units.LOOKUP));
        findTagKeys = new SemanticCallbackReporter(registry, id.tagged("what",
                "find-tag-keys", "unit", Units.LOOKUP));
        findTimeSeries = new SemanticCallbackReporter(registry, id.tagged(
                "what", "find-time-series", "unit", Units.LOOKUP));
        findKeys = new SemanticCallbackReporter(registry, id.tagged("what",
                "find-keys", "unit", Units.LOOKUP));
        write = new SemanticCallbackReporter(registry, id.tagged("what",
                "write", "unit", Units.WRITE));
        writeCacheHit = registry.meter(id.tagged("what", "write-cache-hit",
                "unit", Units.HIT));
        writeCacheMiss = registry.meter(id.tagged("what", "write-cache-miss",
                "unit", Units.MISS));
        writeSuccess = registry.meter(id.tagged("what", "write-success",
                "unit", Units.WRITE));
        writeFailure = registry.meter(id.tagged("what", "write-failure",
                "unit", Units.FAILURE));
        writeBatchDuration = registry.histogram(id.tagged("what",
                "write-bulk-duration", "unit", Units.MILLISECOND));
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
                id.tagged("what", "write-thread-pool-size", "unit", Units.BYTE),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return provider.getQueueSize();
                    }
                });
    }

    @Override
    public void reportWriteSuccess(long n) {
        writeSuccess.mark(n);
    }

    @Override
    public void reportWriteFailure(long n) {
        writeFailure.mark(n);
    }

    @Override
    public void reportWriteBatchDuration(long millis) {
        writeBatchDuration.update(millis);
    }
}
