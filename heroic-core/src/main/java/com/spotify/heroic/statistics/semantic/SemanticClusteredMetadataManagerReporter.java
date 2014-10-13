package com.spotify.heroic.statistics.semantic;

import com.spotify.heroic.statistics.ClusteredMetadataManagerReporter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticClusteredMetadataManagerReporter implements ClusteredMetadataManagerReporter {
    private static final String COMPONENT = "clustered-metadata-manager";

    private final FutureReporter findTags;
    private final FutureReporter findKeys;
    private final FutureReporter findSeries;
    private final FutureReporter deleteSeries;
    private final FutureReporter write;

    public SemanticClusteredMetadataManagerReporter(SemanticMetricRegistry registry) {
        final MetricId id = MetricId.build().tagged("component", COMPONENT);
        this.findTags = new SemanticFutureReporter(registry, id.tagged("what", "find-tags", "unit", Units.READ));
        this.findKeys = new SemanticFutureReporter(registry, id.tagged("what", "find-keys", "unit", Units.READ));
        this.findSeries = new SemanticFutureReporter(registry, id.tagged("what", "find-series", "unit", Units.READ));
        this.deleteSeries = new SemanticFutureReporter(registry, id.tagged("what", "delete-series", "unit",
                Units.DELETE));
        this.write = new SemanticFutureReporter(registry, id.tagged("what", "write", "unit", Units.WRITE));
    }

    @Override
    public FutureReporter.Context reportFindTags() {
        return findTags.setup();
    }

    @Override
    public FutureReporter.Context reportFindKeys() {
        return findKeys.setup();
    }

    @Override
    public FutureReporter.Context reportFindSeries() {
        return findSeries.setup();
    }

    @Override
    public FutureReporter.Context reportDeleteSeries() {
        return deleteSeries.setup();
    }

    @Override
    public FutureReporter.Context reportWrite() {
        return write.setup();
    }
}
