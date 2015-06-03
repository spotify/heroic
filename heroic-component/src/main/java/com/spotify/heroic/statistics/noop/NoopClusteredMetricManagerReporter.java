package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.statistics.ClusteredMetricManagerReporter;
import com.spotify.heroic.statistics.FutureReporter.Context;

public class NoopClusteredMetricManagerReporter implements ClusteredMetricManagerReporter {
    private NoopClusteredMetricManagerReporter() {
    }

    @Override
    public Context reportQuery() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportWrite() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportShardFullQuery(NodeMetadata metadata) {
        return NoopFutureReporterContext.get();
    }

    private static final NoopClusteredMetricManagerReporter instance = new NoopClusteredMetricManagerReporter();

    public static NoopClusteredMetricManagerReporter get() {
        return instance;
    }
}
