package com.spotify.heroic.statistics;

import com.spotify.heroic.cluster.model.NodeMetadata;

public interface ClusteredMetricManagerReporter {
    FutureReporter.Context reportQuery();

    FutureReporter.Context reportWrite();

    FutureReporter.Context reportShardFullQuery(NodeMetadata metadata);
}
