package com.spotify.heroic.statistics;


public interface ClusteredMetadataManagerReporter {
    FutureReporter.Context reportFindTags();

    FutureReporter.Context reportFindKeys();

    FutureReporter.Context reportFindSeries();

    FutureReporter.Context reportDeleteSeries();

    FutureReporter.Context reportWrite();
}
