package com.spotify.heroic.statistics;

public interface LocalMetadataManagerReporter {
    public FutureReporter.Context reportRefresh();

    public FutureReporter.Context reportFindTags();

    public FutureReporter.Context reportFindTimeSeries();

    public FutureReporter.Context reportFindKeys();

    public LocalMetadataBackendReporter newMetadataBackend(String id);
}
