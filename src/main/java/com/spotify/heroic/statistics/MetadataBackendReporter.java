package com.spotify.heroic.statistics;


public interface MetadataBackendReporter {
    public CallbackReporter.Context reportRefresh();

    public CallbackReporter.Context reportFindTags();

    public CallbackReporter.Context reportFindTagKeys();

    public CallbackReporter.Context reportFindTimeSeries();

    public CallbackReporter.Context reportFindKeys();

    public CallbackReporter.Context reportWrite();

    public void reportWriteCacheHit();

    public void reportWriteCacheMiss();
}
