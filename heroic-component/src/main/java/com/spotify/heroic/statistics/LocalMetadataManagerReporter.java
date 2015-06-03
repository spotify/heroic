package com.spotify.heroic.statistics;


public interface LocalMetadataManagerReporter {
    public FutureReporter.Context reportRefresh();

    public FutureReporter.Context reportFindTags();

    public FutureReporter.Context reportFindTimeSeries();

    public FutureReporter.Context reportFindKeys();

    public FutureReporter.Context reportTagKeySuggest();

    public FutureReporter.Context reportTagSuggest();

    public FutureReporter.Context reportKeySuggest();

    public FutureReporter.Context reportCountSeries();

    public FutureReporter.Context reportTagValuesSuggest();

    public FutureReporter.Context reportTagValueSuggest();

    public LocalMetadataBackendReporter newMetadataBackend(String id);
}