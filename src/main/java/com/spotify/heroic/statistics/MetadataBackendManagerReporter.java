package com.spotify.heroic.statistics;

import com.spotify.heroic.yaml.ConfigContext;

public interface MetadataBackendManagerReporter {
    public CallbackReporter.Context reportRefresh();

    public CallbackReporter.Context reportFindTags();

    public CallbackReporter.Context reportFindTimeSeries();

    public CallbackReporter.Context reportFindKeys();

    MetadataBackendReporter newMetadataBackend(ConfigContext context);
}
