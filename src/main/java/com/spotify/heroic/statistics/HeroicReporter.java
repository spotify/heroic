package com.spotify.heroic.statistics;

public interface HeroicReporter {
    BackendManagerReporter newBackendManager(String context);

    AggregationCacheReporter newAggregationCache(String context);

    BackendReporter newBackend(String context);

    AggregationCacheBackendReporter newAggregationCacheBackend(String context);
}
