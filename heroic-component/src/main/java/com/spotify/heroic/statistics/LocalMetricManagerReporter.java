package com.spotify.heroic.statistics;

public interface LocalMetricManagerReporter {
    MetricBackendReporter newBackend(String id);
}
