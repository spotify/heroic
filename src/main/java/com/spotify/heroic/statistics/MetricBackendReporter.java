package com.spotify.heroic.statistics;

public interface MetricBackendReporter {
    void reportRowCount(long rows);
}
