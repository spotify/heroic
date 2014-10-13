package com.spotify.heroic.statistics;

public interface MetricBackendsReporter {
    FutureReporter.Context reportQuery();

    FutureReporter.Context reportWrite();
}
