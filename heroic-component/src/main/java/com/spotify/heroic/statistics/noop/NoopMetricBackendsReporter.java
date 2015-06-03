package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.statistics.FutureReporter.Context;
import com.spotify.heroic.statistics.MetricBackendGroupReporter;

public class NoopMetricBackendsReporter implements MetricBackendGroupReporter {
    private NoopMetricBackendsReporter() {
    }

    @Override
    public Context reportQuery() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportWrite() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportWriteBatch() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportQueryMetrics() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindSeries() {
        return NoopFutureReporterContext.get();
    }

    private static final NoopMetricBackendsReporter instance = new NoopMetricBackendsReporter();

    public static NoopMetricBackendsReporter get() {
        return instance;
    }
}
