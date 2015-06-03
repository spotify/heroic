package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.statistics.ThreadPoolReporter;

public class NoopMetricBackendReporter implements MetricBackendReporter {
    private NoopMetricBackendReporter() {
    }

    @Override
    public FutureReporter.Context reportWriteBatch() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public FutureReporter.Context reportFetch() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public ThreadPoolReporter newThreadPool() {
        return NoopThreadPoolReporter.get();
    }

    private static final NoopMetricBackendReporter instance = new NoopMetricBackendReporter();

    public static NoopMetricBackendReporter get() {
        return instance;
    }
}
