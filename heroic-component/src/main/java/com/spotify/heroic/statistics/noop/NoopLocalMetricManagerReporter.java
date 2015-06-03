package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.statistics.LocalMetricManagerReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;

public class NoopLocalMetricManagerReporter implements LocalMetricManagerReporter {
    private NoopLocalMetricManagerReporter() {
    }

    @Override
    public MetricBackendReporter newBackend(String id) {
        return NoopMetricBackendReporter.get();
    }

    private static final NoopLocalMetricManagerReporter instance = new NoopLocalMetricManagerReporter();

    public static NoopLocalMetricManagerReporter get() {
        return instance;
    }
}
