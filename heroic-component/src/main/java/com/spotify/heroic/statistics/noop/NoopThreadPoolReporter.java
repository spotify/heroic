package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.statistics.ThreadPoolReporter;
import com.spotify.heroic.statistics.ThreadPoolReporterProvider;

public class NoopThreadPoolReporter implements ThreadPoolReporter {
    private NoopThreadPoolReporter() {
    }

    @Override
    public Context newThreadPoolContext(String name, ThreadPoolReporterProvider provider) {
        return NoopThreadPoolContext.get();
    }

    private static final NoopThreadPoolReporter instance = new NoopThreadPoolReporter();

    public static NoopThreadPoolReporter get() {
        return instance;
    }
}
