package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.statistics.ThreadPoolReporter;

public class NoopThreadPoolContext implements ThreadPoolReporter.Context {
    private NoopThreadPoolContext() {
    }

    @Override
    public void stop() {
    }

    private static final NoopThreadPoolContext instance = new NoopThreadPoolContext();

    public static NoopThreadPoolContext get() {
        return instance;
    }
}
