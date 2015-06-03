package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.statistics.FutureReporter.Context;

public class NoopFutureReporterContext implements Context {
    private NoopFutureReporterContext() {
    }

    @Override
    public void failed(Throwable e) throws Exception {
    }

    @Override
    public void resolved(Object result) throws Exception {
    }

    @Override
    public void cancelled() throws Exception {
    }

    private static final NoopFutureReporterContext instance = new NoopFutureReporterContext();

    public static Context get() {
        return instance;
    }
}