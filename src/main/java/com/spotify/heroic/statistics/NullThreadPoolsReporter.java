package com.spotify.heroic.statistics;


public class NullThreadPoolsReporter implements
        ThreadPoolsReporter {
    @Override
    public Context newThreadPoolContext(String string,
            ThreadPoolReporterProvider threadPoolReporterProvider) {
        return new ThreadPoolsReporter.Context() {
            @Override
            public void stop() {
            }
        };
    }
}