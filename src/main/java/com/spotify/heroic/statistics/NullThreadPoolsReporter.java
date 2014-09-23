package com.spotify.heroic.statistics;

public class NullThreadPoolsReporter implements ThreadPoolReporter {
    @Override
    public Context newThreadPoolContext(String string,
            ThreadPoolReporterProvider threadPoolReporterProvider) {
        return new ThreadPoolReporter.Context() {
            @Override
            public void stop() {
            }
        };
    }
}