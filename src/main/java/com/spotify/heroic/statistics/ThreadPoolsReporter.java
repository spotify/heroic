package com.spotify.heroic.statistics;

public interface ThreadPoolsReporter {
    public interface Context {
        public void stop();
    }

    Context newThreadPoolContext(String string, ThreadPoolReporterProvider provider);
}
