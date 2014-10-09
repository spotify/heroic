package com.spotify.heroic.statistics;

public interface ThreadPoolReporter {
    public interface Context {
        public void stop();
    }

    Context newThreadPoolContext(String name, ThreadPoolReporterProvider provider);
}
