package com.spotify.heroic.statistics;

public interface ThreadPoolReporterProvider {
    long getQueueSize();

    long getActiveThreads();

    long getPoolSize();

    long getCorePoolSize();
}