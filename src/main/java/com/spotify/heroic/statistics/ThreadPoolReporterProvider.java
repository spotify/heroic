package com.spotify.heroic.statistics;

public interface ThreadPoolReporterProvider {
    long getQueueSize();
}