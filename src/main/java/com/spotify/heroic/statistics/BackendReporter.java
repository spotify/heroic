package com.spotify.heroic.statistics;

public interface BackendReporter {
    CallbackReporter.Context reportWriteBatch();

    ThreadPoolReporter newThreadPool();
}
