package com.spotify.heroic.statistics;

public interface BackendReporter {
    void reportRowCount(long rows);
}
