package com.spotify.heroic.statistics;



public interface BackendManagerReporter {
    HeroicTimer.Context timeGetAllRows();

    HeroicTimer.Context timeQueryMetrics();

    HeroicTimer.Context timeStreamMetrics();

    HeroicTimer.Context timeStreamMetricsChunk();
}
