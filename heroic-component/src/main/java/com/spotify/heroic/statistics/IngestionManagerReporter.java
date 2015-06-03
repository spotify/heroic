package com.spotify.heroic.statistics;

public interface IngestionManagerReporter {
    FutureReporter.Context reportMetadataWrite();
}