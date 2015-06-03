package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.IngestionManagerReporter;

public class NoopIngestionManagerReporter implements IngestionManagerReporter {
    private NoopIngestionManagerReporter() {
    }

    @Override
    public FutureReporter.Context reportMetadataWrite() {
        return NoopFutureReporterContext.get();
    }

    private static final NoopIngestionManagerReporter instance = new NoopIngestionManagerReporter();

    public static NoopIngestionManagerReporter get() {
        return instance;
    }
}
