package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.statistics.FutureReporter.Context;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;

public class NoopLocalMetadataManagerReporter implements LocalMetadataManagerReporter {
    private NoopLocalMetadataManagerReporter() {
    }

    @Override
    public Context reportRefresh() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindTags() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindTimeSeries() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportCountSeries() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindKeys() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportTagKeySuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportTagSuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportKeySuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportTagValuesSuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportTagValueSuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public LocalMetadataBackendReporter newMetadataBackend(String id) {
        return NoopLocalMetadataBackendReporter.get();
    }

    private static final NoopLocalMetadataManagerReporter instance = new NoopLocalMetadataManagerReporter();

    public static NoopLocalMetadataManagerReporter get() {
        return instance;
    }
}
