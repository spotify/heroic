package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.statistics.FutureReporter.Context;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;
import com.spotify.heroic.statistics.ThreadPoolProvider;
import com.spotify.heroic.statistics.ThreadPoolReporter;

public class NoopLocalMetadataBackendReporter implements LocalMetadataBackendReporter {
    private NoopLocalMetadataBackendReporter() {
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
    public Context reportFindTagKeys() {
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
    public Context reportWrite() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public void reportWriteCacheHit() {
    }

    @Override
    public void reportWriteCacheMiss() {
    }

    @Override
    public void reportWriteSuccess(long n) {
    }

    @Override
    public void reportWriteFailure(long n) {
    }

    @Override
    public void reportWriteBatchDuration(long millis) {
    }

    @Override
    public void newWriteThreadPool(ThreadPoolProvider provider) {
    }

    @Override
    public ThreadPoolReporter newThreadPool() {
        return NoopThreadPoolReporter.get();
    }

    private static final NoopLocalMetadataBackendReporter instance = new NoopLocalMetadataBackendReporter();

    public static NoopLocalMetadataBackendReporter get() {
        return instance;
    }
}
