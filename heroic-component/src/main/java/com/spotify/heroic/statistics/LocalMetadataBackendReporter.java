package com.spotify.heroic.statistics;

public interface LocalMetadataBackendReporter {
    public FutureReporter.Context reportRefresh();

    public FutureReporter.Context reportFindTags();

    public FutureReporter.Context reportFindTagKeys();

    public FutureReporter.Context reportFindTimeSeries();

    public FutureReporter.Context reportFindKeys();

    public FutureReporter.Context reportWrite();

    public void reportWriteCacheHit();

    public void reportWriteCacheMiss();

    /**
     * report number of successful operations in a batch
     *
     * @param n
     *            number of successes
     */
    public void reportWriteSuccess(long n);

    /**
     * report number of failed operations in a batch
     *
     * @param n
     *            number of failures
     */
    public void reportWriteFailure(long n);

    public void reportWriteBatchDuration(long millis);

    public void newWriteThreadPool(ThreadPoolProvider provider);

    public ThreadPoolReporter newThreadPool();
}
