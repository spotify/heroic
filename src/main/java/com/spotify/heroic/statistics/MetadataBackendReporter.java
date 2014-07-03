package com.spotify.heroic.statistics;

public interface MetadataBackendReporter {
    public CallbackReporter.Context reportRefresh();

    public CallbackReporter.Context reportFindTags();

    public CallbackReporter.Context reportFindTagKeys();

    public CallbackReporter.Context reportFindTimeSeries();

    public CallbackReporter.Context reportFindKeys();

    public CallbackReporter.Context reportWrite();

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
}
