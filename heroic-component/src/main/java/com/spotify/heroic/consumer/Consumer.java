package com.spotify.heroic.consumer;

import lombok.Data;

import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.metric.model.WriteResult;

import eu.toolchain.async.AsyncFuture;

public interface Consumer extends LifeCycle {
    @Data
    public static class Statistics {
        private final boolean ok;
        private final long errors;
        private final long consumingThreads;
        private final long totalThreads;
    }

    public AsyncFuture<WriteResult> write(WriteMetric entry);

    public Statistics getStatistics();
}
