package com.spotify.heroic.consumer;

import lombok.Data;

import com.spotify.heroic.consumer.exceptions.WriteException;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metric.model.WriteMetric;

public interface Consumer extends LifeCycle {
    @Data
    public static class Statistics {
        private final boolean ok;
        private final long errors;
    }

    public void write(WriteMetric entry) throws WriteException, InterruptedException;

    public Statistics getStatistics();
}
