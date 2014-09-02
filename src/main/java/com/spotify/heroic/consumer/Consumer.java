package com.spotify.heroic.consumer;

import lombok.Data;

import com.spotify.heroic.consumer.exceptions.WriteException;
import com.spotify.heroic.injection.Lifecycle;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.yaml.ConfigContext;
import com.spotify.heroic.yaml.ValidationException;

public interface Consumer extends Lifecycle {
    public interface YAML {
        public Consumer build(ConfigContext context, ConsumerReporter reporter)
                throws ValidationException;
    }

    @Data
    public static class Statistics {
        private final boolean ok;
        private final long errors;
    }

    public void write(WriteMetric entry) throws WriteException,
            InterruptedException;

    public Statistics getStatistics();
}
