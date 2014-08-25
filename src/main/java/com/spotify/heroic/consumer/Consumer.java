package com.spotify.heroic.consumer;

import com.spotify.heroic.consumer.exceptions.WriteException;
import com.spotify.heroic.injection.Lifecycle;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.model.WriteMetric;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.yaml.ValidationException;

public interface Consumer extends Lifecycle {
    public interface YAML {
        public Consumer build(String context, ConsumerReporter reporter)
                throws ValidationException;
    }

    public MetadataBackendManager getMetadataManager();

    public MetricBackendManager getMetricBackendManager();

    public void write(WriteMetric entry) throws WriteException;
}
