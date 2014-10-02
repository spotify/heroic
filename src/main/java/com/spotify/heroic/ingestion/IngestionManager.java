package com.spotify.heroic.ingestion;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metric.ClusteredMetricManager;
import com.spotify.heroic.metric.MetricFormatException;
import com.spotify.heroic.metric.error.BufferEnqueueException;
import com.spotify.heroic.metric.model.WriteMetric;

@Slf4j
public class IngestionManager {
    @Inject
    @Named("updateMetadata")
    private boolean updateMetadata;

    @Inject
    @Named("updateMetrics")
    private boolean updateMetrics;

    @Inject
    private MetadataBackendManager metadata;

    @Inject
    private ClusteredMetricManager metrics;

    public void write(WriteMetric write) throws IngestionException, FatalIngestionException {
        if (updateMetadata) {
            metadata.bufferWrite(write);
        }

        if (updateMetrics) {
            try {
                metrics.bufferWrite(null, write);
            } catch (InterruptedException | BufferEnqueueException e) {
                throw new FatalIngestionException("Failed to buffer up metric", e);
            } catch (final MetricFormatException e) {
                log.error("Invalid write: {}", write, e);
            }
        }
    }
}
