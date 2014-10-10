package com.spotify.heroic.ingestion;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.ingestion.exceptions.FatalIngestionException;
import com.spotify.heroic.ingestion.exceptions.IngestionException;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.ClusteredMetricManager;
import com.spotify.heroic.metric.error.BufferEnqueueException;
import com.spotify.heroic.metric.exceptions.MetricFormatException;
import com.spotify.heroic.metric.model.WriteMetric;

@Slf4j
public class IngestionManagerImpl implements IngestionManager {
    @Inject
    @Named("updateMetadata")
    private boolean updateMetadata;

    @Inject
    @Named("updateMetrics")
    private boolean updateMetrics;

    @Inject
    private MetadataManager metadata;

    @Inject
    private ClusteredMetricManager metrics;

    public void write(WriteMetric write) throws IngestionException, FatalIngestionException {
        if (updateMetadata) {
            metadata.bufferWrite(write);
        }

        if (updateMetrics) {
            try {
                metrics.write(null, write);
            } catch (InterruptedException | BufferEnqueueException e) {
                throw new FatalIngestionException("Failed to buffer up metric", e);
            } catch (final MetricFormatException e) {
                log.error("Invalid write: {}", write, e);
            }
        }
    }
}
