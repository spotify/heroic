package com.spotify.heroic.ingestion;

import com.spotify.heroic.ingestion.exceptions.FatalIngestionException;
import com.spotify.heroic.ingestion.exceptions.IngestionException;
import com.spotify.heroic.metric.model.WriteMetric;

public interface IngestionManager {
    public void write(WriteMetric write) throws IngestionException, FatalIngestionException;
}
