package com.spotify.heroic.ingestion;

import com.spotify.heroic.exceptions.BackendGroupException;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.metric.model.WriteResult;

import eu.toolchain.async.AsyncFuture;

public interface IngestionManager {

    public AsyncFuture<WriteResult> write(String group, WriteMetric write) throws BackendGroupException;
}
