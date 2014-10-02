package com.spotify.heroic.metadata;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.model.Series;

public interface MetadataBackend extends LifeCycle {
    public Callback<FindTags> findTags(Filter filter) throws MetadataOperationException;

    /**
     * Buffer a write for the specified series.
     *
     * @param id
     *            Id of series to write.
     * @param series
     *            Series to write.
     * @throws MetadataOperationException
     *             If write could not be buffered.
     */
    public void write(String id, Series series) throws MetadataOperationException;

    public Callback<FindSeries> findSeries(Filter filter) throws MetadataOperationException;

    public Callback<DeleteSeries> deleteSeries(Filter filter) throws MetadataOperationException;

    public Callback<FindKeys> findKeys(Filter filter) throws MetadataOperationException;

    public Callback<Void> refresh();

    @Override
    public boolean isReady();
}