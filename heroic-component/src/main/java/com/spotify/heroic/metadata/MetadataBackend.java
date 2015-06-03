package com.spotify.heroic.metadata;

import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.model.CountSeries;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.MetadataEntry;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.utils.Grouped;
import com.spotify.heroic.utils.Initializing;

import eu.toolchain.async.AsyncFuture;

public interface MetadataBackend extends Grouped, Initializing {
    /**
     * Buffer a write for the specified series.
     *
     * @param id
     *            Id of series to write.
     * @param series
     *            Series to write.
     * @throws MetadataException
     *             If write could not be buffered.
     */
    public AsyncFuture<WriteResult> write(Series series, DateRange range);

    public AsyncFuture<Void> refresh();

    /**
     * Iterate <em>all</em> available metadata.
     *
     * This should perform pagination internally to avoid using too much memory.
     *
     * @throws MetadataException
     */
    public Iterable<MetadataEntry> entries(Filter filter, DateRange range);

    public AsyncFuture<FindTags> findTags(RangeFilter filter);

    public AsyncFuture<FindSeries> findSeries(RangeFilter filter);

    public AsyncFuture<CountSeries> countSeries(RangeFilter filter);

    public AsyncFuture<DeleteSeries> deleteSeries(RangeFilter filter);

    public AsyncFuture<FindKeys> findKeys(RangeFilter filter);
}