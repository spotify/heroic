package com.spotify.heroic.metrics;

import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metrics.heroic.HeroicBackend;
import com.spotify.heroic.metrics.kairosdb.KairosBackend;
import com.spotify.heroic.metrics.model.BackendEntry;
import com.spotify.heroic.metrics.model.FetchData;
import com.spotify.heroic.metrics.model.WriteBatchResult;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = HeroicBackend.class, name = "heroic"),
        @JsonSubTypes.Type(value = KairosBackend.class, name = "kairosdb") })
public interface Backend extends LifeCycle {
    public String getGroup();

    /**
     * Execute a single write.
     *
     * @param write
     * @return
     */
    public Callback<WriteBatchResult> write(WriteMetric write);

    /**
     * Write a collection of datapoints for a specific time series.
     *
     * @param series
     *            Time serie to write to.
     * @param data
     *            Datapoints to write.
     * @return A callback indicating if the write was successful or not.
     */
    public Callback<WriteBatchResult> write(Collection<WriteMetric> writes);

    /**
     * Query for data points that is part of the specified list of rows and
     * range.
     *
     * @param query
     *            The query for fetching data points. The query contains rows
     *            and a specified time range.
     *
     * @return A list of asynchronous data handlers for the resulting data
     *         points. This is suitable to use with GroupQuery. There will be
     *         one query per row.
     *
     * @throws QueryException
     */
    public List<Callback<FetchData>> fetch(final Series series,
            final DateRange range);

    /**
     * Gets the total number of columns that are in the given rows
     *
     * @param rows
     * @return
     */
    public Callback<Long> getColumnCount(final Series series, DateRange range);

    @Override
    public boolean isReady();

    /**
     * List all series directly from the database.
     *
     * This will be incredibly slow.
     *
     * @return An iterator over all found time series.
     */
    public Iterable<BackendEntry> listEntries();
}
