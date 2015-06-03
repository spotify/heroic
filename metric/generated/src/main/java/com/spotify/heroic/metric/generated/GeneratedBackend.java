package com.spotify.heroic.metric.generated;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.ToString;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.model.BackendEntry;
import com.spotify.heroic.metric.model.BackendKey;
import com.spotify.heroic.metric.model.FetchData;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Event;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.TimeData;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@ToString
public class GeneratedBackend implements MetricBackend, LifeCycle {
    private static final List<BackendEntry> EMPTY_ENTRIES = new ArrayList<>();

    @Inject
    private AsyncFramework async;

    @Inject
    private Generator generator;

    @Inject
    @Named("groups")
    private Set<String> groups;

    @Override
    public AsyncFuture<Void> start() throws Exception {
        return async.resolved(null);
    }

    @Override
    public AsyncFuture<Void> stop() throws Exception {
        return async.resolved(null);
    }

    @Override
    public Set<String> getGroups() {
        return groups;
    }

    @Override
    public AsyncFuture<WriteResult> write(WriteMetric write) {
        return async.resolved(WriteResult.EMPTY);
    }

    @Override
    public AsyncFuture<WriteResult> write(Collection<WriteMetric> writes) {
        return async.resolved(WriteResult.EMPTY);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends TimeData> AsyncFuture<FetchData<T>> fetch(Class<T> source, Series series, DateRange range,
            FetchQuotaWatcher watcher) {
        final long start = System.nanoTime();

        if (source == DataPoint.class) {
            final List<DataPoint> data = generator.generate(series, range, watcher);
            final long diff = System.nanoTime() - start;
            final AsyncFuture<? extends FetchData<? extends TimeData>> f = async.resolved(new FetchData<DataPoint>(
                    series, data, ImmutableList.of(diff)));
            return (AsyncFuture<FetchData<T>>) f;
        }

        if (source == Event.class) {
            final List<Event> data = generator.generateEvents(series, range, watcher);
            final long diff = System.nanoTime() - start;
            final AsyncFuture<? extends FetchData<? extends TimeData>> f = async.resolved(new FetchData<Event>(series,
                    data, ImmutableList.of(diff)));
            return (AsyncFuture<FetchData<T>>) f;
        }

        throw new IllegalArgumentException("unsupported source: " + source.getName());
    }

    @Override
    public AsyncFuture<List<BackendKey>> keys(BackendKey start, BackendKey end, int limit) {
        return async.resolved((List<BackendKey>) new ArrayList<BackendKey>());
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        return EMPTY_ENTRIES;
    }
}