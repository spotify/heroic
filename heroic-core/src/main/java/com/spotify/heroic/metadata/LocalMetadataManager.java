package com.spotify.heroic.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.async.DefaultStreamReducer;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.Futures;
import com.spotify.heroic.async.Reducers;
import com.spotify.heroic.async.ResolvedFuture;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.statistics.MetadataManagerReporter;

@Slf4j
@NoArgsConstructor
public class LocalMetadataManager implements MetadataManager {
    @Inject
    @Named("backends")
    private Set<MetadataBackend> backends;

    @Inject
    private MetadataManagerReporter reporter;

    @Override
    public List<MetadataBackend> getBackends() {
        return ImmutableList.copyOf(backends);
    }

    @Override
    public Future<FindTags> findTags(final Filter filter) {
        final List<Future<FindTags>> callbacks = new ArrayList<Future<FindTags>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findTags(filter));
            } catch (final MetadataOperationException e) {
                log.error("Failed to query backend", e);
            }
        }

        return Futures.reduce(callbacks, FindTags.reduce()).register(reporter.reportFindTags());
    }

    @Override
    public Future<String> bufferWrite(WriteMetric write) {
        return bufferWrite(write.getSeries());
    }

    @Override
    public Future<String> bufferWrite(Series series) {
        final String id = MetadataUtils.buildId(series);

        for (final MetadataBackend backend : backends) {
            try {
                backend.write(id, series);
            } catch (final MetadataOperationException e) {
                log.error("Failed to write to backend", e);
            }
        }

        return new ResolvedFuture<>(id);
    }

    @Override
    public Future<List<String>> bufferWrites(Collection<WriteMetric> writes) {
        final List<Future<String>> callbacks = new ArrayList<>();

        for (final WriteMetric write : writes) {
            callbacks.add(bufferWrite(write.getSeries()));
        }

        return Futures.reduce(callbacks, Reducers.<String> list());
    }

    @Override
    public Future<FindSeries> findSeries(final Filter filter) {
        final List<Future<FindSeries>> callbacks = new ArrayList<Future<FindSeries>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findSeries(filter));
            } catch (final MetadataOperationException e) {
                log.error("Failed to query backend", e);
            }
        }

        return Futures.reduce(callbacks, FindSeries.reduce()).register(reporter.reportFindTimeSeries());
    }

    @Override
    public Future<DeleteSeries> deleteSeries(final Filter filter) {
        final List<Future<DeleteSeries>> callbacks = new ArrayList<Future<DeleteSeries>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.deleteSeries(filter));
            } catch (final MetadataOperationException e) {
                log.error("Failed to query backend", e);
            }
        }

        return Futures.reduce(callbacks, DeleteSeries.reduce());
    }

    @Override
    public Future<FindKeys> findKeys(final Filter filter) {
        final List<Future<FindKeys>> callbacks = new ArrayList<Future<FindKeys>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findKeys(filter));
            } catch (final MetadataOperationException e) {
                log.error("Failed to query backend", e);
            }
        }

        return Futures.reduce(callbacks, FindKeys.reduce()).register(reporter.reportFindKeys());
    }

    @Override
    public Future<Boolean> refresh() {
        final List<Future<Void>> callbacks = new ArrayList<Future<Void>>();

        for (final MetadataBackend backend : backends) {
            callbacks.add(backend.refresh());
        }

        return Futures.reduce(callbacks, new DefaultStreamReducer<Void, Boolean>() {
            @Override
            public Boolean resolved(int successful, int failed, int cancelled) throws Exception {
                return failed == 0 && cancelled == 0;
            }
        }).register(reporter.reportRefresh());
    }

    @Override
    public boolean isReady() {
        boolean ready = true;

        for (final MetadataBackend backend : backends) {
            ready = ready && backend.isReady();
        }

        return ready;
    }
}
