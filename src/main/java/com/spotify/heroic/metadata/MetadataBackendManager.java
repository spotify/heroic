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
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.Reducers;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.statistics.MetadataBackendManagerReporter;

@Slf4j
@NoArgsConstructor
public class MetadataBackendManager {
    @Inject
    @Named("backends")
    private Set<MetadataBackend> backends;

    @Inject
    private MetadataBackendManagerReporter reporter;

    public List<MetadataBackend> getBackends() {
        return ImmutableList.copyOf(backends);
    }

    public Callback<FindTags> findTags(final Filter filter) {
        final List<Callback<FindTags>> callbacks = new ArrayList<Callback<FindTags>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findTags(filter));
            } catch (final MetadataOperationException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks, FindTags.reduce()).register(reporter.reportFindTags());
    }

    public Callback<String> bufferWrite(WriteMetric write) {
        return bufferWrite(write.getSeries());
    }

    public Callback<String> bufferWrite(Series series) {
        final String id = MetadataUtils.buildId(series);

        for (final MetadataBackend backend : backends) {
            try {
                backend.write(id, series);
            } catch (final MetadataOperationException e) {
                log.error("Failed to write to backend", e);
            }
        }

        return new ResolvedCallback<>(id);
    }

    public Callback<List<String>> bufferWrites(Collection<WriteMetric> writes) {
        final List<Callback<String>> callbacks = new ArrayList<>();

        for (final WriteMetric write : writes) {
            callbacks.add(bufferWrite(write.getSeries()));
        }

        return ConcurrentCallback.newReduce(callbacks, Reducers.<String> list());
    }

    public Callback<FindSeries> findSeries(final Filter filter) {
        final List<Callback<FindSeries>> callbacks = new ArrayList<Callback<FindSeries>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findSeries(filter));
            } catch (final MetadataOperationException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks, FindSeries.reduce()).register(reporter.reportFindTimeSeries());
    }

    public Callback<DeleteSeries> deleteSeries(final Filter filter) {
        final List<Callback<DeleteSeries>> callbacks = new ArrayList<Callback<DeleteSeries>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.deleteSeries(filter));
            } catch (final MetadataOperationException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks, DeleteSeries.reduce());
    }

    public Callback<FindKeys> findKeys(final Filter filter) {
        final List<Callback<FindKeys>> callbacks = new ArrayList<Callback<FindKeys>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findKeys(filter));
            } catch (final MetadataOperationException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks, FindKeys.reduce()).register(reporter.reportFindKeys());
    }

    public Callback<Boolean> refresh() {
        final List<Callback<Void>> callbacks = new ArrayList<Callback<Void>>();

        for (final MetadataBackend backend : backends) {
            callbacks.add(backend.refresh());
        }

        return ConcurrentCallback.newReduce(callbacks, new Callback.DefaultStreamReducer<Void, Boolean>() {
            @Override
            public Boolean resolved(int successful, int failed, int cancelled) throws Exception {
                return failed == 0 && cancelled == 0;
            }
        }).register(reporter.reportRefresh());
    }

    public boolean isReady() {
        boolean ready = true;

        for (final MetadataBackend backend : backends) {
            ready = ready && backend.isReady();
        }

        return ready;
    }
}
