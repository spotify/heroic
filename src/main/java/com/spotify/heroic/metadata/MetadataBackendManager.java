package com.spotify.heroic.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.metadata.async.FindTagsReducer;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.metrics.async.MergeWriteResult;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.WriteResult;
import com.spotify.heroic.model.filter.Filter;
import com.spotify.heroic.statistics.MetadataBackendManagerReporter;

@Slf4j
@RequiredArgsConstructor
public class MetadataBackendManager {
    private final MetadataBackendManagerReporter reporter;
    private final List<MetadataBackend> backends;

    public Callback<WriteResult> write(Series series) {
        final List<Callback<WriteResult>> callbacks = new ArrayList<Callback<WriteResult>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.write(series));
            } catch (final MetadataQueryException e) {
                log.error("Failed to write to backend", e);
            }
        }

        return ConcurrentCallback
                .newReduce(callbacks, MergeWriteResult.get()).register(
                        reporter.reportFindTags());
    }

    public Callback<FindTags> findTags(final Filter filter) {
        final List<Callback<FindTags>> callbacks = new ArrayList<Callback<FindTags>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findTags(filter));
            } catch (final MetadataQueryException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks, new FindTagsReducer())
                .register(reporter.reportFindTags());
    }

    public Callback<FindTimeSeries> findTimeSeries(final Filter filter) {
        final List<Callback<FindTimeSeries>> callbacks = new ArrayList<Callback<FindTimeSeries>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findTimeSeries(filter));
            } catch (final MetadataQueryException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks,
                new Callback.Reducer<FindTimeSeries, FindTimeSeries>() {
                    @Override
                    public FindTimeSeries resolved(
                            Collection<FindTimeSeries> results,
                            Collection<Exception> errors,
                            Collection<CancelReason> cancelled)
                            throws Exception {
                        return mergeFindTimeSeries(results);
                    }

                    private FindTimeSeries mergeFindTimeSeries(
                            Collection<FindTimeSeries> results) {
                        final Set<Series> series = new HashSet<Series>();
                        int size = 0;

                        for (final FindTimeSeries findTimeSeries : results) {
                            series.addAll(findTimeSeries.getSeries());
                            size += findTimeSeries.getSize();
                        }

                        return new FindTimeSeries(series, size);
                    }
                }).register(reporter.reportFindTimeSeries());
    }

    public Callback<FindKeys> findKeys(final Filter filter) {
        final List<Callback<FindKeys>> callbacks = new ArrayList<Callback<FindKeys>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findKeys(filter));
            } catch (final MetadataQueryException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks,
                new Callback.Reducer<FindKeys, FindKeys>() {
                    @Override
                    public FindKeys resolved(Collection<FindKeys> results,
                            Collection<Exception> errors,
                            Collection<CancelReason> cancelled)
                            throws Exception {
                        return mergeFindKeys(results);
                    }

                    private FindKeys mergeFindKeys(Collection<FindKeys> results) {
                        final Set<String> keys = new HashSet<String>();
                        int size = 0;

                        for (final FindKeys findKeys : results) {
                            keys.addAll(findKeys.getKeys());
                            size += findKeys.getSize();
                        }

                        return new FindKeys(keys, size);
                    }
                }).register(reporter.reportFindKeys());
    }

    public Callback<Boolean> refresh() {
        final List<Callback<Void>> callbacks = new ArrayList<Callback<Void>>();

        for (final MetadataBackend backend : backends) {
            callbacks.add(backend.refresh());
        }

        return ConcurrentCallback.newReduce(callbacks,
                new Callback.DefaultStreamReducer<Void, Boolean>() {
                    @Override
                    public Boolean resolved(int successful, int failed,
                            int cancelled) throws Exception {
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
