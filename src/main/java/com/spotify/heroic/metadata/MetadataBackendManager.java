package com.spotify.heroic.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.metadata.async.FindTagsReducer;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.metadata.model.TimeSerieQuery;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteResponse;
import com.spotify.heroic.statistics.MetadataBackendManagerReporter;

@RequiredArgsConstructor
@Slf4j
/**
 * TODO: Do not ignore errors from individual backends.
 * @author udoprog
 */
public class MetadataBackendManager {
    private final MetadataBackendManagerReporter reporter;

    @Inject
    private Set<MetadataBackend> backends;

    public Callback<WriteResponse> write(TimeSerie timeSerie) {
        final List<Callback<WriteResponse>> callbacks = new ArrayList<Callback<WriteResponse>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.write(timeSerie));
            } catch (MetadataQueryException e) {
                log.error("Failed to write to backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks,
                new Callback.Reducer<WriteResponse, WriteResponse>() {
                    @Override
                    public WriteResponse resolved(
                            Collection<WriteResponse> results,
                            Collection<Exception> errors,
                            Collection<CancelReason> cancelled)
                            throws Exception {
                        for (final Exception e : errors) {
                            log.error("Failed to write", e);
                        }

                        return new WriteResponse();
                    }
                }).register(reporter.reportFindTags());
    }

    public Callback<FindTags> findTags(final TimeSerieQuery query,
            Set<String> include, Set<String> exclude) {
        final List<Callback<FindTags>> callbacks = new ArrayList<Callback<FindTags>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findTags(query, include, exclude));
            } catch (MetadataQueryException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks, new FindTagsReducer())
                .register(reporter.reportFindTags());
    }

    public Callback<FindTimeSeries> findTimeSeries(final TimeSerieQuery query) {
        final List<Callback<FindTimeSeries>> callbacks = new ArrayList<Callback<FindTimeSeries>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findTimeSeries(query));
            } catch (MetadataQueryException e) {
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
                        final Set<TimeSerie> timeSeries = new HashSet<TimeSerie>();
                        int size = 0;

                        for (final FindTimeSeries findTimeSeries : results) {
                            timeSeries.addAll(findTimeSeries.getTimeSeries());
                            size += findTimeSeries.getSize();
                        }

                        return new FindTimeSeries(timeSeries, size);
                    }
                }).register(reporter.reportFindTimeSeries());
    }

    public Callback<FindKeys> findKeys(final TimeSerieQuery query) {
        final List<Callback<FindKeys>> callbacks = new ArrayList<Callback<FindKeys>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findKeys(query));
            } catch (MetadataQueryException e) {
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

        for (MetadataBackend backend : backends) {
            ready = ready && backend.isReady();
        }

        return ready;
    }
}
