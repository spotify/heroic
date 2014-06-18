package com.spotify.heroic.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.BackendException;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.statistics.MetadataBackendManagerReporter;

@RequiredArgsConstructor
@Slf4j
public class MetadataBackendManager {
    private final MetadataBackendManagerReporter reporter;

    @Inject
    private Set<MetadataBackend> backends;

    public Callback<FindTags> findTags(TimeSerieMatcher matcher, Set<String> include, Set<String> exclude) {
        final List<Callback<FindTags>> callbacks = new ArrayList<Callback<FindTags>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findTags(matcher, include, exclude));
            } catch (BackendException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks, new Callback.Reducer<FindTags, FindTags>() {
            @Override
            public FindTags resolved(Collection<FindTags> results,
                    Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                return mergeFindTags(results);
            }

            private FindTags mergeFindTags(Collection<FindTags> results) {
                final Map<String, Set<String>> tags = new HashMap<String, Set<String>>();
                int size = 0;

                for (final FindTags findTags : results) {
                    for (Map.Entry<String, Set<String>> entry : findTags.getTags().entrySet()) {
                        Set<String> entries = tags.get(entry.getKey());

                        if (entries == null) {
                            entries = new HashSet<String>();
                        }

                        entries.addAll(entry.getValue());
                    }

                    size += findTags.getSize();
                }

                return new FindTags(tags, size);
            }
        }).register(reporter.reportFindTags());
    }

    public Callback<FindTimeSeries> findTimeSeries(TimeSerieMatcher matcher) {
        final List<Callback<FindTimeSeries>> callbacks = new ArrayList<Callback<FindTimeSeries>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findTimeSeries(matcher));
            } catch (BackendException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks, new Callback.Reducer<FindTimeSeries, FindTimeSeries>() {
            @Override
            public FindTimeSeries resolved(Collection<FindTimeSeries> results,
                    Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                return mergeFindTimeSeries(results);
            }

            private FindTimeSeries mergeFindTimeSeries(Collection<FindTimeSeries> results) {
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

    public Callback<FindKeys> findKeys(TimeSerieMatcher matcher) {
        final List<Callback<FindKeys>> callbacks = new ArrayList<Callback<FindKeys>>();

        for (final MetadataBackend backend : backends) {
            try {
                callbacks.add(backend.findKeys(matcher));
            } catch (BackendException e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(callbacks, new Callback.Reducer<FindKeys, FindKeys>() {
            @Override
            public FindKeys resolved(Collection<FindKeys> results,
                    Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                return null;
            }
        }).register(reporter.reportFindKeys());
    }

    public Callback<Boolean> refresh() {
        final List<Callback<Void>> callbacks = new ArrayList<Callback<Void>>();

        for (final MetadataBackend backend : backends) {
            callbacks.add(backend.refresh());
        }

        return ConcurrentCallback.newReduce(callbacks, new Callback.DefaultStreamReducer<Void, Boolean>() {
            @Override
            public Boolean resolved(int successful, int failed, int cancelled)
                    throws Exception {
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
