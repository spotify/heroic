package com.spotify.heroic.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.metadata.async.FindTagsReducer;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.filter.Filter;
import com.spotify.heroic.statistics.MetadataBackendManagerReporter;
import com.spotify.heroic.yaml.ConfigContext;
import com.spotify.heroic.yaml.ValidationException;

@Slf4j
@RequiredArgsConstructor
public class MetadataBackendManager {
    @Data
    public static final class YAML {
        private List<MetadataBackend.YAML> backends = new ArrayList<>();

        public MetadataBackendManager build(ConfigContext ctx,
                MetadataBackendManagerReporter reporter)
                throws ValidationException {
            final List<MetadataBackend> backends = buildBackends(ctx, reporter);
            return new MetadataBackendManager(reporter, backends);
        }

        private List<MetadataBackend> buildBackends(ConfigContext ctx,
                MetadataBackendManagerReporter reporter)
                        throws ValidationException {
            final List<MetadataBackend> result = new ArrayList<>();

            for (final ConfigContext.Entry<MetadataBackend.YAML> c : ctx
                    .iterate(backends, "backends")) {
                final MetadataBackend backend = c.getValue().build(
                        c.getContext(),
                        reporter.newMetadataBackend(c.getContext()));
                result.add(backend);
            }

            return result;
        }
    }

    private final MetadataBackendManagerReporter reporter;
    private final List<MetadataBackend> backends;

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

        return ConcurrentCallback.newReduce(callbacks, new FindTagsReducer())
                .register(reporter.reportFindTags());
    }

    public String write(Series series) throws MetadataOperationException {
        final String id = MetadataUtils.buildId(series);

        for (final MetadataBackend backend : backends) {
            try {
                backend.write(id, series);
            } catch (final MetadataOperationException e) {
                log.error("Failed to write to backend", e);
            }
        }

        return id;
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

        final Callback.Reducer<FindSeries, FindSeries> reducer = new Callback.Reducer<FindSeries, FindSeries>() {
            @Override
            public FindSeries resolved(Collection<FindSeries> results,
                    Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                for (final Exception e : errors) {
                    log.error("Query failed", e);
                }

                if (!errors.isEmpty() || !cancelled.isEmpty())
                    throw new Exception("Query failed");

                final Set<Series> series = new HashSet<Series>();
                int duplicates = 0;

                for (final FindSeries result : results) {
                    series.addAll(result.getSeries());
                    duplicates += result.getDuplicates();
                }

                return new FindSeries(series, series.size(), duplicates);
            }
        };

        return ConcurrentCallback.newReduce(callbacks, reducer).register(
                reporter.reportFindTimeSeries());
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

        final Callback.Reducer<DeleteSeries, DeleteSeries> reducer = new Callback.Reducer<DeleteSeries, DeleteSeries>() {
            @Override
            public DeleteSeries resolved(Collection<DeleteSeries> results,
                    Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {

                if (!errors.isEmpty() || !cancelled.isEmpty())
                    throw new Exception("Delete failed");

                int successful = 0;
                int failed = 0;

                for (final DeleteSeries result : results) {
                    successful += result.getSuccessful();
                    failed += result.getFailed();
                }

                return new DeleteSeries(successful, failed);
            }
        };

        return ConcurrentCallback.newReduce(callbacks, reducer);
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
