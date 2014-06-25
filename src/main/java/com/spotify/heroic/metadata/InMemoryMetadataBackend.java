package com.spotify.heroic.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Inject;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.FailedCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.async.Transformers;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.metadata.model.TimeSerieQuery;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteResponse;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.yaml.ValidationException;

/**
 * An in-memory implementation of MetadataBackend.
 * 
 * This causes MetricBackends to have support iteration of their backends
 * (through {@link MetricBackend#getAllTimeSeries()} because it requests all
 * available time-series and builds a (sort of) efficient in-memory index of
 * them.
 * 
 * @author udoprog
 */
@RequiredArgsConstructor
@Slf4j
public class InMemoryMetadataBackend implements MetadataBackend {
    public static class YAML implements MetadataBackend.YAML {
        public static String TYPE = "!in-memory-metadata";

        @Override
        public MetadataBackend build(String context,
                MetadataBackendReporter reporter) throws ValidationException {
            return new InMemoryMetadataBackend(reporter);
        }
    }

    private final MetadataBackendReporter reporter;

    @Inject
    private MetricBackendManager backendManager;

    private Map<Map.Entry<String, String>, List<TimeSerie>> byTag;
    private Map<String, List<TimeSerie>> byKey;
    private List<TimeSerie> all;
    private boolean ready = false;

    private final AtomicBoolean inProgress = new AtomicBoolean(false);

    @Override
    public Callback<Void> refresh() {
        if (!inProgress.compareAndSet(false, true)) {
            log.warn("Refresh already in progress");
            return new ResolvedCallback<Void>(null);
        }

        log.info("Refreshing tags cache");

        final Callback<Set<TimeSerie>> callback = backendManager
                .getAllTimeSeries();

        callback.register(new Callback.Handle<Set<TimeSerie>>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                log.warn("Request for tags cache refresh was cancelled: "
                        + reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                log.error("Failed to refresh tags cache", e);
            }

            @Override
            public void resolved(Set<TimeSerie> result) throws Exception {
                log.info("Successfully refreshed with {} timeserie(s)",
                        result.size());

                final List<TimeSerie> timeSeries = new ArrayList<TimeSerie>(
                        result);

                final Map<Map.Entry<String, String>, List<TimeSerie>> byTag = calculateByTag(timeSeries);
                final Map<String, List<TimeSerie>> byKey = calculateByKey(timeSeries);

                synchronized (InMemoryMetadataBackend.this) {
                    InMemoryMetadataBackend.this.byTag = byTag;
                    InMemoryMetadataBackend.this.byKey = byKey;
                    InMemoryMetadataBackend.this.all = timeSeries;
                    InMemoryMetadataBackend.this.ready = true;
                }
            }
        });

        callback.register(new Callback.Finishable() {
            @Override
            public void finished() {
                log.info("Refresh ended");
                inProgress.set(false);
            }
        });

        return callback.transform(Transformers.<Set<TimeSerie>> toVoid())
                .register(reporter.reportRefresh());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.spotify.heroic.backend.MetadataBackend#findTags(com.spotify.heroic
     * .backend.TimeSerieMatcher, java.util.Set, java.util.Set)
     */
    @Override
    public Callback<FindTags> findTags(TimeSerieQuery query,
            Set<String> include, Set<String> exclude) {
        final Map<String, Set<String>> result = new HashMap<String, Set<String>>();

        final List<TimeSerie> timeSeries = findBestMatch(query.getMatchKey(),
                query.getMatchTags());
        final TimeSerieMatcher matcher = new TimeSerieMatcher(query);

        for (final TimeSerie timeSerie : filter(timeSeries, matcher)) {
            for (final Map.Entry<String, String> entry : timeSerie.getTags()
                    .entrySet()) {
                if (include != null && !include.contains(entry.getKey()))
                    continue;

                if (exclude != null && exclude.contains(entry.getKey()))
                    continue;

                Set<String> current = result.get(entry.getKey());

                if (current == null) {
                    current = new HashSet<String>();
                    result.put(entry.getKey(), current);
                }

                current.add(entry.getValue());
            }
        }

        return new ResolvedCallback<FindTags>(new FindTags(result,
                timeSeries.size()));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.spotify.heroic.backend.MetadataBackend#findTimeSeries(com.spotify
     * .heroic.backend.TimeSerieMatcher)
     */
    @Override
    public Callback<FindTimeSeries> findTimeSeries(TimeSerieQuery query) {
        final List<TimeSerie> timeSeries = findBestMatch(query.getMatchKey(),
                query.getMatchTags());

        final Set<TimeSerie> result = new HashSet<TimeSerie>();
        final TimeSerieMatcher matcher = new TimeSerieMatcher(query);

        for (final TimeSerie timeSerie : filter(timeSeries, matcher)) {
            result.add(timeSerie);
        }

        return new ResolvedCallback<FindTimeSeries>(new FindTimeSeries(result,
                timeSeries.size()));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.spotify.heroic.backend.MetadataBackend#findKeys(com.spotify.heroic
     * .backend.TimeSerieMatcher)
     */
    @Override
    public Callback<FindKeys> findKeys(TimeSerieQuery query) {
        final SortedSet<String> result = new TreeSet<String>();

        final List<TimeSerie> timeSeries = findBestMatch(query.getMatchKey(),
                query.getMatchTags());
        final TimeSerieMatcher matcher = new TimeSerieMatcher(query);

        for (final TimeSerie timeSerie : filter(timeSeries, matcher)) {
            result.add(timeSerie.getKey());
        }

        return new ResolvedCallback<FindKeys>(new FindKeys(result,
                timeSeries.size()));
    }

    @Override
    public Callback<WriteResponse> write(TimeSerie timeSerie) {
        return new FailedCallback<WriteResponse>(new Exception(
                "writes are not supported"));
    }

    private static Iterable<TimeSerie> filter(final List<TimeSerie> series,
            final TimeSerieMatcher matcher) {
        return new Iterable<TimeSerie>() {
            @Override
            public Iterator<TimeSerie> iterator() {
                return new TimeSerieIterator(series.iterator(), matcher);
            }
        };
    }

    /**
     * Build index by tag.
     * 
     * @param timeSeries
     * @return
     */
    private Map<Map.Entry<String, String>, List<TimeSerie>> calculateByTag(
            Collection<TimeSerie> timeSeries) {
        final Map<Map.Entry<String, String>, List<TimeSerie>> byTag = new HashMap<Map.Entry<String, String>, List<TimeSerie>>();

        for (final TimeSerie timeSerie : timeSeries) {
            for (final Map.Entry<String, String> entry : timeSerie.getTags()
                    .entrySet()) {
                List<TimeSerie> series = byTag.get(entry);

                if (series == null) {
                    series = new ArrayList<TimeSerie>();
                    byTag.put(entry, series);
                }

                series.add(timeSerie);
            }
        }

        return byTag;
    }

    /**
     * Build index by key.
     * 
     * @param timeseries
     * @return
     */
    private Map<String, List<TimeSerie>> calculateByKey(
            Collection<TimeSerie> timeseries) {
        final Map<String, List<TimeSerie>> byTag = new HashMap<String, List<TimeSerie>>();

        for (final TimeSerie t : timeseries) {
            List<TimeSerie> series = byTag.get(t.getKey());

            if (series == null) {
                series = new ArrayList<TimeSerie>();
                byTag.put(t.getKey(), series);
            }

            series.add(t);
        }

        return byTag;
    }

    /**
     * Attempt to find the best match among multiple candidate time series.
     * 
     * @param key
     *            Key to match for.
     * @param filter
     *            Filter of tags to match for, each key/value combination is
     *            indexed.
     * @return The smallest possible list of TimeSerie's that has to be scanned.
     */
    private List<TimeSerie> findBestMatch(String key, Map<String, String> filter) {
        List<TimeSerie> smallest = null;
        @SuppressWarnings("unused")
        Map.Entry<String, String> matchedTag = null;
        @SuppressWarnings("unused")
        String matchedKey = null;

        if (filter != null) {
            final Map<Map.Entry<String, String>, List<TimeSerie>> byTag = getByTag();

            for (final Map.Entry<String, String> entry : filter.entrySet()) {
                final List<TimeSerie> candidate = byTag.get(entry);

                if (candidate == null) {
                    continue;
                }

                if (smallest == null || candidate.size() < smallest.size()) {
                    smallest = candidate;
                    matchedTag = entry;
                }
            }
        }

        if (key != null) {
            final Map<String, List<TimeSerie>> byKey = getByKey();

            final List<TimeSerie> candidate = byKey.get(key);

            if (candidate != null) {
                if (smallest == null || candidate.size() < smallest.size()) {
                    smallest = candidate;
                    matchedKey = key;
                }
            }
        }

        // Be nice and return a list regardless.
        if (smallest == null) {
            log.info("No match for query, using all: key:{} filter:{}", key,
                    filter);
            return getAll();
        }

        // log.info("{} matche(s) for query: key:{} filter:{} matched(tag:{} key:{})",
        // smallest.size(), key, filter, matchedTag, matchedKey);
        return smallest;
    }

    private synchronized Map<Map.Entry<String, String>, List<TimeSerie>> getByTag() {
        return byTag;
    }

    private synchronized Map<String, List<TimeSerie>> getByKey() {
        return byKey;
    }

    private synchronized List<TimeSerie> getAll() {
        return all;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.spotify.heroic.backend.MetadataBackend#isReady()
     */
    @Override
    public synchronized boolean isReady() {
        return ready;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }
}
