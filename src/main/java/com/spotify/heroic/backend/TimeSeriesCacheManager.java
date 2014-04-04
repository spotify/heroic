package com.spotify.heroic.backend;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Inject;
import javax.inject.Singleton;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.BackendManager.GetAllTimeSeriesResult;

@Singleton
@Slf4j
public class TimeSeriesCacheManager {
    @Inject
    private BackendManager backendManager;
    private Map<Map.Entry<String, String>, List<TimeSerie>> byTag;
    private Map<String, List<TimeSerie>> byKey;
    private List<TimeSerie> all;

    private final AtomicBoolean inProgress = new AtomicBoolean(false);

    public void refresh() {
        if (!inProgress.compareAndSet(false, true)) {
            log.warn("Refresh already in progress");
            return;
        }

        log.info("Refreshing tags cache");

        final Callback<GetAllTimeSeriesResult> callback = backendManager
                .getAllRows();

        callback.register(new Callback.Handle<BackendManager.GetAllTimeSeriesResult>() {
            @Override
            public void cancel() throws Exception {
                log.warn("Request for tags cache refresh was cancelled");
            }

            @Override
            public void error(Throwable e) throws Exception {
                log.error("Failed to refresh tags cache", e);
            }

            @Override
            public void finish(GetAllTimeSeriesResult result) throws Exception {
                log.info("Successfully refreshed with {} timeserie(s)", result
                        .getTimeSeries().size());

                List<TimeSerie> timeSeries = new ArrayList<TimeSerie>(result
                        .getTimeSeries());

                final Map<Map.Entry<String, String>, List<TimeSerie>> byTag = calculateByTag(timeSeries);
                final Map<String, List<TimeSerie>> byKey = calculateByKey(timeSeries);

                synchronized (TimeSeriesCacheManager.this) {
                    TimeSeriesCacheManager.this.byTag = byTag;
                    TimeSeriesCacheManager.this.byKey = byKey;
                    TimeSeriesCacheManager.this.all = timeSeries;
                }
            }
        });

        callback.register(new Callback.Ended() {
            @Override
            public void ended() {
                inProgress.set(false);
            }
        });
    }

    public static class FindTagsResult {
        @Getter
        private final Map<String, Set<String>> tags;

        @Getter
        private final int size;

        public FindTagsResult(Map<String, Set<String>> tags, int size) {
            this.tags = tags;
            this.size = size;
        }
    }

    public FindTagsResult findTags(String key, Map<String, String> tags,
            Set<String> includes) {
        final Map<String, Set<String>> result = new HashMap<String, Set<String>>();

        final List<TimeSerie> timeSeries = findBestMatch(key, tags);

        for (final TimeSerie timeSerie : filter(timeSeries, key, tags, includes)) {
            for (Map.Entry<String, String> entry : timeSerie.getTags()
                    .entrySet()) {
                Set<String> current = result.get(entry.getKey());

                if (current == null) {
                    current = new HashSet<String>();
                    result.put(entry.getKey(), current);
                }

                current.add(entry.getValue());
            }
        }

        return new FindTagsResult(result, timeSeries.size());
    }

    public static class FindTimeSeriesResult {
        @Getter
        private final List<TimeSerie> timeSeries;

        @Getter
        private final int size;

        public FindTimeSeriesResult(List<TimeSerie> timeSeries, int size) {
            this.timeSeries = timeSeries;
            this.size = size;
        }
    }

    public FindTimeSeriesResult findTimeSeries(String key,
            Map<String, String> tags, Set<String> includes) {
        final List<TimeSerie> timeSeries = findBestMatch(key, tags);
        final List<TimeSerie> result = new LinkedList<TimeSerie>();

        for (final TimeSerie timeSerie : filter(timeSeries, key, tags, includes)) {
            result.add(timeSerie);
        }

        return new FindTimeSeriesResult(result, timeSeries.size());
    }

    public static class FindKeysResult {
        @Getter
        private final Set<String> keys;

        @Getter
        private final int size;

        public FindKeysResult(Set<String> keys, int size) {
            this.keys = keys;
            this.size = size;
        }
    }

    public FindKeysResult findKeys(String key, Map<String, String> tags,
            Set<String> includes) {
        final SortedSet<String> result = new TreeSet<String>();

        final List<TimeSerie> timeSeries = findBestMatch(key, tags);

        for (final TimeSerie timeSerie : filter(timeSeries, key, tags, includes)) {
            result.add(timeSerie.getKey());
        }

        return new FindKeysResult(result, timeSeries.size());
    }

    private static Iterable<TimeSerie> filter(final List<TimeSerie> series,
            final String key, final Map<String, String> filter,
            final Set<String> includes) {
        return new Iterable<TimeSerie>() {
            @Override
            public Iterator<TimeSerie> iterator() {
                return new FilteringTimeSerieIterator(series.iterator(), key,
                        filter, includes);
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
            List<TimeSerie> timeSeries) {
        final Map<Map.Entry<String, String>, List<TimeSerie>> byTag = new HashMap<Map.Entry<String, String>, List<TimeSerie>>();

        for (TimeSerie timeSerie : timeSeries) {
            for (Map.Entry<String, String> entry : timeSerie.getTags()
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
            List<TimeSerie> timeseries) {
        final Map<String, List<TimeSerie>> byTag = new HashMap<String, List<TimeSerie>>();

        for (TimeSerie t : timeseries) {
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
        Map.Entry<String, String> matchedTag = null;
        String matchedKey = null;

        if (filter != null) {
            final Map<Map.Entry<String, String>, List<TimeSerie>> byTag = getByTag();

            for (Map.Entry<String, String> entry : filter.entrySet()) {
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

        log.info(
                "{} matche(s) for query: key:{} filter:{} matched(tag:{} key:{})",
                smallest.size(), key, filter, matchedTag, matchedKey);
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
}
