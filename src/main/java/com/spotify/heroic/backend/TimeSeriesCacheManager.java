package com.spotify.heroic.backend;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    protected Set<TimeSeries> timeSeries;
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

                synchronized (TimeSeriesCacheManager.this) {
                    TimeSeriesCacheManager.this.timeSeries = result
                            .getTimeSeries();
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

        public FindTagsResult(Map<String, Set<String>> tags) {
            this.tags = tags;
        }
    }

    public FindTagsResult findTags(String key, Map<String, String> filter,
            Set<String> namesFilter) {
        final Map<String, Set<String>> tags = new HashMap<String, Set<String>>();

        final Set<TimeSeries> timeSeries = getTimeSeries();

        for (final TimeSeries timeSerie : timeSeries) {
            if (!matchingTags(timeSerie.getTags(), filter)) {
                continue;
            }

            if (key != null && !timeSerie.getKey().equals(key)) {
                continue;
            }

            for (Map.Entry<String, String> entry : timeSerie.getTags()
                    .entrySet()) {
                Set<String> current = tags.get(entry.getKey());

                if (current == null) {
                    current = new HashSet<String>();
                    tags.put(entry.getKey(), current);
                }

                current.add(entry.getValue());
            }
        }

        return new FindTagsResult(tags);
    }

    public static class FindTimeSeriesResult {
        @Getter
        private final List<TimeSeries> timeSeries;

        public FindTimeSeriesResult(List<TimeSeries> timeSeries) {
            this.timeSeries = timeSeries;
        }
    }

    public FindTimeSeriesResult findTimeSeries(String key,
            Map<String, String> filter, Set<String> namesFilter) {
        final List<TimeSeries> result = new LinkedList<TimeSeries>();

        final Set<TimeSeries> timeSeries = getTimeSeries();

        for (final TimeSeries timeSerie : timeSeries) {
            if (!matchingTags(timeSerie.getTags(), filter)) {
                continue;
            }

            if (key != null && !timeSerie.getKey().equals(key)) {
                continue;
            }

            result.add(timeSerie);
        }

        return new FindTimeSeriesResult(result);
    }

    public static class FindKeysResult {
        @Getter
        private final Set<String> keys;

        public FindKeysResult(Set<String> keys) {
            this.keys = keys;
        }
    }

    public FindKeysResult findKeys(String key, Map<String, String> filter,
            Set<String> namesFilter) {
        final Set<String> result = new HashSet<String>();

        final Set<TimeSeries> timeSeries = getTimeSeries();

        for (final TimeSeries timeSerie : timeSeries) {
            if (!matchingTags(timeSerie.getTags(), filter)) {
                continue;
            }

            if (key != null && !timeSerie.getKey().equals(key)) {
                continue;
            }

            result.add(timeSerie.getKey());
        }

        return new FindKeysResult(result);
    }

    private static boolean matchingTags(Map<String, String> tags,
            Map<String, String> queryTags) {
        // query not specified.
        if (queryTags != null) {
            // match the row tags with the query tags.
            for (final Map.Entry<String, String> entry : queryTags.entrySet()) {
                // check tags for the actual row.
                final String tagValue = tags.get(entry.getKey());

                if (tagValue == null || !tagValue.equals(entry.getValue())) {
                    return false;
                }
            }
        }

        return true;
    }

    private synchronized Set<TimeSeries> getTimeSeries() {
        return timeSeries;
    }
}
