package com.spotify.heroic.backend;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Inject;
import javax.inject.Singleton;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.backend.BackendManager.GetAllRowsResult;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;

@Singleton
@Slf4j
public class TagsCacheManager {
    @Inject
    private BackendManager backendManager;
    protected Map<String, List<DataPointsRowKey>> rows;
    private final AtomicBoolean inProgress = new AtomicBoolean(false);

    public void refresh() {
        if (!inProgress.compareAndSet(false, true)) {
            log.warn("Refresh already in progress");
            return;
        }

        log.info("Refreshing tags cache");

        final Callback<GetAllRowsResult> callback = backendManager.getAllRows();
        callback.listen(new Callback.Handle<BackendManager.GetAllRowsResult>() {
            @Override
            public void cancel() throws Exception {
                log.warn("request for tags cache refresh was cancelled.");
                inProgress.set(false);
            }

            @Override
            public void error(Throwable e) throws Exception {
                log.error("Failed to refresh tags cache", e);
                inProgress.set(false);
            }

            @Override
            public void finish(GetAllRowsResult result) throws Exception {
                synchronized (TagsCacheManager.this) {
                    TagsCacheManager.this.rows = result.getRows();
                }
                inProgress.set(false);
            }
        });
    }

    public static class FindTagsResult {
        @Getter
        private final Map<String, Set<String>> tags;

        @Getter
        private final List<String> metrics;

        public FindTagsResult(Map<String, Set<String>> tags,
                List<String> metrics) {
            this.tags = tags;
            this.metrics = metrics;
        }
    }

    public FindTagsResult findTags(Map<String, String> filter,
            Set<String> namesFilter) {
        final Map<String, Set<String>> tags = new HashMap<String, Set<String>>();
        final List<String> metrics = new ArrayList<String>();

        final Map<String, List<DataPointsRowKey>> rows = getRows();
        for (final Map.Entry<String, List<DataPointsRowKey>> row : rows
                .entrySet()) {
            boolean anyMatch = false;

            for (final DataPointsRowKey rowKey : row.getValue()) {
                if (!matchingTags(rowKey.getTags(), filter)) {
                    continue;
                }

                for (final Map.Entry<String, String> entry : rowKey.getTags()
                        .entrySet()) {
                    if (namesFilter != null
                            && !namesFilter.contains(entry.getKey())) {
                        continue;
                    }

                    anyMatch = true;

                    Set<String> values = tags.get(entry.getKey());

                    if (values == null) {
                        values = new HashSet<String>();
                        tags.put(entry.getKey(), values);
                    }

                    values.add(entry.getValue());
                }
            }

            if (anyMatch)
                metrics.add(row.getKey());
        }

        return new FindTagsResult(tags, metrics);
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

    private synchronized Map<String, List<DataPointsRowKey>> getRows() {
        return rows;
    }
}
