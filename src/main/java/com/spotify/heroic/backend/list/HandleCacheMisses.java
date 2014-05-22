package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.backend.BackendManager.DataPointGroup;
import com.spotify.heroic.backend.BackendManager.QueryMetricsResult;
import com.spotify.heroic.backend.RowStatistics;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.model.CachePutResult;
import com.spotify.heroic.cache.model.CacheQueryResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;

final class HandleCacheMisses implements
        Callback.Reducer<QueryMetricsResult, QueryMetricsResult> {
    private static final class JoinResult {
        @Getter
        final Map<Map<String, String>, Map<Long, DataPoint>> groups;
        @Getter
        final Map<TimeSerie, List<DataPoint>> cacheUpdates;
        @Getter
        final long sampleSize;
        @Getter
        final long outOfBounds;
        @Getter
        final RowStatistics statistics;

        public JoinResult(
                Map<Map<String, String>, Map<Long, DataPoint>> groups,
                Map<TimeSerie, List<DataPoint>> cacheUpdates,
                long sampleSize, long outOfBounds, RowStatistics statistics) {
            this.groups = groups;
            this.cacheUpdates = cacheUpdates;
            this.sampleSize = sampleSize;
            this.outOfBounds = outOfBounds;
            this.statistics = statistics;
        }
    }

    private final AggregationCache cache;
    private final CacheQueryResult cacheResult;
    private final boolean singular;

    public HandleCacheMisses(AggregationCache cache,
            CacheQueryResult cacheResult, boolean singular) {
        this.cache = cache;
        this.cacheResult = cacheResult;
        this.singular = singular;
    }

    @Override
    public QueryMetricsResult done(Collection<QueryMetricsResult> results,
            Collection<Throwable> errors, Collection<CancelReason> cancelled)
            throws Exception {

        final HandleCacheMisses.JoinResult joinResults = joinResults(results);
        final List<DataPointGroup> groups = buildDataPointGroups(joinResults);

        updateCache(joinResults.getCacheUpdates());

        return new QueryMetricsResult(groups, joinResults.getSampleSize(),
                joinResults.getOutOfBounds(), joinResults.getStatistics());
    }

    private List<Callback<CachePutResult>> updateCache(
            Map<TimeSerie, List<DataPoint>> cacheUpdates) {
        List<Callback<CachePutResult>> queries = new ArrayList<Callback<CachePutResult>>(
                cacheUpdates.size());

        for (Map.Entry<TimeSerie, List<DataPoint>> update : cacheUpdates
                .entrySet()) {
            final TimeSerie timeSerie = update.getKey();
            final List<DataPoint> datapoints = update.getValue();
            queries.add(cache.put(timeSerie, cacheResult.getAggregator(),
                    datapoints));
        }

        return queries;
    }

    private List<DataPointGroup> buildDataPointGroups(HandleCacheMisses.JoinResult joinResult) {
        final List<DataPointGroup> groups = new ArrayList<DataPointGroup>();

        for (Map.Entry<Map<String, String>, Map<Long, DataPoint>> entry : joinResult
                .getGroups().entrySet()) {
            final Map<String, String> tags = entry.getKey();
            final List<DataPoint> datapoints = new ArrayList<DataPoint>(
                    entry.getValue().values());

            Collections.sort(datapoints);

            groups.add(new DataPointGroup(tags, datapoints));
        }

        return groups;
    }

    /**
     * Use a map from <code>{tags -> {long -> DataPoint}}</code> to
     * deduplicate overlapping datapoints.
     * 
     * These overlaps are called 'cache duplicates', which mean that we've
     * somehow managed to fetch duplicate entries from one of.
     * 
     * <ul>
     * <li>The cache backend.</li>
     * <li>The raw backends.</li>
     * </ul>
     * 
     * While this contributes to unecessary overhead, it's not the end of
     * the world. These duplicates are reported as cacheDuplicates in
     * RowStatistics.
     */
    private HandleCacheMisses.JoinResult joinResults(Collection<QueryMetricsResult> results) {
        final Map<Map<String, String>, Map<Long, DataPoint>> resultGroups = new HashMap<Map<String, String>, Map<Long, DataPoint>>();
        final Map<TimeSerie, List<DataPoint>> cacheUpdates = new HashMap<TimeSerie, List<DataPoint>>();

        long sampleSize = 0;
        long outOfBounds = 0;

        int rowsSuccessful = 0;
        int rowsFailed = 0;
        int rowsCancelled = 0;

        int cacheDuplicates = addCachedResults(resultGroups);

        for (final QueryMetricsResult result : results) {
            sampleSize += result.getSampleSize();
            outOfBounds += result.getOutOfBounds();

            final RowStatistics statistics = result.getRowStatistics();
            rowsSuccessful += statistics.getSuccessful();
            rowsFailed += statistics.getFailed();
            rowsCancelled += statistics.getCancelled();

            for (final DataPointGroup group : result.getGroups()) {
                Map<Long, DataPoint> resultSet = resultGroups.get(group
                        .getTags());

                final Map<String, String> tags;

                if (singular) {
                    tags = null;
                } else {
                    tags = group.getTags();
                }

                if (resultSet == null) {
                    resultSet = new HashMap<Long, DataPoint>();
                    resultGroups.put(tags, resultSet);
                }

                for (final DataPoint d : group.getDatapoints()) {
                    if (resultSet.put(d.getTimestamp(), d) != null) {
                        cacheDuplicates += 1;
                    }
                }

                cacheUpdates.put(cacheResult.getSlice().getTimeSerie(),
                        group.getDatapoints());
            }
        }

        final RowStatistics rowStatistics = new RowStatistics(
                rowsSuccessful, rowsFailed, rowsCancelled, cacheDuplicates);

        return new JoinResult(resultGroups, cacheUpdates, sampleSize,
                outOfBounds, rowStatistics);
    }

    /**
     * Add the results previously retrieved from cache.
     * 
     * @param resultGroups
     * @return
     */
    private int addCachedResults(
            final Map<Map<String, String>, Map<Long, DataPoint>> resultGroups) {
        int cacheDuplicates = 0;

        final Map<Long, DataPoint> resultSet = new HashMap<Long, DataPoint>();

        for (final DataPoint d : cacheResult.getResult()) {
            if (resultSet.put(d.getTimestamp(), d) != null) {
                cacheDuplicates += 1;
            }
        }

        final Map<String, String> tags;

        if (singular) {
            tags = null;
        } else {
            tags = cacheResult.getSlice().getTimeSerie().getTags();
        }

        resultGroups.put(tags, resultSet);
        return cacheDuplicates;
    }
}