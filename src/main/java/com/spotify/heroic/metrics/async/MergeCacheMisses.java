package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.CacheOperationException;
import com.spotify.heroic.cache.model.CacheBackendKey;
import com.spotify.heroic.cache.model.CachePutResult;
import com.spotify.heroic.cache.model.CacheQueryResult;
import com.spotify.heroic.metrics.model.MetricGroup;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.RequestError;
import com.spotify.heroic.metrics.model.Statistics;
import com.spotify.heroic.model.DataPoint;

@Slf4j
@RequiredArgsConstructor
final class MergeCacheMisses implements
        Callback.Reducer<MetricGroups, MetricGroups> {
    @Data
    private static final class JoinResult {
        private final Map<Long, DataPoint> resultSet;
        private final Map<Map<String, String>, List<DataPoint>> cacheUpdates;
        private final Statistics statistics;
        private final List<RequestError> errors;
    }

    private final AggregationCache cache;
    private final CacheQueryResult cacheResult;

    @Override
    public MetricGroups resolved(Collection<MetricGroups> results,
            Collection<Exception> errors, Collection<CancelReason> cancelled)
            throws Exception {

        final MergeCacheMisses.JoinResult joinResults = joinResults(results);
        final List<MetricGroup> groups = buildDataPointGroups(joinResults);

        final Statistics statistics = joinResults.getStatistics();

        if (statistics.getRow().getFailed() == 0) {
            updateCache(joinResults.getCacheUpdates());
        } else {
            log.warn(
                    "Not updating cache because failed requests is non-zero: {}",
                    statistics);
        }

        return new MetricGroups(groups, joinResults.getStatistics(),
                joinResults.getErrors());
    }

    private List<Callback<CachePutResult>> updateCache(
            Map<Map<String, String>, List<DataPoint>> cacheUpdates)
                    throws CacheOperationException {
        final List<Callback<CachePutResult>> queries = new ArrayList<Callback<CachePutResult>>(
                cacheUpdates.size());

        for (final Map.Entry<Map<String, String>, List<DataPoint>> update : cacheUpdates
                .entrySet()) {
            final Map<String, String> group = update.getKey();
            final List<DataPoint> datapoints = update.getValue();

            final CacheBackendKey key = cacheResult.getKey();
            queries.add(cache.put(key.getFilter(), group, key.getAggregation(),
                    datapoints));
        }

        return queries;
    }

    private List<MetricGroup> buildDataPointGroups(
            MergeCacheMisses.JoinResult joinResult) {
        final List<MetricGroup> groups = new ArrayList<MetricGroup>();
        final List<DataPoint> datapoints = new ArrayList<DataPoint>(joinResult
                .getResultSet().values());
        Collections.sort(datapoints);

        groups.add(new MetricGroup(cacheResult.getKey().getGroup(), datapoints));
        return groups;
    }

    /**
     * Use a map from <code>{tags -> {long -> DataPoint}}</code> to deduplicate
     * overlapping datapoints.
     *
     * These overlaps are called 'cache duplicates', which mean that we've
     * somehow managed to fetch duplicate entries from one of.
     *
     * <ul>
     * <li>The cache backend.</li>
     * <li>The raw backends.</li>
     * </ul>
     *
     * While this contributes to unnecessary overhead, it's not the end of the
     * world. These duplicates are reported as cacheDuplicates in RowStatistics.
     */
    private MergeCacheMisses.JoinResult joinResults(
            Collection<MetricGroups> results) {
        final Map<Map<String, String>, List<DataPoint>> cacheUpdates = new HashMap<Map<String, String>, List<DataPoint>>();

        int cacheConflicts = 0;

        final Map<Long, DataPoint> resultSet = new HashMap<Long, DataPoint>();

        final AddCached cachedResults = addCached(resultSet,
                cacheResult.getResult());

        Statistics statistics = Statistics.EMPTY;

        final List<RequestError> errors = new ArrayList<>();

        for (final MetricGroups result : results) {
            errors.addAll(result.getErrors());
            statistics = statistics.merge(result.getStatistics());

            for (final MetricGroup group : result.getGroups()) {
                for (final DataPoint d : group.getDatapoints()) {
                    if (resultSet.put(d.getTimestamp(), d) != null) {
                        ++cacheConflicts;
                    }
                }

                cacheUpdates.put(group.getGroup(), group.getDatapoints());
            }
        }

        return new JoinResult(resultSet, cacheUpdates, Statistics
                .builder(statistics)
                .cache(new Statistics.Cache(cachedResults.getHits(),
                        cacheConflicts, cachedResults.getConflicts(),
                        cachedResults.getNans())).build(), errors);
    }

    @RequiredArgsConstructor
    private static final class AddCached {
        @Getter
        private final int conflicts;
        @Getter
        private final int hits;
        @Getter
        private final int nans;
    }

    /**
     * Add the results previously retrieved from cache.
     *
     * @param resultGroups
     * @return
     */
    private AddCached addCached(Map<Long, DataPoint> resultSet,
            List<DataPoint> datapoints) {
        int conflicts = 0;
        int hits = 0;
        int nans = 0;

        for (final DataPoint d : datapoints) {
            if (Double.isNaN(d.getValue())) {
                ++nans;
                continue;
            }

            if (resultSet.put(d.getTimestamp(), d) != null) {
                ++conflicts;
            } else {
                ++hits;
            }
        }

        return new AddCached(conflicts, hits, nans);
    }
}
