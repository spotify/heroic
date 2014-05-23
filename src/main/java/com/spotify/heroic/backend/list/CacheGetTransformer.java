package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.BackendManager.DataPointGroup;
import com.spotify.heroic.backend.BackendManager.QueryMetricsResult;
import com.spotify.heroic.backend.Statistics;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.model.CacheQueryResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerieSlice;

/**
 * Common class for taking a cache query result and building up new queries for the missing 'slices'.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public abstract class CacheGetTransformer implements Callback.Transformer<CacheQueryResult, QueryMetricsResult> {
    private final Map<String, String> tags;
    private final AggregationCache cache;

    public void transform(CacheQueryResult cacheResult, Callback<QueryMetricsResult> callback) throws Exception {
        final List<Callback<QueryMetricsResult>> missQueries = new ArrayList<Callback<QueryMetricsResult>>();

        for (final TimeSerieSlice slice : cacheResult.getMisses()) {
            missQueries.add(cacheMiss(slice));
        }

        /**
         * EVERYTHING in cache!
         */
        if (missQueries.isEmpty()) {
            final List<DataPoint> datapoints = cacheResult.getResult();
            final DataPointGroup group = new DataPointGroup(tags, datapoints);
            final List<DataPointGroup> groups = new ArrayList<DataPointGroup>();

            groups.add(group);

            callback.finish(new QueryMetricsResult(groups, 0, 0, new Statistics(0, 0, 0, 0, 0, datapoints.size())));
            return;
        }

        /**
         * Merge with queried data.
         */
        callback.reduce(missQueries, new CacheMissMerger(
                cache, tags, cacheResult));
    }

    public abstract Callback<QueryMetricsResult> cacheMiss(TimeSerieSlice slice) throws Exception;
}