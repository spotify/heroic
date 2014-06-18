package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.model.CacheQueryResult;
import com.spotify.heroic.metrics.model.MetricGroup;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.Statistics;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;

/**
 * Common class for taking a cache query result and building up new queries for the missing 'slices'.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public abstract class CacheGetTransformer implements Callback.DeferredTransformer<CacheQueryResult, MetricGroups> {
    private final TimeSerie timeSerie;
    private final AggregationCache cache;

    public void transform(CacheQueryResult cacheResult, Callback<MetricGroups> callback) throws Exception {
        final List<Callback<MetricGroups>> missQueries = new ArrayList<Callback<MetricGroups>>();

        for (final TimeSerieSlice slice : cacheResult.getMisses()) {
            missQueries.add(cacheMiss(slice));
        }

        /**
         * EVERYTHING in cache!
         */
        if (missQueries.isEmpty()) {
            final List<DataPoint> datapoints = cacheResult.getResult();
            final MetricGroup group = new MetricGroup(timeSerie, datapoints);
            final List<MetricGroup> groups = new ArrayList<MetricGroup>();

            groups.add(group);

            final Statistics stat = new Statistics();
            stat.setCache(new Statistics.Cache(datapoints.size(), 0, 0));

            callback.resolve(new MetricGroups(groups, stat));
            return;
        }

        /**
         * Merge with queried data.
         */
        callback.reduce(missQueries, new MergeCacheMisses(
                cache, timeSerie, cacheResult));
    }

    public abstract Callback<MetricGroups> cacheMiss(TimeSerieSlice slice) throws Exception;
}