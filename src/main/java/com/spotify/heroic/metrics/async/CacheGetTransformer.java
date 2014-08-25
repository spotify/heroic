package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.model.CacheQueryResult;
import com.spotify.heroic.metrics.model.MetricGroup;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.Statistics;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.SeriesSlice;

/**
 * Common class for taking a cache query result and building up new queries for
 * the missing 'slices'.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public abstract class CacheGetTransformer implements
        Callback.DeferredTransformer<CacheQueryResult, MetricGroups> {
    private final Series series;
    private final AggregationCache cache;

    @Override
    public Callback<MetricGroups> transform(CacheQueryResult cacheResult)
            throws Exception {
        final List<Callback<MetricGroups>> missQueries = new ArrayList<Callback<MetricGroups>>();

        for (final SeriesSlice slice : cacheResult.getMisses()) {
            missQueries.add(cacheMiss(slice));
        }

        /**
         * EVERYTHING in cache!
         */
        if (missQueries.isEmpty()) {
            final List<DataPoint> datapoints = cacheResult.getResult();
            final MetricGroup group = new MetricGroup(series, datapoints);
            final List<MetricGroup> groups = new ArrayList<MetricGroup>();

            groups.add(group);

            final Statistics stat = Statistics.builder()
                    .cache(new Statistics.Cache(datapoints.size(), 0, 0, 0))
                    .build();

            return new ResolvedCallback<MetricGroups>(new MetricGroups(groups,
                    stat));
        }

        /**
         * Merge with queried data.
         */
        return ConcurrentCallback.newReduce(missQueries, new MergeCacheMisses(
                cache, series, cacheResult));
    }

    public abstract Callback<MetricGroups> cacheMiss(SeriesSlice slice)
            throws Exception;
}
