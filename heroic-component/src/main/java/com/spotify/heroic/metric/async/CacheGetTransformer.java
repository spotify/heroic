package com.spotify.heroic.metric.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregationcache.AggregationCache;
import com.spotify.heroic.aggregationcache.model.CacheQueryResult;
import com.spotify.heroic.async.DeferredTransformer;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.Futures;
import com.spotify.heroic.metric.model.MetricGroup;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Statistics;

/**
 * Common class for taking a cache query result and building up new queries for the missing 'slices'.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public abstract class CacheGetTransformer implements DeferredTransformer<CacheQueryResult, MetricGroups> {
    private final AggregationCache cache;

    @Override
    public Future<MetricGroups> transform(CacheQueryResult cacheResult) throws Exception {
        final List<Future<MetricGroups>> missQueries = new ArrayList<Future<MetricGroups>>();

        for (final DateRange miss : cacheResult.getMisses()) {
            missQueries.add(cacheMiss(cacheResult.getKey().getGroup(), miss));
        }

        /**
         * EVERYTHING in cache!
         */
        if (missQueries.isEmpty()) {
            final List<DataPoint> datapoints = cacheResult.getResult();
            final MetricGroup group = new MetricGroup(cacheResult.getKey().getGroup(), datapoints);
            final List<MetricGroup> groups = new ArrayList<MetricGroup>();

            groups.add(group);

            final Statistics stat = Statistics.builder().cache(new Statistics.Cache(datapoints.size(), 0, 0, 0))
                    .build();

            return Futures.resolved(MetricGroups.fromResult(groups, stat));
        }

        /**
         * Merge with queried data.
         */
        return Futures.reduce(missQueries, new MergeCacheMisses(cache, cacheResult));
    }

    public abstract Future<MetricGroups> cacheMiss(Map<String, String> group, DateRange miss) throws Exception;
}
