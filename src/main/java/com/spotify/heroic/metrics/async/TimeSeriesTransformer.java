package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.CancelledCallback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.CacheOperationException;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.metrics.model.FetchData;
import com.spotify.heroic.metrics.model.GroupedSeries;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@RequiredArgsConstructor
public final class TimeSeriesTransformer implements
        Callback.DeferredTransformer<List<GroupedSeries>, MetricGroups> {
    private final AggregationCache cache;
    private final Filter filter;
    private final AggregationGroup aggregation;
    private final DateRange range;

    @Override
    public Callback<MetricGroups> transform(List<GroupedSeries> result)
            throws Exception {
        if (!cache.isConfigured() || aggregation == null)
            return ConcurrentCallback.newReduce(execute(result),
                    new MergeMetricGroups());

        return ConcurrentCallback.newReduce(executeCached(result),
                new MergeMetricGroups());
    }

    private List<Callback<MetricGroups>> execute(
            final List<GroupedSeries> result) throws Exception {
        final List<Callback<MetricGroups>> queries = new ArrayList<Callback<MetricGroups>>();

        for (final GroupedSeries r : result) {
            queries.add(buildLookup(r.getBackend(), r.getGroup(), range,
                    r.getSeries()));
        }

        return queries;
    }

    private List<Callback<MetricGroups>> executeCached(
            List<GroupedSeries> result) throws CacheOperationException {
        final List<Callback<MetricGroups>> callbacks = new ArrayList<Callback<MetricGroups>>();

        for (final GroupedSeries r : result) {
            callbacks.add(buildCachedLookup(r.getBackend(), r.getGroup(),
                    r.getSeries()));
        }

        return callbacks;
    }

    private Callback<MetricGroups> buildCachedLookup(final Backend backend,
            final Map<String, String> group, final Set<Series> series)
                    throws CacheOperationException {
        final CacheGetTransformer transformer = new CacheGetTransformer(cache) {
            @Override
            public Callback<MetricGroups> cacheMiss(Map<String, String> group,
                    DateRange range) throws Exception {
                return buildLookup(backend, group, range, series);
            }
        };

        return cache.get(filter, group, aggregation, range).transform(
                transformer);
    }

    private Callback<MetricGroups> buildLookup(final Backend backend,
            final Map<String, String> group, final DateRange range,
            final Set<Series> series) {
        final List<Callback<FetchData>> callbacks = new ArrayList<Callback<FetchData>>();

        final DateRange modified = modifiedRange(range);

        for (final Series serie : series) {
            callbacks.addAll(backend.fetch(serie, modified));
        }

        if (callbacks.isEmpty())
            return new CancelledCallback<MetricGroups>(
                    CancelReason.BACKEND_MISMATCH);

        return ConcurrentCallback.newReduce(callbacks,
                buildReducer(group, range));
    }

    private DateRange modifiedRange(final DateRange range) {
        if (aggregation == null)
            return range;

        return range.shiftStart(-aggregation.getSampling().getExtent());
    }

    private Callback.StreamReducer<FetchData, MetricGroups> buildReducer(
            Map<String, String> group, final DateRange range) {
        if (aggregation == null)
            return new SimpleCallbackStream(group);

        return new AggregatedCallbackStream(group, aggregation.session(range));
    }
}