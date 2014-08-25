package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.CancelledCallback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.GroupedTimeSeries;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.SeriesSlice;

@RequiredArgsConstructor
public final class TimeSeriesTransformer implements
        Callback.DeferredTransformer<List<GroupedTimeSeries>, MetricGroups> {
    private final AggregationCache cache;
    private final AggregationGroup aggregation;
    private final DateRange range;

    @Override
    public Callback<MetricGroups> transform(List<GroupedTimeSeries> result)
            throws Exception {
        if (cache == null || aggregation == null)
            return ConcurrentCallback.newReduce(execute(result),
                    new MergeMetricGroups());

        return ConcurrentCallback.newReduce(executeCached(result),
                new MergeMetricGroups());
    }

    private List<Callback<MetricGroups>> execute(
            final List<GroupedTimeSeries> result) throws Exception {
        final List<Callback<MetricGroups>> queries = new ArrayList<Callback<MetricGroups>>();

        for (final GroupedTimeSeries r : result) {
            final SeriesSlice slice = r.getKey().slice(range);
            queries.add(buildLookup(r.getBackend(), slice, r.getSeries()));
        }

        return queries;
    }

    private List<Callback<MetricGroups>> executeCached(
            List<GroupedTimeSeries> result) {
        final List<Callback<MetricGroups>> callbacks = new ArrayList<Callback<MetricGroups>>();

        for (final GroupedTimeSeries r : result) {
            callbacks.add(buildCachedLookup(r.getBackend(), r.getKey(),
                    r.getSeries()));
        }

        return callbacks;
    }

    private Callback<MetricGroups> buildCachedLookup(final Backend backend,
            final Series series, final Set<Series> seriesSet) {
        final CacheGetTransformer transformer = new CacheGetTransformer(series,
                cache) {
            @Override
            public Callback<MetricGroups> cacheMiss(SeriesSlice slice)
                    throws Exception {
                return buildLookup(backend, slice, seriesSet);
            }
        };

        return cache.get(series.slice(range), aggregation).transform(
                transformer);
    }

    private Callback<MetricGroups> buildLookup(final Backend backend,
            final SeriesSlice slice, final Set<Series> series) {
        final List<Callback<FetchDataPoints.Result>> callbacks = new ArrayList<Callback<FetchDataPoints.Result>>();

        final DateRange range = modifiedRange(slice);

        for (final Series serie : series) {
            callbacks.addAll(backend.query(serie, range));
        }

        if (callbacks.isEmpty())
            return new CancelledCallback<MetricGroups>(
                    CancelReason.BACKEND_MISMATCH);

        return ConcurrentCallback.newReduce(callbacks, buildReducer(slice));
    }

    private DateRange modifiedRange(final SeriesSlice slice) {
        if (aggregation == null)
            return slice.getRange();

        return slice.getRange().shiftStart(
                -aggregation.getSampling().getExtent());
    }

    private Callback.StreamReducer<FetchDataPoints.Result, MetricGroups> buildReducer(
            SeriesSlice slice) {
        if (aggregation == null)
            return new SimpleCallbackStream(slice);

        final Aggregation.Session session = aggregation.session(slice
                .getRange());
        return new AggregatedCallbackStream(slice, session);
    }
}