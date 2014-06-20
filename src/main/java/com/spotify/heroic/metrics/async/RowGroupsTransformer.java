package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.FindTimeSeries;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;

@RequiredArgsConstructor
public final class RowGroupsTransformer implements
        Callback.DeferredTransformer<List<FindTimeSeries.Result>, MetricGroups> {
    private final AggregationCache cache;
    private final AggregatorGroup aggregator;
    private final DateRange range;

    @Override
    public Callback<MetricGroups> transform(List<FindTimeSeries.Result> result)
            throws Exception {
        if (cache == null) {
            return ConcurrentCallback.newReduce(executeQueries(result),
                    new MergeMetricGroups());
        }

        return ConcurrentCallback.newReduce(executeCacheQueries(result),
                new MergeMetricGroups());
    }

    private List<Callback<MetricGroups>> executeCacheQueries(
            List<FindTimeSeries.Result> result) {
        final List<Callback<MetricGroups>> callbacks = new ArrayList<Callback<MetricGroups>>();

        for (final FindTimeSeries.Result r : result) {
            final MetricBackend backend = r.getBackend();

            for (Entry<TimeSerie, Set<TimeSerie>> entry : r.getRowGroups()
                    .entrySet()) {
                final TimeSerie timeSerie = entry.getKey();
                final Set<TimeSerie> timeSeries = entry.getValue();
                final TimeSerieSlice slice = timeSerie.slice(range);

                final CacheGetTransformer transformer = new CacheGetTransformer(
                        timeSerie, cache) {
                    @Override
                    public Callback<MetricGroups> cacheMiss(TimeSerieSlice slice)
                            throws Exception {
                        return executeSingle(slice, backend, timeSeries);
                    }
                };

                callbacks.add(cache.get(slice, aggregator).transform(
                        transformer));
            }
        }

        return callbacks;
    }

    private List<Callback<MetricGroups>> executeQueries(
            final List<FindTimeSeries.Result> result) throws Exception {
        final List<Callback<MetricGroups>> queries = new ArrayList<Callback<MetricGroups>>();

        for (final FindTimeSeries.Result r : result) {
            for (Entry<TimeSerie, Set<TimeSerie>> entry : r.getRowGroups()
                    .entrySet()) {
                final TimeSerieSlice slice = entry.getKey().slice(range);
                queries.add(executeSingle(slice, r.getBackend(),
                        entry.getValue()));
            }
        }

        return queries;
    }

    private Callback<MetricGroups> executeSingle(TimeSerieSlice slice,
            MetricBackend backend, Set<TimeSerie> timeSeries) {
        final Aggregator.Session session = aggregator.session(slice.getRange());

        final Callback<MetricGroups> partial = new ConcurrentCallback<MetricGroups>();
        final Callback.StreamReducer<FetchDataPoints.Result, MetricGroups> reducer;

        if (session == null) {
            reducer = new SimpleCallbackStream(slice);
        } else {
            reducer = new AggregatedCallbackStream(slice, session);
        }

        final List<Callback<FetchDataPoints.Result>> backendQueries = new ArrayList<Callback<FetchDataPoints.Result>>();

        for (final TimeSerie timeSerie : timeSeries) {
            backendQueries.addAll(backend.query(timeSerie, slice.getRange()));
        }

        return partial.reduce(backendQueries, reducer);
    }
}