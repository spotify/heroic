package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.model.MetricGroups;
import com.spotify.heroic.backend.model.PreparedGroup;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;

@RequiredArgsConstructor
public final class RowGroupsTransformer implements Callback.DeferredTransformer<RowGroups, MetricGroups> {
    private final AggregationCache cache;
    private final AggregatorGroup aggregator;
    private final DateRange range;

    public void transform(RowGroups result, Callback<MetricGroups> callback) throws Exception {
        if (cache == null) {
            callback.reduce(executeQueries(result.getGroups()), new JoinQueryMetricsResult());
            return;
        }

        callback.reduce(executeCacheQueries(result.getGroups()), new JoinQueryMetricsResult());
    }

    private List<Callback<MetricGroups>> executeCacheQueries(
            Map<TimeSerie, List<PreparedGroup>> groups) {
        final List<Callback<MetricGroups>> callbacks = new ArrayList<Callback<MetricGroups>>();

        for (Map.Entry<TimeSerie, List<PreparedGroup>> entry : groups.entrySet()) {
            final TimeSerie timeSerie = entry.getKey();
            final TimeSerieSlice slice = timeSerie.slice(range);
            final List<PreparedGroup> preparedGroups = entry.getValue();

            final CacheGetTransformer transformer = new CacheGetTransformer(timeSerie, cache) {
                @Override
                public Callback<MetricGroups> cacheMiss(TimeSerieSlice slice) throws Exception {
                    return executeSingle(slice, preparedGroups);
                }
            };

            callbacks.add(cache.get(slice, aggregator).transform(transformer));
        }

        return callbacks;
    }

    private Callback<MetricGroups> executeSingle(TimeSerieSlice slice, List<PreparedGroup> preparedGroups) {
        final Aggregator.Session session = aggregator.session(slice.getRange());

        final Callback<MetricGroups> partial = new ConcurrentCallback<MetricGroups>();
        final Callback.StreamReducer<FetchDataPoints.Result, MetricGroups> reducer;

        if (session == null) {
            reducer = new SimpleCallbackStream(slice);
        } else {
            reducer = new AggregatedCallbackStream(slice, session);
        }

        final List<Callback<FetchDataPoints.Result>> backendQueries = new ArrayList<Callback<FetchDataPoints.Result>>();

        for (final PreparedGroup prepared : preparedGroups) {
            final MetricBackend backend = prepared.getBackend();
            backendQueries.addAll(
                    backend.query(new FetchDataPoints(prepared.getTimeSerie(), slice.getRange())));
        }

        return partial.reduce(backendQueries, reducer);
    }

    private List<Callback<MetricGroups>> executeQueries(
            final Map<TimeSerie, List<PreparedGroup>> groups)
            throws Exception {
        final List<Callback<MetricGroups>> queries = new ArrayList<Callback<MetricGroups>>();

        for (final Map.Entry<TimeSerie, List<PreparedGroup>> entry : groups.entrySet()) {
            final TimeSerie timeSerie = entry.getKey();
            final TimeSerieSlice slice = timeSerie.slice(range);
            final List<PreparedGroup> preparedGroups = entry.getValue();
            queries.add(executeSingle(slice, preparedGroups));
        }

        return queries;
    }
}