package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.BackendManager.QueryMetricsResult;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.list.FindRowGroupsReducer.PreparedGroup;
import com.spotify.heroic.backend.model.FetchDataPoints;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;

@RequiredArgsConstructor
public final class RowGroupsTransformer implements Callback.Transformer<RowGroups, QueryMetricsResult> {
    private final AggregationCache cache;
    private final AggregatorGroup aggregator;
    private final DateRange range;

    public void transform(RowGroups result, Callback<QueryMetricsResult> callback) throws Exception {
        if (cache == null) {
            callback.reduce(executeQueries(result.getGroups()), new JoinQueryMetricsResult());
            return;
        }

        callback.reduce(executeCacheQueries(result.getGroups()), new JoinQueryMetricsResult());
    }

    private List<Callback<QueryMetricsResult>> executeCacheQueries(
            Map<TimeSerie, List<PreparedGroup>> groups) {
        final List<Callback<QueryMetricsResult>> callbacks = new ArrayList<Callback<QueryMetricsResult>>();

        for (Map.Entry<TimeSerie, List<PreparedGroup>> entry : groups.entrySet()) {
            final TimeSerie timeSerie = entry.getKey();
            final TimeSerieSlice slice = timeSerie.slice(range);
            final List<PreparedGroup> preparedGroups = entry.getValue();

            final CacheGetTransformer transformer = new CacheGetTransformer(timeSerie, cache) {
                @Override
                public Callback<QueryMetricsResult> cacheMiss(TimeSerieSlice slice) throws Exception {
                    return executeSingle(slice, preparedGroups);
                }
            };

            callbacks.add(cache.get(slice, aggregator).transform(transformer));
        }

        return callbacks;
    }

    private Callback<QueryMetricsResult> executeSingle(TimeSerieSlice slice, List<PreparedGroup> preparedGroups) {
        final Aggregator.Session session = aggregator.session(slice.getRange());

        final Callback<QueryMetricsResult> partial = new ConcurrentCallback<QueryMetricsResult>();
        final Callback.StreamReducer<FetchDataPoints.Result, QueryMetricsResult> reducer;

        if (session == null) {
            reducer = new SimpleCallbackStream(slice);
        } else {
            reducer = new AggregatedCallbackStream(slice, session);
        }

        final List<Callback<FetchDataPoints.Result>> backendQueries = new ArrayList<Callback<FetchDataPoints.Result>>();

        for (final PreparedGroup prepared : preparedGroups) {
            final MetricBackend backend = prepared.getBackend();
            backendQueries.addAll(
                    backend.query(new FetchDataPoints(prepared.getRows(), slice.getRange())));
        }

        return partial.reduce(backendQueries, reducer);
    }

    private List<Callback<QueryMetricsResult>> executeQueries(
            final Map<TimeSerie, List<PreparedGroup>> groups)
            throws Exception {
        final List<Callback<QueryMetricsResult>> queries = new ArrayList<Callback<QueryMetricsResult>>();

        for (final Map.Entry<TimeSerie, List<PreparedGroup>> entry : groups.entrySet()) {
            final TimeSerie timeSerie = entry.getKey();
            final TimeSerieSlice slice = timeSerie.slice(range);
            final List<PreparedGroup> preparedGroups = entry.getValue();
            queries.add(executeSingle(slice, preparedGroups));
        }

        return queries;
    }
}