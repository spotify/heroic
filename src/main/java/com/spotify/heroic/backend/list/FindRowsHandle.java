package com.spotify.heroic.backend.list;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackGroup;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.BackendManager.QueryMetricsResult;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.backend.model.FetchDataPoints;
import com.spotify.heroic.backend.model.FindRows;
import com.spotify.heroic.model.TimeSerieSlice;

@RequiredArgsConstructor
public final class FindRowsHandle implements
        CallbackGroup.Handle<FindRows.Result> {
    private final TimeSerieSlice slice;
    private final Callback<QueryMetricsResult> callback;
    private final AggregatorGroup aggregator;
    private final long maxQueriableDataPoints;

    @Override
    public void done(final Collection<FindRows.Result> results,
            Collection<Throwable> errors, Collection<CancelReason> cancelled)
            throws Exception {

        final Aggregator.Session session = aggregator.session(slice.getRange());

        if (session == null) {
            countTheProcessDataPoints(results);
            return;
        }

        processDataPoints(results, session);
    }

    /**
     * Performs processDataPoints only if the amount of data points selected
     * does not exceed {@link #maxQueriableDataPoints}
     * 
     * @param results
     */
    private void countTheProcessDataPoints(
            final Collection<FindRows.Result> results) {
        final List<Callback<Long>> counters = buildCountRequests(results);

        final Callback<Void> check = new ConcurrentCallback<Void>();

        ConcurrentCallback.newReduce(counters, new CountThresholdCallbackStream(
                maxQueriableDataPoints, check))
                .register(new Callback.Handle<Void>() {
            @Override
            public void cancel(CancelReason reason) throws Exception {
                callback.cancel(reason);
            }

            @Override
            public void error(Throwable e) throws Exception {
                callback.fail(e);
            }

            @Override
            public void finish(Void result) throws Exception {
                processDataPoints(results, null);
            }
        });
    }

    /**
     * Sets up count requests for all the collected results.
     * 
     * @param results
     * @return
     */
    private List<Callback<Long>> buildCountRequests(
            final Collection<FindRows.Result> results) {
        final List<Callback<Long>> counters = new LinkedList<Callback<Long>>();

        for (final FindRows.Result result : results) {
            if (result.isEmpty())
                continue;

            final MetricBackend backend = result.getBackend();

            for (final DataPointsRowKey row : result.getRows()) {
                counters.add(backend.getColumnCount(row, slice.getRange()));
            }
        }

        return counters;
    }

    private void processDataPoints(Collection<FindRows.Result> results,
            final Aggregator.Session session) {
        final List<Callback<FetchDataPoints.Result>> queries = new LinkedList<Callback<FetchDataPoints.Result>>();

        for (final FindRows.Result result : results) {
            if (result.isEmpty())
                continue;

            final MetricBackend backend = result.getBackend();
            queries.addAll(backend.query(new FetchDataPoints(result
                    .getRows(), slice.getRange())));
        }

        final Callback.StreamReducer<FetchDataPoints.Result, QueryMetricsResult> reducer;

        if (session == null) {
            reducer = new SimpleCallbackStream(slice);
        } else {
            reducer = new AggregatedCallbackStream(slice, session);
        }

        callback.reduce(queries, reducer);
    }
}