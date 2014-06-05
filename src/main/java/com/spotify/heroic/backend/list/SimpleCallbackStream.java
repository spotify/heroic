package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackStream;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.backend.BackendManager.MetricGroup;
import com.spotify.heroic.backend.BackendManager.MetricGroups;
import com.spotify.heroic.backend.Statistics;
import com.spotify.heroic.backend.model.FetchDataPoints;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;

@Slf4j
@RequiredArgsConstructor
public final class SimpleCallbackStream implements
        Callback.StreamReducer<FetchDataPoints.Result, MetricGroups> {
    private final TimeSerieSlice slice;

    private final Queue<FetchDataPoints.Result> results = new ConcurrentLinkedQueue<FetchDataPoints.Result>();
    private final Queue<Throwable> errors = new ConcurrentLinkedQueue<Throwable>();
    private final Queue<CancelReason> cancellations = new ConcurrentLinkedQueue<CancelReason>();

    @Override
    public void finish(CallbackStream<FetchDataPoints.Result> stream,
            Callback<FetchDataPoints.Result> callback,
            FetchDataPoints.Result result) throws Exception {
        results.add(result);
    }

    @Override
    public void error(CallbackStream<FetchDataPoints.Result> stream,
            Callback<FetchDataPoints.Result> callback, Exception error)
            throws Exception {
        errors.add(error);
    }

    @Override
    public void cancel(CallbackStream<FetchDataPoints.Result> stream,
            Callback<FetchDataPoints.Result> callback, CancelReason reason)
            throws Exception {
        cancellations.add(reason);
    }

    @Override
    public MetricGroups done(int successful, int failed, int cancelled)
            throws Exception {
        if (!errors.isEmpty()) {
            log.error("{} error(s) encountered when processing request", failed);

            int i = 0;

            for (final Throwable error : errors) {
                log.error("Error #{}", i++, error);
            }
        }

        if (!cancellations.isEmpty()) {
            log.error("{} cancellation(s) encountered when processing request", cancelled);

            int i = 0;

            for (final CancelReason reason : cancellations) {
                log.error("Cancel #{}: {}", i++, reason);
            }
        }

        final List<DataPoint> datapoints = joinRawResults();
        final TimeSerie timeSerie = slice.getTimeSerie();

        final Statistics statistics = new Statistics();
        statistics.setRow(new Statistics.Row(successful, failed, cancelled));
        statistics.setAggregator(new Statistics.Aggregator(datapoints.size(), 0));

        final List<MetricGroup> groups = new ArrayList<MetricGroup>();
        groups.add(new MetricGroup(timeSerie, datapoints));

        return new MetricGroups(groups, statistics);
    }

    private List<DataPoint> joinRawResults() {
        final List<DataPoint> datapoints = new ArrayList<DataPoint>();

        for (final FetchDataPoints.Result result : results) {
            datapoints.addAll(result.getDatapoints());
        }

        Collections.sort(datapoints);
        return datapoints;
    }
}