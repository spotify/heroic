package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.MetricGroup;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.Statistics;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;

@Slf4j
@RequiredArgsConstructor
public class AggregatedCallbackStream implements
        Callback.StreamReducer<FetchDataPoints.Result, MetricGroups> {
    private final TimeSerieSlice slice;
    private final Aggregator.Session session;

    private final Queue<Throwable> errors = new ConcurrentLinkedQueue<Throwable>();
    private final Queue<CancelReason> cancellations = new ConcurrentLinkedQueue<CancelReason>();

    @Override
    public void resolved(Callback<FetchDataPoints.Result> callback,
            FetchDataPoints.Result result) throws Exception {
        session.stream(result.getDatapoints());
    }

    @Override
    public void failed(Callback<FetchDataPoints.Result> callback, Exception error)
            throws Exception {
        errors.add(error);
    }

    @Override
    public void cancelled(Callback<FetchDataPoints.Result> callback, CancelReason reason)
            throws Exception {
        cancellations.add(reason);
    }

    @Override
    public MetricGroups resolved(int successful, int failed, int cancelled)
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

        final Aggregator.Result result = session.result();

        final Statistics stat = new Statistics();
        stat.setAggregator(result.getStatistics());
        stat.setRow(new Statistics.Row(successful, failed, cancelled));

        final TimeSerie timeSerie = slice.getTimeSerie();

        final List<MetricGroup> groups = new ArrayList<MetricGroup>();
        groups.add(new MetricGroup(timeSerie, result.getResult()));

        return new MetricGroups(groups, stat);
    }
}