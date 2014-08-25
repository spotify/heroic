package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.MetricGroup;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.Statistics;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.SeriesSlice;

@Slf4j
@RequiredArgsConstructor
public final class SimpleCallbackStream implements
Callback.StreamReducer<FetchDataPoints.Result, MetricGroups> {
    private final SeriesSlice slice;

    private final Queue<FetchDataPoints.Result> results = new ConcurrentLinkedQueue<FetchDataPoints.Result>();

    @Override
    public void resolved(Callback<FetchDataPoints.Result> callback,
            FetchDataPoints.Result result) throws Exception {
        results.add(result);
    }

    @Override
    public void failed(Callback<FetchDataPoints.Result> callback,
            Exception error) throws Exception {
        log.error("Error encountered when processing request", error);
    }

    @Override
    public void cancelled(Callback<FetchDataPoints.Result> callback,
            CancelReason reason) throws Exception {
        log.error("Cancel encountered when processing request", reason);
    }

    @Override
    public MetricGroups resolved(int successful, int failed, int cancelled)
            throws Exception {
        final List<DataPoint> datapoints = joinRawResults();
        final Series series = slice.getSeries();

        final Statistics statistics = Statistics.builder()
                .row(new Statistics.Row(successful, failed, cancelled))
                .aggregator(new Statistics.Aggregator(datapoints.size(), 0, 0))
                .build();

        final List<MetricGroup> groups = new ArrayList<MetricGroup>();
        groups.add(new MetricGroup(series, datapoints));

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