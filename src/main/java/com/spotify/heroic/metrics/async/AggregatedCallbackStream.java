package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregation.Aggregation;
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
public class AggregatedCallbackStream implements Callback.StreamReducer<FetchDataPoints.Result, MetricGroups> {
    private final TimeSerieSlice slice;
    private final Aggregation.Session session;

    @Override
    public void resolved(Callback<FetchDataPoints.Result> callback,
            FetchDataPoints.Result result) throws Exception {
        session.stream(result.getDatapoints());
    }

    @Override
    public void failed(Callback<FetchDataPoints.Result> callback, Exception error)
            throws Exception {
        log.error("Error encountered when processing request", error);
    }

    @Override
    public void cancelled(Callback<FetchDataPoints.Result> callback, CancelReason reason)
            throws Exception {
        log.error("Cancel encountered when processing request", reason);
    }

    @Override
    public MetricGroups resolved(int successful, int failed, int cancelled) {
        final Aggregation.Result result = session.result();

        final Statistics stat = Statistics.builder()
                .aggregator(result.getStatistics())
                .row(new Statistics.Row(successful, failed, cancelled)).build();

        final TimeSerie timeSerie = slice.getTimeSerie();

        final List<MetricGroup> groups = new ArrayList<MetricGroup>();
        groups.add(new MetricGroup(timeSerie, result.getResult()));

        return new MetricGroups(groups, stat);
    }
}