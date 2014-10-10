package com.spotify.heroic.metric.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.StreamReducer;
import com.spotify.heroic.metric.model.FetchData;
import com.spotify.heroic.metric.model.MetricGroup;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.model.Statistics;

@Slf4j
@RequiredArgsConstructor
public class AggregatedCallbackStream implements StreamReducer<FetchData, MetricGroups> {
    private final Map<String, String> group;
    private final Aggregation.Session session;

    @Override
    public void resolved(Future<FetchData> callback, FetchData result) throws Exception {
        session.update(result.getDatapoints());
    }

    @Override
    public void failed(Future<FetchData> callback, Exception error) throws Exception {
        log.error("Request failed: " + error.toString(), error);
    }

    @Override
    public void cancelled(Future<FetchData> callback, CancelReason reason) throws Exception {
    }

    @Override
    public MetricGroups resolved(int successful, int failed, int cancelled) throws Exception {
        if (failed > 0)
            throw new Exception("Some time series could not be fetched from the database");

        final Aggregation.Result result = session.result();

        final Statistics stat = Statistics.builder().aggregator(result.getStatistics())
                .row(new Statistics.Row(successful, failed, cancelled)).build();

        final List<MetricGroup> groups = new ArrayList<MetricGroup>();
        groups.add(new MetricGroup(group, result.getResult()));

        return MetricGroups.fromResult(groups, stat);
    }
}