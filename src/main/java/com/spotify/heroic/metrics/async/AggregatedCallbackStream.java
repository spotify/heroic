package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.metrics.model.FetchData;
import com.spotify.heroic.metrics.model.MetricGroup;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.Statistics;

@RequiredArgsConstructor
public class AggregatedCallbackStream implements
Callback.StreamReducer<FetchData, MetricGroups> {
    private final Map<String, String> group;
    private final Aggregation.Session session;

    @Override
    public void resolved(Callback<FetchData> callback, FetchData result)
            throws Exception {
        session.update(result.getDatapoints());
    }

    @Override
    public void failed(Callback<FetchData> callback, Exception error)
            throws Exception {
    }

    @Override
    public void cancelled(Callback<FetchData> callback, CancelReason reason)
            throws Exception {
    }

    @Override
    public MetricGroups resolved(int successful, int failed, int cancelled) {
        final Aggregation.Result result = session.result();

        final Statistics stat = Statistics.builder()
                .aggregator(result.getStatistics())
                .row(new Statistics.Row(successful, failed, cancelled)).build();

        final List<MetricGroup> groups = new ArrayList<MetricGroup>();
        groups.add(new MetricGroup(group, result.getResult()));

        return new MetricGroups(groups, stat);
    }
}