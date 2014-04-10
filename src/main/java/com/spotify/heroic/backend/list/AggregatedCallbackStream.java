package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackStream;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.backend.BackendManager.DataPointGroup;
import com.spotify.heroic.backend.BackendManager.QueryMetricsResult;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.RowStatistics;

@Slf4j
public class AggregatedCallbackStream implements
        CallbackStream.Handle<MetricBackend.DataPointsResult> {
    private final Map<String, String> tags;
    private final Aggregator.Session session;
    private final Callback<QueryMetricsResult> callback;

    AggregatedCallbackStream(Map<String, String> tags,
            Aggregator.Session session, Callback<QueryMetricsResult> callback) {
        this.tags = tags;
        this.session = session;
        this.callback = callback;
    }

    @Override
    public void finish(Callback<MetricBackend.DataPointsResult> callback,
            MetricBackend.DataPointsResult result) throws Exception {
        session.stream(result.getDatapoints());
    }

    @Override
    public void error(Callback<MetricBackend.DataPointsResult> callback,
            Throwable error) throws Exception {
        log.error("Result failed: " + error, error);
    }

    @Override
    public void cancel(Callback<MetricBackend.DataPointsResult> callback,
            CancelReason reason) throws Exception {
        log.error("Result cancelled: " + reason);
    }

    @Override
    public void done(int successful, int failed, int cancelled)
            throws Exception {
        final Aggregator.Result result = session.result();
        final RowStatistics rowStatistics = new RowStatistics(successful,
                failed, cancelled);

        final List<DataPointGroup> groups = new ArrayList<DataPointGroup>();
        groups.add(new DataPointGroup(tags, result.getResult()));

        callback.finish(new QueryMetricsResult(groups, result.getSampleSize(),
                result.getOutOfBounds(), rowStatistics));
    }
}