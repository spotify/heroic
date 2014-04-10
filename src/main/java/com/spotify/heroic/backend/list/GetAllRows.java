package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.codahale.metrics.Timer;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackGroup;
import com.spotify.heroic.async.CallbackGroupHandle;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.BackendManager.GetAllRowsResult;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.TimeSerie;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;

public class GetAllRows {
    private final List<MetricBackend> backends;
    private final Timer timer;

    public GetAllRows(List<MetricBackend> backends, Timer timer) {
        this.backends = backends;
        this.timer = timer;
    }

    public Callback<GetAllRowsResult> execute() {
        final List<Callback<MetricBackend.GetAllRowsResult>> queries = new ArrayList<Callback<MetricBackend.GetAllRowsResult>>();
        final Callback<GetAllRowsResult> handle = new ConcurrentCallback<GetAllRowsResult>();

        for (final MetricBackend backend : backends) {
            queries.add(backend.getAllRows());
        }

        final CallbackGroupHandle<GetAllRowsResult, MetricBackend.GetAllRowsResult> groupHandle = new CallbackGroupHandle<GetAllRowsResult, MetricBackend.GetAllRowsResult>(
                handle, timer) {
            @Override
            public GetAllRowsResult execute(
                    Collection<MetricBackend.GetAllRowsResult> results,
                    Collection<Throwable> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                final Set<TimeSerie> result = new HashSet<TimeSerie>();

                for (final MetricBackend.GetAllRowsResult backendResult : results) {
                    final Map<String, List<DataPointsRowKey>> rows = backendResult
                            .getRows();

                    for (final Map.Entry<String, List<DataPointsRowKey>> entry : rows
                            .entrySet()) {
                        for (final DataPointsRowKey rowKey : entry.getValue()) {
                            result.add(new TimeSerie(rowKey.getMetricName(),
                                    rowKey.getTags()));
                        }
                    }
                }

                return new GetAllRowsResult(result);
            }
        };

        final CallbackGroup<MetricBackend.GetAllRowsResult> group = new CallbackGroup<MetricBackend.GetAllRowsResult>(
                queries, groupHandle);

        handle.register(group);

        return handle;
    }
}
