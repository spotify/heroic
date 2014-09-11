package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.PreparedQuery;
import com.spotify.heroic.model.DateRange;

@RequiredArgsConstructor
public final class PreparedQueryTransformer implements
        Callback.DeferredTransformer<List<PreparedQuery>, MetricGroups> {
    private final DateRange rounded;
    private final AggregationGroup aggregation;

    @Override
    public Callback<MetricGroups> transform(List<PreparedQuery> queries)
            throws Exception {
        final List<Callback<MetricGroups>> callbacks = new ArrayList<>();

        for (final PreparedQuery query : queries) {
            callbacks.add(query.query(rounded, aggregation));
        }

        return ConcurrentCallback.newReduce(callbacks, MetricGroups.merger());
    }
}