package com.spotify.heroic.metric.async;

import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.DeferredTransformer;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.Futures;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.PreparedQuery;
import com.spotify.heroic.model.DateRange;

@RequiredArgsConstructor
public final class PreparedQueryTransformer implements DeferredTransformer<List<PreparedQuery>, MetricGroups> {
    private final DateRange rounded;
    private final AggregationGroup aggregation;

    @Override
    public Future<MetricGroups> transform(List<PreparedQuery> queries) throws Exception {
        final List<Future<MetricGroups>> callbacks = new ArrayList<>();

        for (final PreparedQuery query : queries) {
            callbacks.add(query.query(rounded, aggregation));
        }

        return Futures.reduce(callbacks, MetricGroups.merger());
    }
}