package com.spotify.heroic.metrics.async;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.QueryMetricsResult;
import com.spotify.heroic.model.DateRange;

@RequiredArgsConstructor
public class MetricGroupsTransformer implements
        Callback.Transformer<MetricGroups, QueryMetricsResult> {
    private final DateRange rounded;

    @Override
    public QueryMetricsResult transform(MetricGroups result) throws Exception {
        return new QueryMetricsResult(rounded, result);
    }
}