package com.spotify.heroic.metric.async;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Transformer;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.QueryMetricsResult;
import com.spotify.heroic.model.DateRange;

@RequiredArgsConstructor
public class MetricGroupsTransformer implements Transformer<MetricGroups, QueryMetricsResult> {
    private final DateRange rounded;

    @Override
    public QueryMetricsResult transform(MetricGroups result) throws Exception {
        return new QueryMetricsResult(rounded, result);
    }
}