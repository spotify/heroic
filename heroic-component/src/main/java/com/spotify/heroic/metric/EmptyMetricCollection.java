package com.spotify.heroic.metric;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.common.Series;

public class EmptyMetricCollection extends MetricCollection {
    public EmptyMetricCollection() {
        super(MetricType.POINT, ImmutableList.of());
    }

    @Override
    public void updateAggregation(AggregationSession session, Map<String, String> tags, Set<Series> series) {
    }

    @Override
    public void updateBucket(Bucket bucket, Map<String, String> tags) {
    }
}