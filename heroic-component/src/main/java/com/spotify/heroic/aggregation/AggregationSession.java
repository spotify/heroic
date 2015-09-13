package com.spotify.heroic.aggregation;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

public interface AggregationSession {
    public void updatePoints(Map<String, String> group, Set<Series> series, List<Point> values);

    public void updateEvents(Map<String, String> group, Set<Series> series, List<Event> values);

    public void updateSpreads(Map<String, String> group, Set<Series> series, List<Spread> values);

    public void updateGroup(Map<String, String> group, Set<Series> series, List<MetricGroup> values);

    /**
     * Get the result of this aggregator.
     */
    public AggregationResult result();
}