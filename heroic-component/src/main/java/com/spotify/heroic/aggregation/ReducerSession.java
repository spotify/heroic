package com.spotify.heroic.aggregation;

import java.util.List;
import java.util.Map;

import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

/**
 * A reducer session is the last step of a distributed aggregation.
 *
 * It is responsible for non-destructively combine the result of several aggregations.
 * 
 * @see AggregationInstance#distributed()
 * @author udoprog
 */
public interface ReducerSession {
    public void updatePoints(Map<String, String> group, List<Point> values);

    public void updateEvents(Map<String, String> group, List<Event> values);

    public void updateSpreads(Map<String, String> group, List<Spread> values);

    public void updateGroup(Map<String, String> group, List<MetricGroup> values);

    public ReducerResult result();
}