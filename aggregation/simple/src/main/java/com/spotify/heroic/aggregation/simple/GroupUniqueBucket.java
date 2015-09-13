package com.spotify.heroic.aggregation.simple;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.AbstractBucket;
import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.MetricTypedGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class GroupUniqueBucket extends AbstractBucket implements Bucket {
    final Set<Point> points = Collections.newSetFromMap(new ConcurrentHashMap<Point, Boolean>());
    final Set<Event> events = Collections.newSetFromMap(new ConcurrentHashMap<Event, Boolean>());
    final Set<Spread> spreads = Collections.newSetFromMap(new ConcurrentHashMap<Spread, Boolean>());
    final Set<MetricGroup> groups = Collections.newSetFromMap(new ConcurrentHashMap<MetricGroup, Boolean>());

    final long timestamp;

    public List<MetricTypedGroup> groups() {
        final ImmutableList.Builder<MetricTypedGroup> result = ImmutableList.builder();

        if (!points.isEmpty()) {
            result.add(new MetricTypedGroup(MetricType.POINT, ImmutableList.copyOf(points)));
        }

        if (!events.isEmpty()) {
            result.add(new MetricTypedGroup(MetricType.EVENT, ImmutableList.copyOf(events)));
        }

        if (!spreads.isEmpty()) {
            result.add(new MetricTypedGroup(MetricType.SPREAD, ImmutableList.copyOf(spreads)));
        }

        if (!groups.isEmpty()) {
            result.add(new MetricTypedGroup(MetricType.SPREAD, ImmutableList.copyOf(groups)));
        }

        return result.build();
    }

    @Override
    public void updatePoint(Map<String, String> tags, Point sample) {
        points.add(sample);
    }

    @Override
    public void updateEvent(Map<String, String> tags, Event sample) {
        events.add(sample);
    }

    @Override
    public void updateSpread(Map<String, String> tags, Spread sample) {
        spreads.add(sample);
    }

    @Override
    public void updateGroup(Map<String, String> tags, MetricGroup sample) {
        groups.add(sample);
    }

    @Override
    public long timestamp() {
        return timestamp;
    }
}