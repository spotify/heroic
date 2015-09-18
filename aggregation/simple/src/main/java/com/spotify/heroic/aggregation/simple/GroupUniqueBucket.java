package com.spotify.heroic.aggregation.simple;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.AbstractBucket;
import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class GroupUniqueBucket extends AbstractBucket implements Bucket {
    final SortedSet<Point> points = new ConcurrentSkipListSet<Point>(MetricType.POINT.comparator());
    final SortedSet<Event> events = new ConcurrentSkipListSet<Event>(MetricType.EVENT.comparator());
    final SortedSet<Spread> spreads = new ConcurrentSkipListSet<Spread>(MetricType.SPREAD.comparator());
    final SortedSet<MetricGroup> groups = new ConcurrentSkipListSet<MetricGroup>(MetricType.GROUP.comparator());

    final long timestamp;

    public List<MetricCollection> groups() {
        final ImmutableList.Builder<MetricCollection> result = ImmutableList.builder();

        if (!points.isEmpty()) {
            result.add(MetricCollection.points(ImmutableList.copyOf(points)));
        }

        if (!events.isEmpty()) {
            result.add(MetricCollection.events(ImmutableList.copyOf(events)));
        }

        if (!spreads.isEmpty()) {
            result.add(MetricCollection.spreads(ImmutableList.copyOf(spreads)));
        }

        if (!groups.isEmpty()) {
            result.add(MetricCollection.groups(ImmutableList.copyOf(groups)));
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