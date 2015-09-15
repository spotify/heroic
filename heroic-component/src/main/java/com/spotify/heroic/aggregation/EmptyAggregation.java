package com.spotify.heroic.aggregation;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@EqualsAndHashCode(of = { "NAME" })
public class EmptyAggregation implements Aggregation {
    public static final Map<String, String> EMPTY_GROUP = ImmutableMap.of();
    public static final EmptyAggregation INSTANCE = new EmptyAggregation();
    public static final String NAME = "empty";

    @Override
    public long estimate(DateRange range) {
        return 0;
    }

    @Override
    public AggregationTraversal session(List<AggregationState> states, DateRange range) {
        return new AggregationTraversal(states, new CollectorSession());
    }

    @Override
    public long extent() {
        return 0;
    }

    @Override
    public long cadence() {
        return 0;
    }

    /**
     * A trivial session that collects all values provided to it.
     */
    @Data
    @ToString(of = {})
    private static final class CollectorSession implements AggregationSession {
        private final ConcurrentLinkedQueue<Collected<Point>> points = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<Collected<Event>> events = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<Collected<Spread>> spreads = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<Collected<MetricGroup>> groups = new ConcurrentLinkedQueue<>();

        @Override
        public void updatePoints(Map<String, String> group, Set<Series> series, List<Point> values) {
            points.add(new Collected<Point>(group, series, values));
        }

        @Override
        public void updateEvents(Map<String, String> group, Set<Series> series, List<Event> values) {
            events.add(new Collected<Event>(group, series, values));
        }

        @Override
        public void updateSpreads(Map<String, String> group, Set<Series> series, List<Spread> values) {
            spreads.add(new Collected<Spread>(group, series, values));
        }

        @Override
        public void updateGroup(Map<String, String> group, Set<Series> series, List<MetricGroup> values) {
            groups.add(new Collected<MetricGroup>(group, series, values));
        }

        @Override
        public AggregationResult result() {
            final ImmutableList.Builder<AggregationData> groups = ImmutableList.builder();

            for (final Collected<Point> c : this.points) {
                groups.add(new AggregationData(EMPTY_GROUP, c.getSeries(), c.getValues(), MetricType.POINT));
            }

            for (final Collected<Event> c : this.events) {
                groups.add(new AggregationData(EMPTY_GROUP, c.getSeries(), c.getValues(), MetricType.EVENT));
            }

            for (final Collected<Spread> c : this.spreads) {
                groups.add(new AggregationData(EMPTY_GROUP, c.getSeries(), c.getValues(), MetricType.SPREAD));
            }

            for (final Collected<MetricGroup> c : this.groups) {
                groups.add(new AggregationData(EMPTY_GROUP, c.getSeries(), c.getValues(), MetricType.GROUP));
            }

            return new AggregationResult(groups.build(), Statistics.EMPTY);
        }
    }

    @Data
    private static final class Collected<T extends Metric> {
        private final Map<String, String> group;
        private final Set<Series> series;
        private final List<T> values;
    }
}