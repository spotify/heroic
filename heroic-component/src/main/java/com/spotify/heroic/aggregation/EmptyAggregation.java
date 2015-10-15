package com.spotify.heroic.aggregation;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
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

            if (!this.groups.isEmpty()) {
                groups.add(collectGroup(this.groups, MetricType.GROUP.comparator(), MetricCollection::groups));
            }

            if (!this.points.isEmpty()) {
                groups.add(collectGroup(this.points, MetricType.POINT.comparator(), MetricCollection::points));
            }

            if (!this.events.isEmpty()) {
                groups.add(collectGroup(this.events, MetricType.EVENT.comparator(), MetricCollection::events));
            }

            if (!this.spreads.isEmpty()) {
                groups.add(collectGroup(this.spreads, MetricType.SPREAD.comparator(), MetricCollection::spreads));
            }

            return new AggregationResult(groups.build(), Statistics.empty());
        }

        private <T extends Metric> AggregationData collectGroup(final ConcurrentLinkedQueue<Collected<T>> collected,
                final Comparator<? super T> comparator, final Function<List<T>, MetricCollection> builder) {
            final ImmutableSet.Builder<Series> series = ImmutableSet.builder();

            final ImmutableList.Builder<List<T>> iterables = ImmutableList.builder();

            for (final Collected<T> d : collected) {
                series.addAll(d.getSeries());
                iterables.add(d.getValues());
            }

            /* no need to merge, single results are already sorted */
            if (collected.size() == 1) {
                return new AggregationData(EMPTY_GROUP, series.build(),
                        builder.apply(iterables.build().iterator().next()));
            }

            final ImmutableList<Iterator<T>> iterators = ImmutableList
                    .copyOf(iterables.build().stream().map(Iterable::iterator).iterator());
            final Iterator<T> metrics = Iterators.mergeSorted(iterators, comparator);

            return new AggregationData(EMPTY_GROUP, series.build(), builder.apply(ImmutableList.copyOf(metrics)));
        }
    }

    @Data
    private static final class Collected<T extends Metric> {
        private final Map<String, String> group;
        private final Set<Series> series;
        private final List<T> values;
    }
}