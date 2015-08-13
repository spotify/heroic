package com.spotify.heroic.aggregation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.MetricType;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.Statistics;
import com.spotify.heroic.model.TimeData;

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
        final Set<Series> series = new HashSet<>();

        for (final AggregationState s : states)
            series.addAll(s.getSeries());

        return new AggregationTraversal(states, new CollectorSession(series));
    }

    @Override
    public long extent() {
        return 0;
    }

    /**
     * A trivial session that collects all values provided to it.
     */
    @Data
    private static final class CollectorSession implements AggregationSession {
        private final ConcurrentLinkedQueue<AggregationData> input = new ConcurrentLinkedQueue<AggregationData>();
        private final Set<Series> series;

        @Override
        public void update(AggregationData update) {
            input.add(update);
        }

        @Override
        public AggregationResult result() {
            final Map<MetricType, List<Iterable<? extends TimeData>>> entries = new HashMap<>();

            for (final AggregationData in : input) {
                List<Iterable<? extends TimeData>> iterables = entries.get(in.getType());

                if (iterables == null) {
                    iterables = new ArrayList<>();
                    entries.put(in.getType(), iterables);
                }

                iterables.add(in.getValues());
            }

            final ImmutableList.Builder<AggregationData> groups = ImmutableList.builder();

            for (final Map.Entry<MetricType, List<Iterable<? extends TimeData>>> e : entries.entrySet()) {
                final List<? extends TimeData> data = ImmutableList.copyOf(Iterables.mergeSorted(e.getValue(),
                        TimeData.comparator()));
                groups.add(new AggregationData(EMPTY_GROUP, series, data, e.getKey()));
            }

            return new AggregationResult(groups.build(), Statistics.Aggregator.EMPTY);
        }
    }
}