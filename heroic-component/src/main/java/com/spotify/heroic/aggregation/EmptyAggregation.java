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

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.model.DateRange;
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
            final Map<Class<? extends TimeData>, ArrayList<TimeData>> entries = new HashMap<>();

            for (final AggregationData in : input) {
                ArrayList<TimeData> data = entries.get(in.getOutput());

                if (data == null) {
                    data = new ArrayList<>();
                    entries.put(in.getOutput(), data);
                }

                data.addAll(in.getValues());
            }

            final List<AggregationData> groups = new ArrayList<>();

            for (final Map.Entry<Class<? extends TimeData>, ArrayList<TimeData>> e : entries.entrySet()) {
                final Class<? extends TimeData> output = e.getKey();
                final List<TimeData> data = e.getValue();
                groups.add(new AggregationData(EMPTY_GROUP, series, data, output));
            }

            return new AggregationResult(groups, Statistics.Aggregator.EMPTY);
        }
    }
}