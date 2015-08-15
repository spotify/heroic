package com.spotify.heroic.aggregation;

import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.Series;

@Data
public class AggregationState {
    final Map<String, String> key;
    final Set<Series> series;

    /**
     * Create a state that only contains a single time series.
     *
     * @param s The time series contained in the state.
     * @return The new traverse state.
     */
    public static AggregationState forSeries(Series s) {
        return new AggregationState(s.getTags(), ImmutableSet.of(s));
    }
}