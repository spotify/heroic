package com.spotify.heroic.aggregation;

import java.util.List;

import lombok.Data;

/**
 * Hold on to intermediate traverse states, and a session.
 */
@Data
public class AggregationTraversal {
    private final List<AggregationState> states;
    private final AggregationSession session;
}