package com.spotify.heroic.aggregation;

public interface AggregationSession {
    /**
     * Stream datapoints into this aggregator.
     *
     * Must be thread-safe.
     *
     * @param datapoints
     */
    public void update(AggregationData update);

    /**
     * Get the result of this aggregator.
     */
    public AggregationResult result();
}