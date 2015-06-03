package com.spotify.heroic.aggregation;

public interface AggregationQuery<T extends Aggregation> {
    public T build();
}
