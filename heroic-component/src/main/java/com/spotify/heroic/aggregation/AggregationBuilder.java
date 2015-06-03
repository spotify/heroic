package com.spotify.heroic.aggregation;

import java.util.List;
import java.util.Map;

import com.spotify.heroic.grammar.Value;

public interface AggregationBuilder<T> {
    public T build(List<Value> args, Map<String, Value> keywords);
}
