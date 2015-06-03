package com.spotify.heroic.aggregation;

import java.util.List;
import java.util.Map;

import com.spotify.heroic.grammar.Value;

/**
 * Factory to dynamically build aggregations.
 *
 * Used in Query DSL.
 *
 * @author udoprog
 */
public interface AggregationFactory {
    public Aggregation build(String name, List<Value> args, Map<String, Value> keywords);
}