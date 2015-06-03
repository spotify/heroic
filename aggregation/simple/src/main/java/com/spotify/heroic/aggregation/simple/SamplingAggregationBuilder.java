package com.spotify.heroic.aggregation.simple;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.spotify.heroic.aggregation.AggregationBuilder;
import com.spotify.heroic.grammar.DiffValue;
import com.spotify.heroic.grammar.Value;
import com.spotify.heroic.model.Sampling;

public abstract class SamplingAggregationBuilder<T> implements AggregationBuilder<T> {
    private static final long DEFAULT_SIZE = TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES);

    @Override
    public T build(List<Value> args, Map<String, Value> keywords) {
        final long size;
        final long extent;

        if (args.size() > 0) {
            size = args.get(0).cast(DiffValue.class).toMilliseconds();
        } else {
            if (keywords.containsKey("sampling")) {
                size = keywords.get("sampling").cast(DiffValue.class).toMilliseconds();
            } else {
                size = DEFAULT_SIZE;
            }
        }

        if (args.size() > 1) {
            extent = args.get(1).cast(DiffValue.class).toMilliseconds();
        } else {
            if (keywords.containsKey("extent")) {
                extent = keywords.get("extent").cast(DiffValue.class).toMilliseconds();
            } else {
                extent = size;
            }
        }

        return buildWith(new Sampling(size, extent), keywords);
    }

    protected abstract T buildWith(Sampling sampling, Map<String, Value> keywords);
}
