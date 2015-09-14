package com.spotify.heroic.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public abstract class Aggregations {
    /**
     * Creates an aggregation chain.
     *
     * An empty chain is the same as an instance of {@link EmptyAggregation}. A chain with a single entry will return
     * that single item. More than one entry will construct a new instance of {@link AggregationChain}.
     *
     * @param input The input chain.
     * @return A new aggregation for the given chain.
     */
    public static Aggregation chain(Iterable<Aggregation> input) {
        final Iterator<Aggregation> it = input.iterator();

        if (!it.hasNext()) {
            return EmptyAggregation.INSTANCE;
        }

        final Aggregation first = it.next();

        if (!it.hasNext()) {
            return first;
        }

        final List<Aggregation> chain = new ArrayList<>();
        chain.add(first);

        while (it.hasNext()) {
            chain.add(it.next());
        }

        return new ChainAggregation(chain);
    }

    /**
     * Same as {@link #chain(Iterable)}, but with arrays.
     *
     * @param aggregations The input chain.
     * @return A new aggregation for the given chain.
     */
    public static Aggregation chain(Aggregation... aggregations) {
        if (aggregations.length == 0)
            return EmptyAggregation.INSTANCE;

        if (aggregations.length == 1)
            return aggregations[0];

        return new ChainAggregation(Arrays.asList(aggregations));
    }

    public static Aggregation empty() {
        return EmptyAggregation.INSTANCE;
    }
}