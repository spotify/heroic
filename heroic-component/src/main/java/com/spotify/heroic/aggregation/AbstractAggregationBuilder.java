package com.spotify.heroic.aggregation;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.Sampling;
import com.spotify.heroic.grammar.AggregationValue;
import com.spotify.heroic.grammar.DiffValue;
import com.spotify.heroic.grammar.ListValue;
import com.spotify.heroic.grammar.Value;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AbstractAggregationBuilder<T> implements AggregationBuilder<T> {
    private static final long DEFAULT_SIZE = TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES);

    protected final AggregationFactory factory;

    protected List<Aggregation> flatten(final AggregationContext context, final Value value) {
        if (value == null) {
            return ImmutableList.of();
        }

        if (value instanceof ListValue) {
            final ImmutableList.Builder<Aggregation> aggregations = ImmutableList.builder();

            for (final Value item : ((ListValue) value).getList()) {
                aggregations.addAll(flatten(context, item));
            }

            return aggregations.build();
        }

        final AggregationValue a = value.cast(AggregationValue.class);
        return ImmutableList.of(factory.build(context, a.getName(), a.getArguments(), a.getKeywordArguments()));
    }

    protected Sampling parseSampling(AggregationContext context, Deque<Value> args, Map<String, Value> keywords) {
        final long size;
        final long extent;

        boolean preferDefault = true;

        if (!args.isEmpty()) {
            size = args.removeFirst().cast(DiffValue.class).toMilliseconds();
        } else {
            if (keywords.containsKey("sampling")) {
                size = keywords.get("sampling").cast(DiffValue.class).toMilliseconds();
                preferDefault = false;
            } else {
                size = DEFAULT_SIZE;
            }
        }

        if (!args.isEmpty()) {
            extent = args.removeFirst().cast(DiffValue.class).toMilliseconds();
        } else {
            if (keywords.containsKey("extent")) {
                extent = keywords.get("extent").cast(DiffValue.class).toMilliseconds();
                preferDefault = false;
            } else {
                extent = size;
            }
        }

        final Sampling candidate = new Sampling(size, extent);

        if (!preferDefault) {
            return candidate;
        }

        return context.getSampling().or(candidate);
    }

    protected Aggregation parseAggregation(Map<String, Value> keywords, final Deque<Value> a,
            final OptionsContext c) {
        final AggregationValue aggregation;

        if (!a.isEmpty()) {
            aggregation = a.removeFirst().cast(AggregationValue.class);
        } else {
            if (!keywords.containsKey("aggregation")) {
                throw new IllegalArgumentException("Missing aggregation argument");
            }

            aggregation = keywords.get("aggregation").cast(AggregationValue.class);
        }

        return factory.build(c, aggregation.getName(), aggregation.getArguments(), aggregation.getKeywordArguments());
    }
}