package com.spotify.heroic.aggregation;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.grammar.AggregationValue;
import com.spotify.heroic.grammar.DiffValue;
import com.spotify.heroic.grammar.ListValue;
import com.spotify.heroic.grammar.Value;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AbstractAggregationBuilder<T> implements AggregationBuilder<T> {
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

    protected Optional<Long> parseDiffMillis(Deque<Value> args, Map<String, Value> keywords,
            String key) {
        if (!args.isEmpty()) {
            return Optional.of(args.removeFirst().cast(DiffValue.class).toMilliseconds());
        }

        if (keywords.containsKey(key)) {
            return Optional.of(keywords.get(key).cast(DiffValue.class).toMilliseconds());
        }

        return Optional.absent();
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