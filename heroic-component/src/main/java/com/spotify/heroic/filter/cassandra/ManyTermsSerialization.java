package com.spotify.heroic.filter.cassandra;

import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.ManyTermsFilter;
import com.spotify.heroic.filter.ManyTermsFilterBuilder;

@RequiredArgsConstructor
public class ManyTermsSerialization<T extends ManyTermsFilter> implements FilterSerialization<T> {
    private static final IntegerSerializer integerSerializer = IntegerSerializer.get();

    private final ManyTermsFilterBuilder<T> builder;

    @Override
    public void serialize(Composite c, T obj) {
        c.addComponent(obj.terms().size(), integerSerializer);

        for (final Filter f : obj.terms())
            c.addComponent(f, FilterSerializer.get());
    }

    @Override
    public T deserialize(Composite c) {
        final Integer size = c.get(2, integerSerializer);

        final List<Filter> statements = new ArrayList<>();

        for (int i = 0; i < size; i++)
            statements.add(c.get(3 + i, FilterSerializer.get()));

        return builder.build(statements);
    }
}