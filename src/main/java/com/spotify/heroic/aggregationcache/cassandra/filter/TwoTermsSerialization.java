package com.spotify.heroic.aggregationcache.cassandra.filter;

import lombok.RequiredArgsConstructor;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.ext.serializers.SafeStringSerializer;
import com.spotify.heroic.filter.TwoTermsFilter;
import com.spotify.heroic.filter.TwoTermsFilterBuilder;

@RequiredArgsConstructor
public final class TwoTermsSerialization<T extends TwoTermsFilter> implements FilterSerialization<T> {
    private final TwoTermsFilterBuilder<T> builder;

    private static final SafeStringSerializer stringSerializer = SafeStringSerializer.get();

    @Override
    public void serialize(Composite c, T obj) {
        c.addComponent(obj.first(), stringSerializer);
        c.addComponent(obj.second(), stringSerializer);
    }

    @Override
    public T deserialize(Composite c) {
        final String first = c.get(2, stringSerializer);
        final String second = c.get(3, stringSerializer);
        return builder.build(first, second);
    }
}