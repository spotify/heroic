package com.spotify.heroic.cache.cassandra.filter;

import lombok.RequiredArgsConstructor;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.ext.serializers.SafeStringSerializer;
import com.spotify.heroic.filter.OneTermFilter;
import com.spotify.heroic.filter.OneTermFilterBuilder;

@RequiredArgsConstructor
public class OneTermSerialization<T extends OneTermFilter> implements
        FilterSerialization<T> {
    private static final SafeStringSerializer stringSerializer = SafeStringSerializer
            .get();

    private final OneTermFilterBuilder<T> builder;

    @Override
    public void serialize(Composite c, T obj) {
        c.addComponent(obj.first(), stringSerializer);
    }

    @Override
    public T deserialize(Composite c) {
        final String first = c.get(2, stringSerializer);
        return builder.build(first);
    }
}