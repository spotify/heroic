package com.spotify.heroic.cache.cassandra.filter;

import lombok.RequiredArgsConstructor;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.filter.OneTermFilter;
import com.spotify.heroic.filter.OneTermFilterBuilder;

@RequiredArgsConstructor
public class OneTermSerialization<T extends OneTermFilter<O>, O> implements
        FilterSerialization<T> {
    private final Serializer<O> serializer;
    private final OneTermFilterBuilder<T, O> builder;

    @Override
    public void serialize(Composite c, T obj) {
        c.addComponent(obj.first(), serializer);
    }

    @Override
    public T deserialize(Composite c) {
        final O first = c.get(2, serializer);
        return builder.build(first);
    }
}