package com.spotify.heroic.aggregationcache.cassandra.filter;

import lombok.RequiredArgsConstructor;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.filter.NoTermFilter;
import com.spotify.heroic.filter.NoTermFilterBuilder;

@RequiredArgsConstructor
public class NoTermSerialization<T extends NoTermFilter> implements
FilterSerialization<T> {
    private final NoTermFilterBuilder<T> builder;

    @Override
    public void serialize(Composite c, T obj) {
    }

    @Override
    public T deserialize(Composite c) {
        return builder.build();
    }
}