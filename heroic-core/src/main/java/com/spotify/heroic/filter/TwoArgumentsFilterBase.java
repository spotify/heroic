package com.spotify.heroic.filter;

import java.io.IOException;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class TwoArgumentsFilterBase<T extends Filter.TwoArgs<A, B>, A, B> implements
        TwoArgumentsFilter<T, A, B>, FilterJsonSerialization<T> {
    private final FilterJsonSerialization<A> first;
    private final FilterJsonSerialization<B> second;

    @Override
    public T deserialize(FilterJsonSerialization.Deserializer deserializer) throws IOException {
        final A first = this.first.deserialize(deserializer);
        final B second = this.second.deserialize(deserializer);
        return build(first, second);
    }

    @Override
    public void serialize(FilterJsonSerialization.Serializer serializer, T filter) throws IOException {
        first.serialize(serializer, filter.first());
        second.serialize(serializer, filter.second());
    }

    public abstract T build(A first, B second);
}