package com.spotify.heroic.filter;

import java.io.IOException;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class OneArgumentFilterBase<T extends Filter.OneArg<A>, A> implements OneArgumentFilter<T, A>,
        FilterJsonSerialization<T> {
    private final FilterJsonSerialization<A> argument;

    @Override
    public T deserialize(FilterJsonSerialization.Deserializer deserializer) throws IOException {
        final A first = argument.deserialize(deserializer);
        return build(first);
    }

    public void serialize(FilterJsonSerialization.Serializer serializer, T filter) throws IOException {
        argument.serialize(serializer, filter.first());
    }

    public abstract T build(A first);
}