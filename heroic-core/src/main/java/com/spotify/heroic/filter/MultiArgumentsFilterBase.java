package com.spotify.heroic.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class MultiArgumentsFilterBase<T extends Filter.MultiArgs<A>, A> implements MultiArgumentsFilter<T, A>,
        FilterJsonSerialization<T> {
    private final FilterJsonSerialization<A> term;

    @Override
    public T deserialize(FilterJsonSerialization.Deserializer deserializer) throws IOException {
        final List<A> terms = new ArrayList<>();

        A f = this.term.deserialize(deserializer);

        while (f != null) {
            terms.add(f);
            f = this.term.deserialize(deserializer);
        }

        return build(terms);
    }

    @Override
    public void serialize(FilterJsonSerialization.Serializer serializer, T filter) throws IOException {
        for (final A term : filter.terms())
            this.term.serialize(serializer, term);
    }

    public abstract T build(Collection<A> terms);
}