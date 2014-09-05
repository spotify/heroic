package com.spotify.heroic.cache.cassandra.filter;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.ext.serializers.SafeStringSerializer;
import com.spotify.heroic.filter.TwoTermsFilter;

public abstract class TwoTermsSerialization<T extends TwoTermsFilter>
        implements FilterSerialization<T> {
    private static final SafeStringSerializer stringSerializer = SafeStringSerializer
            .get();

    @Override
    public void serialize(Composite c, T obj) {
        c.addComponent(obj.first(), stringSerializer);
        c.addComponent(obj.second(), stringSerializer);
    }

    @Override
    public T deserialize(Composite c) {
        final String first = c.get(2, stringSerializer);
        final String second = c.get(3, stringSerializer);
        return build(first, second);
    }

    protected abstract T build(String first, String second);
}