package com.spotify.heroic.cache.cassandra.filter;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.ext.serializers.SafeStringSerializer;
import com.spotify.heroic.filter.OneTermFilter;

public abstract class OneTermSerialization<T extends OneTermFilter> implements
        FilterSerialization<T> {
    private static final SafeStringSerializer stringSerializer = SafeStringSerializer
            .get();

    @Override
    public void serialize(Composite c, T obj) {
        c.addComponent(obj.first(), stringSerializer);
    }

    @Override
    public T deserialize(Composite c) {
        final String first = c.get(2, stringSerializer);
        return build(first);
    }

    protected abstract T build(String first);
}