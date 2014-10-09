package com.spotify.heroic.filter.cassandra;

import com.netflix.astyanax.model.Composite;
import com.spotify.heroic.filter.Filter;

public interface FilterSerialization<T extends Filter> {
    public void serialize(Composite c, T obj);

    public T deserialize(Composite c);
}