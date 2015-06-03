package com.spotify.heroic.filter;

public abstract class NoArgumentFilterBase<T extends Filter.NoArg> implements NoArgumentFilter<T>,
        FilterJsonSerialization<T> {
    @Override
    public T deserialize(FilterJsonSerialization.Deserializer deserializer) {
        return build();
    }

    @Override
    public void serialize(FilterJsonSerialization.Serializer serializer, T filter) {
    }

    public abstract T build();
}