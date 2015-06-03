package com.spotify.heroic.filter;

public interface FilterJsonSerializer {
    public <T extends Filter> void register(Class<T> type, FilterJsonSerialization<? super T> serializer);
}
