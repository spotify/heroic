package com.spotify.heroic.filter;

public interface FilterJsonDeserializer {
    public <T extends Filter> void register(String id, FilterJsonSerialization<T> serializer);
}
