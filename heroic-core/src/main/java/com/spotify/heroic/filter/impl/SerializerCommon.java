package com.spotify.heroic.filter.impl;

import java.io.IOException;

import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterJsonSerialization;

public final class SerializerCommon {
    public static final FilterJsonSerialization<String> STRING = new FilterJsonSerialization<String>() {
        @Override
        public String deserialize(Deserializer deserializer) throws IOException {
            return deserializer.string();
        }

        @Override
        public void serialize(Serializer serializer, String value) throws IOException {
            serializer.string(value);
        }
    };

    public static final FilterJsonSerialization<Filter> FILTER = new FilterJsonSerialization<Filter>() {
        @Override
        public Filter deserialize(Deserializer deserializer) throws IOException {
            return deserializer.filter();
        }

        @Override
        public void serialize(Serializer serializer, Filter value) throws IOException {
            serializer.filter(value);
        }
    };
}
