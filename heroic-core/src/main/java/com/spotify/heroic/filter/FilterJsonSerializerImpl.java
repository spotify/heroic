package com.spotify.heroic.filter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class FilterJsonSerializerImpl implements FilterJsonSerializer {
    private final Map<Class<? extends Filter>, JsonSerializer<Filter>> impl = new HashMap<>();

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Filter> void register(Class<T> type, FilterJsonSerialization<? super T> serializer) {
        final JsonSerializerImpl s = new JsonSerializerImpl((FilterJsonSerialization<? super Filter>) serializer);
        impl.put(type, s);
    }

    public void configure(SimpleModule module) {
        for (final Map.Entry<Class<? extends Filter>, JsonSerializer<Filter>> e : this.impl.entrySet()) {
            module.addSerializer(e.getKey(), e.getValue());
        }
    }

    @RequiredArgsConstructor
    private static final class Serializer implements FilterJsonSerialization.Serializer {
        private final JsonGenerator generator;

        @Override
        public void string(String string) throws IOException {
            generator.writeString(string);
        }

        @Override
        public void filter(Filter filter) throws IOException {
            generator.writeObject(filter);
        }
    }

    @RequiredArgsConstructor
    private static final class JsonSerializerImpl extends JsonSerializer<Filter> {
        private final FilterJsonSerialization<? super Filter> serializer;

        @Override
        public void serialize(Filter value, JsonGenerator g, SerializerProvider provider) throws IOException,
                JsonProcessingException {
            g.writeStartArray();
            g.writeString(value.operator());

            final Serializer s = new Serializer(g);
            serializer.serialize(s, value);
            g.writeEndArray();
        }
    }
}