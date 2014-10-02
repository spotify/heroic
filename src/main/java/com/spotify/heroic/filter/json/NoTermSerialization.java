package com.spotify.heroic.filter.json;

import java.io.IOException;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.spotify.heroic.filter.NoTermFilter;
import com.spotify.heroic.filter.NoTermFilterBuilder;

@RequiredArgsConstructor
public class NoTermSerialization<T extends NoTermFilter> implements FilterSerialization<T> {
    private final NoTermFilterBuilder<T> builder;

    @Override
    public T deserialize(JsonParser p, DeserializationContext c) throws IOException, JsonProcessingException {
        return builder.build();
    }

    @Override
    public void serialize(JsonGenerator g, NoTermFilter f) throws IOException {
    }
}