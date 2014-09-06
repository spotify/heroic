package com.spotify.heroic.filter.json;

import java.io.IOException;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.spotify.heroic.filter.OneTermFilter;
import com.spotify.heroic.filter.OneTermFilterBuilder;

@RequiredArgsConstructor
public class OneTermSerialization<T extends OneTermFilter> implements
        FilterSerialization<T> {
    private final OneTermFilterBuilder<T> builder;

    @Override
    public T deserialize(JsonParser p, DeserializationContext c)
            throws IOException, JsonProcessingException {
        final String first;

        if (p.nextToken() != JsonToken.VALUE_STRING)
            throw c.mappingException("Expected string (first)");

        first = p.readValueAs(String.class);

        if (p.nextToken() != JsonToken.END_ARRAY)
            throw c.mappingException("Expected end of array");

        return builder.build(first);
    }

    @Override
    public void serialize(JsonGenerator g, OneTermFilter f) throws IOException {
        g.writeString(f.first());
    }
}