package com.spotify.heroic.filter.json;

import java.io.IOException;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.spotify.heroic.filter.TwoTermsFilter;
import com.spotify.heroic.filter.TwoTermsFilterBuilder;

@RequiredArgsConstructor
public class TwoTermsSerialization<T extends TwoTermsFilter> implements
        FilterSerialization<T> {
    private final TwoTermsFilterBuilder<T> builder;

    @Override
    public T deserialize(JsonParser p, DeserializationContext c)
            throws IOException, JsonProcessingException {
        final String tag;

        if (p.nextToken() != JsonToken.VALUE_STRING)
            throw c.mappingException("Expected string (first)");

        tag = p.readValueAs(String.class);

        final String value;

        if (p.nextToken() != JsonToken.VALUE_STRING)
            throw c.mappingException("Expected string (second)");

        value = p.readValueAs(String.class);

        if (p.nextToken() != JsonToken.END_ARRAY)
            throw c.mappingException("Expected end of array");

        return builder.build(tag, value);
    }

    @Override
    public void serialize(JsonGenerator g, TwoTermsFilter f) throws IOException {
        g.writeString(f.first());
        g.writeString(f.second());
    }
}