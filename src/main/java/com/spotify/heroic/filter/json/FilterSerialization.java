package com.spotify.heroic.filter.json;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;

public interface FilterSerialization<T> {
    public T deserialize(JsonParser p, DeserializationContext c) throws IOException, JsonProcessingException;

    public void serialize(JsonGenerator g, T filter) throws IOException, JsonProcessingException;
}