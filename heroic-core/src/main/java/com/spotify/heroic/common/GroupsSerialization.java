package com.spotify.heroic.common;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.collect.ImmutableSet;

public class GroupsSerialization {
    public static class Deserializer extends JsonDeserializer<Groups> {
        private static final TypeReference<List<String>> LIST_OF_STRINGS = new TypeReference<List<String>>() {};

        @Override
        public Groups deserialize(JsonParser p, DeserializationContext c) throws IOException, JsonProcessingException {
            /* fallback to default parser if object */
            if (p.getCurrentToken() == JsonToken.START_ARRAY) {
                final List<String> groups = p.readValueAs(LIST_OF_STRINGS);
                return new Groups(ImmutableSet.copyOf(groups));
            }

            if (p.getCurrentToken() == JsonToken.VALUE_STRING) {
                final Groups g = new Groups(ImmutableSet.of(p.getText()));
                p.nextToken();
                return g;
            }

            throw c.wrongTokenException(p, JsonToken.START_ARRAY, null);
        }
    }
}