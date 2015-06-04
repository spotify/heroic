/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.model;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableMap;

public class EventSerializer {
    public static class Deserializer extends JsonDeserializer<Event> {
        @Override
        public Event deserialize(JsonParser p, DeserializationContext c) throws IOException, JsonProcessingException {

            if (p.getCurrentToken() != JsonToken.START_ARRAY)
                throw c.mappingException("Expected start of array");

            final Long timestamp;

            {
                if (p.nextToken() != JsonToken.VALUE_NUMBER_INT)
                    throw c.mappingException("Expected number (timestamp)");

                timestamp = p.readValueAs(Long.class);
            }

            if (p.nextToken() != JsonToken.START_OBJECT)
                throw c.mappingException("Expected start of payload");

            ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();

            while (p.nextToken() == JsonToken.FIELD_NAME) {
                final String key = p.getCurrentName();

                switch (p.nextToken()) {
                case VALUE_NUMBER_INT:
                    builder.put(key, p.getValueAsInt());
                    break;
                case VALUE_NUMBER_FLOAT:
                    builder.put(key, p.getValueAsDouble());
                    break;
                case VALUE_STRING:
                    builder.put(key, p.getValueAsString());
                    break;
                case VALUE_FALSE:
                case VALUE_TRUE:
                    builder.put(key, p.getValueAsBoolean());
                    break;
                default:
                    throw c.mappingException("unexpected token");
                }
            }

            if (p.getCurrentToken() != JsonToken.END_OBJECT)
                throw c.mappingException("expected field name");

            return new Event(timestamp, builder.build());
        }
    }

    public static class Serializer extends JsonSerializer<Event> {
        @Override
        public void serialize(Event d, JsonGenerator g, SerializerProvider provider) throws IOException,
                JsonProcessingException {
            g.writeStartArray();
            g.writeNumber(d.getTimestamp());

            g.writeStartObject();

            for (final Map.Entry<String, Object> e : d.getPayload().entrySet()) {
                g.writeObjectField(e.getKey(), e.getValue());
            }

            g.writeEndObject();
            g.writeEndArray();
        }
    }
}