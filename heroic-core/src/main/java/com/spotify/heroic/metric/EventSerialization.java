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

package com.spotify.heroic.metric;

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

public class EventSerialization {
    public static class Deserializer extends JsonDeserializer<Event> {
        @Override
        public Event deserialize(JsonParser p, DeserializationContext c) throws IOException, JsonProcessingException {

            if (p.getCurrentToken() != JsonToken.START_ARRAY) {
                throw c.mappingException(String.format("Expected start of array, not %s", p.getCurrentToken()));
            }

            final Long timestamp;

            {
                if (!p.nextToken().isNumeric()) {
                    throw c.mappingException(String.format("Expected timestamp (number), not %s", p.getCurrentToken()));
                }

                timestamp = p.getLongValue();
            }

            if (p.nextToken() != JsonToken.START_OBJECT) {
                throw c.mappingException(String.format("Expected start of payload (object), not %s",
                        p.getCurrentToken()));
            }

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
                    throw c.mappingException(String.format("Unexpected token %s", p.getCurrentToken()));
                }
            }

            if (p.getCurrentToken() != JsonToken.END_OBJECT) {
                throw c.mappingException(String.format("Expected end of object, not %s", p.getCurrentToken()));
            }

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