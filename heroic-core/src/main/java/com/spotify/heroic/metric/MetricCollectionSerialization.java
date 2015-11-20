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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.google.common.collect.ImmutableList;

import lombok.RequiredArgsConstructor;

public class MetricCollectionSerialization {
    private static final String TYPE = "type";
    private static final String DATA = "data";

    @RequiredArgsConstructor
    public static class Deserializer extends JsonDeserializer<MetricCollection> {
        @Override
        public MetricCollection deserialize(JsonParser p, DeserializationContext c)
                throws IOException, JsonProcessingException {
            if (p.getCurrentToken() != JsonToken.START_OBJECT) {
                throw c.wrongTokenException(p, JsonToken.START_OBJECT, null);
            }

            MetricType type = null;
            JsonNode data = null;

            while (p.nextToken() == JsonToken.FIELD_NAME) {
                final String name = p.getCurrentName();

                if (name == null) {
                    throw c.mappingException("Expected field name");
                }

                switch (name) {
                case TYPE:
                    if (p.nextToken() != JsonToken.VALUE_STRING) {
                        throw c.wrongTokenException(p, JsonToken.VALUE_STRING, null);
                    }

                    type = p.readValueAs(MetricType.class);
                    break;
                case DATA:
                    if (p.nextToken() != JsonToken.START_ARRAY) {
                        throw c.wrongTokenException(p, JsonToken.START_ARRAY,
                                "expected array for data");
                    }

                    data = p.readValueAsTree();
                    break;
                default:
                    // skip unknown
                    p.skipChildren();
                    continue;
                }
            }

            if (type == null) {
                throw c.mappingException("'type' not specified");
            }

            if (data == null) {
                throw c.mappingException("'data' not specified");
            }

            final ImmutableList.Builder<Metric> d = ImmutableList.builder();

            for (final JsonNode child : data) {
                d.add(new TreeTraversingParser(child, p.getCodec()).readValueAs(type.type()));
            }

            if (p.getCurrentToken() != JsonToken.END_OBJECT) {
                throw c.wrongTokenException(p, JsonToken.END_OBJECT, null);
            }

            return MetricCollection.build(type, d.build());
        }
    }

    public static class Serializer extends JsonSerializer<MetricCollection> {
        @Override
        public void serialize(MetricCollection group, JsonGenerator g, SerializerProvider provider)
                throws IOException, JsonProcessingException {
            g.writeStartObject();
            g.writeObjectField(TYPE, group.getType());
            g.writeObjectField(DATA, group.getData());
            g.writeEndObject();
        }
    }
}
