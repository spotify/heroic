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
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableList;

public class MetricTypedGroupSerialization {
    private static final String TYPE = "type";
    private static final String DATA = "data";

    public static class Deserializer extends JsonDeserializer<MetricTypedGroup> {
        @Override
        public MetricTypedGroup deserialize(JsonParser p, DeserializationContext c) throws IOException,
                JsonProcessingException {
            MetricType type = null;
            List<Metric> data = null;

            while (p.nextToken() == JsonToken.FIELD_NAME) {
                final String name = p.getCurrentName();

                if (name == null) {
                    throw c.mappingException("Expected field name");
                }

                if (TYPE.equals(name)) {
                    final String identifier = p.nextTextValue();

                    if (identifier == null) {
                        throw c.mappingException("Invalid type");
                    }

                    type = MetricType.fromIdentifier(identifier);

                    if (type == null) {
                        throw c.mappingException("Invalid metric type: " + identifier);
                    }

                    continue;
                }

                if (DATA.equals(name)) {
                    if (type == null) {
                        throw c.mappingException("'type' must be specified first");
                    }

                    if (p.nextToken() != JsonToken.START_ARRAY) {
                        throw c.mappingException("Expected start of array");
                    }

                    final ImmutableList.Builder<Metric> metrics = ImmutableList.builder();

                    while (p.nextToken() != JsonToken.END_ARRAY) {
                        metrics.add(p.readValueAs(type.type()));
                    }

                    data = metrics.build();
                    continue;
                }
            }

            if (p.nextToken() != JsonToken.END_OBJECT) {
                throw c.mappingException("Expected end of object");
            }

            if (type == null) {
                throw c.mappingException("'type' not specified");
            }

            if (data == null) {
                throw c.mappingException("'data' not specified");
            }

            return new MetricTypedGroup(type, data);
        }
    }

    public static class Serializer extends JsonSerializer<MetricTypedGroup> {
        @Override
        public void serialize(MetricTypedGroup group, JsonGenerator g, SerializerProvider provider) throws IOException,
                JsonProcessingException {
            g.writeStartObject();
            g.writeObjectField(TYPE, group.getType());
            g.writeObjectField(DATA, group.getData());
            g.writeEndObject();
        }
    }
}