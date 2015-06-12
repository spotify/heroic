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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class SpreadSerializer {
    private static final String COUNT = "count";
    private static final String MIN = "min";
    private static final String MAX = "max";
    private static final String SUM = "sum";

    public static class Deserializer extends JsonDeserializer<Spread> {
        @Override
        public Spread deserialize(JsonParser p, DeserializationContext c) throws IOException,
                JsonProcessingException {

            if (p.getCurrentToken() != JsonToken.START_ARRAY)
                throw c.mappingException("Expected start of array");

            final Long timestamp;

            {
                if (p.nextToken() != JsonToken.VALUE_NUMBER_INT)
                    throw c.mappingException("Expected number (timestamp)");

                timestamp = p.readValueAs(Long.class);
            }

            if (p.nextToken() != JsonToken.START_OBJECT)
                throw c.mappingException("Expected start of object");

            long count = 0;
            double sum = Double.NaN;
            double min = Double.NaN;
            double max = Double.NaN;

            while (p.nextToken() == JsonToken.FIELD_NAME) {
                final String name = p.getCurrentName();

                if (name == null)
                    throw c.mappingException("Expected field name");

                if (COUNT.equals(name)) {
                    count = nextLong(p, c);
                    continue;
                }

                if (MIN.equals(name)) {
                    min = nextDouble(p, c);
                    continue;
                }

                if (MAX.equals(name)) {
                    max = nextDouble(p, c);
                    continue;
                }

                if (SUM.equals(name)) {
                    sum = nextDouble(p, c);
                    continue;
                }
            }

            if (p.getCurrentToken() != JsonToken.END_OBJECT)
                throw c.mappingException("Expected end of object");

            if (p.getCurrentToken() != JsonToken.END_ARRAY)
                throw c.mappingException("Expected end of array");

            return new Spread(timestamp, count, sum, min, max);
        }

        private long nextLong(JsonParser p, DeserializationContext c) throws IOException {
            switch (p.nextToken()) {
            case VALUE_NUMBER_FLOAT:
                return p.readValueAs(Double.class).longValue();
            case VALUE_NUMBER_INT:
                return p.readValueAs(Long.class);
            default:
                throw c.mappingException("Expected long");
            }
        }

        private Double nextDouble(JsonParser p, DeserializationContext c) throws IOException {
            switch (p.nextToken()) {
            case VALUE_NUMBER_FLOAT:
                return p.readValueAs(Double.class);
            case VALUE_NUMBER_INT:
                return p.readValueAs(Long.class).doubleValue();
            case VALUE_NULL:
                return Double.NaN;
            default:
                throw c.mappingException("Expected double");
            }
        }
    }

    public static class Serializer extends JsonSerializer<Spread> {
        @Override
        public void serialize(Spread d, JsonGenerator g, SerializerProvider provider) throws IOException,
                JsonProcessingException {
            g.writeStartArray();
            g.writeNumber(d.getTimestamp());

            g.writeStartObject();
            g.writeFieldName(COUNT);
            g.writeNumber(d.getCount());

            g.writeFieldName(SUM);
            writeDouble(g, d.getSum());

            g.writeFieldName(MIN);
            writeDouble(g, d.getMin());

            g.writeFieldName(MAX);
            writeDouble(g, d.getMax());

            g.writeEndObject();
            g.writeEndArray();
        }

        private void writeDouble(JsonGenerator g, double value) throws IOException {
            if (Double.isNaN(value)) {
                g.writeNull();
            } else {
                g.writeNumber(value);
            }
        }
    }
}