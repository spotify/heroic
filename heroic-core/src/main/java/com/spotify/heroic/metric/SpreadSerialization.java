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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class SpreadSerialization {
    private static final String COUNT = "count";
    private static final String MIN = "min";
    private static final String MAX = "max";
    private static final String SUM = "sum";
    private static final String SUM2 = "sum2";

    public static class Deserializer extends JsonDeserializer<Spread> {
        @Override
        public Spread deserialize(JsonParser p, DeserializationContext c)
            throws IOException, JsonProcessingException {

            if (p.getCurrentToken() != JsonToken.START_ARRAY) {
                throw c.mappingException(
                    String.format("Expected start of array, not %s", p.getCurrentToken()));
            }

            final long timestamp;

            {
                if (!p.nextToken().isNumeric()) {
                    throw c.mappingException(
                        String.format("Expected timestamp (number), not %s", p.getCurrentToken()));
                }

                timestamp = p.getLongValue();
            }

            if (p.nextToken() != JsonToken.START_OBJECT) {
                throw c.mappingException("Expected start of object");
            }

            long count = 0;
            double sum = Double.NaN;
            double sumx2 = Double.NaN;
            double min = Double.NaN;
            double max = Double.NaN;

            while (p.nextToken() == JsonToken.FIELD_NAME) {
                final String name = p.getCurrentName();

                if (name == null) {
                    throw c.mappingException("Expected field name");
                }

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

                if (SUM2.equals(name)) {
                    sumx2 = nextDouble(p, c);
                    continue;
                }
            }

            if (p.getCurrentToken() != JsonToken.END_OBJECT) {
                throw c.mappingException(
                    String.format("Expected end of object, not %s", p.getCurrentToken()));
            }

            if (p.nextToken() != JsonToken.END_ARRAY) {
                throw c.mappingException(
                    String.format("Expected end of array, not %s", p.getCurrentToken()));
            }

            return new Spread(timestamp, count, sum, sumx2, min, max);
        }

        private long nextLong(JsonParser p, DeserializationContext c) throws IOException {
            if (!p.nextToken().isNumeric()) {
                throw c.mappingException(
                    String.format("Expected numeric, not %s", p.getCurrentToken()));
            }

            return p.getLongValue();
        }

        private Double nextDouble(JsonParser p, DeserializationContext c) throws IOException {
            final JsonToken token = p.nextToken();

            if (token == JsonToken.VALUE_NULL) {
                return Double.NaN;
            }

            if (!token.isNumeric()) {
                throw c.wrongTokenException(p, JsonToken.VALUE_NUMBER_FLOAT, null);
            }

            return p.getDoubleValue();
        }
    }

    public static class Serializer extends JsonSerializer<Spread> {
        @Override
        public void serialize(Spread d, JsonGenerator g, SerializerProvider provider)
            throws IOException, JsonProcessingException {
            g.writeStartArray();
            g.writeNumber(d.getTimestamp());

            g.writeStartObject();
            g.writeFieldName(COUNT);
            g.writeNumber(d.getCount());

            g.writeFieldName(SUM);
            writeDouble(g, d.getSum());

            g.writeFieldName(SUM2);
            writeDouble(g, d.getSum2());

            g.writeFieldName(MIN);
            writeDouble(g, d.getMin());

            g.writeFieldName(MAX);
            writeDouble(g, d.getMax());

            g.writeEndObject();
            g.writeEndArray();
        }

        private void writeDouble(JsonGenerator g, double value) throws IOException {
            if (Double.isFinite(value)) {
                g.writeNumber(value);
            } else {
                g.writeNull();
            }
        }
    }
}
