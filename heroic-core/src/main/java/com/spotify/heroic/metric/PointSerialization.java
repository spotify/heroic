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
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class PointSerialization {
    public static class Deserializer extends JsonDeserializer<Point> {
        @Override
        public Point deserialize(JsonParser p, DeserializationContext c)
                throws IOException, JsonProcessingException {
            if (p.getCurrentToken() != JsonToken.START_ARRAY) {
                throw c.mappingException(
                        String.format("Expected start of array, not %s", p.getCurrentToken()));
            }

            if (!p.nextToken().isNumeric()) {
                throw c.wrongTokenException(p, JsonToken.VALUE_NUMBER_INT,
                        "Expected timestamp (number)");
            }

            final long timestamp = p.getLongValue();

            if (!p.nextToken().isNumeric()) {
                throw c.wrongTokenException(p, JsonToken.VALUE_NUMBER_FLOAT,
                        "Expected value (number)");
            }

            final double value = p.getDoubleValue();

            if (p.nextToken() != JsonToken.END_ARRAY) {
                throw c.mappingException(
                        String.format("Expected end of array, not %s", p.getCurrentToken()));
            }

            return new Point(timestamp, value);
        }
    }

    public static class Serializer extends JsonSerializer<Point> {
        @Override
        public void serialize(Point d, JsonGenerator g, SerializerProvider provider)
                throws IOException, JsonProcessingException {
            g.writeStartArray();
            g.writeNumber(d.getTimestamp());

            final double value = d.getValue();

            if (Double.isFinite(value)) {
                g.writeNumber(value);
            } else {
                g.writeNull();
            }

            g.writeEndArray();
        }
    }
}
