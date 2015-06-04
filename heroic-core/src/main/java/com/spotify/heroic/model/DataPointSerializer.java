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

public class DataPointSerializer {
    public static class Deserializer extends JsonDeserializer<DataPoint> {
        @Override
        public DataPoint deserialize(JsonParser p, DeserializationContext c) throws IOException,
                JsonProcessingException {

            if (p.getCurrentToken() != JsonToken.START_ARRAY)
                throw c.mappingException("Expected start of array");

            final Long timestamp;

            {
                if (p.nextToken() != JsonToken.VALUE_NUMBER_INT)
                    throw c.mappingException("Expected number (timestamp)");

                timestamp = p.readValueAs(Long.class);
            }

            final Double value;

            switch (p.nextToken()) {
            case VALUE_NUMBER_FLOAT:
                value = p.readValueAs(Double.class);
                break;
            case VALUE_NUMBER_INT:
                value = p.readValueAs(Long.class).doubleValue();
                break;
            case VALUE_NULL:
                value = Double.NaN;
                break;
            default:
                throw c.mappingException("Expected float (value)");
            }

            if (p.nextToken() != JsonToken.END_ARRAY)
                throw c.mappingException("Expected end of array");

            return new DataPoint(timestamp, value);
        }
    }

    public static class Serializer extends JsonSerializer<DataPoint> {
        @Override
        public void serialize(DataPoint d, JsonGenerator g, SerializerProvider provider) throws IOException,
                JsonProcessingException {
            g.writeStartArray();
            g.writeNumber(d.getTimestamp());

            final double value = d.getValue();

            if (Double.isNaN(value)) {
                g.writeNull();
            } else {
                g.writeNumber(value);
            }

            g.writeEndArray();
        }
    }
}