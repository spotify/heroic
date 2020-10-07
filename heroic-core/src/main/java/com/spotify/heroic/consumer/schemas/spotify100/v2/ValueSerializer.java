/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic.consumer.schemas.spotify100.v2;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;

/**
 * Distribution histogram point value type serializer.
 * This serializer supports {@link Value.DistributionValue}
 * and {@link Value.DoubleValue} types.
 */
public class ValueSerializer extends StdSerializer<Value> {

    private static final long serialVersionUID = 6300597228325654588L;

    public ValueSerializer() {
        this(Value.class);
    }



    public ValueSerializer(Class<Value> t) {
        super(t);
    }


    @Override
    public void serialize(Value value, JsonGenerator jsonGenerator,
                          SerializerProvider serializerProvider) throws IOException {

        if (value instanceof  Value.DistributionValue) {
            Value.DistributionValue distributionValue = (Value.DistributionValue) value;
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField("distributionValue",
                distributionValue.getValue().toByteArray());
            jsonGenerator.writeEndObject();
        } else if (value instanceof  Value.DoubleValue) {
            Value.DoubleValue doubleValue = (Value.DoubleValue) value;
            jsonGenerator.writeStartObject();
            jsonGenerator.writeNumberField("doubleValue", doubleValue.getValue());
            jsonGenerator.writeEndObject();
        } else {
            throw new RuntimeException("Failed to serialize. Value type is not supported " + value);
        }

    }
}
