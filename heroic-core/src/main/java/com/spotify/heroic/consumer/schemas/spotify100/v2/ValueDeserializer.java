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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.protobuf.ByteString;
import java.io.IOException;

/**
 * Distribution histogram point value type deserializer
 * This deserialize supports  {@link Value.DistributionValue}
 * and {@link Value.DoubleValue} types.
 */
public class ValueDeserializer extends StdDeserializer<Value> {

    private static final long serialVersionUID = 6224613339173782914L;

    public ValueDeserializer() {
        this(Value.class);
    }


    public ValueDeserializer(final Class<Value> classz) {
        super(classz);
    }

    @Override
    public Value deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException {

        JsonNode node = jsonParser.getCodec().readTree(jsonParser);

        if (node.get("distributionValue") != null) {
            byte[] bytes = node.get("distributionValue").binaryValue();
            ByteString byteString = ByteString.copyFrom(bytes);
            return Value.DistributionValue.create(byteString);
        }

        if (node.get("doubleValue") != null) {
            double val = node.get("doubleValue").asDouble();
            return Value.DoubleValue.create(val);
        }

        throw new RuntimeException("Unrecognized value type");
    }
}
