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

package com.spotify.heroic.common;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.collect.ImmutableSet;

public class GroupsSerialization {
    public static class Deserializer extends JsonDeserializer<Groups> {
        private static final TypeReference<List<String>> LIST_OF_STRINGS =
                new TypeReference<List<String>>() {
                };

        @Override
        public Groups deserialize(JsonParser p, DeserializationContext c)
                throws IOException, JsonProcessingException {
            /* fallback to default parser if object */
            if (p.getCurrentToken() == JsonToken.START_ARRAY) {
                final List<String> groups = p.readValueAs(LIST_OF_STRINGS);
                return new Groups(ImmutableSet.copyOf(groups));
            }

            if (p.getCurrentToken() == JsonToken.VALUE_STRING) {
                return new Groups(ImmutableSet.of(p.getText()));
            }

            throw c.wrongTokenException(p, JsonToken.START_ARRAY, null);
        }
    }
}
