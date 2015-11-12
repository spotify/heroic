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

package com.spotify.heroic.filter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class FilterJsonSerializerImpl implements FilterJsonSerializer {
    private final Map<Class<? extends Filter>, JsonSerializer<Filter>> impl = new HashMap<>();

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Filter> void register(Class<T> type,
            FilterJsonSerialization<? super T> serializer) {
        final JsonSerializerImpl s =
                new JsonSerializerImpl((FilterJsonSerialization<? super Filter>) serializer);
        impl.put(type, s);
    }

    public void configure(SimpleModule module) {
        for (final Map.Entry<Class<? extends Filter>, JsonSerializer<Filter>> e : this.impl
                .entrySet()) {
            module.addSerializer(e.getKey(), e.getValue());
        }
    }

    @RequiredArgsConstructor
    private static final class Serializer implements FilterJsonSerialization.Serializer {
        private final JsonGenerator generator;

        @Override
        public void string(String string) throws IOException {
            generator.writeString(string);
        }

        @Override
        public void filter(Filter filter) throws IOException {
            generator.writeObject(filter);
        }
    }

    @RequiredArgsConstructor
    private static final class JsonSerializerImpl extends JsonSerializer<Filter> {
        private final FilterJsonSerialization<? super Filter> serializer;

        @Override
        public void serialize(Filter value, JsonGenerator g, SerializerProvider provider)
                throws IOException, JsonProcessingException {
            g.writeStartArray();
            g.writeString(value.operator());

            final Serializer s = new Serializer(g);
            serializer.serialize(s, value);
            g.writeEndArray();
        }
    }
}
