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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.grammar.QueryParser;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class CoreFilterRegistry implements FilterRegistry {
    private final Map<String, FilterJsonSerialization<? extends Filter>> deserializers =
        new HashMap<>();

    private final Map<Class<? extends Filter>, JsonSerializer<Filter>> serializers =
        new HashMap<>();

    private final SerializerFramework s;

    private final Serializer<Integer> intNumber;
    private final Serializer<String> string;

    private final HashMap<String, Serializer<? extends Filter>> idMapping = new HashMap<>();
    private final HashMap<Class<?>, String> typeMapping = new HashMap<>();

    @Override
    public <T extends Filter.OneArg<A>, A> void register(
        String id, Class<T> type, OneArgumentFilter<T, A> builder, Serializer<A> first
    ) {
        registerJson(id, type, builder);
        register(id, type, new OneTermSerialization<>(builder, first));
    }

    @Override
    public <T extends Filter.TwoArgs<A, B>, A, B> void register(
        String id, Class<T> type, TwoArgumentsFilter<T, A, B> builder, Serializer<A> first,
        Serializer<B> second
    ) {
        registerJson(id, type, builder);
        register(id, type, new TwoTermsSerialization<>(builder, first, second));
    }

    @Override
    public <T extends Filter.MultiArgs<A>, A> void register(
        String id, Class<T> type, MultiArgumentsFilter<T, A> builder, Serializer<A> term
    ) {
        registerJson(id, type, builder);
        register(id, type, new ManyTermsSerialization<>(builder, s.list(term)));
    }

    @Override
    public <T extends Filter.NoArg> void register(
        String id, Class<T> type, NoArgumentFilter<T> builder
    ) {
        registerJson(id, type, builder);
        register(id, type, new NoTermSerialization<>(builder));
    }

    @Override
    public Module module(final QueryParser parser) {
        final SimpleModule m = new SimpleModule("filter");

        for (final Map.Entry<Class<? extends Filter>, JsonSerializer<Filter>> e : this
            .serializers.entrySet()) {
            m.addSerializer(e.getKey(), e.getValue());
        }

        final CoreFilterJsonDeserializer deserializer =
            new CoreFilterJsonDeserializer(ImmutableMap.copyOf(deserializers), parser);
        m.addDeserializer(Filter.class, deserializer);
        return m;
    }

    @SuppressWarnings("unchecked")
    private <T extends Filter> void registerJson(
        String id, Class<T> type, FilterJsonSerialization<T> serialization
    ) {
        serializers.put(type,
            new JsonSerializerImpl((FilterJsonSerialization<? super Filter>) serialization));
        deserializers.put(id, serialization);
    }

    @Override
    public FilterSerializer newFilterSerializer() {
        return new CoreFilterSerializer(intNumber, string, idMapping, typeMapping);
    }

    private <T extends Filter> void register(String id, Class<T> type, Serializer<T> serializer) {
        if (idMapping.put(id, serializer) != null) {
            throw new IllegalStateException("Multiple mappings for single id: " + id);
        }

        if (typeMapping.put(type, id) != null) {
            throw new IllegalStateException("Multiple mappings for single type: " + type);
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

            final SerializerImpl s = new SerializerImpl(g);
            serializer.serialize(s, value);
            g.writeEndArray();
        }

        @RequiredArgsConstructor
        private static final class SerializerImpl implements FilterJsonSerialization.Serializer {
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
    }

    @RequiredArgsConstructor
    private static class ManyTermsSerialization<T extends Filter.MultiArgs<A>, A>
        implements Serializer<T> {
        private final MultiArgumentsFilter<T, A> builder;
        private final Serializer<List<A>> list;

        @Override
        public void serialize(SerialWriter buffer, T value) throws IOException {
            list.serialize(buffer, value.terms());
        }

        @Override
        public T deserialize(SerialReader buffer) throws IOException {
            final List<A> terms = list.deserialize(buffer);
            return builder.build(terms);
        }
    }

    @RequiredArgsConstructor
    private static class NoTermSerialization<T extends Filter.NoArg> implements Serializer<T> {
        private final NoArgumentFilter<T> builder;

        @Override
        public void serialize(SerialWriter buffer, T value) {
        }

        @Override
        public T deserialize(SerialReader buffer) {
            return builder.build();
        }
    }

    @RequiredArgsConstructor
    private static class OneTermSerialization<T extends Filter.OneArg<A>, A>
        implements Serializer<T> {
        private final OneArgumentFilter<T, A> builder;
        private final Serializer<A> first;

        @Override
        public void serialize(SerialWriter buffer, T value) throws IOException {
            first.serialize(buffer, value.first());
        }

        @Override
        public T deserialize(SerialReader buffer) throws IOException {
            final A first = this.first.deserialize(buffer);
            return builder.build(first);
        }
    }

    @RequiredArgsConstructor
    private static final class TwoTermsSerialization<T extends Filter.TwoArgs<A, B>, A, B>
        implements Serializer<T> {
        private final TwoArgumentsFilter<T, A, B> builder;
        private final Serializer<A> first;
        private final Serializer<B> second;

        @Override
        public void serialize(SerialWriter buffer, T value) throws IOException {
            first.serialize(buffer, value.first());
            second.serialize(buffer, value.second());
        }

        @Override
        public T deserialize(SerialReader buffer) throws IOException {
            final A first = this.first.deserialize(buffer);
            final B second = this.second.deserialize(buffer);
            return builder.build(first, second);
        }
    }
}
