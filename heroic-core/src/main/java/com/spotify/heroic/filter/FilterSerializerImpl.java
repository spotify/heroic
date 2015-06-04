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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.model.CacheKey;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;

/**
 * Serializes aggregation configurations.
 *
 * Each aggregation configuration is packed into a Composite which has the type of the aggregation as a prefixed short.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public class FilterSerializerImpl implements FilterSerializer {
    private final SerializerFramework s;
    private final Serializer<Integer> intNumber;
    private final Serializer<String> string;

    private final HashMap<String, Serializer<? extends Filter>> idMapping = new HashMap<>();
    private final HashMap<Class<?>, String> typeMapping = new HashMap<>();

    // lock to prevent duplicate work when looking for an unknown type id.
    private final Object $getTypeId = new Object();

    /**
     * Caches concrete type implementations to their corresponding id depending on which filter interface they impement.
     */
    private final ConcurrentHashMap<Class<?>, String> typeCache = new ConcurrentHashMap<>();

    private <T extends Filter> void register(String id, Class<T> type, Serializer<T> serializer) {
        if (idMapping.put(id, serializer) != null)
            throw new IllegalStateException("Multiple mappings for single id: " + id);

        if (typeMapping.put(type, id) != null)
            throw new IllegalStateException("Multiple mappings for single type: " + type);
    }

    @Override
    public <T extends Filter.OneArg<A>, A> void register(String id, Class<T> type, OneArgumentFilter<T, A> builder,
            Serializer<A> first) {
        register(id, type, new OneTermSerialization<>(builder, first));
    }

    @Override
    public <T extends Filter.TwoArgs<A, B>, A, B> void register(String id, Class<T> type,
            TwoArgumentsFilter<T, A, B> builder, Serializer<A> first, Serializer<B> second) {
        register(id, type, new TwoTermsSerialization<>(builder, first, second));
    }

    @Override
    public <T extends Filter.MultiArgs<A>, A> void register(String id, Class<T> type,
            MultiArgumentsFilter<T, A> builder, Serializer<A> term) {
        register(id, type, new ManyTermsSerialization<>(builder, s.list(term)));
    }

    @Override
    public <T extends Filter.NoArg> void register(String id, Class<T> type, NoArgumentFilter<T> builder) {
        register(id, type, new NoTermSerialization<>(builder));
    }

    /**
     * Scan the type hierarchy of the provided type to find an implementation.
     *
     * @param type
     * @return
     */
    private String getTypeId(Class<? extends Filter> type) {
        final String id = typeCache.get(type);

        if (id != null)
            return id;

        synchronized ($getTypeId) {
            final String checkId = typeCache.get(type);

            if (checkId != null)
                return checkId;

            final LinkedList<Class<?>> queue = new LinkedList<>(Arrays.asList(type.getInterfaces()));

            while (!queue.isEmpty()) {
                final Class<?> candidateType = queue.pop();
                queue.addAll(Arrays.asList(candidateType.getInterfaces()));

                if (!Filter.class.isAssignableFrom(candidateType))
                    continue;

                final String candidateId = typeMapping.get(candidateType);

                if (candidateId == null)
                    continue;

                typeCache.put(type, candidateId);
                return candidateId;
            }
        }

        // discourage calling this method again with the given type.
        throw new RuntimeException("No serializer for type " + type);
    }

    private Serializer<Filter> getSerializer(String id) {
        @SuppressWarnings("unchecked")
        final Serializer<Filter> serializer = (Serializer<Filter>) idMapping.get(id);

        if (serializer == null)
            throw new RuntimeException("No serializer for type id " + id);

        return serializer;
    }

    @Override
    public void serialize(SerialWriter buffer, Filter value) throws IOException {
        final Filter optimized = value.optimize();

        final String typeId = getTypeId(optimized.getClass());

        final Serializer<Filter> serializer = getSerializer(typeId);

        intNumber.serialize(buffer, CacheKey.VERSION);
        string.serialize(buffer, typeId);

        final SerialWriter.Scope scope = buffer.scope();
        serializer.serialize(scope, optimized);
        scope.close();
    }

    @Override
    public Filter deserialize(SerialReader buffer) throws IOException {
        final int version = intNumber.deserialize(buffer);
        final String typeId = string.deserialize(buffer);

        if (version != CacheKey.VERSION) {
            buffer.skip();
            return null;
        }

        final Serializer<Filter> serializer = getSerializer(typeId);

        if (serializer == null) {
            buffer.skip();
            return null;
        }

        return serializer.deserialize(buffer.scope());
    }

    @RequiredArgsConstructor
    private static class ManyTermsSerialization<T extends Filter.MultiArgs<A>, A> implements Serializer<T> {
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
    private static class OneTermSerialization<T extends Filter.OneArg<A>, A> implements Serializer<T> {
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
    private static final class TwoTermsSerialization<T extends Filter.TwoArgs<A, B>, A, B> implements Serializer<T> {
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