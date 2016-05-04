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

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
public class CoreFilterSerializer implements FilterSerializer {
    public static final int VERSION = 1;

    private final Serializer<Integer> intNumber;
    private final Serializer<String> string;

    private final HashMap<String, Serializer<? extends Filter>> idMapping;
    private final HashMap<Class<?>, String> typeMapping;

    // lock to prevent duplicate work when looking for an unknown type id.
    private final Object typeIdLock = new Object();

    /**
     * Caches concrete type implementations to their corresponding id depending on which filter
     * interface they impement.
     */
    private final ConcurrentHashMap<Class<?>, String> typeCache = new ConcurrentHashMap<>();

    @Override
    public void serialize(SerialWriter buffer, Filter value) throws IOException {
        final Filter optimized = value.optimize();

        final String typeId = getTypeId(optimized.getClass());

        final Serializer<Filter> serializer = getSerializer(typeId);

        intNumber.serialize(buffer, VERSION);
        string.serialize(buffer, typeId);

        final SerialWriter.Scope scope = buffer.scope();
        serializer.serialize(scope, optimized);
        scope.close();
    }

    @Override
    public Filter deserialize(SerialReader buffer) throws IOException {
        final int version = intNumber.deserialize(buffer);
        final String typeId = string.deserialize(buffer);

        if (version != VERSION) {
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

    /**
     * Scan the type hierarchy of the provided type to find an implementation.
     *
     * @param type
     * @return
     */
    private String getTypeId(Class<? extends Filter> type) {
        final String id = typeCache.get(type);

        if (id != null) {
            return id;
        }

        synchronized (typeIdLock) {
            final String checkId = typeCache.get(type);

            if (checkId != null) {
                return checkId;
            }

            final LinkedList<Class<?>> queue =
                new LinkedList<>(Arrays.asList(type.getInterfaces()));

            while (!queue.isEmpty()) {
                final Class<?> candidateType = queue.pop();
                queue.addAll(Arrays.asList(candidateType.getInterfaces()));

                if (!Filter.class.isAssignableFrom(candidateType)) {
                    continue;
                }

                final String candidateId = typeMapping.get(candidateType);

                if (candidateId == null) {
                    continue;
                }

                typeCache.put(type, candidateId);
                return candidateId;
            }
        }

        // discourage calling this method again with the given type.
        throw new RuntimeException("No serializer for type " + type);
    }

    private Serializer<Filter> getSerializer(String id) {
        @SuppressWarnings("unchecked") final Serializer<Filter> serializer =
            (Serializer<Filter>) idMapping.get(id);

        if (serializer == null) {
            throw new RuntimeException("No serializer for type id " + id);
        }

        return serializer;
    }
}
