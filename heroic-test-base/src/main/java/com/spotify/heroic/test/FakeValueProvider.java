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

package com.spotify.heroic.test;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import lombok.RequiredArgsConstructor;
import org.mockito.Mockito;

import java.beans.ConstructorProperties;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;

/**
 * Provides mock values for test classes.
 */
@RequiredArgsConstructor
public class FakeValueProvider {
    private final ValueSuppliers suppliers;

    private final Map<Class<?>, Object> primary = new HashMap<>();
    private final Map<Class<?>, Object> secondary = new HashMap<>();
    private final Object lock = new Object();

    private final UUID uuid1 = UUID.nameUUIDFromBytes(new byte[]{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00
    });

    private final UUID uuid2 = UUID.nameUUIDFromBytes(new byte[]{
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01
    });

    public Object lookup(final Type type, final boolean secondary, final String name) {
        final Optional<Object> supplied = suppliers.lookup(type, secondary, name);

        if (supplied.isPresent()) {
            return supplied.get();
        }

        return lookup(type, secondary);
    }

    /**
     * Lookup a value for the given type.
     *
     * @param type Type to provide a value for.
     * @param secondary If a secondary value should be provided or not. It is guaranteed that a
     * secondary value is not equal to a primary value.
     * @return an object
     */
    public Object lookup(final Type type, final boolean secondary) {
        if (type instanceof ParameterizedType) {
            return lookupParameterized((ParameterizedType) type, secondary);
        }

        if (!(type instanceof Class)) {
            throw new IllegalArgumentException("Lookup for type (" + type + ") failed");
        }

        final Class<?> cls = (Class<?>) type;

        if (cls.equals(boolean.class) || cls.equals(Boolean.class)) {
            return secondary;
        }

        if (cls.equals(int.class) || cls.equals(Integer.class)) {
            return secondary ? 1 : 2;
        }

        if (cls.equals(long.class) || cls.equals(Long.class)) {
            return secondary ? 100L : 200L;
        }

        if (cls.equals(float.class) || cls.equals(Float.class)) {
            return secondary ? 1.0F : 2.0F;
        }

        if (cls.equals(double.class) || cls.equals(Double.class)) {
            return secondary ? 1.1D : 2.2D;
        }

        if (cls.equals(String.class)) {
            return secondary ? "a" : "b";
        }

        if (cls.isEnum()) {
            final Object[] constants = cls.getEnumConstants();

            if (constants.length < 2) {
                throw new IllegalArgumentException(
                    "Enum type (" + cls + ") has less than two constants");
            }

            return secondary ? constants[1] : constants[0];
        }

        if (cls.equals(UUID.class)) {
            return secondary ? uuid1 : uuid2;
        }

        return lookupClass(cls, secondary).orElseGet(() -> lookupMock(cls, secondary));
    }

    private Object lookupMock(final Class<?> cls, final boolean secondary) {
        synchronized (lock) {
            final Map<Class<?>, Object> instances = secondary ? this.secondary : this.primary;

            Object instance = instances.get(cls);

            if (instance == null) {
                instance = Mockito.mock(cls);
                instances.put(cls, instance);
            }

            return instance;
        }
    }

    private Optional<Object> lookupClass(final Class<?> cls, final boolean secondary) {
        return Arrays
            .stream(cls.getConstructors())
            .filter(c -> c.isAnnotationPresent(ConstructorProperties.class))
            .findAny()
            .map(c -> {
                final String[] names = c.getAnnotation(ConstructorProperties.class).value();
                final Type[] types = c.getGenericParameterTypes();

                if (names.length != types.length) {
                    throw new IllegalArgumentException(
                        "@ConstructorProperties number of arguments do not match number of " +
                            "parameters: " + c);
                }

                final Object[] arguments = new Object[types.length];

                for (int i = 0; i < types.length; i++) {
                    arguments[i] = lookup(types[i], secondary, names[i]);
                }

                try {
                    return c.newInstance(arguments);
                } catch (final ReflectiveOperationException e) {
                    throw Throwables.propagate(e);
                }
            });
    }

    private Object lookupParameterized(
        final ParameterizedType type, final boolean secondary
    ) {
        if (type.getRawType().equals(Optional.class)) {
            final Object inner = lookup(type.getActualTypeArguments()[0], secondary);
            return Optional.of(inner);
        }

        if (type.getRawType().equals(List.class)) {
            if (secondary) {
                return ImmutableList.of();
            }

            final Object a = lookup(type.getActualTypeArguments()[0], !secondary);
            return ImmutableList.of(a);
        }

        if (type.getRawType().equals(Set.class)) {
            if (secondary) {
                return ImmutableSet.of();
            }

            final Object a = lookup(type.getActualTypeArguments()[0], !secondary);
            return ImmutableSet.of(a);
        }

        if (type.getRawType().equals(SortedSet.class)) {
            if (secondary) {
                return ImmutableSortedSet.of();
            }

            final Object a = lookup(type.getActualTypeArguments()[0], !secondary);
            return ImmutableSortedSet.of((Comparable) a);
        }

        if (type.getRawType().equals(Map.class)) {
            if (secondary) {
                return ImmutableMap.of();
            }

            final Object keya = lookup(type.getActualTypeArguments()[0], !secondary);
            final Object a = lookup(type.getActualTypeArguments()[1], !secondary);
            return ImmutableMap.of(keya, a);
        }

        if (type.getRawType().equals(Multimap.class)) {
            final TreeMultimap<Comparable<?>, Comparable<?>> mm = TreeMultimap.create();

            if (secondary) {
                return mm;
            }

            final Object keya = lookup(type.getActualTypeArguments()[0], !secondary);
            final Object a = lookup(type.getActualTypeArguments()[1], !secondary);
            mm.put((Comparable<?>) keya, (Comparable<?>) a);
            return mm;
        }

        throw new IllegalArgumentException("Lookup for type (" + type + ") failed");
    }
}
