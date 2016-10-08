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

import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import com.google.common.base.Throwables;

import java.beans.ConstructorProperties;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class LombokDataTest {
    private static final FakeValueProvider TYPE_PROVIDERS = new FakeValueProvider();

    private static final Converter<String, String> LOWER_TO_UPPER =
        CaseFormat.LOWER_CAMEL.converterTo(CaseFormat.UPPER_CAMEL);

    public static void verifyClass(final Class<?> cls) {
        final Constructor<?> constructor = Arrays
            .stream(cls.getConstructors())
            .filter(c -> c.isAnnotationPresent(ConstructorProperties.class))
            .findAny()
            .orElseThrow(() -> new IllegalArgumentException(
                "No constructor annotated with @ConstructorProperties"));

        final List<FieldSpec> specs = buildFieldSpecs(cls, constructor);

        verifyGetters(constructor, specs);
        verifyEquals(constructor, specs);
        verifyToString(constructor, specs);
    }

    private static void verifyGetters(
        final Constructor<?> constructor, final List<FieldSpec> specs
    ) {
        final Object instance = newInstance(constructor, specs, false);

        /* test getters */
        for (final FieldSpec f : specs) {
            final Object value;

            try {
                value = f.getGetter().invoke(instance);
            } catch (final ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }

            assertEquals("Expected value from getter (" + f.getName() + ") is same", f.getPrimary(),
                value);
        }
    }

    private static void verifyToString(
        final Constructor<?> constructor, final List<FieldSpec> specs
    ) {
        final Object instance = newInstance(constructor, specs, false);

        final Class<?> cls = constructor.getDeclaringClass();

        final Method toString;

        try {
            toString = cls.getMethod("toString");
        } catch (final NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }

        /* test toString */
        try {
            final String toStringResult = (String) toString.invoke(instance);
            assertNotNull(toStringResult);
            assertTrue(toStringResult.contains(cls.getSimpleName()));
        } catch (final ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    private static void verifyEquals(
        final Constructor<?> constructor, final List<FieldSpec> specs
    ) {
        final Object current = newInstance(constructor, specs, false);
        final Object other = newInstance(constructor, specs, false);
        final Object different = newInstance(constructor, specs, true);

        assertNotSame(current, other);

        final Method equals;

        try {
            equals = constructor.getDeclaringClass().getMethod("equals", Object.class);
        } catch (final NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }

        assertTrue(invoke(equals, current, current));
        assertTrue(invoke(equals, current, other));
        assertFalse(invoke(equals, current, different));
    }

    @SuppressWarnings("unchecked")
    private static <T> T invoke(
        final Method equals, final Object instance, final Object... arguments
    ) {
        try {
            return (T) equals.invoke(instance, arguments);
        } catch (final ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    private static Object newInstance(
        final Constructor<?> constructor, final List<FieldSpec> specs, final boolean secondary
    ) {
        final Function<FieldSpec, Object> accessor =
            secondary ? FieldSpec::getSecondary : FieldSpec::getPrimary;
        final Object[] arguments = specs.stream().map(accessor).toArray(Object[]::new);

        try {
            return constructor.newInstance(arguments);
        } catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    private static List<FieldSpec> buildFieldSpecs(
        final Class<?> cls, final Constructor<?> constructor
    ) {
        final Parameter[] parameters = constructor.getParameters();

        final Type[] types =
            Arrays.stream(parameters).map(Parameter::getParameterizedType).toArray(Type[]::new);

        final String[] names = constructor.getAnnotation(ConstructorProperties.class).value();

        if (types.length != names.length) {
            throw new IllegalArgumentException(
                "@ConstructorProperties length does not match number of parameters");
        }

        final List<FieldSpec> specs = new ArrayList<>();

        for (int i = 0; i < types.length; i++) {
            final Class<?> type = toRawType(types[i]);
            final String name = names[i];

            final Method getter;

            if (type.equals(boolean.class)) {
                getter = getGetterMethod(cls, "is" + LOWER_TO_UPPER.convert(name));
            } else {
                getter = getGetterMethod(cls, "get" + LOWER_TO_UPPER.convert(name));
            }

            final Object primary = TYPE_PROVIDERS.lookup(types[i]);
            final Object secondary = TYPE_PROVIDERS.lookup(types[i], true);
            specs.add(new FieldSpec(type, primary, secondary, name, getter));
        }

        return specs;
    }

    private static Class<?> toRawType(final Type type) {
        if (type instanceof Class) {
            return (Class<?>) type;
        }

        if (type instanceof ParameterizedType) {
            return toRawType(((ParameterizedType) type).getRawType());
        }

        throw new IllegalArgumentException("Cannot get raw type for (" + type + ")");
    }

    private static Method getGetterMethod(final Class<?> cls, final String name) {
        try {
            return cls.getMethod(name);
        } catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    @lombok.Data
    static class FieldSpec {
        private final Class<?> rawType;
        private final Object primary;
        private final Object secondary;
        private final String name;
        private final Method getter;
    }
}
