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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import com.google.common.base.Throwables;
import lombok.RequiredArgsConstructor;

import java.beans.ConstructorProperties;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class LombokDataTest {
    private static final Converter<String, String> LOWER_TO_UPPER =
        CaseFormat.LOWER_CAMEL.converterTo(CaseFormat.UPPER_CAMEL);

    public static void verifyClass(final Class<?> cls) {
        verifyClassBuilder(cls).verify();
    }

    @lombok.Data
    static class FieldSpec {
        private final Class<?> rawType;
        private final Object primary;
        private final Object secondary;
        private final String name;
        private final Optional<Method> getter;
    }

    public static Builder verifyClassBuilder(final Class<?> cls) {
        return new Builder(cls);
    }

    @RequiredArgsConstructor
    public static class Builder {
        private final Class<?> cls;

        /**
         * Check that there are no conflicting value builders.
         */
        private boolean checkJson = true;

        /**
         * Custom value suppliers.
         */
        private final List<ValueSuppliers.ValueSupplier> valueSuppliers = new ArrayList<>();

        /**
         * Getters to ignore.
         */
        private final Set<String> ignoreGetters = new HashSet<>();

        /**
         * Ignore checking getters.
         */
        private boolean checkGetters = true;

        public Builder checkJson(final boolean checkJson) {
            this.checkJson = checkJson;
            return this;
        }

        public Builder valueSupplier(ValueSuppliers.ValueSupplier valueSupplier) {
            this.valueSuppliers.add(valueSupplier);
            return this;
        }

        public Builder ignoreGetter(final String name) {
            this.ignoreGetters.add(name);
            return this;
        }

        public Builder checkGetters(final boolean checkGetters) {
            this.checkGetters = checkGetters;
            return this;
        }

        public void verify() {
            final ValueSuppliers suppliers = new ValueSuppliers(valueSuppliers);

            final FakeValueProvider valueProvider = new FakeValueProvider(suppliers);

            final Constructor<?> constructor = Arrays
                .stream(cls.getConstructors())
                .filter(c -> c.isAnnotationPresent(ConstructorProperties.class))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException(
                    "No constructor annotated with @ConstructorProperties"));

            final List<FieldSpec> specs = buildFieldSpecs(constructor, valueProvider);

            if (checkGetters) {
                verifyGetters(constructor, specs);
            }

            verifyEquals(constructor, specs);
            verifyToString(constructor, specs);

            if (checkJson) {
                verifyJson(cls);
            }
        }

        private void verifyJson(final Class<?> cls) {
            final List<Constructor<?>> constructors = Arrays
                .stream(cls.getConstructors())
                .filter(c -> c.isAnnotationPresent(ConstructorProperties.class) ||
                    c.isAnnotationPresent(JsonCreator.class))
                .collect(Collectors.toList());

            final List<Method> jsonCreators = Arrays
                .stream(cls.getMethods())
                .filter(m -> ((m.getModifiers() & Modifier.STATIC) != 0) &&
                    m.isAnnotationPresent(JsonCreator.class))
                .collect(Collectors.toList());

            if (constructors.size() + jsonCreators.size() != 1) {
                throw new AssertionError(
                    "Constructors (" + constructors + ") and @JsonCreator methods (" +
                        jsonCreators + ") conflicting for JSON de-serialization");
            }
        }

        private void verifyGetters(
            final Constructor<?> constructor, final List<FieldSpec> specs
        ) {
            final Object instance = newInstance(constructor, specs, false);

            /* test getters */
            for (final FieldSpec f : specs) {
                f.getGetter().ifPresent(getter -> {
                    final Object value;

                    try {
                        value = getter.invoke(instance);
                    } catch (final ReflectiveOperationException e) {
                        throw Throwables.propagate(e);
                    }

                    assertEquals("Expected value from getter (" + f.getName() + ") is same",
                        f.getPrimary(), value);
                });
            }
        }

        private void verifyToString(
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

        private void verifyEquals(
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
        private <T> T invoke(
            final Method equals, final Object instance, final Object... arguments
        ) {
            try {
                return (T) equals.invoke(instance, arguments);
            } catch (final ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        }

        private Object newInstance(
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

        private List<FieldSpec> buildFieldSpecs(
            final Constructor<?> constructor, final FakeValueProvider valueProvider
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
                final Type type = types[i];
                final Class<?> rawType = toRawType(type);
                final String name = names[i];

                final Optional<Method> getter;

                if (!checkGetters || ignoreGetters.contains(name)) {
                    getter = Optional.empty();
                } else {
                    if (rawType.equals(boolean.class)) {
                        getter =
                            Optional.of(getGetterMethod(cls, "is" + LOWER_TO_UPPER.convert(name)));
                    } else {
                        getter =
                            Optional.of(getGetterMethod(cls, "get" + LOWER_TO_UPPER.convert(name)));
                    }
                }

                final Object primary = valueProvider.lookup(type, false, name);
                final Object secondary = valueProvider.lookup(type, true, name);

                specs.add(new FieldSpec(rawType, primary, secondary, name, getter));
            }

            return specs;
        }

        private Class<?> toRawType(final Type type) {
            if (type instanceof Class) {
                return (Class<?>) type;
            }

            if (type instanceof ParameterizedType) {
                return toRawType(((ParameterizedType) type).getRawType());
            }

            throw new IllegalArgumentException("Cannot get raw type for (" + type + ")");
        }

        private Method getGetterMethod(final Class<?> cls, final String name) {
            try {
                return cls.getMethod(name);
            } catch (NoSuchMethodException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
