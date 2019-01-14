/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.heroic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.Data;

@Data
public class ObjectHasher {
    private enum Kind {
        START_OBJECT, END_OBJECT, LIST, SET, FIELD, INTEGER, LONG, DOUBLE, STRING, BOOLEAN, ENUM,
        OPTIONAL_PRESENT, OPTIONAL_ABSENT
    }

    private final Hasher hasher;

    public <T> void putField(
        final String name, final T value, final Consumer<T> hashTo
    ) {
        putKind(Kind.FIELD);
        putString(name);
        hashTo.accept(value);
    }

    public void putObject(final Class<?> cls) {
        putObject(cls, () -> {
        });
    }

    public void putObject(final Class<?> cls, final Runnable runnable) {
        putKind(Kind.START_OBJECT);
        putString(cls.getCanonicalName());
        runnable.run();
        putKind(Kind.END_OBJECT);
    }

    public <T> Consumer<T> with(final BiConsumer<T, ObjectHasher> inner) {
        return value -> inner.accept(value, this);
    }

    public <T extends Enum> Consumer<T> enumValue() {
        return value -> {
            putKind(Kind.ENUM);
            putString(value.getClass().getCanonicalName());
            putString(value.name());
        };
    }

    public <T> Consumer<List<T>> list(final Consumer<T> hashTo) {
        return list -> {
            putKind(Kind.LIST);
            hasher.putInt(list.size());

            for (final T val : list) {
                hashTo.accept(val);
            }
        };
    }

    public <T> Consumer<Optional<T>> optional(final Consumer<T> hashTo) {
        return optional -> {
            if (optional.isPresent()) {
                putKind(Kind.OPTIONAL_PRESENT);
                hashTo.accept(optional.get());
            } else {
                putKind(Kind.OPTIONAL_ABSENT);
            }
        };
    }

    public Consumer<String> string() {
        return this::putString;
    }

    public Consumer<Double> doubleValue() {
        return value -> {
            putKind(Kind.DOUBLE);
            hasher.putDouble(value);
        };
    }

    public Consumer<Integer> integer() {
        return value -> {
            putKind(Kind.INTEGER);
            hasher.putInt(value);
        };
    }

    public Consumer<Long> longValue() {
        return value -> {
            putKind(Kind.LONG);
            hasher.putLong(value);
        };
    }

    public Consumer<Boolean> bool() {
        return value -> {
            putKind(Kind.BOOLEAN);
            hasher.putBoolean(value);
        };
    }

    public <T> Consumer<SortedSet<T>> sortedSet(final Consumer<T> inner) {
        return values -> {
            putKind(Kind.SET);
            hasher.putInt(values.size());

            for (final T value : values) {
                inner.accept(value);
            }
        };
    }

    @VisibleForTesting
    protected String result() {
        return hasher.hash().toString();
    }

    private void putKind(final Kind kind) {
        hasher.putInt(kind.ordinal());
    }

    private void putString(final String values) {
        putKind(Kind.STRING);
        hasher.putInt(values.length());
        hasher.putString(values, StandardCharsets.UTF_8);
    }
}
