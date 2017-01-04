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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
@JsonSerialize(using = EmptyOptionalLimit.Serializer.class)
class EmptyOptionalLimit implements OptionalLimit {
    static final OptionalLimit INSTANCE = new EmptyOptionalLimit();

    @Override
    public boolean isGreater(final long size) {
        return false;
    }

    @Override
    public boolean isGreater(final LongSupplier size) {
        return false;
    }

    @Override
    public boolean isGreaterOrEqual(final long size) {
        return false;
    }

    @Override
    public boolean isGreaterOrEqual(final LongSupplier size) {
        return false;
    }

    @Override
    public boolean isZero() {
        return false;
    }

    @Override
    public Optional<Integer> asInteger() {
        return Optional.empty();
    }

    @Override
    public Optional<Long> asLong() {
        return Optional.empty();
    }

    @Override
    public int asMaxInteger(final int maxValue) {
        return maxValue;
    }

    @Override
    public <T> List<T> limitList(final List<T> input) {
        return input;
    }

    @Override
    public <T> Set<T> limitSet(final Set<T> input) {
        return input;
    }

    @Override
    public <T> SortedSet<T> limitSortedSet(final SortedSet<T> input) {
        return input;
    }

    @Override
    public <T> Stream<T> limitStream(final Stream<T> stream) {
        return stream;
    }

    @Override
    public void ifPresent(final Consumer<Long> consumer) {
    }

    @Override
    public OptionalLimit add(final long size) {
        return this;
    }

    @Override
    public OptionalLimit orElse(final OptionalLimit other) {
        return other;
    }

    @Override
    public String toString() {
        return "[empty]";
    }

    static class Serializer extends JsonSerializer<OptionalLimit> {
        @Override
        public void serialize(
            final OptionalLimit in, final JsonGenerator g, final SerializerProvider p
        ) throws IOException {
            g.writeNull();
        }
    }
}
