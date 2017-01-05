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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

@JsonDeserialize(using = OptionalLimit.Deserializer.class)
public interface OptionalLimit {
    boolean isGreater(long size);

    boolean isGreater(LongSupplier size);

    boolean isGreaterOrEqual(long size);

    boolean isGreaterOrEqual(LongSupplier size);

    boolean isZero();

    <T> List<T> limitList(List<T> input);

    <T> Set<T> limitSet(Set<T> set);

    <T> SortedSet<T> limitSortedSet(SortedSet<T> values);

    <T> Stream<T> limitStream(Stream<T> stream);

    Optional<Integer> asInteger();

    Optional<Long> asLong();

    /**
     * Get the current value of the limit as an integer constrained to a max or default value.
     * <p>
     * If limit is empty, the value will assume maxValue. If limit is above max value, it will be
     * capped to max value.
     *
     * @param maxValue Max or default value.
     * @return An integer following the above restrictions.
     */
    int asMaxInteger(int maxValue);

    /**
     * Consume the limit if present.
     *
     * @param consumer The consumer to consume the value.
     */
    void ifPresent(Consumer<Long> consumer);

    /**
     * Add the given size to the limit. Does nothing for empty limits.
     *
     * @param size Size to add.
     * @return A new Optional limit that has been increased with the given size.
     */
    OptionalLimit add(long size);

    OptionalLimit orElse(OptionalLimit other);

    static OptionalLimit empty() {
        return EmptyOptionalLimit.INSTANCE;
    }

    static OptionalLimit of(long limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be a positive value");
        }

        return new ValueOptionalLimit(limit);
    }

    class Deserializer extends JsonDeserializer<OptionalLimit> {
        @Override
        public OptionalLimit getNullValue(final DeserializationContext ctx)
            throws JsonMappingException {
            return EmptyOptionalLimit.INSTANCE;
        }

        @Override
        public OptionalLimit deserialize(
            final JsonParser p, final DeserializationContext ctx
        ) throws IOException {
            switch (p.getCurrentToken()) {
                case VALUE_NUMBER_FLOAT:
                case VALUE_NUMBER_INT:
                    return new ValueOptionalLimit(p.getIntValue());
                default:
                    throw ctx.wrongTokenException(p, JsonToken.VALUE_NUMBER_INT,
                        "Expected limit or null");
            }
        }
    }
}
