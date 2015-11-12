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

import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public final class Optionals {
    public static <T> Optional<List<T>> mergeOptionalList(final Optional<List<T>> a,
            Optional<List<T>> b) {
        if (a.isPresent() && b.isPresent()) {
            return Optional.of(ImmutableList.copyOf(Iterables.concat(a.get(), b.get())));
        }

        return pickOptional(a, b);
    }

    public static <T> Optional<T> mergeOptional(Optional<T> a, Optional<T> b,
            BinaryOperator<T> merger) {
        if (a.isPresent() && b.isPresent()) {
            return Optional.of(merger.apply(a.get(), b.get()));
        }

        return pickOptional(a, b);
    }

    public static <T> Optional<T> pickOptional(Optional<T> a, Optional<T> b) {
        return b.isPresent() ? b : a;
    }

    public static <T> Optional<T> firstPresent(Optional<T> a, Optional<T> b) {
        return a.isPresent() ? a : b;
    }
}
