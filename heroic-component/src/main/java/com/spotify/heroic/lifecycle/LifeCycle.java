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

package com.spotify.heroic.lifecycle;

import com.google.common.collect.ImmutableList;

import java.util.stream.Stream;

public interface LifeCycle {
    void install();

    static final LifeCycle EMPTY = new EmptyLifeCycle();

    static LifeCycle empty() {
        return EMPTY;
    }

    static LifeCycle combined(Iterable<LifeCycle> many) {
        return ManyLifeCycle.of(ImmutableList.copyOf(many));
    }

    static LifeCycle combined(Stream<LifeCycle> many) {
        final ImmutableList.Builder<LifeCycle> cycles = ImmutableList.builder();
        many.forEach(cycles::add);
        return ManyLifeCycle.of(cycles.build());
    }
}
