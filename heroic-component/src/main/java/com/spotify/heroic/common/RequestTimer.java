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

import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class RequestTimer<T> {
    private final Stopwatch watch = Stopwatch.createStarted();
    private final Function<Long, T> builder;

    @java.beans.ConstructorProperties({ "builder" })
    public RequestTimer(final Function<Long, T> builder) {
        this.builder = builder;
    }

    public T end() {
        return builder.apply(watch.elapsed(TimeUnit.MICROSECONDS));
    }
}
