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

package com.spotify.heroic.suggest;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.RequestError;
import eu.toolchain.async.Collector;
import java.util.List;
import lombok.Data;

@Data
public class WriteSuggest {
    private final List<RequestError> errors;
    private final List<String> ids;

    public static WriteSuggest of() {
        return new WriteSuggest(ImmutableList.of(), ImmutableList.of());
    }

    public static Collector<WriteSuggest, WriteSuggest> reduce() {
        return requests -> {
            final ImmutableList.Builder<RequestError> errors = ImmutableList.builder();
            final ImmutableList.Builder<String> ids = ImmutableList.builder();

            for (final WriteSuggest r : requests) {
                errors.addAll(r.getErrors());
                ids.addAll(r.getIds());
            }

            return new WriteSuggest(errors.build(), ids.build());
        };
    }

    @Data
    public static class Request {
        private final Series series;
        private final DateRange range;
    }
}
