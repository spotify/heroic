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

package com.spotify.heroic.metric.model;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public final class ResultGroup {
    private final List<TagValues> tags;
    private final List<?> values;
    private final Class<?> type;

    @SuppressWarnings("unchecked")
    public <T> List<T> valuesFor(Class<T> expected) {
        if (!expected.isAssignableFrom(type))
            throw new RuntimeException(String.format("incompatible payload type between %s (expected) and %s (actual)",
                    expected.getCanonicalName(), type.getCanonicalName()));

        return (List<T>) values;
    }

    @JsonCreator
    public ResultGroup(@JsonProperty("tags") List<TagValues> tags, @JsonProperty("values") List<?> values,
            @JsonProperty("type") Class<?> type) {
        this.tags = tags;
        this.values = values;
        this.type = type;
    }
}