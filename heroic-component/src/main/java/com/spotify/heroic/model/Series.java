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

package com.spotify.heroic.model;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import lombok.Getter;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@ToString(of = { "key", "tags" })
public class Series {
    public static final Series EMPTY = new Series(null, ImmutableMap.<String, String> of());

    @Getter
    private final String key;
    @Getter
    private final Map<String, String> tags;

    private final int hashCode;

    @JsonCreator
    public Series(@JsonProperty("key") String key, @JsonProperty("tags") Map<String, String> tags) {
        this.key = key;
        this.tags = checkNotNull(tags, "tags must not be null");
        this.hashCode = generateHashCode();
    }

    public int generateHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + ((tags == null) ? 0 : tags.hashCode());
        return result;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null)
            return false;

        if (Series.class != obj.getClass())
            return false;

        final Series o = (Series) obj;
        return hashCode == o.hashCode;
    }
}