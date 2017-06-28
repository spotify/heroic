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

package com.spotify.heroic.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.ServiceInfo;
import java.util.Map;
import java.util.UUID;
import lombok.Data;

@Data
public class NodeMetadata {
    private final int version;
    private final UUID id;
    private final Map<String, String> tags;
    private final ServiceInfo service;
    private final boolean darkload;

    @JsonCreator
    public NodeMetadata(
        @JsonProperty("version") Integer version, @JsonProperty("id") UUID id,
        @JsonProperty("tags") Map<String, String> tags,
        @JsonProperty("service") ServiceInfo service, @JsonProperty("darkload") boolean darkload
    ) {
        this.version = version;
        this.id = id;
        this.tags = tags;
        this.service = service;
        this.darkload = darkload;
    }

    /**
     * Checks if both the given tags and capability matches.
     */
    public boolean matches(Map<String, String> tags) {
        if (!matchesTags(tags)) {
            return false;
        }

        return true;
    }

    /**
     * Checks if the set of 'other' tags matches the tags of this meta data.
     */
    public boolean matchesTags(Map<String, String> tags) {
        if (this.tags == null) {
            return true;
        }

        if (tags == null) {
            return false;
        }

        for (final Map.Entry<String, String> entry : this.tags.entrySet()) {
            final String value = entry.getValue();
            final String tagValue = tags.get(entry.getKey());

            if (tagValue == null) {
                if (value == null) {
                    continue;
                }

                return false;
            }

            if (!tagValue.equals(value)) {
                return false;
            }
        }

        return true;
    }
}
